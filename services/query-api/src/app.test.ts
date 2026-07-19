import { describe, expect, it, vi } from 'vitest';
import { buildApp } from './app.js';

const API_KEY = 'raw-test-key';
const ADVERTISER_ID = 'adv_1';
const OWNED_AD_ID = 'ad_owned';
const OTHER_AD_ID = 'ad_other_advertiser';

function buildTestApp(overrides: Partial<{
  resolveApiKey: ReturnType<typeof vi.fn>;
  getAdOwner: ReturnType<typeof vi.fn>;
  getLatestAggregate: ReturnType<typeof vi.fn>;
}> = {}) {
  const deps = {
    resolveApiKey: vi.fn(async (key: string) => (key === API_KEY ? { advertiserId: ADVERTISER_ID } : null)),
    getAdOwner: vi.fn(async (adId: string) => {
      if (adId === OWNED_AD_ID) return ADVERTISER_ID;
      if (adId === OTHER_AD_ID) return 'adv_someone_else';
      return null;
    }),
    getLatestAggregate: vi.fn(async () => ({ windowStart: 60_000, clicks: 842 })),
    ...overrides,
  };
  return buildApp(deps);
}

function get(app: ReturnType<typeof buildApp>, adId: string, authorization?: string) {
  return app.inject({
    method: 'GET',
    url: `/v1/ads/${adId}/aggregates`,
    headers: authorization ? { authorization } : {},
  });
}

describe('GET /v1/ads/:adId/aggregates', () => {
  it('returns the latest aggregate for an ad the caller owns', async () => {
    const app = buildTestApp();

    const response = await get(app, OWNED_AD_ID, `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      adId: OWNED_AD_ID,
      windowStart: new Date(60_000).toISOString(),
      clicks: 842,
      exact: false,
    });
  });

  it('returns clicks: 0 and the current window when no aggregate exists yet', async () => {
    const app = buildTestApp({ getLatestAggregate: vi.fn(async () => null) });

    const response = await get(app, OWNED_AD_ID, `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(200);
    expect(response.json().clicks).toBe(0);
  });

  it('returns 404 for an ad owned by a different advertiser', async () => {
    const app = buildTestApp();

    const response = await get(app, OTHER_AD_ID, `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'not_found' });
  });

  it('returns the identical 404 body for a nonexistent ad', async () => {
    const app = buildTestApp();

    const response = await get(app, 'ad_does_not_exist', `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'not_found' });
  });

  it('returns 401 for a missing Authorization header', async () => {
    const app = buildTestApp();

    const response = await get(app, OWNED_AD_ID);

    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({ error: 'unauthorized' });
  });

  it('returns 401 for an unknown API key', async () => {
    const app = buildTestApp();

    const response = await get(app, OWNED_AD_ID, 'Bearer not-a-real-key');

    expect(response.statusCode).toBe(401);
  });
});

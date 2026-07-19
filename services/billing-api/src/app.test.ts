import { describe, expect, it, vi } from 'vitest';
import { buildApp } from './app.js';

const API_KEY = 'raw-test-key';
const ADVERTISER_ID = 'adv_1';
const OWNED_CAMPAIGN_ID = 'cmp_owned';
const OTHER_CAMPAIGN_ID = 'cmp_other_advertiser';
const OPS_TOKEN = 'test-ops-token';

const SAMPLE_STATEMENT = {
  campaignId: OWNED_CAMPAIGN_ID, period: '2026-07-11', billedClicks: 118402, excludedInvalidClicks: 613,
  reconciledAt: '2026-07-12T02:11:00Z', sourceArchive: 's3://ad-clicks-raw/dt=2026-07-11/',
};

function buildTestApp(overrides: Partial<{
  resolveApiKey: ReturnType<typeof vi.fn>;
  getCampaignOwner: ReturnType<typeof vi.fn>;
  getStatement: ReturnType<typeof vi.fn>;
  reconcileAndStore: ReturnType<typeof vi.fn>;
}> = {}) {
  const deps = {
    opsToken: OPS_TOKEN,
    resolveApiKey: vi.fn(async (key: string) => (key === API_KEY ? { advertiserId: ADVERTISER_ID } : null)),
    getCampaignOwner: vi.fn(async (campaignId: string) => {
      if (campaignId === OWNED_CAMPAIGN_ID) return ADVERTISER_ID;
      if (campaignId === OTHER_CAMPAIGN_ID) return 'adv_someone_else';
      return null;
    }),
    getStatement: vi.fn(async () => SAMPLE_STATEMENT),
    reconcileAndStore: vi.fn(async (date: string) => 3),
    ...overrides,
  };
  return buildApp(deps);
}

function get(app: ReturnType<typeof buildApp>, campaignId: string, authorization?: string) {
  return app.inject({
    method: 'GET',
    url: `/v1/campaigns/${campaignId}/statement`,
    headers: authorization ? { authorization } : {},
  });
}

function rerun(app: ReturnType<typeof buildApp>, date: string, opsToken?: string) {
  return app.inject({
    method: 'POST',
    url: `/v1/reconciliation/${date}/rerun`,
    headers: opsToken ? { 'x-ops-token': opsToken } : {},
  });
}

describe('GET /v1/campaigns/:campaignId/statement', () => {
  it('returns the statement for a campaign the caller owns', async () => {
    const app = buildTestApp();

    const response = await get(app, OWNED_CAMPAIGN_ID, `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ ...SAMPLE_STATEMENT, exact: true });
  });

  it('returns 404 when no statement has been reconciled yet', async () => {
    const app = buildTestApp({ getStatement: vi.fn(async () => null) });

    const response = await get(app, OWNED_CAMPAIGN_ID, `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'not_found' });
  });

  it('returns 404 for a campaign owned by a different advertiser', async () => {
    const app = buildTestApp();

    const response = await get(app, OTHER_CAMPAIGN_ID, `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'not_found' });
  });

  it('returns the identical 404 body for a nonexistent campaign', async () => {
    const app = buildTestApp();

    const response = await get(app, 'cmp_does_not_exist', `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'not_found' });
  });

  it('returns 401 for a missing Authorization header', async () => {
    const app = buildTestApp();

    const response = await get(app, OWNED_CAMPAIGN_ID);

    expect(response.statusCode).toBe(401);
  });

  it('returns 401 for an unknown API key', async () => {
    const app = buildTestApp();

    const response = await get(app, OWNED_CAMPAIGN_ID, 'Bearer not-a-real-key');

    expect(response.statusCode).toBe(401);
  });
});

describe('POST /v1/reconciliation/:date/rerun', () => {
  it('reconciles and returns the campaign count with a valid ops token and date', async () => {
    const app = buildTestApp();

    const response = await rerun(app, '2026-07-11', OPS_TOKEN);

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ date: '2026-07-11', campaignsReconciled: 3 });
  });

  it('returns 401 for a missing or wrong ops token', async () => {
    const app = buildTestApp();

    expect((await rerun(app, '2026-07-11')).statusCode).toBe(401);
    expect((await rerun(app, '2026-07-11', 'wrong-token')).statusCode).toBe(401);
  });

  it('returns 400 for a malformed date', async () => {
    const app = buildTestApp();

    const response = await rerun(app, 'not-a-date', OPS_TOKEN);

    expect(response.statusCode).toBe(400);
  });
});

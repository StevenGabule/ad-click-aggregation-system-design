import { describe, expect, it, vi } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from './app.js';
import { computeSignature } from '@app/click-signature';
import type { AdDirectoryEntry } from '@app/db';

const secret = 'test-secret';
const entry: AdDirectoryEntry = {
  adId: 'ad_881203', campaignId: 'cmp_44210', advertiserId: 'adv_1',
  signingSecret: secret, landingUrl: 'https://advertiser.example.com/landing',
};

function validEvent(overrides: Partial<Record<string, string>> = {}) {
  const fields = {
    cid: 'clk_9f2k4x', ad_id: entry.adId, campaign_id: entry.campaignId,
    pub_id: 'pub_6612', ts: '2026-07-12T09:14:32.118Z',
    // Fold overrides into the signable fields *before* signing, so an override
    // like `cid` (used below to make each event unique) still produces a
    // self-consistent, correctly-signed event. `sig` overrides applied via the
    // final spread below still land after signing, so bad-signature test
    // cases work as intended.
    ...overrides,
  };
  const sig = computeSignature(secret, fields);
  return { ...fields, sig, ...overrides };
}

function buildTestApp() {
  const publish = vi.fn().mockResolvedValue(undefined);
  const directoryCache = { lookup: (adId: string) => (adId === entry.adId ? entry : undefined) };
  const app = buildApp({ directoryCache, publish });
  return { app, publish };
}

function post(app: FastifyInstance, body: unknown) {
  return app.inject({
    method: 'POST',
    url: '/v1/events/clicks',
    headers: { 'content-type': 'application/json' },
    payload: JSON.stringify(body),
  });
}

describe('POST /v1/events/clicks', () => {
  it('accepts a batch of valid events and publishes each one', async () => {
    const { app, publish } = buildTestApp();
    const events = [validEvent({ cid: 'clk_1' }), validEvent({ cid: 'clk_2' })];

    const response = await post(app, { events });

    expect(response.statusCode).toBe(202);
    expect(response.json()).toEqual({ accepted: 2, rejected: 0 });
    expect(publish).toHaveBeenCalledTimes(2);
  });

  it('rejects individual bad events without failing the whole batch', async () => {
    const { app, publish } = buildTestApp();
    const { ad_id, ...missingFieldEvent } = validEvent({ cid: 'clk_missing_field' });
    const events = [
      validEvent({ cid: 'clk_good' }),
      validEvent({ cid: 'clk_bad_sig', sig: '0'.repeat(64) }),
      missingFieldEvent,
    ];

    const response = await post(app, { events });

    expect(response.statusCode).toBe(202);
    expect(response.json()).toEqual({ accepted: 1, rejected: 2 });
    expect(publish).toHaveBeenCalledTimes(1);
  });

  it('returns accepted: 0 for a missing events array', async () => {
    const { app } = buildTestApp();

    const response = await post(app, {});

    expect(response.statusCode).toBe(202);
    expect(response.json()).toEqual({ accepted: 0, rejected: 0 });
  });

  it('rejects an oversized batch with 413 before processing any events', async () => {
    const { app, publish } = buildTestApp();
    const events = Array.from({ length: 1001 }, () => ({}));

    const response = await post(app, { events });

    expect(response.statusCode).toBe(413);
    expect(response.json()).toEqual({ error: 'batch_too_large', maxBatchSize: 1000 });
    expect(publish).not.toHaveBeenCalled();
  });

  it('preserves accurate accounting for a valid batch larger than PUBLISH_CONCURRENCY', async () => {
    const { app, publish } = buildTestApp();
    const events = Array.from({ length: 25 }, (_, i) => validEvent({ cid: `clk_${i}` }));

    const response = await post(app, { events });

    expect(response.statusCode).toBe(202);
    expect(response.json()).toEqual({ accepted: 25, rejected: 0 });
    expect(publish).toHaveBeenCalledTimes(25);
  });
});

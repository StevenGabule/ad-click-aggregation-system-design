import { describe, expect, it, vi } from 'vitest';
import { buildApp } from './app.js';
import { computeSignature } from '@app/click-signature';
import type { AdDirectoryEntry } from '@app/db';

const secret = 'test-secret';
const entry: AdDirectoryEntry = {
  adId: 'ad_881203', campaignId: 'cmp_44210', advertiserId: 'adv_1',
  signingSecret: secret, landingUrl: 'https://advertiser.example.com/landing',
};

function validQuery() {
  const fields = {
    cid: 'clk_9f2k4x', ad_id: entry.adId, campaign_id: entry.campaignId,
    pub_id: 'pub_6612', ts: '2026-07-12T09:14:32.118Z',
  };
  const sig = computeSignature(secret, fields);
  return { ...fields, sig, r: encodeURIComponent(entry.landingUrl) };
}

function buildTestApp() {
  const publish = vi.fn().mockResolvedValue(undefined);
  const directoryCache = { lookup: (adId: string) => (adId === entry.adId ? entry : undefined) };
  const app = buildApp({ directoryCache, publish });
  return { app, publish };
}

function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

describe('GET /click', () => {
  it('redirects to the landing URL and publishes the click', async () => {
    const { app, publish } = buildTestApp();
    const query = validQuery();

    const response = await app.inject({ method: 'GET', url: '/click', query });

    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe(entry.landingUrl);

    await flush();
    expect(publish).toHaveBeenCalledWith(expect.objectContaining({ cid: query.cid, ad_id: query.ad_id }));
  });

  it('rejects an unknown ad_id with 400 and does not publish', async () => {
    const { app, publish } = buildTestApp();
    const query = { ...validQuery(), ad_id: 'ad_unknown' };

    const response = await app.inject({ method: 'GET', url: '/click', query });
    await flush();

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'invalid_request' });
    expect(publish).not.toHaveBeenCalled();
  });

  it('rejects a tampered signature with 400', async () => {
    const { app } = buildTestApp();
    const query = { ...validQuery(), campaign_id: 'cmp_tampered' };

    const response = await app.inject({ method: 'GET', url: '/click', query });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'invalid_request' });
  });

  it('rejects a landing URL that does not match the directory with 400', async () => {
    const { app } = buildTestApp();
    const query = { ...validQuery(), r: encodeURIComponent('https://evil.example.com') };

    const response = await app.inject({ method: 'GET', url: '/click', query });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'invalid_request' });
  });
});

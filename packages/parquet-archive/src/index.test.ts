import { describe, expect, it } from 'vitest';
import { createArchiveDb, archiveBatch, reconcileDate, bucketPrefixForDate } from './index.js';

const TEST_DATE = '2026-01-01';
const env = {
  AWS_REGION: 'us-east-1',
  AWS_ENDPOINT_URL: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
  DATABASE_URL: 'unused',
  REDIS_URL: 'unused',
};

describe('parquet-archive', () => {
  it('writes and reads back distinct-counted click events per campaign', async () => {
    const db = await createArchiveDb(env);
    const prefix = bucketPrefixForDate(TEST_DATE);
    const campaignId = `cmp_pa_test_${Date.now()}`;
    const repeatedCid = `clk_pa_${Date.now()}_a`;

    await archiveBatch(db, prefix, [
      { cid: repeatedCid, ad_id: 'ad_1', campaign_id: campaignId, pub_id: 'pub_1', ts: '2026-01-01T00:00:00Z', sig: 'x', receivedAt: Date.now() },
      { cid: `clk_pa_${Date.now()}_b`, ad_id: 'ad_1', campaign_id: campaignId, pub_id: 'pub_1', ts: '2026-01-01T00:01:00Z', sig: 'x', receivedAt: Date.now() },
    ]);
    await archiveBatch(db, prefix, [
      { cid: repeatedCid, ad_id: 'ad_1', campaign_id: campaignId, pub_id: 'pub_1', ts: '2026-01-01T00:00:00Z', sig: 'x', receivedAt: Date.now() },
    ]);

    const results = await reconcileDate(db, prefix);
    const row = results.find((r) => r.campaignId === campaignId);

    expect(row).toEqual({ campaignId, billedClicks: 2, excludedInvalidClicks: 0 });
  }, 30_000);

  it('moves excluded cids from billedClicks into excludedInvalidClicks', async () => {
    const db = await createArchiveDb(env);
    const prefix = bucketPrefixForDate(TEST_DATE);
    const campaignId = `cmp_pa_test_${Date.now()}`;
    const goodCid = `clk_pa_${Date.now()}_good`;
    const badCid = `clk_pa_${Date.now()}_bad`;

    await archiveBatch(db, prefix, [
      { cid: goodCid, ad_id: 'ad_2', campaign_id: campaignId, pub_id: 'pub_1', ts: '2026-01-01T00:00:00Z', sig: 'x', receivedAt: Date.now() },
      { cid: badCid, ad_id: 'ad_2', campaign_id: campaignId, pub_id: 'pub_1', ts: '2026-01-01T00:00:00Z', sig: 'x', receivedAt: Date.now() },
    ]);

    const results = await reconcileDate(db, prefix, new Set([badCid]));
    const row = results.find((r) => r.campaignId === campaignId);

    expect(row).toEqual({ campaignId, billedClicks: 1, excludedInvalidClicks: 1 });
  }, 30_000);
});

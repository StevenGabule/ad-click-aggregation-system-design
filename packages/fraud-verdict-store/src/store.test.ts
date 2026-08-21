import { describe, expect, it } from 'vitest';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { putVerdict, listExcludedCids } from './store.js';

const TEST_DATE = '2026-01-15';

function testClient(): DynamoDBClient {
  return new DynamoDBClient({
    region: 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
}

describe('fraud-verdict-store', () => {
  it('listExcludedCids returns suspicious and invalid cids but not legitimate ones', async () => {
    const client = testClient();
    const legitimateCid = `clk_fv_${Date.now()}_legit`;
    const suspiciousCid = `clk_fv_${Date.now()}_susp`;
    const invalidCid = `clk_fv_${Date.now()}_bad`;

    await putVerdict(client, { date: TEST_DATE, cid: legitimateCid, verdict: 'legitimate' });
    await putVerdict(client, { date: TEST_DATE, cid: suspiciousCid, verdict: 'suspicious' });
    await putVerdict(client, { date: TEST_DATE, cid: invalidCid, verdict: 'invalid' });

    const excluded = await listExcludedCids(client, TEST_DATE);

    expect(excluded.has(legitimateCid)).toBe(false);
    expect(excluded.has(suspiciousCid)).toBe(true);
    expect(excluded.has(invalidCid)).toBe(true);
  }, 20_000);

  it('returns an empty set for a date with no verdicts', async () => {
    const client = testClient();
    expect(await listExcludedCids(client, '2099-12-31')).toEqual(new Set());
  }, 20_000);
});

import { describe, expect, it } from 'vitest';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { putStatement, getStatement } from './index.js';

function testClient(): DynamoDBClient {
  return new DynamoDBClient({
    region: 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
}

describe('statements-store', () => {
  it('round-trips a statement by campaignId and period', async () => {
    const client = testClient();
    const campaignId = `cmp_ss_test_${Date.now()}`;
    const statement = {
      campaignId, period: '2026-07-11', billedClicks: 118402, excludedInvalidClicks: 613,
      reconciledAt: '2026-07-12T02:11:00Z', sourceArchive: 's3://ad-clicks-raw/dt=2026-07-11/',
    };

    await putStatement(client, statement);

    expect(await getStatement(client, campaignId, '2026-07-11')).toEqual(statement);
  }, 20_000);

  it('a second put for the same key fully replaces the first', async () => {
    const client = testClient();
    const campaignId = `cmp_ss_test_${Date.now()}`;
    const first = {
      campaignId, period: '2026-07-11', billedClicks: 100, excludedInvalidClicks: 5,
      reconciledAt: '2026-07-12T02:11:00Z', sourceArchive: 's3://ad-clicks-raw/dt=2026-07-11/',
    };
    const second = { ...first, billedClicks: 999, excludedInvalidClicks: 0, reconciledAt: '2026-07-12T03:00:00Z' };

    await putStatement(client, first);
    await putStatement(client, second);

    expect(await getStatement(client, campaignId, '2026-07-11')).toEqual(second);
  }, 20_000);

  it('getStatement with no period returns the most recently-put one across periods', async () => {
    const client = testClient();
    const campaignId = `cmp_ss_test_${Date.now()}`;
    const older = {
      campaignId, period: '2026-07-10', billedClicks: 1, excludedInvalidClicks: 0,
      reconciledAt: '2026-07-11T02:00:00Z', sourceArchive: 's3://ad-clicks-raw/dt=2026-07-10/',
    };
    const newer = { ...older, period: '2026-07-11', billedClicks: 2, reconciledAt: '2026-07-12T02:00:00Z' };

    await putStatement(client, older);
    await putStatement(client, newer);

    expect(await getStatement(client, campaignId)).toEqual(newer);
  }, 20_000);
});

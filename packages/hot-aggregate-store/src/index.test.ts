import { describe, expect, it } from 'vitest';
import { DynamoDBClient, GetItemCommand } from '@aws-sdk/client-dynamodb';
import { createHotAggregateStore, getLatestAggregate } from './index.js';

function testClient(): DynamoDBClient {
  return new DynamoDBClient({
    region: 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
}

describe('hot-aggregate-store', () => {
  it('two flushes of the same (adId, windowStart) sum instead of overwrite', async () => {
    const client = testClient();
    const store = createHotAggregateStore(client);
    const adId = `ad_test_${Date.now()}`;
    const windowStart = 60_000;

    await store.flush(adId, windowStart, 5);
    await store.flush(adId, windowStart, 3);

    const { Item } = await client.send(new GetItemCommand({
      TableName: 'click-aggregates',
      Key: { adId: { S: adId }, windowStart: { N: String(windowStart) } },
    }));

    expect(Item?.count.N).toBe('8');
  }, 20_000);
});

describe('getLatestAggregate', () => {
  it('returns the most recent flushed window', async () => {
    const client = testClient();
    const store = createHotAggregateStore(client);
    const adId = `ad_test_${Date.now()}`;

    await store.flush(adId, 60_000, 4);
    await store.flush(adId, 120_000, 2);

    expect(await getLatestAggregate(client, adId)).toEqual({ windowStart: 120_000, clicks: 2 });
  }, 20_000);

  it('returns null for an ad with no flushed windows', async () => {
    const client = testClient();
    expect(await getLatestAggregate(client, `ad_never_flushed_${Date.now()}`)).toBeNull();
  }, 20_000);
});

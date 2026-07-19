import { describe, expect, it } from 'vitest';
import { createClient as createRedisClient } from 'redis';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { createDedupStore } from './index.js';

function testDynamo(): DynamoDBClient {
  return new DynamoDBClient({
    region: 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
}

describe('click-dedup', () => {
  it('accepts a cid the first time and rejects it the second time', async () => {
    const redis = createRedisClient({ url: process.env.REDIS_URL ?? 'redis://localhost:6379' });
    await redis.connect();
    const dedupStore = createDedupStore(redis, testDynamo());
    const cid = `clk_test_${Date.now()}_a`;

    expect(await dedupStore.isNew(cid)).toBe(true);
    expect(await dedupStore.isNew(cid)).toBe(false);

    await redis.quit();
  }, 20_000);

  it('still rejects a duplicate after Redis loses its record (DynamoDB backstop)', async () => {
    const redis = createRedisClient({ url: process.env.REDIS_URL ?? 'redis://localhost:6379' });
    await redis.connect();
    const dedupStore = createDedupStore(redis, testDynamo());
    const cid = `clk_test_${Date.now()}_b`;

    expect(await dedupStore.isNew(cid)).toBe(true);
    await redis.del(`click:${cid}`);

    expect(await dedupStore.isNew(cid)).toBe(false);

    await redis.quit();
  }, 20_000);
});

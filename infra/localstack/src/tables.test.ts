import { describe, expect, it } from 'vitest';
import { DynamoDBClient, DescribeTableCommand } from '@aws-sdk/client-dynamodb';
import { ensureDedupTable, ensureHotAggregateTable, DEDUP_TABLE_NAME, HOT_AGGREGATE_TABLE_NAME } from './tables.js';

function testClient(): DynamoDBClient {
  return new DynamoDBClient({
    region: 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
}

describe('DynamoDB table bootstrap', () => {
  it('creates the dedup table, and is a no-op the second time', async () => {
    const client = testClient();
    await ensureDedupTable(client);
    await ensureDedupTable(client);

    const description = await client.send(new DescribeTableCommand({ TableName: DEDUP_TABLE_NAME }));
    expect(description.Table?.TableStatus).toBe('ACTIVE');
  }, 20_000);

  it('creates the hot aggregate table with a composite key, and is a no-op the second time', async () => {
    const client = testClient();
    await ensureHotAggregateTable(client);
    await ensureHotAggregateTable(client);

    const description = await client.send(new DescribeTableCommand({ TableName: HOT_AGGREGATE_TABLE_NAME }));
    expect(description.Table?.TableStatus).toBe('ACTIVE');
    expect(description.Table?.KeySchema).toEqual([
      { AttributeName: 'adId', KeyType: 'HASH' },
      { AttributeName: 'windowStart', KeyType: 'RANGE' },
    ]);
  }, 20_000);
});

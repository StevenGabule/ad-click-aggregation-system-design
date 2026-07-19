import { describe, expect, it } from 'vitest';
import {
  KinesisClient, ListShardsCommand, GetShardIteratorCommand, GetRecordsCommand,
} from '@aws-sdk/client-kinesis';
import { publishClickEvent } from './index.js';
import type { ClickEvent } from '@app/event-schema';

const STREAM_NAME = 'ad-clicks-raw';

function testClient(): KinesisClient {
  return new KinesisClient({
    region: 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
}

async function readAllRecords(client: KinesisClient) {
  const { Shards } = await client.send(new ListShardsCommand({ StreamName: STREAM_NAME }));
  const records: { partitionKey: string; payload: Record<string, unknown> }[] = [];

  for (const shard of Shards ?? []) {
    const { ShardIterator } = await client.send(new GetShardIteratorCommand({
      StreamName: STREAM_NAME,
      ShardId: shard.ShardId!,
      ShardIteratorType: 'TRIM_HORIZON',
    }));
    const { Records } = await client.send(new GetRecordsCommand({ ShardIterator: ShardIterator! }));
    for (const record of Records ?? []) {
      records.push({
        partitionKey: record.PartitionKey!,
        payload: JSON.parse(Buffer.from(record.Data!).toString('utf-8')),
      });
    }
  }
  return records;
}

describe('publishClickEvent', () => {
  it('publishes a record with a salted partition key and JSON payload', async () => {
    const client = testClient();
    const event: ClickEvent = {
      cid: `clk_test_${Date.now()}`,
      ad_id: 'ad_881203',
      campaign_id: 'cmp_44210',
      pub_id: 'pub_6612',
      ts: '2026-07-12T09:14:32.118Z',
      sig: 'deadbeef',
    };

    await publishClickEvent(client, STREAM_NAME, event);

    const records = await readAllRecords(client);
    const match = records.find((r) => r.payload.cid === event.cid);

    expect(match?.payload).toMatchObject(event);
    expect(match?.payload.receivedAt).toEqual(expect.any(Number));
    expect(match?.partitionKey).toMatch(/^ad_881203#[0-7]$/);
  }, 20_000);
});

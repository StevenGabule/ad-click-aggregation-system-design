import { describe, expect, it } from 'vitest';
import { KinesisClient, PutRecordCommand, ListShardsCommand } from '@aws-sdk/client-kinesis';
import { runPollingConsumer } from './index.js';

const STREAM_NAME = 'ad-clicks-raw';

function testClient(): KinesisClient {
  return new KinesisClient({
    region: 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
}

describe('runPollingConsumer', () => {
  it('invokes onRecord for published records and stops cleanly on abort', async () => {
    const kinesis = testClient();
    const marker = `loop_test_${Date.now()}`;
    await kinesis.send(new PutRecordCommand({
      StreamName: STREAM_NAME,
      PartitionKey: `loop-test-key-${Date.now()}`,
      Data: Buffer.from(JSON.stringify({ marker })),
    }));

    const { Shards } = await kinesis.send(new ListShardsCommand({ StreamName: STREAM_NAME }));
    const seen: string[] = [];
    const controller = new AbortController();
    setTimeout(() => controller.abort(), 3000);

    await Promise.all((Shards ?? []).map((shard) =>
      runPollingConsumer({
        kinesis,
        streamName: STREAM_NAME,
        shardId: shard.ShardId!,
        pollIntervalMs: 200,
        shardIteratorType: 'TRIM_HORIZON',
        signal: controller.signal,
        onRecord: async (data) => {
          const parsed = JSON.parse(data.toString('utf-8'));
          if (parsed.marker === marker) seen.push(parsed.marker);
        },
      })
    ));

    expect(seen).toContain(marker);
  }, 10_000);
});

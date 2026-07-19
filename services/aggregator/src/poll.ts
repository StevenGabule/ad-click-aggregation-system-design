import { KinesisClient, GetShardIteratorCommand, GetRecordsCommand } from '@aws-sdk/client-kinesis';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { createClient as createRedisClient } from 'redis';
import { createDedupStore } from '@app/click-dedup';
import { createWindowedAggregator } from '@app/windowed-aggregator';
import { createHotAggregateStore } from '@app/hot-aggregate-store';
import { loadEnv } from '@app/config';
import { handleRecord, flushClosedWindows, type RawClickEvent } from './core.js';

const STREAM_NAME = 'ad-clicks-raw';
const POLL_INTERVAL_MS = 1000;
const FLUSH_INTERVAL_MS = 5000;

async function main() {
  const env = loadEnv();
  const shardId = process.env.KINESIS_SHARD_ID;
  if (!shardId) throw new Error('KINESIS_SHARD_ID is required');

  const awsClientConfig = {
    region: env.AWS_REGION,
    endpoint: env.AWS_ENDPOINT_URL,
    // ponytail: LocalStack dummy creds; swap for the default AWS credential provider chain when targeting real AWS
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  };
  const kinesis = new KinesisClient(awsClientConfig);
  const dynamo = new DynamoDBClient(awsClientConfig);
  const redis = createRedisClient({ url: env.REDIS_URL });
  await redis.connect();

  const dedupStore = createDedupStore(redis, dynamo);
  const aggregator = createWindowedAggregator({ windowMs: 60_000, watermarkMs: 120_000 });
  const hotStore = createHotAggregateStore(dynamo);

  setInterval(() => {
    flushClosedWindows(aggregator, hotStore, Date.now()).catch((err) => console.error('flush tick failed', err));
  }, FLUSH_INTERVAL_MS).unref();

  let { ShardIterator: iterator } = await kinesis.send(new GetShardIteratorCommand({
    StreamName: STREAM_NAME,
    ShardId: shardId,
    ShardIteratorType: 'LATEST',
  }));

  while (iterator) {
    const { Records, NextShardIterator } = await kinesis.send(new GetRecordsCommand({ ShardIterator: iterator }));

    for (const record of Records ?? []) {
      try {
        const event = JSON.parse(Buffer.from(record.Data!).toString('utf-8')) as RawClickEvent;
        await handleRecord({ dedupStore, aggregator }, event);
      } catch (err) {
        console.error('failed to process record, skipping', { sequenceNumber: record.SequenceNumber, err });
      }
    }

    iterator = NextShardIterator;
    if (!Records || Records.length === 0) {
      await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

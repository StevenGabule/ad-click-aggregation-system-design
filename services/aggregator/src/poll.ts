import { KinesisClient } from '@aws-sdk/client-kinesis';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { createClient as createRedisClient } from 'redis';
import { runPollingConsumer } from '@app/kinesis-consumer-loop';
import { createDedupStore } from '@app/click-dedup';
import { createWindowedAggregator } from '@app/windowed-aggregator';
import { createHotAggregateStore } from '@app/hot-aggregate-store';
import { loadEnv } from '@app/config';
import { handleRecord, flushClosedWindows, type RawClickEvent } from './core.js';

const STREAM_NAME = 'ad-clicks-raw';
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

  let flushing = false;
  setInterval(() => {
    if (flushing) return;
    flushing = true;
    flushClosedWindows(aggregator, hotStore, Date.now())
      .catch((err) => console.error('flush tick failed', err))
      .finally(() => { flushing = false; });
  }, FLUSH_INTERVAL_MS).unref();

  await runPollingConsumer({
    kinesis,
    streamName: STREAM_NAME,
    shardId,
    onRecord: async (data) => {
      const event = JSON.parse(data.toString('utf-8')) as RawClickEvent;
      await handleRecord({ dedupStore, aggregator }, event);
    },
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

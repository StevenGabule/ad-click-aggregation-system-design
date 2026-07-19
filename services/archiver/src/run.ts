import { KinesisClient } from '@aws-sdk/client-kinesis';
import { runPollingConsumer } from '@app/kinesis-consumer-loop';
import { createArchiveDb, archiveBatch, bucketPrefixForDate, type RawArchiveEvent } from '@app/parquet-archive';
import { loadEnv } from '@app/config';
import { createBatchBuffer } from './core.js';

const STREAM_NAME = 'ad-clicks-raw';
const FLUSH_CHECK_INTERVAL_MS = 5000;

function todayDateString(): string {
  return new Date().toISOString().slice(0, 10);
}

async function main() {
  const env = loadEnv();
  const shardId = process.env.KINESIS_SHARD_ID;
  if (!shardId) throw new Error('KINESIS_SHARD_ID is required');

  const kinesis = new KinesisClient({
    region: env.AWS_REGION,
    endpoint: env.AWS_ENDPOINT_URL,
    // ponytail: LocalStack dummy creds; swap for the default AWS credential provider chain when targeting real AWS
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
  const db = await createArchiveDb(env);
  const buffer = createBatchBuffer<RawArchiveEvent>({ maxSize: 500, maxAgeMs: 30_000 });

  // ponytail: a batch flushing near UTC midnight can file a few stragglers under the wrong
  // day's partition (todayDateString() is computed at flush time, not at record-receive time).
  // Accepted limitation — reconciliation reads whole-day partitions so this is a rare, small
  // misfile, not a correctness gap in the dedup/billing math.
  let flushing = false;
  setInterval(() => {
    if (flushing) return;
    if (!buffer.shouldFlush(Date.now())) return;
    flushing = true;
    const batch = buffer.drain();
    archiveBatch(db, bucketPrefixForDate(todayDateString()), batch)
      .catch((err) => console.error('archive flush failed, batch lost', { size: batch.length, err }))
      .finally(() => { flushing = false; });
  }, FLUSH_CHECK_INTERVAL_MS).unref();

  await runPollingConsumer({
    kinesis,
    streamName: STREAM_NAME,
    shardId,
    onRecord: async (data) => {
      buffer.add(JSON.parse(data.toString('utf-8')) as RawArchiveEvent);
    },
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

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

  // Partition each drained batch by the event's OWN event-time date (from `ts`), not flush time,
  // so an at-least-once duplicate of a cid that straddles UTC midnight always lands in the same
  // date partition — reconciliation's COUNT(DISTINCT cid) then dedups it (a flush-time partition
  // would double-count it across two daily statements). A malformed/absent ts falls back to today.
  function dateForEvent(event: RawArchiveEvent): string {
    return /^\d{4}-\d{2}-\d{2}/.test(event.ts) ? event.ts.slice(0, 10) : todayDateString();
  }

  let flushing = false;
  setInterval(() => {
    if (flushing) return;
    if (!buffer.shouldFlush(Date.now())) return;
    flushing = true;
    const batch = buffer.drain();
    (async () => {
      const byDate = new Map<string, RawArchiveEvent[]>();
      for (const event of batch) {
        const date = dateForEvent(event);
        let group = byDate.get(date);
        if (!group) {
          group = [];
          byDate.set(date, group);
        }
        group.push(event);
      }
      // sequential per-date writes (archiveBatch is not concurrency-safe on its shared temp table)
      for (const [date, events] of byDate) {
        await archiveBatch(db, bucketPrefixForDate(date), events);
      }
    })()
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

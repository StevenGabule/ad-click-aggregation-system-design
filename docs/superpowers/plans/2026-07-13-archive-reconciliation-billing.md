# Archive + Reconciliation + Billing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The durable, exact path — a raw Parquet archive on S3, a DuckDB reconciliation query over it, the statements table, and the billing/rerun endpoints that read and trigger it.

**Architecture:** A shared, extracted Kinesis polling loop (used by the new archiver and retrofitted onto sub-project 3's aggregator); a DuckDB-backed archive package doing both the Parquet write and the reconciliation read against S3; a small overwrite-semantics statements store, deliberately unlike the hot-aggregate-store's ADD semantics; a billing service with two very different auth models on its two routes.

**Tech Stack:** Node.js 24, TypeScript 5.x (strict, ESM), `duckdb-async`, `@aws-sdk/client-s3`, `@aws-sdk/client-dynamodb`, `@aws-sdk/client-kinesis`, Fastify 5, Vitest 3.

## Global Constraints

- Node.js 24, TypeScript strict mode, ESM — same as prior sub-projects.
- The archiver writes every raw record, unfiltered — no dedup on the write side (spec §"Why the raw archive isn't deduped"). Exactness comes from `COUNT(DISTINCT cid)` at reconciliation time.
- `statements-store.putStatement` is a full overwrite (`PutItem`), never `UpdateItem`/`ADD` — the opposite of `hot-aggregate-store.flush`. Getting this backwards silently reintroduces the bug class sub-project 3's spec fix was about.
- `reconcileDate`'s fraud-exclusion parameter is a bulk `Set<string>`, not a per-row callback — the callback shape would defeat doing this in SQL (spec §"Fraud exclusion seam").
- `OPS_TOKEN` is read directly by `services/billing-api`, never added to `@app/config`'s shared `EnvSchema`.
- After Task 2, `services/aggregator/src/poll.ts` no longer contains its own `GetShardIterator`/`GetRecords` loop — it calls `@app/kinesis-consumer-loop`.

---

## File Structure

```
packages/
├── kinesis-consumer-loop/
│   ├── package.json / tsconfig.json
│   └── src/{index.ts, index.test.ts}          # runPollingConsumer
├── parquet-archive/
│   ├── package.json / tsconfig.json
│   └── src/{index.ts, index.test.ts}          # createArchiveDb, archiveBatch, reconcileDate, bucketPrefixForDate
└── statements-store/
    ├── package.json / tsconfig.json
    └── src/{index.ts, index.test.ts}          # putStatement, getStatement

packages/db/src/
├── ownership.ts                                # MODIFIED: add getCampaignOwnerAdvertiserId
└── index.ts                                     # MODIFIED: export it

infra/localstack/src/
├── buckets.ts                                   # NEW: ensureRawArchiveBucket
├── buckets.test.ts
├── tables.ts                                     # MODIFIED: add ensureStatementsTable
├── tables.test.ts                                 # MODIFIED: test it
├── bootstrap.ts                                   # MODIFIED: main() also bootstraps the bucket + table
└── package.json                                    # MODIFIED: add @aws-sdk/client-s3

services/aggregator/src/
└── poll.ts                                        # MODIFIED (retrofit): use @app/kinesis-consumer-loop
services/aggregator/package.json                    # MODIFIED: add @app/kinesis-consumer-loop dependency

services/archiver/
├── package.json / tsconfig.json
└── src/
    ├── core.ts                                   # createBatchBuffer — pure, unit tested
    ├── core.test.ts
    └── run.ts                                     # thin composition, not unit tested

services/billing-api/
├── package.json / tsconfig.json
└── src/
    ├── app.ts                                     # buildApp(deps) — both routes
    ├── app.test.ts
    └── server.ts                                   # composition root, not unit tested
```

---

### Task 1: `packages/kinesis-consumer-loop`

**Files:**
- Create: `packages/kinesis-consumer-loop/package.json`
- Create: `packages/kinesis-consumer-loop/tsconfig.json`
- Create: `packages/kinesis-consumer-loop/src/index.ts`
- Test: `packages/kinesis-consumer-loop/src/index.test.ts`

**Interfaces:**
- Consumes: a `KinesisClient`, an existing stream/shard.
- Produces: `runPollingConsumer(options: PollingConsumerOptions): Promise<void>` where `PollingConsumerOptions = { kinesis, streamName, shardId, onRecord: (data: Buffer, meta: { sequenceNumber: string }) => Promise<void>, pollIntervalMs?, shardIteratorType?: 'LATEST' | 'TRIM_HORIZON', signal?: AbortSignal }` — consumed by Task 2 (retrofit) and Task 5.

- [ ] **Step 1: Write the failing test**

`packages/kinesis-consumer-loop/src/index.test.ts`:
```ts
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
```

- [ ] **Step 2: Create package config**

`packages/kinesis-consumer-loop/package.json`:
```json
{
  "name": "@app/kinesis-consumer-loop",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "test": "vitest run",
    "typecheck": "tsc -p tsconfig.json --noEmit"
  },
  "dependencies": {
    "@aws-sdk/client-kinesis": "^3.716.0"
  }
}
```

`packages/kinesis-consumer-loop/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/kinesis-consumer-loop test`
Expected: FAIL — `./index.js` has no exported member `runPollingConsumer`.

- [ ] **Step 4: Write the implementation**

`packages/kinesis-consumer-loop/src/index.ts`:
```ts
import { KinesisClient, GetShardIteratorCommand, GetRecordsCommand } from '@aws-sdk/client-kinesis';

export interface PollingConsumerOptions {
  kinesis: KinesisClient;
  streamName: string;
  shardId: string;
  onRecord: (data: Buffer, meta: { sequenceNumber: string }) => Promise<void>;
  pollIntervalMs?: number;
  shardIteratorType?: 'LATEST' | 'TRIM_HORIZON';
  signal?: AbortSignal;
}

export async function runPollingConsumer(options: PollingConsumerOptions): Promise<void> {
  const { kinesis, streamName, shardId, onRecord, signal } = options;
  const pollIntervalMs = options.pollIntervalMs ?? 1000;

  let { ShardIterator: iterator } = await kinesis.send(new GetShardIteratorCommand({
    StreamName: streamName,
    ShardId: shardId,
    ShardIteratorType: options.shardIteratorType ?? 'LATEST',
  }));

  while (iterator && !signal?.aborted) {
    const { Records, NextShardIterator } = await kinesis.send(new GetRecordsCommand({ ShardIterator: iterator }));

    for (const record of Records ?? []) {
      try {
        await onRecord(Buffer.from(record.Data!), { sequenceNumber: record.SequenceNumber! });
      } catch (err) {
        console.error('consumer callback failed for record, skipping', { sequenceNumber: record.SequenceNumber, err });
      }
    }

    iterator = NextShardIterator;
    if (!Records || Records.length === 0) {
      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    }
  }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/kinesis-consumer-loop test`
Expected: PASS — 1 test.

- [ ] **Step 6: Commit**

```bash
git add packages/kinesis-consumer-loop
git commit -m "feat: add shared, abortable Kinesis polling consumer loop"
```

---

### Task 2: Retrofit `services/aggregator` onto the shared loop

**Files:**
- Modify: `services/aggregator/package.json`
- Modify: `services/aggregator/src/poll.ts`

**Interfaces:**
- Consumes: `runPollingConsumer` (Task 1).
- Produces: no interface change — `services/aggregator`'s external behavior (what it reads, what it writes) is identical; only its internal polling mechanics change.

- [ ] **Step 1: Add the new dependency**

Modify `services/aggregator/package.json` — add to `dependencies`:
```json
"@app/kinesis-consumer-loop": "workspace:*"
```

- [ ] **Step 2: Replace the polling loop**

Modify `services/aggregator/src/poll.ts` — replace the entire file with:
```ts
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

  setInterval(() => {
    flushClosedWindows(aggregator, hotStore, Date.now()).catch((err) => console.error('flush tick failed', err));
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
```

- [ ] **Step 3: Verify the aggregator's own tests still pass (untouched by this refactor)**

Run: `pnpm --filter @app/aggregator test`
Expected: PASS — 4 tests (`core.test.ts` is unaffected; `poll.ts` was never unit tested, by design).

Run: `pnpm --filter @app/aggregator typecheck`
Expected: exits 0.

- [ ] **Step 4: Commit**

```bash
git add services/aggregator
git commit -m "refactor: retrofit aggregator onto the shared Kinesis polling loop"
```

---

### Task 3: `infra/localstack` — raw archive bucket and statements table

**Files:**
- Create: `infra/localstack/src/buckets.ts`
- Test: `infra/localstack/src/buckets.test.ts`
- Modify: `infra/localstack/src/tables.ts` (add `ensureStatementsTable`)
- Modify: `infra/localstack/src/tables.test.ts` (test it)
- Modify: `infra/localstack/src/bootstrap.ts` (its `main()`)
- Modify: `infra/localstack/package.json` (add `@aws-sdk/client-s3`)

**Interfaces:**
- Consumes: a running LocalStack container.
- Produces: `ensureRawArchiveBucket(client: S3Client): Promise<void>`, `RAW_ARCHIVE_BUCKET_NAME = 'ad-clicks-raw'`, `ensureStatementsTable(client: DynamoDBClient): Promise<void>`, `STATEMENTS_TABLE_NAME = 'click-statements'` — consumed by Tasks 4–8.

- [ ] **Step 1: Write the failing tests**

`infra/localstack/src/buckets.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { S3Client, HeadBucketCommand } from '@aws-sdk/client-s3';
import { ensureRawArchiveBucket, RAW_ARCHIVE_BUCKET_NAME } from './buckets.js';

function testClient(): S3Client {
  return new S3Client({
    region: 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    forcePathStyle: true,
  });
}

describe('ensureRawArchiveBucket', () => {
  it('creates the bucket, and is a no-op the second time', async () => {
    const client = testClient();
    await ensureRawArchiveBucket(client);
    await ensureRawArchiveBucket(client);

    await expect(client.send(new HeadBucketCommand({ Bucket: RAW_ARCHIVE_BUCKET_NAME }))).resolves.toBeDefined();
  }, 20_000);
});
```

Add to `infra/localstack/src/tables.test.ts` (new import, new `describe` block):
```ts
import { ensureStatementsTable, STATEMENTS_TABLE_NAME } from './tables.js';
```
```ts
describe('ensureStatementsTable', () => {
  it('creates the statements table with a composite key, and is a no-op the second time', async () => {
    const client = testClient();
    await ensureStatementsTable(client);
    await ensureStatementsTable(client);

    const description = await client.send(new DescribeTableCommand({ TableName: STATEMENTS_TABLE_NAME }));
    expect(description.Table?.TableStatus).toBe('ACTIVE');
    expect(description.Table?.KeySchema).toEqual([
      { AttributeName: 'campaignId', KeyType: 'HASH' },
      { AttributeName: 'period', KeyType: 'RANGE' },
    ]);
  }, 20_000);
});
```

- [ ] **Step 2: Add the S3 SDK dependency**

Modify `infra/localstack/package.json` — add to `dependencies`:
```json
"@aws-sdk/client-s3": "^3.716.0"
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pnpm install && pnpm --filter @app/infra-localstack test`
Expected: FAIL — `./buckets.js` module not found; `ensureStatementsTable` not exported.

- [ ] **Step 4: Write the bucket bootstrap**

`infra/localstack/src/buckets.ts`:
```ts
import { S3Client, CreateBucketCommand, HeadBucketCommand } from '@aws-sdk/client-s3';

export const RAW_ARCHIVE_BUCKET_NAME = 'ad-clicks-raw';

async function bucketExists(client: S3Client, bucket: string): Promise<boolean> {
  try {
    await client.send(new HeadBucketCommand({ Bucket: bucket }));
    return true;
  } catch {
    return false;
  }
}

export async function ensureRawArchiveBucket(client: S3Client): Promise<void> {
  if (await bucketExists(client, RAW_ARCHIVE_BUCKET_NAME)) return;
  await client.send(new CreateBucketCommand({ Bucket: RAW_ARCHIVE_BUCKET_NAME }));
}
```

- [ ] **Step 5: Add the statements table function**

Modify `infra/localstack/src/tables.ts` — add below `ensureHotAggregateTable`:
```ts
export const STATEMENTS_TABLE_NAME = 'click-statements';

export async function ensureStatementsTable(client: DynamoDBClient): Promise<void> {
  if (await tableExists(client, STATEMENTS_TABLE_NAME)) return;

  await client.send(new CreateTableCommand({
    TableName: STATEMENTS_TABLE_NAME,
    AttributeDefinitions: [
      { AttributeName: 'campaignId', AttributeType: 'S' },
      { AttributeName: 'period', AttributeType: 'S' },
    ],
    KeySchema: [
      { AttributeName: 'campaignId', KeyType: 'HASH' },
      { AttributeName: 'period', KeyType: 'RANGE' },
    ],
    BillingMode: 'PAY_PER_REQUEST',
  }));
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pnpm --filter @app/infra-localstack test`
Expected: PASS — 5 tests (3 from sub-projects 1/3, 2 new).

- [ ] **Step 7: Wire both into the bootstrap entrypoint**

Modify `infra/localstack/src/bootstrap.ts` — add imports:
```ts
import { S3Client } from '@aws-sdk/client-s3';
import { ensureRawArchiveBucket } from './buckets.js';
import { ensureStatementsTable } from './tables.js';
```

Replace `main()` with:
```ts
async function main() {
  const kinesisClient = new KinesisClient({
    region: process.env.AWS_REGION ?? 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
  await ensureClickStream(kinesisClient);

  const dynamoClient = new DynamoDBClient({
    region: process.env.AWS_REGION ?? 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
  await ensureDedupTable(dynamoClient);
  await ensureHotAggregateTable(dynamoClient);
  await ensureStatementsTable(dynamoClient);

  const s3Client = new S3Client({
    region: process.env.AWS_REGION ?? 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    forcePathStyle: true,
  });
  await ensureRawArchiveBucket(s3Client);

  console.log('LocalStack resources ready: Kinesis stream, dedup table, hot aggregate table, statements table, raw archive bucket.');
}
```

Run: `pnpm --filter @app/infra-localstack bootstrap`
Expected: prints the updated ready message.

- [ ] **Step 8: Commit**

```bash
git add infra/localstack
git commit -m "feat: bootstrap raw archive S3 bucket and statements DynamoDB table"
```

---

### Task 4: `packages/parquet-archive`

**Files:**
- Create: `packages/parquet-archive/package.json`
- Create: `packages/parquet-archive/tsconfig.json`
- Create: `packages/parquet-archive/src/index.ts`
- Test: `packages/parquet-archive/src/index.test.ts`

**Interfaces:**
- Consumes: `Env` shape (`@app/config`), a running LocalStack S3 with the bucket from Task 3.
- Produces: `createArchiveDb(env)`, `archiveBatch(db, bucketPrefix, events)`, `reconcileDate(db, bucketPrefix, excludedCids?)`, `bucketPrefixForDate(date)` — consumed by Tasks 5 and 8.

- [ ] **Step 1: Write the failing test**

`packages/parquet-archive/src/index.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { createArchiveDb, archiveBatch, reconcileDate, bucketPrefixForDate } from './index.js';

const TEST_DATE = '2026-01-01';
const env = {
  AWS_REGION: 'us-east-1',
  AWS_ENDPOINT_URL: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
  DATABASE_URL: 'unused',
  REDIS_URL: 'unused',
};

describe('parquet-archive', () => {
  it('writes and reads back distinct-counted click events per campaign', async () => {
    const db = await createArchiveDb(env);
    const prefix = bucketPrefixForDate(TEST_DATE);
    const campaignId = `cmp_pa_test_${Date.now()}`;
    const repeatedCid = `clk_pa_${Date.now()}_a`;

    await archiveBatch(db, prefix, [
      { cid: repeatedCid, ad_id: 'ad_1', campaign_id: campaignId, pub_id: 'pub_1', ts: '2026-01-01T00:00:00Z', sig: 'x', receivedAt: Date.now() },
      { cid: `clk_pa_${Date.now()}_b`, ad_id: 'ad_1', campaign_id: campaignId, pub_id: 'pub_1', ts: '2026-01-01T00:01:00Z', sig: 'x', receivedAt: Date.now() },
    ]);
    await archiveBatch(db, prefix, [
      { cid: repeatedCid, ad_id: 'ad_1', campaign_id: campaignId, pub_id: 'pub_1', ts: '2026-01-01T00:00:00Z', sig: 'x', receivedAt: Date.now() },
    ]);

    const results = await reconcileDate(db, prefix);
    const row = results.find((r) => r.campaignId === campaignId);

    expect(row).toEqual({ campaignId, billedClicks: 2, excludedInvalidClicks: 0 });
  }, 30_000);

  it('moves excluded cids from billedClicks into excludedInvalidClicks', async () => {
    const db = await createArchiveDb(env);
    const prefix = bucketPrefixForDate(TEST_DATE);
    const campaignId = `cmp_pa_test_${Date.now()}`;
    const goodCid = `clk_pa_${Date.now()}_good`;
    const badCid = `clk_pa_${Date.now()}_bad`;

    await archiveBatch(db, prefix, [
      { cid: goodCid, ad_id: 'ad_2', campaign_id: campaignId, pub_id: 'pub_1', ts: '2026-01-01T00:00:00Z', sig: 'x', receivedAt: Date.now() },
      { cid: badCid, ad_id: 'ad_2', campaign_id: campaignId, pub_id: 'pub_1', ts: '2026-01-01T00:00:00Z', sig: 'x', receivedAt: Date.now() },
    ]);

    const results = await reconcileDate(db, prefix, new Set([badCid]));
    const row = results.find((r) => r.campaignId === campaignId);

    expect(row).toEqual({ campaignId, billedClicks: 1, excludedInvalidClicks: 1 });
  }, 30_000);
});
```

- [ ] **Step 2: Create package config**

`packages/parquet-archive/package.json`:
```json
{
  "name": "@app/parquet-archive",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "test": "vitest run",
    "typecheck": "tsc -p tsconfig.json --noEmit"
  },
  "dependencies": {
    "duckdb-async": "^1.1.3"
  }
}
```

`packages/parquet-archive/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/parquet-archive test`
Expected: FAIL — `./index.js` has no exported member `createArchiveDb`.
(Requires Task 3's `ad-clicks-raw` S3 bucket to already exist — run `pnpm --filter @app/infra-localstack bootstrap` first if needed.)

- [ ] **Step 4: Write the implementation**

`packages/parquet-archive/src/index.ts`:
```ts
import { Database } from 'duckdb-async';

export interface RawArchiveEvent {
  cid: string;
  ad_id: string;
  campaign_id: string;
  pub_id: string;
  ts: string;
  sig: string;
  receivedAt: number;
}

interface ArchiveDbEnv {
  AWS_ENDPOINT_URL?: string;
}

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

export function bucketPrefixForDate(date: string): string {
  if (!DATE_PATTERN.test(date)) throw new Error(`invalid date: ${date}`);
  return `s3://ad-clicks-raw/dt=${date}/`;
}

export async function createArchiveDb(env: ArchiveDbEnv): Promise<Database> {
  const db = await Database.create(':memory:');
  const endpointHost = new URL(env.AWS_ENDPOINT_URL!).host;
  await db.exec(`
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='${endpointHost}';
    SET s3_url_style='path';
    SET s3_use_ssl=false;
    SET s3_access_key_id='test';
    SET s3_secret_access_key='test';
  `);
  return db;
}

export async function archiveBatch(db: Database, bucketPrefix: string, events: RawArchiveEvent[]): Promise<void> {
  if (events.length === 0) return;

  await db.run(
    'CREATE OR REPLACE TEMP TABLE batch(cid VARCHAR, ad_id VARCHAR, campaign_id VARCHAR, pub_id VARCHAR, ts VARCHAR, sig VARCHAR, receivedAt BIGINT)'
  );
  const placeholders = events.map(() => '(?, ?, ?, ?, ?, ?, ?)').join(', ');
  const params = events.flatMap((e) => [e.cid, e.ad_id, e.campaign_id, e.pub_id, e.ts, e.sig, e.receivedAt]);
  await db.run(`INSERT INTO batch VALUES ${placeholders}`, ...params);

  const key = `part-${Date.now()}-${Math.random().toString(36).slice(2)}.parquet`;
  await db.run(`COPY batch TO '${bucketPrefix}${key}' (FORMAT PARQUET)`);
}

export async function reconcileDate(
  db: Database,
  bucketPrefix: string,
  excludedCids: ReadonlySet<string> = new Set()
): Promise<{ campaignId: string; billedClicks: number; excludedInvalidClicks: number }[]> {
  const excludedList = [...excludedCids];
  const isExcludedExpr = excludedList.length > 0
    ? `cid IN (${excludedList.map(() => '?').join(', ')})`
    : 'false';

  const rows = await db.all(
    `SELECT
       campaign_id AS campaignId,
       COUNT(DISTINCT CASE WHEN NOT (${isExcludedExpr}) THEN cid END) AS billedClicks,
       COUNT(DISTINCT CASE WHEN (${isExcludedExpr}) THEN cid END) AS excludedInvalidClicks
     FROM read_parquet('${bucketPrefix}*.parquet')
     GROUP BY campaign_id`,
    ...excludedList,
    ...excludedList
  );

  return rows.map((row: Record<string, unknown>) => ({
    campaignId: String(row.campaignId),
    billedClicks: Number(row.billedClicks),
    excludedInvalidClicks: Number(row.excludedInvalidClicks),
  }));
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/parquet-archive test`
Expected: PASS — 2 tests.

- [ ] **Step 6: Commit**

```bash
git add packages/parquet-archive
git commit -m "feat: add DuckDB-backed Parquet archive writer and reconciliation query"
```

---

### Task 5: `services/archiver`

**Files:**
- Create: `services/archiver/package.json`
- Create: `services/archiver/tsconfig.json`
- Create: `services/archiver/src/core.ts`
- Test: `services/archiver/src/core.test.ts`
- Create: `services/archiver/src/run.ts`

**Interfaces:**
- Consumes: `runPollingConsumer` (Task 1), `createArchiveDb`/`archiveBatch`/`bucketPrefixForDate` (Task 4).
- Produces: `createBatchBuffer<T>(options): BatchBuffer<T>` where `BatchBuffer<T> = { add(item): void; shouldFlush(nowMs): boolean; drain(): T[] }`.

- [ ] **Step 1: Write the failing test**

`services/archiver/src/core.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { createBatchBuffer } from './core.js';

describe('createBatchBuffer', () => {
  it('does not flush below both thresholds', () => {
    const buffer = createBatchBuffer<number>({ maxSize: 3, maxAgeMs: 1000 });
    buffer.add(1);

    expect(buffer.shouldFlush(500)).toBe(false);
  });

  it('flushes once maxSize items have been added', () => {
    const buffer = createBatchBuffer<number>({ maxSize: 3, maxAgeMs: 1000 });
    buffer.add(1);
    buffer.add(2);
    buffer.add(3);

    expect(buffer.shouldFlush(0)).toBe(true);
  });

  it('flushes once maxAgeMs has passed since the first item after the last drain', () => {
    const buffer = createBatchBuffer<number>({ maxSize: 100, maxAgeMs: 1000 });
    buffer.add(1);

    expect(buffer.shouldFlush(999)).toBe(false);
    expect(buffer.shouldFlush(1001)).toBe(true);
  });

  it('drain empties the buffer and resets its age', () => {
    const buffer = createBatchBuffer<number>({ maxSize: 3, maxAgeMs: 1000 });
    buffer.add(1);
    buffer.add(2);

    expect(buffer.drain()).toEqual([1, 2]);
    expect(buffer.shouldFlush(1001)).toBe(false);

    buffer.add(3);
    expect(buffer.shouldFlush(1001)).toBe(false);
  });
});
```

- [ ] **Step 2: Create package config**

`services/archiver/package.json`:
```json
{
  "name": "@app/archiver",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "start": "tsx src/run.ts",
    "build": "tsc -p tsconfig.json",
    "test": "vitest run",
    "typecheck": "tsc -p tsconfig.json --noEmit"
  },
  "dependencies": {
    "@aws-sdk/client-kinesis": "^3.716.0",
    "@app/kinesis-consumer-loop": "workspace:*",
    "@app/parquet-archive": "workspace:*",
    "@app/config": "workspace:*"
  },
  "devDependencies": {
    "tsx": "^4.19.2"
  }
}
```

`services/archiver/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/archiver test`
Expected: FAIL — `./core.js` has no exported member `createBatchBuffer`.

- [ ] **Step 4: Write the implementation**

`services/archiver/src/core.ts`:
```ts
export interface BatchBuffer<T> {
  add(item: T): void;
  shouldFlush(nowMs: number): boolean;
  drain(): T[];
}

export function createBatchBuffer<T>(options: { maxSize: number; maxAgeMs: number }): BatchBuffer<T> {
  let items: T[] = [];
  let firstItemAt: number | null = null;

  return {
    add(item) {
      if (items.length === 0) firstItemAt = Date.now();
      items.push(item);
    },
    shouldFlush(nowMs) {
      if (items.length >= options.maxSize) return true;
      if (firstItemAt !== null && nowMs - firstItemAt >= options.maxAgeMs) return true;
      return false;
    },
    drain() {
      const drained = items;
      items = [];
      firstItemAt = null;
      return drained;
    },
  };
}
```

Note: `add()` stamps `firstItemAt` from the real clock (`Date.now()`), but `shouldFlush`'s age check is driven entirely by the caller-supplied `nowMs` relative to that stamp — the tests above call `add()` then immediately assert on `shouldFlush(explicitMs)`, so `firstItemAt` is effectively `~0` at test time and the explicit `nowMs` values (`999`, `1001`) exercise the boundary directly without needing fake timers.

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/archiver test`
Expected: PASS — 4 tests.

- [ ] **Step 6: Write the composition root (not unit tested — pure wiring + I/O loop)**

`services/archiver/src/run.ts`:
```ts
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

  setInterval(() => {
    if (!buffer.shouldFlush(Date.now())) return;
    const batch = buffer.drain();
    archiveBatch(db, bucketPrefixForDate(todayDateString()), batch).catch((err) =>
      console.error('archive flush failed, batch lost', { size: batch.length, err })
    );
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
```

- [ ] **Step 7: Commit**

```bash
git add services/archiver
git commit -m "feat: add raw click archiver (Kinesis to S3 Parquet)"
```

---

### Task 6: `packages/statements-store`

**Files:**
- Create: `packages/statements-store/package.json`
- Create: `packages/statements-store/tsconfig.json`
- Create: `packages/statements-store/src/index.ts`
- Test: `packages/statements-store/src/index.test.ts`

**Interfaces:**
- Consumes: a `DynamoDBClient` against the `click-statements` table (Task 3).
- Produces: `putStatement(dynamo, statement): Promise<void>`, `getStatement(dynamo, campaignId, period?): Promise<Statement | null>` — consumed by Task 8.

- [ ] **Step 1: Write the failing test**

`packages/statements-store/src/index.test.ts`:
```ts
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
```

- [ ] **Step 2: Create package config**

`packages/statements-store/package.json`:
```json
{
  "name": "@app/statements-store",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "test": "vitest run",
    "typecheck": "tsc -p tsconfig.json --noEmit"
  },
  "dependencies": {
    "@aws-sdk/client-dynamodb": "^3.716.0"
  }
}
```

`packages/statements-store/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/statements-store test`
Expected: FAIL — `./index.js` has no exported member `putStatement`.

- [ ] **Step 4: Write the implementation**

`packages/statements-store/src/index.ts`:
```ts
import { DynamoDBClient, PutItemCommand, QueryCommand, GetItemCommand } from '@aws-sdk/client-dynamodb';

const STATEMENTS_TABLE_NAME = 'click-statements';

export interface Statement {
  campaignId: string;
  period: string;
  billedClicks: number;
  excludedInvalidClicks: number;
  reconciledAt: string;
  sourceArchive: string;
}

export async function putStatement(dynamo: DynamoDBClient, statement: Statement): Promise<void> {
  await dynamo.send(new PutItemCommand({
    TableName: STATEMENTS_TABLE_NAME,
    Item: {
      campaignId: { S: statement.campaignId },
      period: { S: statement.period },
      billedClicks: { N: String(statement.billedClicks) },
      excludedInvalidClicks: { N: String(statement.excludedInvalidClicks) },
      reconciledAt: { S: statement.reconciledAt },
      sourceArchive: { S: statement.sourceArchive },
    },
  }));
}

function toStatement(item: Record<string, { S?: string; N?: string }>): Statement {
  return {
    campaignId: item.campaignId.S!,
    period: item.period.S!,
    billedClicks: Number(item.billedClicks.N),
    excludedInvalidClicks: Number(item.excludedInvalidClicks.N),
    reconciledAt: item.reconciledAt.S!,
    sourceArchive: item.sourceArchive.S!,
  };
}

export async function getStatement(dynamo: DynamoDBClient, campaignId: string, period?: string): Promise<Statement | null> {
  if (period) {
    const { Item } = await dynamo.send(new GetItemCommand({
      TableName: STATEMENTS_TABLE_NAME,
      Key: { campaignId: { S: campaignId }, period: { S: period } },
    }));
    return Item ? toStatement(Item as Record<string, { S?: string; N?: string }>) : null;
  }

  const { Items } = await dynamo.send(new QueryCommand({
    TableName: STATEMENTS_TABLE_NAME,
    KeyConditionExpression: 'campaignId = :campaignId',
    ExpressionAttributeValues: { ':campaignId': { S: campaignId } },
    ScanIndexForward: false,
    Limit: 1,
  }));
  const item = Items?.[0];
  return item ? toStatement(item as Record<string, { S?: string; N?: string }>) : null;
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/statements-store test`
Expected: PASS — 3 tests.

- [ ] **Step 6: Commit**

```bash
git add packages/statements-store
git commit -m "feat: add overwrite-semantics statements store"
```

---

### Task 7: `@app/db` — `getCampaignOwnerAdvertiserId`

**Files:**
- Modify: `packages/db/src/ownership.ts`
- Modify: `packages/db/src/ownership.test.ts`
- Modify: `packages/db/src/index.ts`

**Interfaces:**
- Consumes: `prisma` client shape.
- Produces: `getCampaignOwnerAdvertiserId(client, campaignId): Promise<string | null>` — consumed by Task 8.

- [ ] **Step 1: Write the failing test**

Add to `packages/db/src/ownership.test.ts` (new import, new `describe` block, and extend the top-level `afterAll`'s cleanup filters to also cover a new test campaign name):
```ts
import { getCampaignOwnerAdvertiserId } from './ownership.js';
```
```ts
describe('getCampaignOwnerAdvertiserId', () => {
  it('returns the owning advertiser id regardless of campaign status', async () => {
    const advertiser = await prisma.advertiser.create({
      data: { name: 'ownership-test-advertiser-3', signingSecret: 'shh' },
    });
    const campaign = await prisma.campaign.create({
      data: { name: 'ownership-test-campaign-3', advertiserId: advertiser.id, status: 'ENDED' },
    });

    expect(await getCampaignOwnerAdvertiserId(prisma, campaign.id)).toBe(advertiser.id);
  });

  it('returns null for an unknown campaign id', async () => {
    expect(await getCampaignOwnerAdvertiserId(prisma, 'cmp_does_not_exist')).toBeNull();
  });
});
```

Update the file's top-level `afterAll` to also delete the new test rows — replace it with:
```ts
afterAll(async () => {
  const testAdvertiserNames = ['ownership-test-advertiser', 'ownership-test-advertiser-2', 'ownership-test-advertiser-3'];
  await prisma.apiKey.deleteMany({ where: { advertiser: { name: { in: testAdvertiserNames } } } });
  await prisma.campaign.deleteMany({ where: { name: { in: ['ownership-test-campaign', 'ownership-test-campaign-3'] } } });
  await prisma.ad.deleteMany({ where: { name: 'ownership-test-ad' } });
  await prisma.advertiser.deleteMany({ where: { name: { in: testAdvertiserNames } } });
  await prisma.$disconnect();
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db test`
Expected: FAIL — `./ownership.js` has no exported member `getCampaignOwnerAdvertiserId`.

- [ ] **Step 3: Write the implementation**

Add to `packages/db/src/ownership.ts`, below `getAdOwnerAdvertiserId`:
```ts
export async function getCampaignOwnerAdvertiserId(client: PrismaClient, campaignId: string): Promise<string | null> {
  const campaign = await client.campaign.findUnique({
    where: { id: campaignId },
    select: { advertiserId: true },
  });
  return campaign?.advertiserId ?? null;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db test`
Expected: PASS — 11 tests (9 from sub-project 4, 2 new).

- [ ] **Step 5: Export from the package barrel**

Modify `packages/db/src/index.ts` — change the ownership export line to:
```ts
export { getAdOwnerAdvertiserId, resolveApiKey, getCampaignOwnerAdvertiserId } from './ownership.js';
```

- [ ] **Step 6: Commit**

```bash
git add packages/db
git commit -m "feat: add campaign-ownership query for billing tenant scoping"
```

---

### Task 8: `services/billing-api`

**Files:**
- Create: `services/billing-api/package.json`
- Create: `services/billing-api/tsconfig.json`
- Create: `services/billing-api/src/app.ts`
- Test: `services/billing-api/src/app.test.ts`
- Create: `services/billing-api/src/server.ts`

**Interfaces:**
- Consumes: `resolveApiKey`/`getCampaignOwnerAdvertiserId` (`@app/db`), `getStatement`/`putStatement` (`@app/statements-store`), `reconcileDate`/`bucketPrefixForDate`/`createArchiveDb` (`@app/parquet-archive`).
- Produces: `buildApp(deps: BillingApiDeps): FastifyInstance` serving both routes.

- [ ] **Step 1: Write the failing test**

`services/billing-api/src/app.test.ts`:
```ts
import { describe, expect, it, vi } from 'vitest';
import { buildApp } from './app.js';

const API_KEY = 'raw-test-key';
const ADVERTISER_ID = 'adv_1';
const OWNED_CAMPAIGN_ID = 'cmp_owned';
const OTHER_CAMPAIGN_ID = 'cmp_other_advertiser';
const OPS_TOKEN = 'test-ops-token';

const SAMPLE_STATEMENT = {
  campaignId: OWNED_CAMPAIGN_ID, period: '2026-07-11', billedClicks: 118402, excludedInvalidClicks: 613,
  reconciledAt: '2026-07-12T02:11:00Z', sourceArchive: 's3://ad-clicks-raw/dt=2026-07-11/',
};

function buildTestApp(overrides: Partial<{
  resolveApiKey: ReturnType<typeof vi.fn>;
  getCampaignOwner: ReturnType<typeof vi.fn>;
  getStatement: ReturnType<typeof vi.fn>;
  reconcileAndStore: ReturnType<typeof vi.fn>;
}> = {}) {
  const deps = {
    opsToken: OPS_TOKEN,
    resolveApiKey: vi.fn(async (key: string) => (key === API_KEY ? { advertiserId: ADVERTISER_ID } : null)),
    getCampaignOwner: vi.fn(async (campaignId: string) => {
      if (campaignId === OWNED_CAMPAIGN_ID) return ADVERTISER_ID;
      if (campaignId === OTHER_CAMPAIGN_ID) return 'adv_someone_else';
      return null;
    }),
    getStatement: vi.fn(async () => SAMPLE_STATEMENT),
    reconcileAndStore: vi.fn(async (date: string) => 3),
    ...overrides,
  };
  return buildApp(deps);
}

function get(app: ReturnType<typeof buildApp>, campaignId: string, authorization?: string) {
  return app.inject({
    method: 'GET',
    url: `/v1/campaigns/${campaignId}/statement`,
    headers: authorization ? { authorization } : {},
  });
}

function rerun(app: ReturnType<typeof buildApp>, date: string, opsToken?: string) {
  return app.inject({
    method: 'POST',
    url: `/v1/reconciliation/${date}/rerun`,
    headers: opsToken ? { 'x-ops-token': opsToken } : {},
  });
}

describe('GET /v1/campaigns/:campaignId/statement', () => {
  it('returns the statement for a campaign the caller owns', async () => {
    const app = buildTestApp();

    const response = await get(app, OWNED_CAMPAIGN_ID, `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ ...SAMPLE_STATEMENT, exact: true });
  });

  it('returns 404 when no statement has been reconciled yet', async () => {
    const app = buildTestApp({ getStatement: vi.fn(async () => null) });

    const response = await get(app, OWNED_CAMPAIGN_ID, `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'not_found' });
  });

  it('returns 404 for a campaign owned by a different advertiser', async () => {
    const app = buildTestApp();

    const response = await get(app, OTHER_CAMPAIGN_ID, `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'not_found' });
  });

  it('returns the identical 404 body for a nonexistent campaign', async () => {
    const app = buildTestApp();

    const response = await get(app, 'cmp_does_not_exist', `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'not_found' });
  });

  it('returns 401 for a missing Authorization header', async () => {
    const app = buildTestApp();

    const response = await get(app, OWNED_CAMPAIGN_ID);

    expect(response.statusCode).toBe(401);
  });

  it('returns 401 for an unknown API key', async () => {
    const app = buildTestApp();

    const response = await get(app, OWNED_CAMPAIGN_ID, 'Bearer not-a-real-key');

    expect(response.statusCode).toBe(401);
  });
});

describe('POST /v1/reconciliation/:date/rerun', () => {
  it('reconciles and returns the campaign count with a valid ops token and date', async () => {
    const app = buildTestApp();

    const response = await rerun(app, '2026-07-11', OPS_TOKEN);

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ date: '2026-07-11', campaignsReconciled: 3 });
  });

  it('returns 401 for a missing or wrong ops token', async () => {
    const app = buildTestApp();

    expect((await rerun(app, '2026-07-11')).statusCode).toBe(401);
    expect((await rerun(app, '2026-07-11', 'wrong-token')).statusCode).toBe(401);
  });

  it('returns 400 for a malformed date', async () => {
    const app = buildTestApp();

    const response = await rerun(app, 'not-a-date', OPS_TOKEN);

    expect(response.statusCode).toBe(400);
  });
});
```

- [ ] **Step 2: Create package config**

`services/billing-api/package.json`:
```json
{
  "name": "@app/billing-api",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "start": "tsx src/server.ts",
    "build": "tsc -p tsconfig.json",
    "test": "vitest run",
    "typecheck": "tsc -p tsconfig.json --noEmit"
  },
  "dependencies": {
    "fastify": "^5.2.0",
    "@app/db": "workspace:*",
    "@app/statements-store": "workspace:*",
    "@app/parquet-archive": "workspace:*",
    "@app/config": "workspace:*"
  },
  "devDependencies": {
    "tsx": "^4.19.2"
  }
}
```

`services/billing-api/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/billing-api test`
Expected: FAIL — `./app.js` has no exported member `buildApp`.

- [ ] **Step 4: Write the implementation**

`services/billing-api/src/app.ts`:
```ts
import Fastify, { type FastifyInstance } from 'fastify';

export interface BillingApiDeps {
  opsToken: string;
  resolveApiKey(rawKey: string): Promise<{ advertiserId: string } | null>;
  getCampaignOwner(campaignId: string): Promise<string | null>;
  getStatement(campaignId: string, period?: string): Promise<{
    campaignId: string; period: string; billedClicks: number; excludedInvalidClicks: number;
    reconciledAt: string; sourceArchive: string;
  } | null>;
  reconcileAndStore(date: string): Promise<number>;
}

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

export function buildApp(deps: BillingApiDeps): FastifyInstance {
  const app = Fastify({ logger: true });

  app.get<{ Params: { campaignId: string }; Querystring: { period?: string } }>(
    '/v1/campaigns/:campaignId/statement',
    async (req, reply) => {
      const authHeader = req.headers.authorization;
      const rawKey = authHeader?.startsWith('Bearer ') ? authHeader.slice('Bearer '.length) : undefined;
      if (!rawKey) return reply.code(401).send({ error: 'unauthorized' });

      const apiKey = await deps.resolveApiKey(rawKey);
      if (!apiKey) return reply.code(401).send({ error: 'unauthorized' });

      const { campaignId } = req.params;
      const ownerAdvertiserId = await deps.getCampaignOwner(campaignId);
      if (!ownerAdvertiserId || ownerAdvertiserId !== apiKey.advertiserId) {
        return reply.code(404).send({ error: 'not_found' });
      }

      const statement = await deps.getStatement(campaignId, req.query.period);
      if (!statement) return reply.code(404).send({ error: 'not_found' });

      reply.send({ ...statement, exact: true });
    }
  );

  app.post<{ Params: { date: string } }>('/v1/reconciliation/:date/rerun', async (req, reply) => {
    const opsToken = req.headers['x-ops-token'];
    if (opsToken !== deps.opsToken) return reply.code(401).send({ error: 'unauthorized' });

    const { date } = req.params;
    if (!DATE_PATTERN.test(date)) return reply.code(400).send({ error: 'invalid_date' });

    const campaignsReconciled = await deps.reconcileAndStore(date);
    reply.send({ date, campaignsReconciled });
  });

  return app;
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/billing-api test`
Expected: PASS — 9 tests.

- [ ] **Step 6: Write the composition root (not unit tested — pure wiring)**

`services/billing-api/src/server.ts`:
```ts
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { prisma, getCampaignOwnerAdvertiserId, resolveApiKey } from '@app/db';
import { getStatement, putStatement } from '@app/statements-store';
import { createArchiveDb, reconcileDate, bucketPrefixForDate } from '@app/parquet-archive';
import { loadEnv } from '@app/config';
import { buildApp } from './app.js';

const env = loadEnv();
const opsToken = process.env.OPS_TOKEN;
if (!opsToken) throw new Error('OPS_TOKEN is required');

const dynamo = new DynamoDBClient({
  region: env.AWS_REGION,
  endpoint: env.AWS_ENDPOINT_URL,
  // ponytail: LocalStack dummy creds; swap for the default AWS credential provider chain when targeting real AWS
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});
const archiveDb = await createArchiveDb(env);

const app = buildApp({
  opsToken,
  resolveApiKey: (rawKey) => resolveApiKey(prisma, rawKey),
  getCampaignOwner: (campaignId) => getCampaignOwnerAdvertiserId(prisma, campaignId),
  getStatement: (campaignId, period) => getStatement(dynamo, campaignId, period),
  reconcileAndStore: async (date) => {
    const prefix = bucketPrefixForDate(date);
    const results = await reconcileDate(archiveDb, prefix);
    const reconciledAt = new Date().toISOString();
    for (const result of results) {
      await putStatement(dynamo, { ...result, period: date, reconciledAt, sourceArchive: prefix });
    }
    return results.length;
  },
});

await app.listen({ port: Number(process.env.PORT ?? 3003), host: '0.0.0.0' });
```

- [ ] **Step 7: Commit**

```bash
git add services/billing-api
git commit -m "feat: add billing API (statement read, reconciliation rerun)"
```

---

## Self-Review

**Spec coverage:**
- Shared, abortable Kinesis loop, retrofitted onto sub-project 3 → Tasks 1–2.
- Raw archive bucket + statements table bootstrap → Task 3.
- DuckDB Parquet write/read against S3, `COUNT(DISTINCT cid)` doing the exactness work, bulk `excludedCids` seam → Task 4.
- Archiver: unfiltered writes (spec §"Why the raw archive isn't deduped" — `run.ts` never imports `@app/click-dedup`), pure `BatchBuffer` core → Task 5.
- Overwrite-semantics statements store, contrasted explicitly with `hot-aggregate-store` → Task 6.
- Campaign ownership for tenant scoping → Task 7.
- Both billing endpoints, two different auth models, uniform 404s → Task 8.
- UTC-midnight partition edge case → explicitly not solved, documented as a `ponytail:` comment in Task 5's `run.ts`, not silently absent.

**Placeholder scan:** No TBD/TODO; every step has complete, runnable code.

**Type consistency:** `RawArchiveEvent` (Task 4, re-exported and used in Task 5) matches the fields the archiver actually parses off the wire (`cid, ad_id, campaign_id, pub_id, ts, sig, receivedAt`) — the same shape `@app/kinesis-publisher` (sub-project 2) writes, since the archiver reads the identical `ad-clicks-raw` payload the aggregator does, just without the dedup step. `Statement` (Task 6) matches the doc's billing response fields exactly and is what Task 8's `getStatement`/`putStatement` deps pass through unmodified except for adding `exact: true` at the HTTP layer. `BillingApiDeps` (Task 8) matches the return types Tasks 6 and 7 actually produce.

---

**Plan complete and saved to `docs/superpowers/plans/2026-07-13-archive-reconciliation-billing.md`.**

Continuing the planning series. Next: sub-project 6 (fraud scoring) — the last one.

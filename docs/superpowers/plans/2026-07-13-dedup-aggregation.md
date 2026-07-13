# Dedup + Real-Time Aggregation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The Kinesis consumer that dedupes, windows, and pre-aggregates click events into the `click-aggregates` DynamoDB hot store, polling `ad-clicks-raw` from sub-project 2.

**Architecture:** A pure, clock-injected windowing core with no I/O; two small infra-backed stores (Redis+DynamoDB dedup, DynamoDB hot-aggregate writer); a consumer split into a fully unit-tested `core.ts` (record handling, flush orchestration) and a thin, untested `poll.ts` (the actual `GetRecords` loop and `setInterval`).

**Tech Stack:** Node.js 24, TypeScript 5.x (strict, ESM), `@aws-sdk/client-kinesis`, `@aws-sdk/client-dynamodb`, `redis` (node-redis v4), Vitest 3.

## Global Constraints

- Node.js 24, TypeScript strict mode, ESM — same as sub-projects 1–2.
- Polling `GetShardIterator`/`GetRecords`, not `SubscribeToShard` — a deliberate deviation from the doc's sample for LocalStack compatibility (spec §"One deliberate deviation").
- `click-dedup` checks Redis first; DynamoDB is only consulted when Redis says "new," never as a parallel/redundant check (spec §"click-dedup").
- `hot-aggregate-store.flush` always uses `UpdateItem` with `ADD`, never `PutItem` — late stragglers must add to an existing count, not overwrite it.
- `WindowedAggregator.peekClosedWindows` never mutates state; only `removeWindow` does, and only after a successful flush (spec's error-handling fix — this is retry-safety, not a style preference).
- No adaptive hot-key sub-sharding, no `worker_threads`, no ECS auto-scaling config, no dynamic shard-lease rebalancing — all explicitly deferred (spec §"Explicitly out of scope").

---

## File Structure

```
packages/
├── windowed-aggregator/
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       ├── index.ts        # createWindowedAggregator — pure, no I/O
│       └── index.test.ts
├── click-dedup/
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       ├── index.ts        # createDedupStore
│       └── index.test.ts
└── hot-aggregate-store/
    ├── package.json
    ├── tsconfig.json
    └── src/
        ├── index.ts        # createHotAggregateStore
        └── index.test.ts

infra/localstack/src/
├── tables.ts                # NEW: ensureDedupTable, ensureHotAggregateTable
├── tables.test.ts
└── bootstrap.ts              # MODIFIED: main() also bootstraps the two tables

services/aggregator/
├── package.json
├── tsconfig.json
└── src/
    ├── core.ts               # handleRecord, flushClosedWindows — unit tested
    ├── core.test.ts
    └── poll.ts                # GetRecords loop + setInterval — thin, not unit tested
```

---

### Task 1: `packages/windowed-aggregator`

**Files:**
- Create: `packages/windowed-aggregator/package.json`
- Create: `packages/windowed-aggregator/tsconfig.json`
- Create: `packages/windowed-aggregator/src/index.ts`
- Test: `packages/windowed-aggregator/src/index.test.ts`

**Interfaces:**
- Consumes: nothing (leaf package, no I/O — all time values are passed in as explicit parameters, not read from the clock).
- Produces: `createWindowedAggregator(options: { windowMs: number; watermarkMs: number }): WindowedAggregator` where `WindowedAggregator = { record(adId, eventTimeMs): void; peekClosedWindows(nowMs): ClosedWindow[]; removeWindow(windowStart): void }` and `ClosedWindow = { windowStart: number; counts: Map<string, number> }` — consumed by Task 5.

- [ ] **Step 1: Write the failing test**

`packages/windowed-aggregator/src/index.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { createWindowedAggregator } from './index.js';

const WINDOW_MS = 60_000;
const WATERMARK_MS = 120_000;

describe('windowed-aggregator', () => {
  it('accumulates records within the same window across ads, not yet closed', () => {
    const agg = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    agg.record('ad_1', 1_000);
    agg.record('ad_1', 30_000);
    agg.record('ad_2', 15_000);

    expect(agg.peekClosedWindows(40_000)).toEqual([]);
  });

  it('returns a closed window once now has passed windowStart + windowMs + watermarkMs', () => {
    const agg = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    agg.record('ad_1', 1_000);
    agg.record('ad_1', 30_000);
    agg.record('ad_2', 15_000);

    const closed = agg.peekClosedWindows(WINDOW_MS + WATERMARK_MS + 1);

    expect(closed).toHaveLength(1);
    expect(closed[0].windowStart).toBe(0);
    expect(closed[0].counts.get('ad_1')).toBe(2);
    expect(closed[0].counts.get('ad_2')).toBe(1);
  });

  it('peek does not remove — a second peek still sees the window', () => {
    const agg = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    agg.record('ad_1', 1_000);
    const now = WINDOW_MS + WATERMARK_MS + 1;

    agg.peekClosedWindows(now);

    expect(agg.peekClosedWindows(now)).toHaveLength(1);
  });

  it('removeWindow removes it so a later peek no longer sees it', () => {
    const agg = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    agg.record('ad_1', 1_000);
    const now = WINDOW_MS + WATERMARK_MS + 1;

    agg.peekClosedWindows(now);
    agg.removeWindow(0);

    expect(agg.peekClosedWindows(now)).toEqual([]);
  });

  it('a late record for an already-removed window opens a fresh single-entry window', () => {
    const agg = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    agg.record('ad_1', 1_000);
    const now = WINDOW_MS + WATERMARK_MS + 1;
    agg.peekClosedWindows(now);
    agg.removeWindow(0);

    agg.record('ad_1', 500);

    const closed = agg.peekClosedWindows(now + 1);
    expect(closed).toHaveLength(1);
    expect(closed[0].counts.get('ad_1')).toBe(1);
  });
});
```

- [ ] **Step 2: Create package config**

`packages/windowed-aggregator/package.json`:
```json
{
  "name": "@app/windowed-aggregator",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "test": "vitest run",
    "typecheck": "tsc -p tsconfig.json --noEmit"
  }
}
```

`packages/windowed-aggregator/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/windowed-aggregator test`
Expected: FAIL — `./index.js` has no exported member `createWindowedAggregator`.

- [ ] **Step 4: Write the implementation**

`packages/windowed-aggregator/src/index.ts`:
```ts
export interface ClosedWindow {
  windowStart: number;
  counts: Map<string, number>;
}

export interface WindowedAggregator {
  record(adId: string, eventTimeMs: number): void;
  peekClosedWindows(nowMs: number): ClosedWindow[];
  removeWindow(windowStart: number): void;
}

export function createWindowedAggregator(options: { windowMs: number; watermarkMs: number }): WindowedAggregator {
  const { windowMs, watermarkMs } = options;
  const windows = new Map<number, Map<string, number>>();

  function bucketFor(eventTimeMs: number): number {
    return Math.floor(eventTimeMs / windowMs) * windowMs;
  }

  return {
    record(adId, eventTimeMs) {
      const windowStart = bucketFor(eventTimeMs);
      const counts = windows.get(windowStart) ?? windows.set(windowStart, new Map()).get(windowStart)!;
      counts.set(adId, (counts.get(adId) ?? 0) + 1);
    },
    peekClosedWindows(nowMs) {
      const closeBefore = nowMs - watermarkMs;
      const closed: ClosedWindow[] = [];
      for (const [windowStart, counts] of windows) {
        if (windowStart + windowMs > closeBefore) continue;
        closed.push({ windowStart, counts });
      }
      return closed;
    },
    removeWindow(windowStart) {
      windows.delete(windowStart);
    },
  };
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/windowed-aggregator test`
Expected: PASS — 5 tests.

- [ ] **Step 6: Commit**

```bash
git add packages/windowed-aggregator
git commit -m "feat: add pure windowed pre-aggregation with watermarks"
```

---

### Task 2: DynamoDB table bootstrap — `click-dedup` and `click-aggregates`

**Files:**
- Create: `infra/localstack/src/tables.ts`
- Test: `infra/localstack/src/tables.test.ts`
- Modify: `infra/localstack/src/bootstrap.ts` (its `main()`)
- Modify: `infra/localstack/package.json` (add `@aws-sdk/client-dynamodb`)

**Interfaces:**
- Consumes: a running LocalStack container (sub-project 1, Task 4).
- Produces: `ensureDedupTable(client: DynamoDBClient): Promise<void>`, `ensureHotAggregateTable(client: DynamoDBClient): Promise<void>`, and the constants `DEDUP_TABLE_NAME = 'click-dedup'`, `HOT_AGGREGATE_TABLE_NAME = 'click-aggregates'` — consumed by Tasks 3 and 4.

- [ ] **Step 1: Write the failing test**

`infra/localstack/src/tables.test.ts`:
```ts
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
```

- [ ] **Step 2: Add the DynamoDB SDK dependency**

Modify `infra/localstack/package.json` — add to `dependencies`:
```json
"@aws-sdk/client-dynamodb": "^3.716.0"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/infra-localstack test`
Expected: FAIL — `./tables.js` module not found.

- [ ] **Step 4: Write the implementation**

`infra/localstack/src/tables.ts`:
```ts
import {
  DynamoDBClient, CreateTableCommand, DescribeTableCommand, UpdateTimeToLiveCommand,
  ResourceInUseException, ResourceNotFoundException,
} from '@aws-sdk/client-dynamodb';

export const DEDUP_TABLE_NAME = 'click-dedup';
export const HOT_AGGREGATE_TABLE_NAME = 'click-aggregates';

async function tableExists(client: DynamoDBClient, tableName: string): Promise<boolean> {
  try {
    await client.send(new DescribeTableCommand({ TableName: tableName }));
    return true;
  } catch (err) {
    if (err instanceof ResourceNotFoundException) return false;
    throw err;
  }
}

export async function ensureDedupTable(client: DynamoDBClient): Promise<void> {
  if (await tableExists(client, DEDUP_TABLE_NAME)) return;

  try {
    await client.send(new CreateTableCommand({
      TableName: DEDUP_TABLE_NAME,
      AttributeDefinitions: [{ AttributeName: 'cid', AttributeType: 'S' }],
      KeySchema: [{ AttributeName: 'cid', KeyType: 'HASH' }],
      BillingMode: 'PAY_PER_REQUEST',
    }));
  } catch (err) {
    if (!(err instanceof ResourceInUseException)) throw err;
  }

  await client.send(new UpdateTimeToLiveCommand({
    TableName: DEDUP_TABLE_NAME,
    TimeToLiveSpecification: { AttributeName: 'expiresAt', Enabled: true },
  }));
}

export async function ensureHotAggregateTable(client: DynamoDBClient): Promise<void> {
  if (await tableExists(client, HOT_AGGREGATE_TABLE_NAME)) return;

  try {
    await client.send(new CreateTableCommand({
      TableName: HOT_AGGREGATE_TABLE_NAME,
      AttributeDefinitions: [
        { AttributeName: 'adId', AttributeType: 'S' },
        { AttributeName: 'windowStart', AttributeType: 'N' },
      ],
      KeySchema: [
        { AttributeName: 'adId', KeyType: 'HASH' },
        { AttributeName: 'windowStart', KeyType: 'RANGE' },
      ],
      BillingMode: 'PAY_PER_REQUEST',
    }));
  } catch (err) {
    if (!(err instanceof ResourceInUseException)) throw err;
  }

  await client.send(new UpdateTimeToLiveCommand({
    TableName: HOT_AGGREGATE_TABLE_NAME,
    TimeToLiveSpecification: { AttributeName: 'expiresAt', Enabled: true },
  }));
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/infra-localstack test`
Expected: PASS — 3 tests (1 from sub-project 1, 2 new).

- [ ] **Step 6: Wire both tables into the bootstrap entrypoint**

Modify `infra/localstack/src/bootstrap.ts` — replace the existing `main()` function with:
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

  console.log('LocalStack resources ready: Kinesis stream, dedup table, hot aggregate table.');
}
```

And add these two imports at the top of `infra/localstack/src/bootstrap.ts`:
```ts
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { ensureDedupTable, ensureHotAggregateTable } from './tables.js';
```

Run: `pnpm --filter @app/infra-localstack bootstrap`
Expected: prints `LocalStack resources ready: Kinesis stream, dedup table, hot aggregate table.`

- [ ] **Step 7: Commit**

```bash
git add infra/localstack
git commit -m "feat: bootstrap click-dedup and click-aggregates DynamoDB tables"
```

---

### Task 3: `packages/click-dedup`

**Files:**
- Create: `packages/click-dedup/package.json`
- Create: `packages/click-dedup/tsconfig.json`
- Create: `packages/click-dedup/src/index.ts`
- Test: `packages/click-dedup/src/index.test.ts`

**Interfaces:**
- Consumes: a connected `RedisClientType` and a `DynamoDBClient` against the `click-dedup` table from Task 2.
- Produces: `createDedupStore(redis, dynamo, options?): DedupStore` where `DedupStore = { isNew(cid: string): Promise<boolean> }` — consumed by Task 5.

- [ ] **Step 1: Write the failing test**

`packages/click-dedup/src/index.test.ts`:
```ts
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
```

- [ ] **Step 2: Create package config**

`packages/click-dedup/package.json`:
```json
{
  "name": "@app/click-dedup",
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
    "@aws-sdk/client-dynamodb": "^3.716.0",
    "redis": "^4.7.0"
  }
}
```

`packages/click-dedup/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/click-dedup test`
Expected: FAIL — `./index.js` has no exported member `createDedupStore`.
(Requires Task 2's `click-dedup` table to already exist — run `pnpm --filter @app/infra-localstack bootstrap` first if needed.)

- [ ] **Step 4: Write the implementation**

`packages/click-dedup/src/index.ts`:
```ts
import type { RedisClientType } from 'redis';
import { DynamoDBClient, PutItemCommand, ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';

export interface DedupStore {
  isNew(cid: string): Promise<boolean>;
}

const DEDUP_TABLE_NAME = 'click-dedup';

export function createDedupStore(
  redis: RedisClientType,
  dynamo: DynamoDBClient,
  options: { tableName?: string; windowSeconds?: number } = {}
): DedupStore {
  const tableName = options.tableName ?? DEDUP_TABLE_NAME;
  const windowSeconds = options.windowSeconds ?? 600;

  return {
    async isNew(cid) {
      const redisResult = await redis.set(`click:${cid}`, '1', { NX: true, EX: windowSeconds });
      if (redisResult === null) return false;

      try {
        await dynamo.send(new PutItemCommand({
          TableName: tableName,
          Item: {
            cid: { S: cid },
            expiresAt: { N: String(Math.floor(Date.now() / 1000) + windowSeconds) },
          },
          ConditionExpression: 'attribute_not_exists(cid)',
        }));
        return true;
      } catch (err) {
        if (err instanceof ConditionalCheckFailedException) return false;
        throw err;
      }
    },
  };
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/click-dedup test`
Expected: PASS — 2 tests.

- [ ] **Step 6: Commit**

```bash
git add packages/click-dedup
git commit -m "feat: add Redis fast-path + DynamoDB backstop dedup store"
```

---

### Task 4: `packages/hot-aggregate-store`

**Files:**
- Create: `packages/hot-aggregate-store/package.json`
- Create: `packages/hot-aggregate-store/tsconfig.json`
- Create: `packages/hot-aggregate-store/src/index.ts`
- Test: `packages/hot-aggregate-store/src/index.test.ts`

**Interfaces:**
- Consumes: a `DynamoDBClient` against the `click-aggregates` table from Task 2.
- Produces: `createHotAggregateStore(dynamo, tableName?): HotAggregateStore` where `HotAggregateStore = { flush(adId: string, windowStart: number, delta: number): Promise<void> }` — consumed by Task 5.

- [ ] **Step 1: Write the failing test**

`packages/hot-aggregate-store/src/index.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { DynamoDBClient, GetItemCommand } from '@aws-sdk/client-dynamodb';
import { createHotAggregateStore } from './index.js';

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
```

- [ ] **Step 2: Create package config**

`packages/hot-aggregate-store/package.json`:
```json
{
  "name": "@app/hot-aggregate-store",
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

`packages/hot-aggregate-store/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/hot-aggregate-store test`
Expected: FAIL — `./index.js` has no exported member `createHotAggregateStore`.

- [ ] **Step 4: Write the implementation**

`packages/hot-aggregate-store/src/index.ts`:
```ts
import { DynamoDBClient, UpdateItemCommand } from '@aws-sdk/client-dynamodb';

const HOT_AGGREGATE_TABLE_NAME = 'click-aggregates';
const TTL_SECONDS = 48 * 3600;

export interface HotAggregateStore {
  flush(adId: string, windowStart: number, delta: number): Promise<void>;
}

export function createHotAggregateStore(
  dynamo: DynamoDBClient,
  tableName: string = HOT_AGGREGATE_TABLE_NAME
): HotAggregateStore {
  return {
    async flush(adId, windowStart, delta) {
      await dynamo.send(new UpdateItemCommand({
        TableName: tableName,
        Key: {
          adId: { S: adId },
          windowStart: { N: String(windowStart) },
        },
        UpdateExpression: 'ADD #c :delta SET expiresAt = :expiresAt',
        ExpressionAttributeNames: { '#c': 'count' },
        ExpressionAttributeValues: {
          ':delta': { N: String(delta) },
          ':expiresAt': { N: String(Math.floor(Date.now() / 1000) + TTL_SECONDS) },
        },
      }));
    },
  };
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/hot-aggregate-store test`
Expected: PASS — 1 test.

- [ ] **Step 6: Commit**

```bash
git add packages/hot-aggregate-store
git commit -m "feat: add atomic-increment hot aggregate store"
```

---

### Task 5: `services/aggregator`

**Files:**
- Create: `services/aggregator/package.json`
- Create: `services/aggregator/tsconfig.json`
- Create: `services/aggregator/src/core.ts`
- Test: `services/aggregator/src/core.test.ts`
- Create: `services/aggregator/src/poll.ts`

**Interfaces:**
- Consumes: `DedupStore.isNew` (`@app/click-dedup`), `WindowedAggregator.record`/`peekClosedWindows`/`removeWindow` (`@app/windowed-aggregator`), `HotAggregateStore.flush` (`@app/hot-aggregate-store`).
- Produces: `handleRecord(deps, event): Promise<void>` and `flushClosedWindows(aggregator, hotStore, nowMs): Promise<void>` — the two entry points `poll.ts` wires into a real Kinesis loop.

- [ ] **Step 1: Write the failing test**

`services/aggregator/src/core.test.ts`:
```ts
import { describe, expect, it, vi } from 'vitest';
import { handleRecord, flushClosedWindows } from './core.js';

describe('handleRecord', () => {
  it('records the event when the dedup store says it is new', async () => {
    const record = vi.fn();
    const deps = { dedupStore: { isNew: vi.fn().mockResolvedValue(true) }, aggregator: { record } };

    await handleRecord(deps, { cid: 'clk_1', ad_id: 'ad_1', ts: '2026-07-12T09:14:32.118Z' });

    expect(record).toHaveBeenCalledWith('ad_1', new Date('2026-07-12T09:14:32.118Z').getTime());
  });

  it('does not record a duplicate', async () => {
    const record = vi.fn();
    const deps = { dedupStore: { isNew: vi.fn().mockResolvedValue(false) }, aggregator: { record } };

    await handleRecord(deps, { cid: 'clk_1', ad_id: 'ad_1', ts: '2026-07-12T09:14:32.118Z' });

    expect(record).not.toHaveBeenCalled();
  });
});

describe('flushClosedWindows', () => {
  it('flushes every (adId, count) pair per closed window, then removes the window', async () => {
    const closedWindows = [{ windowStart: 0, counts: new Map([['ad_1', 3], ['ad_2', 1]]) }];
    const peekClosedWindows = vi.fn().mockReturnValue(closedWindows);
    const removeWindow = vi.fn();
    const flush = vi.fn().mockResolvedValue(undefined);

    await flushClosedWindows({ peekClosedWindows, removeWindow }, { flush }, 999_999);

    expect(flush).toHaveBeenCalledWith('ad_1', 0, 3);
    expect(flush).toHaveBeenCalledWith('ad_2', 0, 1);
    expect(removeWindow).toHaveBeenCalledWith(0);
  });

  it('does not remove a window whose flush fails, so it can retry next tick', async () => {
    const closedWindows = [{ windowStart: 0, counts: new Map([['ad_1', 3]]) }];
    const peekClosedWindows = vi.fn().mockReturnValue(closedWindows);
    const removeWindow = vi.fn();
    const flush = vi.fn().mockRejectedValue(new Error('DynamoDB unavailable'));

    await flushClosedWindows({ peekClosedWindows, removeWindow }, { flush }, 999_999);

    expect(removeWindow).not.toHaveBeenCalled();
  });
});
```

- [ ] **Step 2: Create package config**

`services/aggregator/package.json`:
```json
{
  "name": "@app/aggregator",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "start": "tsx src/poll.ts",
    "build": "tsc -p tsconfig.json",
    "test": "vitest run",
    "typecheck": "tsc -p tsconfig.json --noEmit"
  },
  "dependencies": {
    "@aws-sdk/client-kinesis": "^3.716.0",
    "@aws-sdk/client-dynamodb": "^3.716.0",
    "redis": "^4.7.0",
    "@app/click-dedup": "workspace:*",
    "@app/windowed-aggregator": "workspace:*",
    "@app/hot-aggregate-store": "workspace:*",
    "@app/config": "workspace:*"
  },
  "devDependencies": {
    "tsx": "^4.19.2"
  }
}
```

`services/aggregator/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/aggregator test`
Expected: FAIL — `./core.js` has no exported member `handleRecord`.

- [ ] **Step 4: Write the implementation**

`services/aggregator/src/core.ts`:
```ts
import type { DedupStore } from '@app/click-dedup';
import type { WindowedAggregator } from '@app/windowed-aggregator';
import type { HotAggregateStore } from '@app/hot-aggregate-store';

export interface RawClickEvent {
  cid: string;
  ad_id: string;
  ts: string;
}

export interface ConsumerDeps {
  dedupStore: Pick<DedupStore, 'isNew'>;
  aggregator: Pick<WindowedAggregator, 'record'>;
}

export async function handleRecord(deps: ConsumerDeps, event: RawClickEvent): Promise<void> {
  const isNew = await deps.dedupStore.isNew(event.cid);
  if (!isNew) return;
  deps.aggregator.record(event.ad_id, new Date(event.ts).getTime());
}

export async function flushClosedWindows(
  aggregator: Pick<WindowedAggregator, 'peekClosedWindows' | 'removeWindow'>,
  hotStore: Pick<HotAggregateStore, 'flush'>,
  nowMs: number
): Promise<void> {
  for (const { windowStart, counts } of aggregator.peekClosedWindows(nowMs)) {
    try {
      for (const [adId, count] of counts) {
        await hotStore.flush(adId, windowStart, count);
      }
      aggregator.removeWindow(windowStart);
    } catch (err) {
      console.error('flush failed for window, will retry next tick', { windowStart, err });
    }
  }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/aggregator test`
Expected: PASS — 4 tests.

- [ ] **Step 6: Write the polling composition root (not unit tested — pure wiring + I/O loop)**

`services/aggregator/src/poll.ts`:
```ts
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
```

- [ ] **Step 7: Commit**

```bash
git add services/aggregator
git commit -m "feat: add aggregator consumer (dedup, window, flush)"
```

---

## Self-Review

**Spec coverage:**
- Redis fast-path + DynamoDB backstop dedup, checked in that order → Task 3.
- Pure windowing with peek/remove split for retry-safety → Task 1.
- Atomic `ADD` flush, never overwrite → Task 4.
- Two new DynamoDB tables, bootstrapped alongside sub-project 1's stream → Task 2.
- Consumer core split from polling shell, matching sub-project 2's app/server pattern → Task 5.
- Polling `GetRecords` instead of `SubscribeToShard` (deliberate deviation) → Task 5, Step 6.
- Poison-pill record handling (log and skip, don't stall the shard) → Task 5, Step 6 (`try/catch` inside the record loop).
- Explicit deferrals (adaptive hot-key sharding, `worker_threads`, ECS auto-scaling, lease rebalancing) → correctly absent; no task claims them.

**Placeholder scan:** No TBD/TODO; every step has complete, runnable code.

**Type consistency:** `ConsumerDeps` (Task 5) matches `Pick<DedupStore, 'isNew'>` and `Pick<WindowedAggregator, 'record'>` exactly as defined in Tasks 1 and 3. `flushClosedWindows`'s parameter types (`Pick<WindowedAggregator, 'peekClosedWindows' | 'removeWindow'>`, `Pick<HotAggregateStore, 'flush'>`) match the interfaces from Tasks 1 and 4 verbatim — the same `peekClosedWindows`/`removeWindow` split introduced in the spec's error-handling fix is what Task 5's tests exercise (the "does not remove on failed flush" test directly checks the retry-safety property the spec fix was for).

---

**Plan complete and saved to `docs/superpowers/plans/2026-07-13-dedup-aggregation.md`.**

Continuing the planning series. Next: sub-project 4 (real-time query API).

# Ingestion APIs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the click redirect hot path (`GET /click`) and batch ingestion endpoint (`POST /v1/events/clicks`), both validating and forwarding click events onto the `ad-clicks-raw` Kinesis stream created in sub-project 1.

**Architecture:** Three small shared packages (signature verification, an in-memory directory cache, a salted-partition-key Kinesis publisher) consumed identically by two thin Fastify services. Each service's route logic is a pure `buildApp(deps)` factory taking injected dependencies, so HTTP-layer tests never touch real infrastructure.

**Tech Stack:** Node.js 24, TypeScript 5.x (strict, ESM), Fastify 5, Zod (via `@app/event-schema`), `@aws-sdk/client-kinesis`, Vitest 3.

## Global Constraints

- Node.js 24, TypeScript strict mode, ESM — same as sub-project 1.
- `sig` is `hex(HMAC-SHA256(signingSecret, cid|ad_id|campaign_id|pub_id|ts))` — never includes `r` (spec §"Two resolved ambiguities").
- Every rejection on `GET /click` returns the same `400 { error: "invalid_request" }` regardless of which check failed (unknown ad, bad signature, or landing-URL mismatch) — distinguishing them in the response would let a caller enumerate valid `ad_id`s (spec §"Error handling summary").
- `POST /v1/events/clicks` has no endpoint-level auth beyond each event's own `sig` — do not add an API-key header check (spec §"Two resolved ambiguities").
- The redirect (`reply.redirect`) must be sent before the Kinesis publish begins — nothing may block or precede it (doc §04).
- The `{ad_id}#0-7` partition-key salting logic exists in exactly one place, `@app/kinesis-publisher` — both services import it, neither reimplements it.

---

## File Structure

```
packages/
├── click-signature/
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       ├── index.ts        # computeSignature, verifySignature
│       └── index.test.ts
├── directory-cache/
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       ├── index.ts        # createDirectoryCache
│       └── index.test.ts
└── kinesis-publisher/
    ├── package.json
    ├── tsconfig.json
    └── src/
        ├── index.ts        # publishClickEvent
        └── index.test.ts

services/
├── click-redirect/
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       ├── app.ts           # buildApp(deps) — GET /click
│       ├── app.test.ts
│       └── server.ts        # composition root, not unit tested
└── batch-ingest/
    ├── package.json
    ├── tsconfig.json
    └── src/
        ├── app.ts           # buildApp(deps) — POST /v1/events/clicks
        ├── app.test.ts
        └── server.ts
```

---

### Task 1: `packages/click-signature`

**Files:**
- Create: `packages/click-signature/package.json`
- Create: `packages/click-signature/tsconfig.json`
- Create: `packages/click-signature/src/index.ts`
- Test: `packages/click-signature/src/index.test.ts`

**Interfaces:**
- Consumes: nothing (leaf package).
- Produces: `computeSignature(secret: string, fields: SignableFields): string`, `verifySignature(secret: string, fields: SignableFields, sig: string): boolean` where `SignableFields = { cid: string; ad_id: string; campaign_id: string; pub_id: string; ts: string }` — consumed by Tasks 4 and 5.

- [ ] **Step 1: Write the failing test**

`packages/click-signature/src/index.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { computeSignature, verifySignature } from './index.js';

const secret = 'advertiser-secret';
const fields = {
  cid: 'clk_9f2k4x',
  ad_id: 'ad_881203',
  campaign_id: 'cmp_44210',
  pub_id: 'pub_6612',
  ts: '2026-07-12T09:14:32.118Z',
};

describe('click-signature', () => {
  it('verifies a signature computed with the same secret', () => {
    const sig = computeSignature(secret, fields);
    expect(verifySignature(secret, fields, sig)).toBe(true);
  });

  it('rejects a signature when any field is tampered', () => {
    const sig = computeSignature(secret, fields);
    expect(verifySignature(secret, { ...fields, ad_id: 'ad_other' }, sig)).toBe(false);
  });

  it('rejects a signature computed with a different secret', () => {
    const sig = computeSignature('other-secret', fields);
    expect(verifySignature(secret, fields, sig)).toBe(false);
  });

  it('rejects a garbage signature of the wrong length', () => {
    expect(verifySignature(secret, fields, 'not-a-real-signature')).toBe(false);
  });
});
```

- [ ] **Step 2: Create package config**

`packages/click-signature/package.json`:
```json
{
  "name": "@app/click-signature",
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

`packages/click-signature/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/click-signature test`
Expected: FAIL — `./index.js` has no exported member `computeSignature`.

- [ ] **Step 4: Write the implementation**

`packages/click-signature/src/index.ts`:
```ts
import { createHmac, timingSafeEqual } from 'node:crypto';

export interface SignableFields {
  cid: string;
  ad_id: string;
  campaign_id: string;
  pub_id: string;
  ts: string;
}

function canonicalize(fields: SignableFields): string {
  return [fields.cid, fields.ad_id, fields.campaign_id, fields.pub_id, fields.ts].join('|');
}

export function computeSignature(secret: string, fields: SignableFields): string {
  return createHmac('sha256', secret).update(canonicalize(fields)).digest('hex');
}

export function verifySignature(secret: string, fields: SignableFields, sig: string): boolean {
  const expected = Buffer.from(computeSignature(secret, fields), 'hex');
  const actual = Buffer.from(sig, 'hex');
  if (expected.length !== actual.length) return false;
  return timingSafeEqual(expected, actual);
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/click-signature test`
Expected: PASS — 4 tests.

- [ ] **Step 6: Commit**

```bash
git add packages/click-signature
git commit -m "feat: add HMAC click signature compute/verify"
```

---

### Task 2: `packages/directory-cache`

**Files:**
- Create: `packages/directory-cache/package.json`
- Create: `packages/directory-cache/tsconfig.json`
- Create: `packages/directory-cache/src/index.ts`
- Test: `packages/directory-cache/src/index.test.ts`

**Interfaces:**
- Consumes: a `loadDirectory: () => Promise<AdDirectoryEntry[]>` function (structurally matching `@app/db`'s `listActiveAdDirectory`; the type is imported from `@app/db` but the runtime function is passed in by the caller, not imported here — keeps this package testable without a database).
- Produces: `createDirectoryCache(loadDirectory, options?): DirectoryCache` where `DirectoryCache = { lookup(adId: string): AdDirectoryEntry | undefined; start(): Promise<void>; stop(): void }` — consumed by Tasks 4 and 5.

- [ ] **Step 1: Write the failing test**

`packages/directory-cache/src/index.test.ts`:
```ts
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createDirectoryCache } from './index.js';
import type { AdDirectoryEntry } from '@app/db';

const entryV1: AdDirectoryEntry = {
  adId: 'ad_1', campaignId: 'cmp_1', advertiserId: 'adv_1',
  signingSecret: 'secret-v1', landingUrl: 'https://example.com/v1',
};
const entryV2: AdDirectoryEntry = { ...entryV1, signingSecret: 'secret-v2' };

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

describe('createDirectoryCache', () => {
  it('populates from loadDirectory on start()', async () => {
    const loadDirectory = vi.fn().mockResolvedValue([entryV1]);
    const cache = createDirectoryCache(loadDirectory, { refreshIntervalMs: 1000 });

    await cache.start();

    expect(cache.lookup('ad_1')).toEqual(entryV1);
    expect(loadDirectory).toHaveBeenCalledTimes(1);
    cache.stop();
  });

  it('refreshes on the configured interval', async () => {
    const loadDirectory = vi.fn().mockResolvedValueOnce([entryV1]).mockResolvedValueOnce([entryV2]);
    const cache = createDirectoryCache(loadDirectory, { refreshIntervalMs: 1000 });

    await cache.start();
    expect(cache.lookup('ad_1')?.signingSecret).toBe('secret-v1');

    await vi.advanceTimersByTimeAsync(1000);

    expect(cache.lookup('ad_1')?.signingSecret).toBe('secret-v2');
    cache.stop();
  });

  it('returns undefined for an unknown adId', async () => {
    const cache = createDirectoryCache(vi.fn().mockResolvedValue([entryV1]), { refreshIntervalMs: 1000 });
    await cache.start();
    expect(cache.lookup('ad_unknown')).toBeUndefined();
    cache.stop();
  });
});
```

- [ ] **Step 2: Create package config**

`packages/directory-cache/package.json`:
```json
{
  "name": "@app/directory-cache",
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
    "@app/db": "workspace:*"
  }
}
```

`packages/directory-cache/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/directory-cache test`
Expected: FAIL — `./index.js` has no exported member `createDirectoryCache`.

- [ ] **Step 4: Write the implementation**

`packages/directory-cache/src/index.ts`:
```ts
import type { AdDirectoryEntry } from '@app/db';

export interface DirectoryCache {
  lookup(adId: string): AdDirectoryEntry | undefined;
  start(): Promise<void>;
  stop(): void;
}

export function createDirectoryCache(
  loadDirectory: () => Promise<AdDirectoryEntry[]>,
  options: { refreshIntervalMs?: number } = {}
): DirectoryCache {
  const refreshIntervalMs = options.refreshIntervalMs ?? 30_000;
  let entries = new Map<string, AdDirectoryEntry>();
  let timer: NodeJS.Timeout | undefined;

  async function refresh(): Promise<void> {
    const rows = await loadDirectory();
    entries = new Map(rows.map((row) => [row.adId, row]));
  }

  return {
    lookup(adId) {
      return entries.get(adId);
    },
    async start() {
      await refresh();
      timer = setInterval(() => {
        refresh().catch((err) => console.error('directory cache refresh failed', err));
      }, refreshIntervalMs);
      timer.unref();
    },
    stop() {
      clearInterval(timer);
    },
  };
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/directory-cache test`
Expected: PASS — 3 tests.

- [ ] **Step 6: Commit**

```bash
git add packages/directory-cache
git commit -m "feat: add in-memory polling directory cache"
```

---

### Task 3: `packages/kinesis-publisher`

**Files:**
- Create: `packages/kinesis-publisher/package.json`
- Create: `packages/kinesis-publisher/tsconfig.json`
- Create: `packages/kinesis-publisher/src/index.ts`
- Test: `packages/kinesis-publisher/src/index.test.ts`

**Interfaces:**
- Consumes: a `KinesisClient` (from `@aws-sdk/client-kinesis`) and the `ad-clicks-raw` stream created by sub-project 1's `ensureClickStream`.
- Produces: `publishClickEvent(client: KinesisClient, streamName: string, event: ClickEvent): Promise<void>` — consumed by Tasks 4 and 5.

- [ ] **Step 1: Write the failing test**

`packages/kinesis-publisher/src/index.test.ts`:
```ts
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
```

- [ ] **Step 2: Create package config**

`packages/kinesis-publisher/package.json`:
```json
{
  "name": "@app/kinesis-publisher",
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
    "@aws-sdk/client-kinesis": "^3.716.0",
    "@app/event-schema": "workspace:*"
  }
}
```

`packages/kinesis-publisher/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/kinesis-publisher test`
Expected: FAIL — `./index.js` has no exported member `publishClickEvent`.
(Requires sub-project 1 Task 4's `docker compose up -d` and Task 5's `pnpm --filter @app/infra-localstack bootstrap` to have already created the stream.)

- [ ] **Step 4: Write the implementation**

`packages/kinesis-publisher/src/index.ts`:
```ts
import { KinesisClient, PutRecordCommand } from '@aws-sdk/client-kinesis';
import type { ClickEvent } from '@app/event-schema';

export async function publishClickEvent(
  client: KinesisClient,
  streamName: string,
  event: ClickEvent
): Promise<void> {
  await client.send(new PutRecordCommand({
    StreamName: streamName,
    PartitionKey: `${event.ad_id}#${Math.floor(Math.random() * 8)}`,
    Data: Buffer.from(JSON.stringify({ ...event, receivedAt: Date.now() })),
  }));
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/kinesis-publisher test`
Expected: PASS — 1 test.

- [ ] **Step 6: Commit**

```bash
git add packages/kinesis-publisher
git commit -m "feat: add salted-partition-key Kinesis click publisher"
```

---

### Task 4: `services/click-redirect` — `GET /click`

**Files:**
- Create: `services/click-redirect/package.json`
- Create: `services/click-redirect/tsconfig.json`
- Create: `services/click-redirect/src/app.ts`
- Test: `services/click-redirect/src/app.test.ts`
- Create: `services/click-redirect/src/server.ts`

**Interfaces:**
- Consumes: `ClickEventSchema` (`@app/event-schema`), `verifySignature` (`@app/click-signature`), `DirectoryCache.lookup` shape (`@app/directory-cache`), a `publish: (event: ClickEvent) => Promise<void>` function.
- Produces: `buildApp(deps: ClickRedirectDeps): FastifyInstance` — the HTTP layer, dependency-injected so tests never touch Kinesis or Postgres directly.

- [ ] **Step 1: Write the failing test**

`services/click-redirect/src/app.test.ts`:
```ts
import { describe, expect, it, vi } from 'vitest';
import { buildApp } from './app.js';
import { computeSignature } from '@app/click-signature';
import type { AdDirectoryEntry } from '@app/db';

const secret = 'test-secret';
const entry: AdDirectoryEntry = {
  adId: 'ad_881203', campaignId: 'cmp_44210', advertiserId: 'adv_1',
  signingSecret: secret, landingUrl: 'https://advertiser.example.com/landing',
};

function validQuery() {
  const fields = {
    cid: 'clk_9f2k4x', ad_id: entry.adId, campaign_id: entry.campaignId,
    pub_id: 'pub_6612', ts: '2026-07-12T09:14:32.118Z',
  };
  const sig = computeSignature(secret, fields);
  return { ...fields, sig, r: encodeURIComponent(entry.landingUrl) };
}

function buildTestApp() {
  const publish = vi.fn().mockResolvedValue(undefined);
  const directoryCache = { lookup: (adId: string) => (adId === entry.adId ? entry : undefined) };
  const app = buildApp({ directoryCache, publish });
  return { app, publish };
}

function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

describe('GET /click', () => {
  it('redirects to the landing URL and publishes the click', async () => {
    const { app, publish } = buildTestApp();
    const query = validQuery();

    const response = await app.inject({ method: 'GET', url: '/click', query });

    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe(entry.landingUrl);

    await flush();
    expect(publish).toHaveBeenCalledWith(expect.objectContaining({ cid: query.cid, ad_id: query.ad_id }));
  });

  it('rejects an unknown ad_id with 400 and does not publish', async () => {
    const { app, publish } = buildTestApp();
    const query = { ...validQuery(), ad_id: 'ad_unknown' };

    const response = await app.inject({ method: 'GET', url: '/click', query });
    await flush();

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'invalid_request' });
    expect(publish).not.toHaveBeenCalled();
  });

  it('rejects a tampered signature with 400', async () => {
    const { app } = buildTestApp();
    const query = { ...validQuery(), campaign_id: 'cmp_tampered' };

    const response = await app.inject({ method: 'GET', url: '/click', query });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'invalid_request' });
  });

  it('rejects a landing URL that does not match the directory with 400', async () => {
    const { app } = buildTestApp();
    const query = { ...validQuery(), r: encodeURIComponent('https://evil.example.com') };

    const response = await app.inject({ method: 'GET', url: '/click', query });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'invalid_request' });
  });
});
```

- [ ] **Step 2: Create package config**

`services/click-redirect/package.json`:
```json
{
  "name": "@app/click-redirect",
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
    "@aws-sdk/client-kinesis": "^3.716.0",
    "@app/event-schema": "workspace:*",
    "@app/click-signature": "workspace:*",
    "@app/directory-cache": "workspace:*",
    "@app/kinesis-publisher": "workspace:*",
    "@app/db": "workspace:*",
    "@app/config": "workspace:*"
  },
  "devDependencies": {
    "tsx": "^4.19.2"
  }
}
```

`services/click-redirect/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/click-redirect test`
Expected: FAIL — `./app.js` has no exported member `buildApp`.

- [ ] **Step 4: Write the implementation**

`services/click-redirect/src/app.ts`:
```ts
import Fastify, { type FastifyInstance } from 'fastify';
import { ClickEventSchema, type ClickEvent } from '@app/event-schema';
import { verifySignature } from '@app/click-signature';
import type { AdDirectoryEntry } from '@app/db';

export interface ClickRedirectDeps {
  directoryCache: { lookup(adId: string): AdDirectoryEntry | undefined };
  publish: (event: ClickEvent) => Promise<void>;
}

export function buildApp(deps: ClickRedirectDeps): FastifyInstance {
  const app = Fastify({ logger: true });

  app.get('/click', async (req, reply) => {
    const query = req.query as Record<string, unknown>;
    const parsed = ClickEventSchema.safeParse(query);
    const rawR = typeof query.r === 'string' ? query.r : undefined;

    if (!parsed.success || !rawR) {
      return reply.code(400).send({ error: 'invalid_request' });
    }

    const event = parsed.data;
    const entry = deps.directoryCache.lookup(event.ad_id);
    if (!entry) {
      req.log.warn({ adId: event.ad_id }, 'unknown ad_id');
      return reply.code(400).send({ error: 'invalid_request' });
    }

    if (!verifySignature(entry.signingSecret, event, event.sig)) {
      req.log.warn({ cid: event.cid }, 'signature verification failed');
      return reply.code(400).send({ error: 'invalid_request' });
    }

    let decodedR: string;
    try {
      decodedR = decodeURIComponent(rawR);
    } catch {
      return reply.code(400).send({ error: 'invalid_request' });
    }

    if (decodedR !== entry.landingUrl) {
      req.log.warn({ cid: event.cid }, 'landing url mismatch');
      return reply.code(400).send({ error: 'invalid_request' });
    }

    reply.redirect(302, entry.landingUrl);

    setImmediate(() => {
      deps.publish(event).catch((err) => req.log.error({ err, cid: event.cid }, 'click enqueue failed'));
    });
  });

  return app;
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/click-redirect test`
Expected: PASS — 4 tests.

- [ ] **Step 6: Write the composition root (not unit tested — pure wiring)**

`services/click-redirect/src/server.ts`:
```ts
import { KinesisClient } from '@aws-sdk/client-kinesis';
import { prisma, listActiveAdDirectory } from '@app/db';
import { createDirectoryCache } from '@app/directory-cache';
import { publishClickEvent } from '@app/kinesis-publisher';
import { loadEnv } from '@app/config';
import { buildApp } from './app.js';

const env = loadEnv();
const kinesis = new KinesisClient({
  region: env.AWS_REGION,
  endpoint: env.AWS_ENDPOINT_URL,
  // ponytail: LocalStack dummy creds; swap for the default AWS credential provider chain when targeting real AWS
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});

const directoryCache = createDirectoryCache(() => listActiveAdDirectory(prisma));
await directoryCache.start();

const app = buildApp({
  directoryCache,
  publish: (event) => publishClickEvent(kinesis, 'ad-clicks-raw', event),
});

await app.listen({ port: Number(process.env.PORT ?? 3000), host: '0.0.0.0' });
```

- [ ] **Step 7: Commit**

```bash
git add services/click-redirect
git commit -m "feat: add click redirect service (GET /click)"
```

---

### Task 5: `services/batch-ingest` — `POST /v1/events/clicks`

**Files:**
- Create: `services/batch-ingest/package.json`
- Create: `services/batch-ingest/tsconfig.json`
- Create: `services/batch-ingest/src/app.ts`
- Test: `services/batch-ingest/src/app.test.ts`
- Create: `services/batch-ingest/src/server.ts`

**Interfaces:**
- Consumes: same four dependencies as Task 4 (`ClickEventSchema`, `verifySignature`, `DirectoryCache.lookup` shape, `publish`).
- Produces: `buildApp(deps: BatchIngestDeps): FastifyInstance` serving `POST /v1/events/clicks`.

- [ ] **Step 1: Write the failing test**

`services/batch-ingest/src/app.test.ts`:
```ts
import { describe, expect, it, vi } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from './app.js';
import { computeSignature } from '@app/click-signature';
import type { AdDirectoryEntry } from '@app/db';

const secret = 'test-secret';
const entry: AdDirectoryEntry = {
  adId: 'ad_881203', campaignId: 'cmp_44210', advertiserId: 'adv_1',
  signingSecret: secret, landingUrl: 'https://advertiser.example.com/landing',
};

function validEvent(overrides: Partial<Record<string, string>> = {}) {
  const fields = {
    cid: 'clk_9f2k4x', ad_id: entry.adId, campaign_id: entry.campaignId,
    pub_id: 'pub_6612', ts: '2026-07-12T09:14:32.118Z',
  };
  const sig = computeSignature(secret, fields);
  return { ...fields, sig, ...overrides };
}

function buildTestApp() {
  const publish = vi.fn().mockResolvedValue(undefined);
  const directoryCache = { lookup: (adId: string) => (adId === entry.adId ? entry : undefined) };
  const app = buildApp({ directoryCache, publish });
  return { app, publish };
}

function post(app: FastifyInstance, body: unknown) {
  return app.inject({
    method: 'POST',
    url: '/v1/events/clicks',
    headers: { 'content-type': 'application/json' },
    payload: JSON.stringify(body),
  });
}

describe('POST /v1/events/clicks', () => {
  it('accepts a batch of valid events and publishes each one', async () => {
    const { app, publish } = buildTestApp();
    const events = [validEvent({ cid: 'clk_1' }), validEvent({ cid: 'clk_2' })];

    const response = await post(app, { events });

    expect(response.statusCode).toBe(202);
    expect(response.json()).toEqual({ accepted: 2, rejected: 0 });
    expect(publish).toHaveBeenCalledTimes(2);
  });

  it('rejects individual bad events without failing the whole batch', async () => {
    const { app, publish } = buildTestApp();
    const { ad_id, ...missingFieldEvent } = validEvent({ cid: 'clk_missing_field' });
    const events = [
      validEvent({ cid: 'clk_good' }),
      validEvent({ cid: 'clk_bad_sig', sig: '0'.repeat(64) }),
      missingFieldEvent,
    ];

    const response = await post(app, { events });

    expect(response.statusCode).toBe(202);
    expect(response.json()).toEqual({ accepted: 1, rejected: 2 });
    expect(publish).toHaveBeenCalledTimes(1);
  });

  it('returns accepted: 0 for a missing events array', async () => {
    const { app } = buildTestApp();

    const response = await post(app, {});

    expect(response.statusCode).toBe(202);
    expect(response.json()).toEqual({ accepted: 0, rejected: 0 });
  });
});
```

- [ ] **Step 2: Create package config**

`services/batch-ingest/package.json`:
```json
{
  "name": "@app/batch-ingest",
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
    "@aws-sdk/client-kinesis": "^3.716.0",
    "@app/event-schema": "workspace:*",
    "@app/click-signature": "workspace:*",
    "@app/directory-cache": "workspace:*",
    "@app/kinesis-publisher": "workspace:*",
    "@app/db": "workspace:*",
    "@app/config": "workspace:*"
  },
  "devDependencies": {
    "tsx": "^4.19.2"
  }
}
```

`services/batch-ingest/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/batch-ingest test`
Expected: FAIL — `./app.js` has no exported member `buildApp`.

- [ ] **Step 4: Write the implementation**

`services/batch-ingest/src/app.ts`:
```ts
import Fastify, { type FastifyInstance } from 'fastify';
import { ClickEventSchema, type ClickEvent } from '@app/event-schema';
import { verifySignature } from '@app/click-signature';
import type { AdDirectoryEntry } from '@app/db';

export interface BatchIngestDeps {
  directoryCache: { lookup(adId: string): AdDirectoryEntry | undefined };
  publish: (event: ClickEvent) => Promise<void>;
}

async function processEvent(deps: BatchIngestDeps, raw: unknown): Promise<boolean> {
  const parsed = ClickEventSchema.safeParse(raw);
  if (!parsed.success) return false;

  const event = parsed.data;
  const entry = deps.directoryCache.lookup(event.ad_id);
  if (!entry) return false;
  if (!verifySignature(entry.signingSecret, event, event.sig)) return false;

  try {
    await deps.publish(event);
    return true;
  } catch {
    return false;
  }
}

export function buildApp(deps: BatchIngestDeps): FastifyInstance {
  const app = Fastify({ logger: true });

  app.post('/v1/events/clicks', async (req, reply) => {
    const body = req.body as { events?: unknown[] } | undefined;
    const events = Array.isArray(body?.events) ? body.events : [];

    const results = await Promise.all(events.map((event) => processEvent(deps, event)));
    const accepted = results.filter(Boolean).length;

    reply.code(202).send({ accepted, rejected: results.length - accepted });
  });

  return app;
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/batch-ingest test`
Expected: PASS — 3 tests.

- [ ] **Step 6: Write the composition root (not unit tested — pure wiring)**

`services/batch-ingest/src/server.ts`:
```ts
import { KinesisClient } from '@aws-sdk/client-kinesis';
import { prisma, listActiveAdDirectory } from '@app/db';
import { createDirectoryCache } from '@app/directory-cache';
import { publishClickEvent } from '@app/kinesis-publisher';
import { loadEnv } from '@app/config';
import { buildApp } from './app.js';

const env = loadEnv();
const kinesis = new KinesisClient({
  region: env.AWS_REGION,
  endpoint: env.AWS_ENDPOINT_URL,
  // ponytail: LocalStack dummy creds; swap for the default AWS credential provider chain when targeting real AWS
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});

const directoryCache = createDirectoryCache(() => listActiveAdDirectory(prisma));
await directoryCache.start();

const app = buildApp({
  directoryCache,
  publish: (event) => publishClickEvent(kinesis, 'ad-clicks-raw', event),
});

await app.listen({ port: Number(process.env.PORT ?? 3001), host: '0.0.0.0' });
```

- [ ] **Step 7: Commit**

```bash
git add services/batch-ingest
git commit -m "feat: add batch ingestion service (POST /v1/events/clicks)"
```

---

## Self-Review

**Spec coverage:**
- Click redirect hot path, verify → redirect → async enqueue → Task 4.
- Batch ingestion, per-event partial accept/reject → Task 5.
- `sig` canonicalization (hex HMAC-SHA256 over the 5 identity fields) → Task 1.
- Directory cache (30s poll, in-memory lookup) → Task 2.
- Salted partition key, single implementation → Task 3.
- Open-redirect fix (`r` validated against stored `landingUrl`) → Task 4, Step 4.
- Uniform `400 invalid_request` across all rejection reasons → Task 4, Step 4.
- No separate batch auth beyond per-event `sig` → Task 5, Step 4 (no API-key check present).
- Known fire-and-forget-drop gap on Kinesis failure — explicitly a spec-documented gap, not silently missing; no task claims to close it.

**Placeholder scan:** No TBD/TODO; every step has complete, runnable code.

**Type consistency:** `SignableFields` (Task 1) matches the 5 fields both services pass from a parsed `ClickEvent` (Tasks 4–5) — `ClickEvent` is a structural superset (adds `sig`), which the TypeScript compiler accepts for a variable reference. `AdDirectoryEntry` (referenced in Tasks 2, 4, 5) matches the corrected 5-field shape from sub-project 1 (`adId`, `campaignId`, `advertiserId`, `signingSecret`, `landingUrl`). `ClickRedirectDeps`/`BatchIngestDeps` both use the same `{ directoryCache: { lookup }, publish }` shape — no drift between the two services' dependency contracts.

---

**Plan complete and saved to `docs/superpowers/plans/2026-07-13-ingestion-apis.md`.**

Continuing the planning series rather than executing yet, per the original request to produce specs and plans before implementation begins. Next: sub-project 3 (dedup + real-time aggregation pipeline).

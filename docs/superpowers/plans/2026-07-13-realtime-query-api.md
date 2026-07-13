# Real-Time Query API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `GET /v1/ads/:adId/aggregates` — API-key-authenticated, tenant-scoped read of the latest window from sub-project 3's `click-aggregates` table.

**Architecture:** Two small additions to existing packages (`@app/db` for ownership/auth resolution, `@app/hot-aggregate-store` for the read side of the table it already writes) plus one new thin Fastify service, dependency-injected like the other two services.

**Tech Stack:** Node.js 24, TypeScript 5.x (strict, ESM), Fastify 5, `@aws-sdk/client-dynamodb`, Vitest 3.

## Global Constraints

- Node.js 24, TypeScript strict mode, ESM — same as prior sub-projects.
- `getAdOwnerAdvertiserId` must not filter by campaign status — that's what distinguishes it from sub-project 2's `listActiveAdDirectory` (spec §"Why this doesn't reuse sub-project 2's directory cache").
- "Ad belongs to another advertiser" and "ad doesn't exist" return the identical `404 { error: "not_found" }` — no distinguishing response (spec's tenant-scoping section).
- `hot-aggregate-store.flush` (sub-project 3) is untouched; `getLatestAggregate` is additive only.

---

## File Structure

```
packages/db/src/
├── ownership.ts        # NEW: getAdOwnerAdvertiserId, resolveApiKey
├── ownership.test.ts
└── index.ts             # MODIFIED: export the two new functions

packages/hot-aggregate-store/src/
├── index.ts              # MODIFIED: add getLatestAggregate
└── index.test.ts          # MODIFIED: add its tests

services/query-api/
├── package.json
├── tsconfig.json
└── src/
    ├── app.ts             # buildApp(deps) — GET /v1/ads/:adId/aggregates
    ├── app.test.ts
    └── server.ts           # composition root, not unit tested
```

---

### Task 1: `@app/db` — `getAdOwnerAdvertiserId` and `resolveApiKey`

**Files:**
- Create: `packages/db/src/ownership.ts`
- Test: `packages/db/src/ownership.test.ts`
- Modify: `packages/db/src/index.ts`

**Interfaces:**
- Consumes: `prisma` client shape (sub-project 1).
- Produces: `getAdOwnerAdvertiserId(client, adId): Promise<string | null>`, `resolveApiKey(client, rawKey): Promise<{ advertiserId: string } | null>` — consumed by Task 3.

- [ ] **Step 1: Write the failing test**

`packages/db/src/ownership.test.ts`:
```ts
import { describe, expect, it, afterAll } from 'vitest';
import { PrismaClient } from '@prisma/client';
import { createHash, randomBytes } from 'node:crypto';
import { getAdOwnerAdvertiserId, resolveApiKey } from './ownership.js';

const prisma = new PrismaClient();

afterAll(async () => {
  await prisma.apiKey.deleteMany({
    where: { advertiser: { name: { in: ['ownership-test-advertiser', 'ownership-test-advertiser-2'] } } },
  });
  await prisma.ad.deleteMany({ where: { name: 'ownership-test-ad' } });
  await prisma.campaign.deleteMany({ where: { name: 'ownership-test-campaign' } });
  await prisma.advertiser.deleteMany({
    where: { name: { in: ['ownership-test-advertiser', 'ownership-test-advertiser-2'] } },
  });
  await prisma.$disconnect();
});

describe('getAdOwnerAdvertiserId', () => {
  it('returns the owning advertiser id regardless of campaign status', async () => {
    const advertiser = await prisma.advertiser.create({
      data: { name: 'ownership-test-advertiser', signingSecret: 'shh' },
    });
    const campaign = await prisma.campaign.create({
      data: { name: 'ownership-test-campaign', advertiserId: advertiser.id, status: 'PAUSED' },
    });
    const ad = await prisma.ad.create({
      data: { name: 'ownership-test-ad', campaignId: campaign.id, landingUrl: 'https://example.com' },
    });

    expect(await getAdOwnerAdvertiserId(prisma, ad.id)).toBe(advertiser.id);
  });

  it('returns null for an unknown ad id', async () => {
    expect(await getAdOwnerAdvertiserId(prisma, 'ad_does_not_exist')).toBeNull();
  });
});

describe('resolveApiKey', () => {
  it('resolves an active key to its advertiser', async () => {
    const advertiser = await prisma.advertiser.create({
      data: { name: 'ownership-test-advertiser-2', signingSecret: 'shh' },
    });
    const rawKey = randomBytes(16).toString('hex');
    await prisma.apiKey.create({
      data: { advertiserId: advertiser.id, hashedKey: createHash('sha256').update(rawKey).digest('hex') },
    });

    expect(await resolveApiKey(prisma, rawKey)).toEqual({ advertiserId: advertiser.id });
  });

  it('returns null for a revoked key', async () => {
    const advertiser = await prisma.advertiser.create({
      data: { name: 'ownership-test-advertiser-2', signingSecret: 'shh' },
    });
    const rawKey = randomBytes(16).toString('hex');
    await prisma.apiKey.create({
      data: {
        advertiserId: advertiser.id,
        hashedKey: createHash('sha256').update(rawKey).digest('hex'),
        revokedAt: new Date(),
      },
    });

    expect(await resolveApiKey(prisma, rawKey)).toBeNull();
  });

  it('returns null for an unknown key', async () => {
    expect(await resolveApiKey(prisma, 'not-a-real-key')).toBeNull();
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db test`
Expected: FAIL — `./ownership.js` module not found.

- [ ] **Step 3: Write the implementation**

`packages/db/src/ownership.ts`:
```ts
import type { PrismaClient } from '@prisma/client';
import { createHash } from 'node:crypto';

export async function getAdOwnerAdvertiserId(client: PrismaClient, adId: string): Promise<string | null> {
  const ad = await client.ad.findUnique({
    where: { id: adId },
    select: { campaign: { select: { advertiserId: true } } },
  });
  return ad?.campaign.advertiserId ?? null;
}

export async function resolveApiKey(client: PrismaClient, rawKey: string): Promise<{ advertiserId: string } | null> {
  const hashedKey = createHash('sha256').update(rawKey).digest('hex');
  const apiKey = await client.apiKey.findUnique({
    where: { hashedKey },
    select: { advertiserId: true, revokedAt: true },
  });
  if (!apiKey || apiKey.revokedAt) return null;
  return { advertiserId: apiKey.advertiserId };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db test`
Expected: PASS — 9 tests (5 from sub-project 1, 4 new).

- [ ] **Step 5: Export from the package barrel**

Modify `packages/db/src/index.ts` — add:
```ts
export { getAdOwnerAdvertiserId, resolveApiKey } from './ownership.js';
```

Run: `pnpm --filter @app/db typecheck`
Expected: exits 0.

- [ ] **Step 6: Commit**

```bash
git add packages/db/src/ownership.ts packages/db/src/ownership.test.ts packages/db/src/index.ts
git commit -m "feat: add ad-ownership and API-key resolution queries"
```

---

### Task 2: `@app/hot-aggregate-store` — `getLatestAggregate`

**Files:**
- Modify: `packages/hot-aggregate-store/src/index.ts`
- Modify: `packages/hot-aggregate-store/src/index.test.ts`

**Interfaces:**
- Consumes: a `DynamoDBClient` against the `click-aggregates` table (sub-project 3).
- Produces: `getLatestAggregate(dynamo, adId, tableName?): Promise<{ windowStart: number; clicks: number } | null>` — consumed by Task 3.

- [ ] **Step 1: Write the failing test**

Append to `packages/hot-aggregate-store/src/index.test.ts` (add `getLatestAggregate` to the existing import, add a new `describe` block below the existing one):
```ts
describe('getLatestAggregate', () => {
  it('returns the most recent flushed window', async () => {
    const client = testClient();
    const store = createHotAggregateStore(client);
    const adId = `ad_test_${Date.now()}`;

    await store.flush(adId, 60_000, 4);
    await store.flush(adId, 120_000, 2);

    expect(await getLatestAggregate(client, adId)).toEqual({ windowStart: 120_000, clicks: 2 });
  }, 20_000);

  it('returns null for an ad with no flushed windows', async () => {
    const client = testClient();
    expect(await getLatestAggregate(client, `ad_never_flushed_${Date.now()}`)).toBeNull();
  }, 20_000);
});
```

The import line at the top of the file becomes:
```ts
import { createHotAggregateStore, getLatestAggregate } from './index.js';
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pnpm --filter @app/hot-aggregate-store test`
Expected: FAIL — `./index.js` has no exported member `getLatestAggregate`.

- [ ] **Step 3: Write the implementation**

Modify `packages/hot-aggregate-store/src/index.ts` — change the import line to:
```ts
import { DynamoDBClient, UpdateItemCommand, QueryCommand } from '@aws-sdk/client-dynamodb';
```

Add below the existing `createHotAggregateStore` function:
```ts
export async function getLatestAggregate(
  dynamo: DynamoDBClient,
  adId: string,
  tableName: string = HOT_AGGREGATE_TABLE_NAME
): Promise<{ windowStart: number; clicks: number } | null> {
  const { Items } = await dynamo.send(new QueryCommand({
    TableName: tableName,
    KeyConditionExpression: 'adId = :adId',
    ExpressionAttributeValues: { ':adId': { S: adId } },
    ScanIndexForward: false,
    Limit: 1,
  }));

  const item = Items?.[0];
  if (!item) return null;

  return {
    windowStart: Number(item.windowStart.N),
    clicks: Number(item.count.N),
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pnpm --filter @app/hot-aggregate-store test`
Expected: PASS — 3 tests (1 from sub-project 3, 2 new).

- [ ] **Step 5: Commit**

```bash
git add packages/hot-aggregate-store
git commit -m "feat: add getLatestAggregate read to hot-aggregate-store"
```

---

### Task 3: `services/query-api` — `GET /v1/ads/:adId/aggregates`

**Files:**
- Create: `services/query-api/package.json`
- Create: `services/query-api/tsconfig.json`
- Create: `services/query-api/src/app.ts`
- Test: `services/query-api/src/app.test.ts`
- Create: `services/query-api/src/server.ts`

**Interfaces:**
- Consumes: `resolveApiKey`/`getAdOwnerAdvertiserId` (`@app/db`), `getLatestAggregate` (`@app/hot-aggregate-store`).
- Produces: `buildApp(deps: QueryApiDeps): FastifyInstance` where `QueryApiDeps = { resolveApiKey(rawKey): Promise<{advertiserId}|null>; getAdOwner(adId): Promise<string|null>; getLatestAggregate(adId): Promise<{windowStart,clicks}|null> }`.

- [ ] **Step 1: Write the failing test**

`services/query-api/src/app.test.ts`:
```ts
import { describe, expect, it, vi } from 'vitest';
import { buildApp } from './app.js';

const API_KEY = 'raw-test-key';
const ADVERTISER_ID = 'adv_1';
const OWNED_AD_ID = 'ad_owned';
const OTHER_AD_ID = 'ad_other_advertiser';

function buildTestApp(overrides: Partial<{
  resolveApiKey: ReturnType<typeof vi.fn>;
  getAdOwner: ReturnType<typeof vi.fn>;
  getLatestAggregate: ReturnType<typeof vi.fn>;
}> = {}) {
  const deps = {
    resolveApiKey: vi.fn(async (key: string) => (key === API_KEY ? { advertiserId: ADVERTISER_ID } : null)),
    getAdOwner: vi.fn(async (adId: string) => {
      if (adId === OWNED_AD_ID) return ADVERTISER_ID;
      if (adId === OTHER_AD_ID) return 'adv_someone_else';
      return null;
    }),
    getLatestAggregate: vi.fn(async () => ({ windowStart: 60_000, clicks: 842 })),
    ...overrides,
  };
  return buildApp(deps);
}

function get(app: ReturnType<typeof buildApp>, adId: string, authorization?: string) {
  return app.inject({
    method: 'GET',
    url: `/v1/ads/${adId}/aggregates`,
    headers: authorization ? { authorization } : {},
  });
}

describe('GET /v1/ads/:adId/aggregates', () => {
  it('returns the latest aggregate for an ad the caller owns', async () => {
    const app = buildTestApp();

    const response = await get(app, OWNED_AD_ID, `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      adId: OWNED_AD_ID,
      windowStart: new Date(60_000).toISOString(),
      clicks: 842,
      exact: false,
    });
  });

  it('returns clicks: 0 and the current window when no aggregate exists yet', async () => {
    const app = buildTestApp({ getLatestAggregate: vi.fn(async () => null) });

    const response = await get(app, OWNED_AD_ID, `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(200);
    expect(response.json().clicks).toBe(0);
  });

  it('returns 404 for an ad owned by a different advertiser', async () => {
    const app = buildTestApp();

    const response = await get(app, OTHER_AD_ID, `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'not_found' });
  });

  it('returns the identical 404 body for a nonexistent ad', async () => {
    const app = buildTestApp();

    const response = await get(app, 'ad_does_not_exist', `Bearer ${API_KEY}`);

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: 'not_found' });
  });

  it('returns 401 for a missing Authorization header', async () => {
    const app = buildTestApp();

    const response = await get(app, OWNED_AD_ID);

    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({ error: 'unauthorized' });
  });

  it('returns 401 for an unknown API key', async () => {
    const app = buildTestApp();

    const response = await get(app, OWNED_AD_ID, 'Bearer not-a-real-key');

    expect(response.statusCode).toBe(401);
  });
});
```

- [ ] **Step 2: Create package config**

`services/query-api/package.json`:
```json
{
  "name": "@app/query-api",
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
    "@aws-sdk/client-dynamodb": "^3.716.0",
    "@app/db": "workspace:*",
    "@app/hot-aggregate-store": "workspace:*",
    "@app/config": "workspace:*"
  },
  "devDependencies": {
    "tsx": "^4.19.2"
  }
}
```

`services/query-api/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/query-api test`
Expected: FAIL — `./app.js` has no exported member `buildApp`.

- [ ] **Step 4: Write the implementation**

`services/query-api/src/app.ts`:
```ts
import Fastify, { type FastifyInstance } from 'fastify';

export interface QueryApiDeps {
  resolveApiKey(rawKey: string): Promise<{ advertiserId: string } | null>;
  getAdOwner(adId: string): Promise<string | null>;
  getLatestAggregate(adId: string): Promise<{ windowStart: number; clicks: number } | null>;
}

const WINDOW_MS = 60_000;

export function buildApp(deps: QueryApiDeps): FastifyInstance {
  const app = Fastify({ logger: true });

  app.get<{ Params: { adId: string } }>('/v1/ads/:adId/aggregates', async (req, reply) => {
    const authHeader = req.headers.authorization;
    const rawKey = authHeader?.startsWith('Bearer ') ? authHeader.slice('Bearer '.length) : undefined;
    if (!rawKey) {
      return reply.code(401).send({ error: 'unauthorized' });
    }

    const apiKey = await deps.resolveApiKey(rawKey);
    if (!apiKey) {
      return reply.code(401).send({ error: 'unauthorized' });
    }

    const { adId } = req.params;
    const ownerAdvertiserId = await deps.getAdOwner(adId);
    if (!ownerAdvertiserId || ownerAdvertiserId !== apiKey.advertiserId) {
      return reply.code(404).send({ error: 'not_found' });
    }

    const latest = await deps.getLatestAggregate(adId);
    const now = Date.now();
    const windowStart = latest?.windowStart ?? Math.floor(now / WINDOW_MS) * WINDOW_MS;

    reply.send({
      adId,
      windowStart: new Date(windowStart).toISOString(),
      clicks: latest?.clicks ?? 0,
      exact: false,
      asOf: new Date(now).toISOString(),
    });
  });

  return app;
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/query-api test`
Expected: PASS — 6 tests.

- [ ] **Step 6: Write the composition root (not unit tested — pure wiring)**

`services/query-api/src/server.ts`:
```ts
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { prisma, getAdOwnerAdvertiserId, resolveApiKey } from '@app/db';
import { getLatestAggregate } from '@app/hot-aggregate-store';
import { loadEnv } from '@app/config';
import { buildApp } from './app.js';

const env = loadEnv();
const dynamo = new DynamoDBClient({
  region: env.AWS_REGION,
  endpoint: env.AWS_ENDPOINT_URL,
  // ponytail: LocalStack dummy creds; swap for the default AWS credential provider chain when targeting real AWS
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});

const app = buildApp({
  resolveApiKey: (rawKey) => resolveApiKey(prisma, rawKey),
  getAdOwner: (adId) => getAdOwnerAdvertiserId(prisma, adId),
  getLatestAggregate: (adId) => getLatestAggregate(dynamo, adId),
});

await app.listen({ port: Number(process.env.PORT ?? 3002), host: '0.0.0.0' });
```

- [ ] **Step 7: Commit**

```bash
git add services/query-api
git commit -m "feat: add real-time query API (GET /v1/ads/:adId/aggregates)"
```

---

## Self-Review

**Spec coverage:**
- API-key → advertiser resolution, revocation honored → Task 1.
- Ad → owning advertiser, status-agnostic (unlike sub-project 2's directory) → Task 1.
- Latest-window read on the existing composite key → Task 2.
- Identical 404 for "not yours" vs "doesn't exist" → Task 3, Step 4.
- Response shape matching the doc's example exactly, including the zero-clicks/current-window fallback → Task 3, Step 4.

**Placeholder scan:** No TBD/TODO; every step has complete, runnable code.

**Type consistency:** `QueryApiDeps` (Task 3) matches the return types of `resolveApiKey`/`getAdOwnerAdvertiserId` (Task 1) and `getLatestAggregate` (Task 2) exactly — `{ advertiserId: string } | null`, `string | null`, `{ windowStart: number; clicks: number } | null` respectively, with no drift between what Task 1/2 produce and what Task 3's `server.ts` passes through unwrapped.

---

**Plan complete and saved to `docs/superpowers/plans/2026-07-13-realtime-query-api.md`.**

Continuing the planning series. Next: sub-project 5 (archive + batch reconciliation + billing).

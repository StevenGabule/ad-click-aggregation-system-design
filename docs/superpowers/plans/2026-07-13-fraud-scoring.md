# Fraud Scoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The two sync fraud checks in front of the redirect, the fourth Kinesis consumer that scores a verdict per `cid`, and wiring real fraud exclusions into sub-project 5's reconciliation — the last sub-project in the series.

**Architecture:** A small pure+I/O signals package feeding an enrichment object through the existing publish path; a pure verdict-scoring function plus a date-keyed DynamoDB verdict store; a fourth thin Kinesis consumer built on sub-project 5's shared loop; and an extraction inside `billing-api` that makes the "fetch exclusions, then reconcile" ordering itself unit-testable instead of buried in untested composition code.

**Tech Stack:** Node.js 24, TypeScript 5.x (strict, ESM), `redis`, `@aws-sdk/client-kinesis`, `@aws-sdk/client-dynamodb`, Vitest 3.

## Global Constraints

- Node.js 24, TypeScript strict mode, ESM — same as prior sub-projects.
- `scoreVerdict` checks `previewBot` before `velocityFlag` — both set scores `invalid`, not `suspicious` (spec's explicit tie-break).
- `fraud-verdicts` table is keyed `(date, cid)`, not `(cid)` alone — `listExcludedCids` must stay a `Query`, never a `Scan`.
- `services/fraud-scorer` never consults `@app/click-dedup` — every record gets scored, redelivered or not (sub-project 3's spec already established why).
- `reconcileAndStore`'s "fetch excluded cids, then reconcile" ordering lives in a small, dependency-injected, unit-tested function — not inlined in `server.ts`'s untested composition, unlike sub-project 5's original single-call version.
- `date` in `fraud-verdicts` is computed the same way as the archiver's partition date (`new Date().toISOString().slice(0, 10)` at processing time) — the two must agree on what "today" means for reconciliation to line up.

---

## File Structure

```
packages/
├── fraud-signals/
│   ├── package.json / tsconfig.json
│   └── src/{previewBot.ts, previewBot.test.ts, velocity.ts, velocity.test.ts, index.ts}
└── fraud-verdict-store/
    ├── package.json / tsconfig.json
    └── src/{verdict.ts, verdict.test.ts, store.ts, store.test.ts, index.ts}

packages/kinesis-publisher/src/
├── index.ts             # MODIFIED: publishClickEvent gains an enrichment param
└── index.test.ts         # MODIFIED: new test for enrichment passthrough

services/click-redirect/
├── package.json          # MODIFIED: add @app/fraud-signals, redis
└── src/
    ├── app.ts             # MODIFIED: sync checks, new ClickRedirectDeps shape
    ├── app.test.ts         # MODIFIED: fixed existing assertion + 2 new tests
    └── server.ts           # MODIFIED: wire in Redis + velocityChecker

infra/localstack/src/
├── tables.ts              # MODIFIED: add ensureFraudVerdictsTable
├── tables.test.ts          # MODIFIED: test it
└── bootstrap.ts             # MODIFIED: main() also bootstraps it

services/fraud-scorer/
├── package.json / tsconfig.json
└── src/run.ts              # thin, not unit tested

services/billing-api/
├── package.json            # MODIFIED: add @app/fraud-verdict-store
└── src/
    ├── reconciliation.ts    # NEW: extracted, tested reconcileAndStore
    ├── reconciliation.test.ts
    └── server.ts             # MODIFIED: wire the extracted function in
```

---

### Task 1: `packages/fraud-signals`

**Files:**
- Create: `packages/fraud-signals/package.json`, `tsconfig.json`
- Create: `packages/fraud-signals/src/previewBot.ts`, `previewBot.test.ts`
- Create: `packages/fraud-signals/src/velocity.ts`, `velocity.test.ts`
- Create: `packages/fraud-signals/src/index.ts`

**Interfaces:**
- Consumes: nothing (`previewBot.ts`, pure); a connected `RedisClientType` (`velocity.ts`).
- Produces: `isPreviewBotRequest(headers): boolean`, `createVelocityChecker(redis, options?): VelocityChecker` where `VelocityChecker = { checkAndIncrement(ip: string): Promise<boolean> }` — consumed by Task 3.

- [ ] **Step 1: Write the failing tests**

`packages/fraud-signals/src/previewBot.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { isPreviewBotRequest } from './previewBot.js';

describe('isPreviewBotRequest', () => {
  it('flags a Purpose: prefetch header', () => {
    expect(isPreviewBotRequest({ purpose: 'prefetch' })).toBe(true);
  });

  it('flags known preview-bot user agents', () => {
    expect(isPreviewBotRequest({ userAgent: 'Slackbot-LinkExpanding 1.0' })).toBe(true);
    expect(isPreviewBotRequest({ userAgent: 'facebookexternalhit/1.1' })).toBe(true);
    expect(isPreviewBotRequest({ userAgent: 'Discordbot/2.0' })).toBe(true);
  });

  it('does not flag an ordinary browser request', () => {
    expect(isPreviewBotRequest({ userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)' })).toBe(false);
  });

  it('does not flag a request with neither header', () => {
    expect(isPreviewBotRequest({})).toBe(false);
  });
});
```

`packages/fraud-signals/src/velocity.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { createClient as createRedisClient } from 'redis';
import { createVelocityChecker } from './velocity.js';

describe('createVelocityChecker', () => {
  it('flags once the threshold is exceeded, then resets after the window elapses', async () => {
    const redis = createRedisClient({ url: process.env.REDIS_URL ?? 'redis://localhost:6379' });
    await redis.connect();
    const checker = createVelocityChecker(redis, { windowSeconds: 1, threshold: 2 });
    const ip = `10.${Date.now() % 255}.0.1`;

    expect(await checker.checkAndIncrement(ip)).toBe(false);
    expect(await checker.checkAndIncrement(ip)).toBe(false);
    expect(await checker.checkAndIncrement(ip)).toBe(true);

    await new Promise((resolve) => setTimeout(resolve, 1100));

    expect(await checker.checkAndIncrement(ip)).toBe(false);

    await redis.quit();
  }, 10_000);
});
```

- [ ] **Step 2: Create package config**

`packages/fraud-signals/package.json`:
```json
{
  "name": "@app/fraud-signals",
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
    "redis": "^4.7.0"
  }
}
```

`packages/fraud-signals/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pnpm install && pnpm --filter @app/fraud-signals test`
Expected: FAIL — `./previewBot.js` and `./velocity.js` modules not found.

- [ ] **Step 4: Write the implementations**

`packages/fraud-signals/src/previewBot.ts`:
```ts
const PREVIEW_BOT_USER_AGENT_PATTERNS = [
  /Slackbot/i,
  /facebookexternalhit/i,
  /Twitterbot/i,
  /Discordbot/i,
  /WhatsApp/i,
  /iMessage/i,
];

export function isPreviewBotRequest(headers: { purpose?: string; userAgent?: string }): boolean {
  if (headers.purpose?.toLowerCase() === 'prefetch') return true;
  if (headers.userAgent && PREVIEW_BOT_USER_AGENT_PATTERNS.some((pattern) => pattern.test(headers.userAgent!))) {
    return true;
  }
  return false;
}
```

`packages/fraud-signals/src/velocity.ts`:
```ts
import type { RedisClientType } from 'redis';

export interface VelocityChecker {
  checkAndIncrement(ip: string): Promise<boolean>;
}

export function createVelocityChecker(
  redis: RedisClientType,
  options: { windowSeconds?: number; threshold?: number } = {}
): VelocityChecker {
  const windowSeconds = options.windowSeconds ?? 60;
  const threshold = options.threshold ?? 20;

  return {
    async checkAndIncrement(ip) {
      const key = `velocity:${ip}`;
      const count = await redis.incr(key);
      if (count === 1) {
        await redis.expire(key, windowSeconds);
      }
      return count > threshold;
    },
  };
}
```

`packages/fraud-signals/src/index.ts`:
```ts
export { isPreviewBotRequest } from './previewBot.js';
export { createVelocityChecker } from './velocity.js';
export type { VelocityChecker } from './velocity.js';
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pnpm --filter @app/fraud-signals test`
Expected: PASS — 5 tests.

- [ ] **Step 6: Commit**

```bash
git add packages/fraud-signals
git commit -m "feat: add preview-bot header check and IP velocity checker"
```

---

### Task 2: Retrofit `packages/kinesis-publisher` with enrichment

**Files:**
- Modify: `packages/kinesis-publisher/src/index.ts`
- Modify: `packages/kinesis-publisher/src/index.test.ts`

**Interfaces:**
- Produces: `publishClickEvent(client, streamName, event, enrichment?): Promise<void>` where `enrichment: { velocityFlag?: boolean; previewBot?: boolean } = {}` — the new optional parameter. `services/batch-ingest` (sub-project 2) is unaffected; it never passes it.

- [ ] **Step 1: Write the failing test**

Add to `packages/kinesis-publisher/src/index.test.ts`, below the existing test:
```ts
it('includes enrichment fields in the published payload when provided', async () => {
  const client = testClient();
  const event: ClickEvent = {
    cid: `clk_test_${Date.now()}`,
    ad_id: 'ad_881203',
    campaign_id: 'cmp_44210',
    pub_id: 'pub_6612',
    ts: '2026-07-12T09:14:32.118Z',
    sig: 'deadbeef',
  };

  await publishClickEvent(client, STREAM_NAME, event, { velocityFlag: true, previewBot: false });

  const records = await readAllRecords(client);
  const match = records.find((r) => r.payload.cid === event.cid);

  expect(match?.payload).toMatchObject({ velocityFlag: true, previewBot: false });
}, 20_000);
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pnpm --filter @app/kinesis-publisher test`
Expected: FAIL — `publishClickEvent` doesn't accept a 4th argument (TypeScript error) / enrichment fields absent from the payload.

- [ ] **Step 3: Modify the implementation**

Modify `packages/kinesis-publisher/src/index.ts` — replace `publishClickEvent` with:
```ts
export async function publishClickEvent(
  client: KinesisClient,
  streamName: string,
  event: ClickEvent,
  enrichment: { velocityFlag?: boolean; previewBot?: boolean } = {}
): Promise<void> {
  await client.send(new PutRecordCommand({
    StreamName: streamName,
    PartitionKey: `${event.ad_id}#${Math.floor(Math.random() * 8)}`,
    Data: Buffer.from(JSON.stringify({ ...event, ...enrichment, receivedAt: Date.now() })),
  }));
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pnpm --filter @app/kinesis-publisher test`
Expected: PASS — 2 tests.

- [ ] **Step 5: Commit**

```bash
git add packages/kinesis-publisher
git commit -m "feat: add optional fraud-signal enrichment to publishClickEvent"
```

---

### Task 3: Retrofit `services/click-redirect` with the sync checks

**Files:**
- Modify: `services/click-redirect/package.json`
- Modify: `services/click-redirect/src/app.ts`
- Modify: `services/click-redirect/src/app.test.ts`
- Modify: `services/click-redirect/src/server.ts`

**Interfaces:**
- Consumes: `isPreviewBotRequest`, `VelocityChecker` (Task 1).
- Produces: `ClickRedirectDeps` grows a `velocityChecker` field; `publish`'s signature becomes `(event, enrichment: { velocityFlag: boolean; previewBot: boolean }) => Promise<void>`.

- [ ] **Step 1: Add the new dependencies**

Modify `services/click-redirect/package.json` — add to `dependencies`:
```json
"@app/fraud-signals": "workspace:*",
"redis": "^4.7.0"
```

- [ ] **Step 2: Update the test — fix the existing assertion, add coverage for both flags**

Replace `services/click-redirect/src/app.test.ts` entirely with:
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

function buildTestApp(overrides: { velocityFlag?: boolean } = {}) {
  const publish = vi.fn().mockResolvedValue(undefined);
  const directoryCache = { lookup: (adId: string) => (adId === entry.adId ? entry : undefined) };
  const velocityChecker = { checkAndIncrement: vi.fn().mockResolvedValue(overrides.velocityFlag ?? false) };
  const app = buildApp({ directoryCache, velocityChecker, publish });
  return { app, publish, velocityChecker };
}

function flush() {
  return new Promise((resolve) => setImmediate(() => setImmediate(resolve)));
}

describe('GET /click', () => {
  it('redirects to the landing URL and publishes the click with enrichment', async () => {
    const { app, publish } = buildTestApp();
    const query = validQuery();

    const response = await app.inject({ method: 'GET', url: '/click', query });

    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe(entry.landingUrl);

    await flush();
    expect(publish).toHaveBeenCalledWith(
      expect.objectContaining({ cid: query.cid, ad_id: query.ad_id }),
      { velocityFlag: false, previewBot: false }
    );
  });

  it('flags a known preview-bot user agent without blocking the redirect', async () => {
    const { app, publish } = buildTestApp();
    const query = validQuery();

    const response = await app.inject({
      method: 'GET', url: '/click', query, headers: { 'user-agent': 'Slackbot-LinkExpanding 1.0' },
    });

    expect(response.statusCode).toBe(302);
    await flush();
    expect(publish).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({ previewBot: true }));
  });

  it('flags an over-threshold velocity result without blocking the redirect', async () => {
    const { app, publish } = buildTestApp({ velocityFlag: true });
    const query = validQuery();

    const response = await app.inject({ method: 'GET', url: '/click', query });

    expect(response.statusCode).toBe(302);
    await flush();
    expect(publish).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({ velocityFlag: true }));
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

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/click-redirect test`
Expected: FAIL — `buildApp` doesn't accept `velocityChecker` in its deps; `publish` called with 1 argument, not 2.

- [ ] **Step 4: Modify the implementation**

Replace `services/click-redirect/src/app.ts` entirely with:
```ts
import Fastify, { type FastifyInstance } from 'fastify';
import { ClickEventSchema, type ClickEvent } from '@app/event-schema';
import { verifySignature } from '@app/click-signature';
import { isPreviewBotRequest, type VelocityChecker } from '@app/fraud-signals';
import type { AdDirectoryEntry } from '@app/db';

export interface ClickEnrichment {
  velocityFlag: boolean;
  previewBot: boolean;
}

export interface ClickRedirectDeps {
  directoryCache: { lookup(adId: string): AdDirectoryEntry | undefined };
  velocityChecker: Pick<VelocityChecker, 'checkAndIncrement'>;
  publish: (event: ClickEvent, enrichment: ClickEnrichment) => Promise<void>;
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

    const previewBot = isPreviewBotRequest({
      purpose: (req.headers['purpose'] ?? req.headers['sec-purpose']) as string | undefined,
      userAgent: req.headers['user-agent'],
    });

    setImmediate(async () => {
      try {
        const velocityFlag = await deps.velocityChecker.checkAndIncrement(req.ip);
        await deps.publish(event, { velocityFlag, previewBot });
      } catch (err) {
        req.log.error({ err, cid: event.cid }, 'click enqueue failed');
      }
    });
  });

  return app;
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/click-redirect test`
Expected: PASS — 6 tests.

- [ ] **Step 6: Wire Redis and the velocity checker into the composition root**

Replace `services/click-redirect/src/server.ts` entirely with:
```ts
import { KinesisClient } from '@aws-sdk/client-kinesis';
import { createClient as createRedisClient } from 'redis';
import { prisma, listActiveAdDirectory } from '@app/db';
import { createDirectoryCache } from '@app/directory-cache';
import { publishClickEvent } from '@app/kinesis-publisher';
import { createVelocityChecker } from '@app/fraud-signals';
import { loadEnv } from '@app/config';
import { buildApp } from './app.js';

const env = loadEnv();
const kinesis = new KinesisClient({
  region: env.AWS_REGION,
  endpoint: env.AWS_ENDPOINT_URL,
  // ponytail: LocalStack dummy creds; swap for the default AWS credential provider chain when targeting real AWS
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});
const redis = createRedisClient({ url: env.REDIS_URL });
await redis.connect();

const directoryCache = createDirectoryCache(() => listActiveAdDirectory(prisma));
await directoryCache.start();
const velocityChecker = createVelocityChecker(redis);

const app = buildApp({
  directoryCache,
  velocityChecker,
  publish: (event, enrichment) => publishClickEvent(kinesis, 'ad-clicks-raw', event, enrichment),
});

await app.listen({ port: Number(process.env.PORT ?? 3000), host: '0.0.0.0' });
```

- [ ] **Step 7: Commit**

```bash
git add services/click-redirect
git commit -m "feat: add sync fraud signal checks to click redirect"
```

---

### Task 4: `infra/localstack` — fraud verdicts table

**Files:**
- Modify: `infra/localstack/src/tables.ts`
- Modify: `infra/localstack/src/tables.test.ts`
- Modify: `infra/localstack/src/bootstrap.ts`

**Interfaces:**
- Produces: `ensureFraudVerdictsTable(client: DynamoDBClient): Promise<void>`, `FRAUD_VERDICTS_TABLE_NAME = 'fraud-verdicts'` — consumed by Task 5.

- [ ] **Step 1: Write the failing test**

Add to `infra/localstack/src/tables.test.ts` (new import, new `describe` block):
```ts
import { ensureFraudVerdictsTable, FRAUD_VERDICTS_TABLE_NAME } from './tables.js';
```
```ts
describe('ensureFraudVerdictsTable', () => {
  it('creates the fraud verdicts table keyed by date and cid, and is a no-op the second time', async () => {
    const client = testClient();
    await ensureFraudVerdictsTable(client);
    await ensureFraudVerdictsTable(client);

    const description = await client.send(new DescribeTableCommand({ TableName: FRAUD_VERDICTS_TABLE_NAME }));
    expect(description.Table?.TableStatus).toBe('ACTIVE');
    expect(description.Table?.KeySchema).toEqual([
      { AttributeName: 'date', KeyType: 'HASH' },
      { AttributeName: 'cid', KeyType: 'RANGE' },
    ]);
  }, 20_000);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pnpm --filter @app/infra-localstack test`
Expected: FAIL — `ensureFraudVerdictsTable` not exported.

- [ ] **Step 3: Write the implementation**

Add to `infra/localstack/src/tables.ts`, below `ensureStatementsTable`:
```ts
export const FRAUD_VERDICTS_TABLE_NAME = 'fraud-verdicts';

export async function ensureFraudVerdictsTable(client: DynamoDBClient): Promise<void> {
  if (await tableExists(client, FRAUD_VERDICTS_TABLE_NAME)) return;

  await client.send(new CreateTableCommand({
    TableName: FRAUD_VERDICTS_TABLE_NAME,
    AttributeDefinitions: [
      { AttributeName: 'date', AttributeType: 'S' },
      { AttributeName: 'cid', AttributeType: 'S' },
    ],
    KeySchema: [
      { AttributeName: 'date', KeyType: 'HASH' },
      { AttributeName: 'cid', KeyType: 'RANGE' },
    ],
    BillingMode: 'PAY_PER_REQUEST',
  }));
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pnpm --filter @app/infra-localstack test`
Expected: PASS — 6 tests (5 from sub-projects 1/3/5, 1 new).

- [ ] **Step 5: Wire it into the bootstrap entrypoint**

Modify `infra/localstack/src/bootstrap.ts` — add `ensureFraudVerdictsTable` to the existing import from `./tables.js`, and in `main()`, add this line after `await ensureStatementsTable(dynamoClient);`:
```ts
  await ensureFraudVerdictsTable(dynamoClient);
```
Update the final `console.log` message to append `, fraud verdicts table.` in place of the trailing period.

Run: `pnpm --filter @app/infra-localstack bootstrap`
Expected: prints the updated ready message.

- [ ] **Step 6: Commit**

```bash
git add infra/localstack
git commit -m "feat: bootstrap fraud-verdicts DynamoDB table"
```

---

### Task 5: `packages/fraud-verdict-store`

**Files:**
- Create: `packages/fraud-verdict-store/package.json`, `tsconfig.json`
- Create: `packages/fraud-verdict-store/src/verdict.ts`, `verdict.test.ts`
- Create: `packages/fraud-verdict-store/src/store.ts`, `store.test.ts`
- Create: `packages/fraud-verdict-store/src/index.ts`

**Interfaces:**
- Consumes: a `DynamoDBClient` against the `fraud-verdicts` table (Task 4).
- Produces: `scoreVerdict(enrichment): Verdict`, `putVerdict(dynamo, { date, cid, verdict }): Promise<void>`, `listExcludedCids(dynamo, date): Promise<Set<string>>` — consumed by Tasks 6 and 7.

- [ ] **Step 1: Write the failing tests**

`packages/fraud-verdict-store/src/verdict.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { scoreVerdict } from './verdict.js';

describe('scoreVerdict', () => {
  it('is legitimate when neither flag is set', () => {
    expect(scoreVerdict({})).toBe('legitimate');
  });

  it('is suspicious when only velocityFlag is set', () => {
    expect(scoreVerdict({ velocityFlag: true })).toBe('suspicious');
  });

  it('is invalid when previewBot is set', () => {
    expect(scoreVerdict({ previewBot: true })).toBe('invalid');
  });

  it('is invalid when both flags are set — previewBot wins', () => {
    expect(scoreVerdict({ velocityFlag: true, previewBot: true })).toBe('invalid');
  });
});
```

`packages/fraud-verdict-store/src/store.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { putVerdict, listExcludedCids } from './store.js';

const TEST_DATE = '2026-01-15';

function testClient(): DynamoDBClient {
  return new DynamoDBClient({
    region: 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
}

describe('fraud-verdict-store', () => {
  it('listExcludedCids returns suspicious and invalid cids but not legitimate ones', async () => {
    const client = testClient();
    const legitimateCid = `clk_fv_${Date.now()}_legit`;
    const suspiciousCid = `clk_fv_${Date.now()}_susp`;
    const invalidCid = `clk_fv_${Date.now()}_bad`;

    await putVerdict(client, { date: TEST_DATE, cid: legitimateCid, verdict: 'legitimate' });
    await putVerdict(client, { date: TEST_DATE, cid: suspiciousCid, verdict: 'suspicious' });
    await putVerdict(client, { date: TEST_DATE, cid: invalidCid, verdict: 'invalid' });

    const excluded = await listExcludedCids(client, TEST_DATE);

    expect(excluded.has(legitimateCid)).toBe(false);
    expect(excluded.has(suspiciousCid)).toBe(true);
    expect(excluded.has(invalidCid)).toBe(true);
  }, 20_000);

  it('returns an empty set for a date with no verdicts', async () => {
    const client = testClient();
    expect(await listExcludedCids(client, '2099-12-31')).toEqual(new Set());
  }, 20_000);
});
```

- [ ] **Step 2: Create package config**

`packages/fraud-verdict-store/package.json`:
```json
{
  "name": "@app/fraud-verdict-store",
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

`packages/fraud-verdict-store/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pnpm install && pnpm --filter @app/fraud-verdict-store test`
Expected: FAIL — `./verdict.js` and `./store.js` modules not found.

- [ ] **Step 4: Write the implementations**

`packages/fraud-verdict-store/src/verdict.ts`:
```ts
export type Verdict = 'legitimate' | 'suspicious' | 'invalid';

export function scoreVerdict(enrichment: { velocityFlag?: boolean; previewBot?: boolean }): Verdict {
  if (enrichment.previewBot) return 'invalid';
  if (enrichment.velocityFlag) return 'suspicious';
  return 'legitimate';
}
```

`packages/fraud-verdict-store/src/store.ts`:
```ts
import { DynamoDBClient, PutItemCommand, QueryCommand } from '@aws-sdk/client-dynamodb';
import type { Verdict } from './verdict.js';

const FRAUD_VERDICTS_TABLE_NAME = 'fraud-verdicts';

export async function putVerdict(
  dynamo: DynamoDBClient,
  params: { date: string; cid: string; verdict: Verdict }
): Promise<void> {
  await dynamo.send(new PutItemCommand({
    TableName: FRAUD_VERDICTS_TABLE_NAME,
    Item: {
      date: { S: params.date },
      cid: { S: params.cid },
      verdict: { S: params.verdict },
    },
  }));
}

export async function listExcludedCids(dynamo: DynamoDBClient, date: string): Promise<Set<string>> {
  const { Items } = await dynamo.send(new QueryCommand({
    TableName: FRAUD_VERDICTS_TABLE_NAME,
    KeyConditionExpression: '#d = :date',
    ExpressionAttributeNames: { '#d': 'date' },
    ExpressionAttributeValues: { ':date': { S: date } },
  }));

  const excluded = new Set<string>();
  for (const item of Items ?? []) {
    if (item.verdict.S !== 'legitimate') excluded.add(item.cid.S!);
  }
  return excluded;
}
```

`packages/fraud-verdict-store/src/index.ts`:
```ts
export { scoreVerdict } from './verdict.js';
export type { Verdict } from './verdict.js';
export { putVerdict, listExcludedCids } from './store.js';
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pnpm --filter @app/fraud-verdict-store test`
Expected: PASS — 6 tests.

- [ ] **Step 6: Commit**

```bash
git add packages/fraud-verdict-store
git commit -m "feat: add fraud verdict scoring and date-keyed verdict store"
```

---

### Task 6: `services/fraud-scorer`

**Files:**
- Create: `services/fraud-scorer/package.json`, `tsconfig.json`
- Create: `services/fraud-scorer/src/run.ts`

**Interfaces:**
- Consumes: `runPollingConsumer` (sub-project 5), `scoreVerdict`/`putVerdict` (Task 5).
- Produces: no exported interface — this is the fourth and final Kinesis consumer, a deployable process only.

- [ ] **Step 1: Create package config**

`services/fraud-scorer/package.json`:
```json
{
  "name": "@app/fraud-scorer",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "start": "tsx src/run.ts",
    "build": "tsc -p tsconfig.json",
    "typecheck": "tsc -p tsconfig.json --noEmit"
  },
  "dependencies": {
    "@aws-sdk/client-kinesis": "^3.716.0",
    "@aws-sdk/client-dynamodb": "^3.716.0",
    "@app/kinesis-consumer-loop": "workspace:*",
    "@app/fraud-verdict-store": "workspace:*",
    "@app/config": "workspace:*"
  },
  "devDependencies": {
    "tsx": "^4.19.2"
  }
}
```

`services/fraud-scorer/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 2: Write the consumer**

`services/fraud-scorer/src/run.ts`:
```ts
import { KinesisClient } from '@aws-sdk/client-kinesis';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { runPollingConsumer } from '@app/kinesis-consumer-loop';
import { scoreVerdict, putVerdict } from '@app/fraud-verdict-store';
import { loadEnv } from '@app/config';

interface EnrichedRecord {
  cid: string;
  velocityFlag?: boolean;
  previewBot?: boolean;
}

function todayDateString(): string {
  return new Date().toISOString().slice(0, 10);
}

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

  await runPollingConsumer({
    kinesis,
    streamName: 'ad-clicks-raw',
    shardId,
    onRecord: async (data) => {
      const event = JSON.parse(data.toString('utf-8')) as EnrichedRecord;
      const verdict = scoreVerdict(event);
      await putVerdict(dynamo, { date: todayDateString(), cid: event.cid, verdict });
    },
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

This file has no dedicated test — it's pure composition of already-tested pieces (`runPollingConsumer`: sub-project 5; `scoreVerdict`/`putVerdict`: Task 5), the same status as `services/aggregator/src/poll.ts` and `services/archiver/src/run.ts`.

- [ ] **Step 3: Verify it typechecks**

Run: `pnpm install && pnpm --filter @app/fraud-scorer typecheck`
Expected: exits 0.

- [ ] **Step 4: Commit**

```bash
git add services/fraud-scorer
git commit -m "feat: add fraud-scorer consumer (fourth Kinesis consumer)"
```

---

### Task 7: Retrofit `services/billing-api` to use real fraud exclusions

**Files:**
- Modify: `services/billing-api/package.json`
- Create: `services/billing-api/src/reconciliation.ts`
- Test: `services/billing-api/src/reconciliation.test.ts`
- Modify: `services/billing-api/src/server.ts`

**Interfaces:**
- Consumes: `listExcludedCids` (Task 5), `reconcileDate`/`bucketPrefixForDate` (sub-project 5), `putStatement` (sub-project 5).
- Produces: `reconcileAndStore(deps: ReconciliationDeps, date: string): Promise<number>` — extracted so the exclusion-before-reconcile ordering is unit-tested, not buried in `server.ts`. `app.ts`'s `BillingApiDeps.reconcileAndStore` signature (`(date) => Promise<number>`) is unchanged — only what's behind it in `server.ts` changes.

- [ ] **Step 1: Write the failing test**

`services/billing-api/src/reconciliation.test.ts`:
```ts
import { describe, expect, it, vi } from 'vitest';
import { reconcileAndStore } from './reconciliation.js';

describe('reconcileAndStore', () => {
  it('fetches excluded cids before reconciling, and passes them through unmodified', async () => {
    const excludedCids = new Set(['clk_bad_1', 'clk_bad_2']);
    const calls: string[] = [];

    const deps = {
      bucketPrefixForDate: (date: string) => `s3://ad-clicks-raw/dt=${date}/`,
      listExcludedCids: vi.fn(async () => {
        calls.push('listExcludedCids');
        return excludedCids;
      }),
      reconcileDate: vi.fn(async (_prefix: string, passedExcludedCids: Set<string>) => {
        calls.push('reconcileDate');
        expect(passedExcludedCids).toBe(excludedCids);
        return [{ campaignId: 'cmp_1', billedClicks: 10, excludedInvalidClicks: 2 }];
      }),
      putStatement: vi.fn().mockResolvedValue(undefined),
    };

    const count = await reconcileAndStore(deps, '2026-07-11');

    expect(calls).toEqual(['listExcludedCids', 'reconcileDate']);
    expect(count).toBe(1);
    expect(deps.putStatement).toHaveBeenCalledWith(expect.objectContaining({
      campaignId: 'cmp_1', billedClicks: 10, excludedInvalidClicks: 2, period: '2026-07-11',
      sourceArchive: 's3://ad-clicks-raw/dt=2026-07-11/',
    }));
  });

  it('stores one statement per campaign the reconciliation query returns', async () => {
    const deps = {
      bucketPrefixForDate: (date: string) => `s3://ad-clicks-raw/dt=${date}/`,
      listExcludedCids: vi.fn().mockResolvedValue(new Set()),
      reconcileDate: vi.fn().mockResolvedValue([
        { campaignId: 'cmp_1', billedClicks: 10, excludedInvalidClicks: 0 },
        { campaignId: 'cmp_2', billedClicks: 5, excludedInvalidClicks: 1 },
      ]),
      putStatement: vi.fn().mockResolvedValue(undefined),
    };

    const count = await reconcileAndStore(deps, '2026-07-11');

    expect(count).toBe(2);
    expect(deps.putStatement).toHaveBeenCalledTimes(2);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/billing-api test`
Expected: FAIL — `./reconciliation.js` module not found.

- [ ] **Step 3: Write the implementation**

`services/billing-api/src/reconciliation.ts`:
```ts
export interface ReconciliationResult {
  campaignId: string;
  billedClicks: number;
  excludedInvalidClicks: number;
}

export interface ReconciliationDeps {
  bucketPrefixForDate(date: string): string;
  listExcludedCids(date: string): Promise<Set<string>>;
  reconcileDate(bucketPrefix: string, excludedCids: Set<string>): Promise<ReconciliationResult[]>;
  putStatement(statement: ReconciliationResult & {
    period: string; reconciledAt: string; sourceArchive: string;
  }): Promise<void>;
}

export async function reconcileAndStore(deps: ReconciliationDeps, date: string): Promise<number> {
  const prefix = deps.bucketPrefixForDate(date);
  const excludedCids = await deps.listExcludedCids(date);
  const results = await deps.reconcileDate(prefix, excludedCids);

  const reconciledAt = new Date().toISOString();
  for (const result of results) {
    await deps.putStatement({ ...result, period: date, reconciledAt, sourceArchive: prefix });
  }

  return results.length;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pnpm --filter @app/billing-api test`
Expected: PASS — 11 tests (9 from sub-project 5, 2 new).

- [ ] **Step 5: Add the new dependency and wire the extracted function into the composition root**

Modify `services/billing-api/package.json` — add to `dependencies`:
```json
"@app/fraud-verdict-store": "workspace:*"
```

Replace `services/billing-api/src/server.ts` entirely with:
```ts
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { prisma, getCampaignOwnerAdvertiserId, resolveApiKey } from '@app/db';
import { getStatement, putStatement } from '@app/statements-store';
import { createArchiveDb, reconcileDate, bucketPrefixForDate } from '@app/parquet-archive';
import { listExcludedCids } from '@app/fraud-verdict-store';
import { loadEnv } from '@app/config';
import { buildApp } from './app.js';
import { reconcileAndStore } from './reconciliation.js';

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
  reconcileAndStore: (date) => reconcileAndStore({
    bucketPrefixForDate,
    listExcludedCids: (d) => listExcludedCids(dynamo, d),
    reconcileDate: (prefix, excludedCids) => reconcileDate(archiveDb, prefix, excludedCids),
    putStatement: (statement) => putStatement(dynamo, statement),
  }, date),
});

await app.listen({ port: Number(process.env.PORT ?? 3003), host: '0.0.0.0' });
```

- [ ] **Step 6: Commit**

```bash
git add services/billing-api
git commit -m "feat: wire real fraud exclusions into reconciliation"
```

---

## Self-Review

**Spec coverage:**
- Preview-bot header check, IP velocity check → Task 1.
- Enrichment threaded through `publishClickEvent` → Task 2.
- Sync checks retrofit into click-redirect, with the flagged existing-test breakage actually fixed → Task 3.
- Fraud verdicts table, date-keyed → Task 4.
- `scoreVerdict`'s exact doc-table mapping plus the previewBot tie-break → Task 5.
- Fourth Kinesis consumer, no dedup consultation (consistent with sub-project 3's own note — `run.ts` never imports `@app/click-dedup`) → Task 6.
- `excludedCids` seam fulfilled, with the ordering dependency made testable rather than left in untested composition → Task 7.
- Every "explicitly out of scope" item (device fingerprinting, landing-page beacon, review workflow) → correctly absent; no task claims them.

**Placeholder scan:** No TBD/TODO; every step has complete, runnable code.

**Type consistency:** `ClickEnrichment` (Task 3) matches the `enrichment` parameter type `publishClickEvent` (Task 2) accepts. `Verdict` (Task 5) is the same three-value union `scoreVerdict` returns and `putVerdict`/`listExcludedCids` consume. `ReconciliationDeps` (Task 7) matches the actual return/parameter types of `bucketPrefixForDate`, `listExcludedCids`, `reconcileDate`, and `putStatement` as defined in sub-project 5 and Task 5 — no drift between what `server.ts` wires in and what `reconcileAndStore` declares it needs.

---

**Plan complete and saved to `docs/superpowers/plans/2026-07-13-fraud-scoring.md`.**

This is the sixth and final planned sub-project. All six specs and all six implementation plans are written and committed. None have been implemented yet, per the original request to produce the full series of plans and specs before implementation begins.

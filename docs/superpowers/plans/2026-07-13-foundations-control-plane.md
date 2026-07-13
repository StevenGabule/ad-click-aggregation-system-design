# Foundations & Control Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the pnpm monorepo, local infrastructure (LocalStack + Postgres + Redis via Docker Compose), and the Prisma-backed control-plane data model (advertisers/campaigns/ads/publishers/API keys) that every later sub-project depends on.

**Architecture:** Three workspace packages (`event-schema`, `config`, `db`) plus one infra package (`infra-localstack`), no HTTP services yet. `db` owns the Prisma schema and exposes a flattened directory query shaped for the in-memory cache the hot-path redirect service will build in sub-project 2.

**Tech Stack:** Node.js 24, TypeScript 5.x (strict, ESM), pnpm workspaces, Prisma 6 + PostgreSQL 16, Zod 3, Vitest 3, LocalStack (Kinesis/DynamoDB/S3/Firehose), Redis 7, `@aws-sdk/client-kinesis`.

## Global Constraints

- Node.js 24, TypeScript strict mode, ESM (`"type": "module"`) in every package — spec §"Repo layout".
- pnpm workspaces only — no Turborepo/Nx (`ponytail`, spec §"Explicitly out of scope").
- No admin/CRUD HTTP service in this plan — control-plane rows are created only via the seed script (spec §"Explicitly out of scope").
- Every AWS SDK client reads its endpoint from `AWS_ENDPOINT_URL` (set for LocalStack, unset for real AWS) — no environment-conditional code branches (spec §"Local infrastructure").
- Vitest for all tests, no Jest.
- Prisma/Postgres is scoped to control-plane data only in this plan. Hot aggregates, dedup keys, and the raw archive are DynamoDB/Redis/S3 and belong to later sub-projects.

---

## File Structure

```
/
├── pnpm-workspace.yaml
├── package.json
├── tsconfig.base.json
├── docker-compose.yml
├── .env.example
├── packages/
│   ├── event-schema/
│   │   ├── package.json
│   │   ├── tsconfig.json
│   │   └── src/
│   │       ├── index.ts          # ClickEventSchema + ClickEvent type
│   │       └── index.test.ts
│   ├── config/
│   │   ├── package.json
│   │   ├── tsconfig.json
│   │   └── src/
│   │       ├── index.ts          # loadEnv()
│   │       └── index.test.ts
│   └── db/
│       ├── package.json
│       ├── tsconfig.json
│       ├── prisma/schema.prisma
│       └── src/
│           ├── client.ts         # shared PrismaClient singleton
│           ├── directory.ts      # listActiveAdDirectory()
│           ├── directory.test.ts
│           ├── seed.ts           # runSeed()
│           ├── seed.test.ts
│           └── index.ts          # barrel export
└── infra/
    └── localstack/
        ├── package.json
        ├── tsconfig.json
        └── src/
            ├── bootstrap.ts      # ensureClickStream()
            └── bootstrap.test.ts
```

---

### Task 1: Monorepo scaffold

**Files:**
- Create: `pnpm-workspace.yaml`
- Create: `package.json`
- Create: `tsconfig.base.json`
- Modify: `.gitignore` (add `node_modules/`, `dist/`, `.env`)

**Interfaces:**
- Produces: the workspace root every later task's `pnpm --filter <pkg> …` command runs against, and `tsconfig.base.json` that every package's `tsconfig.json` extends.

- [ ] **Step 1: Create the workspace config**

`pnpm-workspace.yaml`:
```yaml
packages:
  - "packages/*"
  - "infra/*"
  - "services/*"
```

- [ ] **Step 2: Create the root package.json**

`package.json`:
```json
{
  "name": "ad-click-aggregation",
  "private": true,
  "type": "module",
  "packageManager": "pnpm@11.5.2",
  "scripts": {
    "build": "pnpm -r build",
    "test": "pnpm -r test",
    "typecheck": "pnpm -r typecheck"
  },
  "devDependencies": {
    "typescript": "^5.7.2",
    "vitest": "^3.0.4",
    "@types/node": "^24.0.0"
  }
}
```

- [ ] **Step 3: Create the base tsconfig**

`tsconfig.base.json`:
```json
{
  "compilerOptions": {
    "target": "ES2023",
    "lib": ["ES2023"],
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "declaration": true
  }
}
```

- [ ] **Step 4: Update .gitignore**

Add to `.gitignore` (create the file if it doesn't exist):
```
node_modules/
dist/
.env
```

- [ ] **Step 5: Verify install works**

Run: `pnpm install`
Expected: exits 0. No packages exist yet, so this just establishes the lockfile — that's expected and correct at this step.

- [ ] **Step 6: Commit**

```bash
git add pnpm-workspace.yaml package.json tsconfig.base.json .gitignore pnpm-lock.yaml
git commit -m "chore: scaffold pnpm workspace and base tsconfig"
```

---

### Task 2: `packages/event-schema` — shared click event contract

**Files:**
- Create: `packages/event-schema/package.json`
- Create: `packages/event-schema/tsconfig.json`
- Create: `packages/event-schema/src/index.ts`
- Test: `packages/event-schema/src/index.test.ts`

**Interfaces:**
- Consumes: nothing (leaf package).
- Produces: `ClickEventSchema: ZodObject`, `type ClickEvent = { cid: string; ad_id: string; campaign_id: string; pub_id: string; ts: string; sig: string }` — imported by every later ingestion/aggregation/fraud task as `@app/event-schema`.

- [ ] **Step 1: Write the failing test**

`packages/event-schema/src/index.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { ClickEventSchema } from './index.js';

const validEvent = {
  cid: 'clk_9f2k4x',
  ad_id: 'ad_881203',
  campaign_id: 'cmp_44210',
  pub_id: 'pub_6612',
  ts: '2026-07-12T09:14:32.118Z',
  sig: 'deadbeef',
};

describe('ClickEventSchema', () => {
  it('accepts a valid click event', () => {
    const result = ClickEventSchema.safeParse(validEvent);
    expect(result.success).toBe(true);
  });

  it('rejects an event missing ad_id', () => {
    const { ad_id, ...withoutAdId } = validEvent;
    const result = ClickEventSchema.safeParse(withoutAdId);
    expect(result.success).toBe(false);
  });

  it('rejects a malformed timestamp', () => {
    const result = ClickEventSchema.safeParse({ ...validEvent, ts: 'not-a-date' });
    expect(result.success).toBe(false);
  });

  it('rejects an empty cid', () => {
    const result = ClickEventSchema.safeParse({ ...validEvent, cid: '' });
    expect(result.success).toBe(false);
  });
});
```

- [ ] **Step 2: Create package config**

`packages/event-schema/package.json`:
```json
{
  "name": "@app/event-schema",
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
    "zod": "^3.24.1"
  }
}
```

`packages/event-schema/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/event-schema test`
Expected: FAIL — `./index.js` has no exported member `ClickEventSchema` (file doesn't exist yet).

- [ ] **Step 4: Write the implementation**

`packages/event-schema/src/index.ts`:
```ts
import { z } from 'zod';

export const ClickEventSchema = z.object({
  cid: z.string().min(1),
  ad_id: z.string().min(1),
  campaign_id: z.string().min(1),
  pub_id: z.string().min(1),
  ts: z.string().datetime(),
  sig: z.string().min(1),
});

export type ClickEvent = z.infer<typeof ClickEventSchema>;
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/event-schema test`
Expected: PASS — 4 tests.

- [ ] **Step 6: Commit**

```bash
git add packages/event-schema
git commit -m "feat: add shared click event schema package"
```

---

### Task 3: `packages/config` — typed environment loading

**Files:**
- Create: `packages/config/package.json`
- Create: `packages/config/tsconfig.json`
- Create: `packages/config/src/index.ts`
- Test: `packages/config/src/index.test.ts`

**Interfaces:**
- Consumes: nothing (leaf package).
- Produces: `loadEnv(source?: NodeJS.ProcessEnv): Env` where `Env = { AWS_REGION: string; AWS_ENDPOINT_URL?: string; DATABASE_URL: string; REDIS_URL: string }` — every service task in later sub-projects calls this once at startup instead of reading `process.env` directly.

- [ ] **Step 1: Write the failing test**

`packages/config/src/index.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { loadEnv } from './index.js';

const validEnv = {
  DATABASE_URL: 'postgresql://user:pass@localhost:5432/app',
  REDIS_URL: 'redis://localhost:6379',
};

describe('loadEnv', () => {
  it('loads a valid environment and defaults AWS_REGION', () => {
    const env = loadEnv(validEnv);
    expect(env.AWS_REGION).toBe('us-east-1');
    expect(env.DATABASE_URL).toBe(validEnv.DATABASE_URL);
  });

  it('passes through AWS_ENDPOINT_URL when set (LocalStack)', () => {
    const env = loadEnv({ ...validEnv, AWS_ENDPOINT_URL: 'http://localhost:4566' });
    expect(env.AWS_ENDPOINT_URL).toBe('http://localhost:4566');
  });

  it('throws clearly when DATABASE_URL is missing', () => {
    const { DATABASE_URL, ...withoutDb } = validEnv;
    expect(() => loadEnv(withoutDb)).toThrow('Invalid environment configuration');
  });
});
```

- [ ] **Step 2: Create package config**

`packages/config/package.json`:
```json
{
  "name": "@app/config",
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
    "zod": "^3.24.1"
  }
}
```

`packages/config/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/config test`
Expected: FAIL — `./index.js` has no exported member `loadEnv`.

- [ ] **Step 4: Write the implementation**

`packages/config/src/index.ts`:
```ts
import { z } from 'zod';

const EnvSchema = z.object({
  AWS_REGION: z.string().min(1).default('us-east-1'),
  AWS_ENDPOINT_URL: z.string().url().optional(),
  DATABASE_URL: z.string().min(1),
  REDIS_URL: z.string().min(1),
});

export type Env = z.infer<typeof EnvSchema>;

export function loadEnv(source: NodeJS.ProcessEnv = process.env): Env {
  const result = EnvSchema.safeParse(source);
  if (!result.success) {
    throw new Error(`Invalid environment configuration: ${result.error.message}`);
  }
  return result.data;
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/config test`
Expected: PASS — 3 tests.

- [ ] **Step 6: Commit**

```bash
git add packages/config
git commit -m "feat: add typed environment config loader"
```

---

### Task 4: Local infrastructure — Docker Compose (LocalStack + Postgres + Redis)

**Files:**
- Create: `docker-compose.yml`
- Create: `.env.example`

**Interfaces:**
- Produces: three running containers reachable at `localhost:4566` (LocalStack), `localhost:5432` (Postgres), `localhost:6379` (Redis) — every later task in this plan runs against these.

- [ ] **Step 1: Create the compose file**

`docker-compose.yml`:
```yaml
services:
  localstack:
    image: localstack/localstack:3.8
    ports:
      - "4566:4566"
    environment:
      - SERVICES=kinesis,dynamodb,s3,firehose
      - DEBUG=0
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:4566/_localstack/health"]
      interval: 5s
      timeout: 5s
      retries: 10

  postgres:
    image: postgres:16
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_USER=app
      - POSTGRES_PASSWORD=app
      - POSTGRES_DB=app
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app"]
      interval: 5s
      timeout: 5s
      retries: 10

  redis:
    image: redis:7
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 10
```

- [ ] **Step 2: Create the example env file**

`.env.example`:
```
DATABASE_URL=postgresql://app:app@localhost:5432/app
REDIS_URL=redis://localhost:6379
AWS_REGION=us-east-1
AWS_ENDPOINT_URL=http://localhost:4566
```

- [ ] **Step 3: Bring the stack up and verify health**

Run: `cp .env.example .env && docker compose up -d && docker compose ps`
Expected: all three services show `healthy` within ~30s (LocalStack is the slowest to report healthy).

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml .env.example
git commit -m "chore: add local infra — LocalStack, Postgres, Redis"
```

---

### Task 5: `infra/localstack` — idempotent Kinesis stream bootstrap

**Files:**
- Create: `infra/localstack/package.json`
- Create: `infra/localstack/tsconfig.json`
- Create: `infra/localstack/src/bootstrap.ts`
- Test: `infra/localstack/src/bootstrap.test.ts`

**Interfaces:**
- Consumes: a running LocalStack container from Task 4 (`AWS_ENDPOINT_URL=http://localhost:4566`).
- Produces: `ensureClickStream(client: KinesisClient): Promise<void>` — creates the `ad-clicks-raw` stream if absent, no-ops if present. Sub-project 2's ingestion services depend on this stream existing before they can `PutRecord`.

- [ ] **Step 1: Write the failing test**

`infra/localstack/src/bootstrap.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { KinesisClient, DescribeStreamSummaryCommand } from '@aws-sdk/client-kinesis';
import { ensureClickStream } from './bootstrap.js';

function testClient(): KinesisClient {
  return new KinesisClient({
    region: 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
}

describe('ensureClickStream', () => {
  it('creates the stream, and is a no-op the second time', async () => {
    const client = testClient();
    await ensureClickStream(client);
    await ensureClickStream(client); // must not throw on re-run

    const description = await client.send(
      new DescribeStreamSummaryCommand({ StreamName: 'ad-clicks-raw' })
    );
    expect(description.StreamDescriptionSummary?.StreamStatus).toBe('ACTIVE');
  }, 20_000);
});
```

- [ ] **Step 2: Create package config**

`infra/localstack/package.json`:
```json
{
  "name": "@app/infra-localstack",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "bootstrap": "tsx src/bootstrap.ts",
    "test": "vitest run",
    "typecheck": "tsc -p tsconfig.json --noEmit"
  },
  "dependencies": {
    "@aws-sdk/client-kinesis": "^3.716.0"
  },
  "devDependencies": {
    "tsx": "^4.19.2"
  }
}
```

`infra/localstack/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pnpm install && pnpm --filter @app/infra-localstack test`
Expected: FAIL — `./bootstrap.js` has no exported member `ensureClickStream`.

- [ ] **Step 4: Write the implementation**

`infra/localstack/src/bootstrap.ts`:
```ts
import {
  KinesisClient,
  CreateStreamCommand,
  DescribeStreamSummaryCommand,
  ResourceInUseException,
  ResourceNotFoundException,
} from '@aws-sdk/client-kinesis';

const STREAM_NAME = 'ad-clicks-raw';
const SHARD_COUNT = 2;

export async function ensureClickStream(client: KinesisClient): Promise<void> {
  if (await streamExists(client)) return;

  try {
    await client.send(new CreateStreamCommand({ StreamName: STREAM_NAME, ShardCount: SHARD_COUNT }));
  } catch (err) {
    if (!(err instanceof ResourceInUseException)) throw err;
  }
}

async function streamExists(client: KinesisClient): Promise<boolean> {
  try {
    await client.send(new DescribeStreamSummaryCommand({ StreamName: STREAM_NAME }));
    return true;
  } catch (err) {
    if (err instanceof ResourceNotFoundException) return false;
    throw err;
  }
}

async function main() {
  const client = new KinesisClient({
    region: process.env.AWS_REGION ?? 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
  await ensureClickStream(client);
  console.log(`Kinesis stream "${STREAM_NAME}" is ready.`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pnpm --filter @app/infra-localstack test`
Expected: PASS — 1 test. (Requires Task 4's `docker compose up -d` to already be running.)

- [ ] **Step 6: Commit**

```bash
git add infra/localstack
git commit -m "feat: add idempotent LocalStack Kinesis stream bootstrap"
```

---

### Task 6: `packages/db` — Prisma schema, client, and migration

**Files:**
- Create: `packages/db/package.json`
- Create: `packages/db/tsconfig.json`
- Create: `packages/db/prisma/schema.prisma`
- Create: `packages/db/src/client.ts`
- Test: `packages/db/src/client.test.ts`

**Interfaces:**
- Consumes: a running Postgres container from Task 4 (`DATABASE_URL`).
- Produces: `prisma: PrismaClient` singleton, and the five Prisma models (`Advertiser`, `ApiKey`, `Publisher`, `Campaign`, `Ad`) that Tasks 7 and 8 build on.

- [ ] **Step 1: Create package config**

`packages/db/package.json`:
```json
{
  "name": "@app/db",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "test": "vitest run",
    "typecheck": "tsc -p tsconfig.json --noEmit",
    "migrate": "prisma migrate dev",
    "generate": "prisma generate",
    "seed": "tsx src/seed.ts"
  },
  "dependencies": {
    "@prisma/client": "^6.1.0"
  },
  "devDependencies": {
    "prisma": "^6.1.0",
    "tsx": "^4.19.2"
  }
}
```

`packages/db/tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src" },
  "include": ["src"]
}
```

- [ ] **Step 2: Write the Prisma schema**

`packages/db/prisma/schema.prisma`:
```prisma
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

model Advertiser {
  id            String     @id @default(cuid())
  name          String
  signingSecret String
  apiKeys       ApiKey[]
  campaigns     Campaign[]
  createdAt     DateTime   @default(now())
}

model ApiKey {
  id           String     @id @default(cuid())
  advertiserId String
  advertiser   Advertiser @relation(fields: [advertiserId], references: [id])
  hashedKey    String     @unique
  createdAt    DateTime   @default(now())
  revokedAt    DateTime?
}

model Publisher {
  id        String   @id @default(cuid())
  name      String
  createdAt DateTime @default(now())
}

model Campaign {
  id           String         @id @default(cuid())
  advertiserId String
  advertiser   Advertiser     @relation(fields: [advertiserId], references: [id])
  name         String
  status       CampaignStatus @default(ACTIVE)
  ads          Ad[]
  createdAt    DateTime       @default(now())
}

enum CampaignStatus {
  ACTIVE
  PAUSED
  ENDED
}

model Ad {
  id         String   @id @default(cuid())
  campaignId String
  campaign   Campaign @relation(fields: [campaignId], references: [id])
  name       String
  landingUrl String
  createdAt  DateTime @default(now())
}
```

- [ ] **Step 3: Run the migration against the local Postgres**

Run: `pnpm install && cd packages/db && DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm migrate --name init`
Expected: `Your database is now in sync with your schema.` A new `packages/db/prisma/migrations/<timestamp>_init/migration.sql` is created.

Run: `pnpm generate`
Expected: `Generated Prisma Client` with no errors.

- [ ] **Step 4: Write the client + a connectivity test**

`packages/db/src/client.ts`:
```ts
import { PrismaClient } from '@prisma/client';

export const prisma = new PrismaClient();
```

`packages/db/src/client.test.ts`:
```ts
import { describe, expect, it, afterAll } from 'vitest';
import { prisma } from './client.js';

describe('prisma client', () => {
  afterAll(async () => {
    await prisma.$disconnect();
  });

  it('connects to Postgres', async () => {
    const result = await prisma.$queryRaw<{ ok: number }[]>`SELECT 1 as ok`;
    expect(result[0].ok).toBe(1);
  });
});
```

- [ ] **Step 5: Run test to verify it passes**

Run: `DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db test`
Expected: PASS — 1 test.

- [ ] **Step 6: Commit**

```bash
git add packages/db
git commit -m "feat: add Prisma schema and client for control-plane data"
```

---

### Task 7: `packages/db` — idempotent seed script

**Files:**
- Create: `packages/db/src/seed.ts`
- Test: `packages/db/src/seed.test.ts`

**Interfaces:**
- Consumes: `prisma` client shape from Task 6 (models: `Advertiser`, `ApiKey`, `Campaign`, `Ad`, `Publisher`).
- Produces: `runSeed(prisma: PrismaClient): Promise<SeedResult>` where `SeedResult = { advertiserId: string; campaignId: string; adId: string; publisherId: string; rawApiKey: string }`.

- [ ] **Step 1: Write the failing test**

`packages/db/src/seed.test.ts`:
```ts
import { describe, expect, it, afterAll } from 'vitest';
import { PrismaClient } from '@prisma/client';
import { runSeed } from './seed.js';

const prisma = new PrismaClient();

describe('runSeed', () => {
  afterAll(async () => {
    await prisma.$disconnect();
  });

  it('is idempotent: running twice does not duplicate rows', async () => {
    await runSeed(prisma);
    await runSeed(prisma);

    const advertisers = await prisma.advertiser.findMany({ where: { id: 'seed-advertiser-1' } });
    const ads = await prisma.ad.findMany({ where: { id: 'seed-ad-1' } });

    expect(advertisers).toHaveLength(1);
    expect(ads).toHaveLength(1);
  });

  it('returns a fresh raw API key each run (never stored raw)', async () => {
    const first = await runSeed(prisma);
    const second = await runSeed(prisma);
    expect(first.rawApiKey).not.toEqual(second.rawApiKey);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db test`
Expected: FAIL — `./seed.js` has no exported member `runSeed`.

- [ ] **Step 3: Write the implementation**

`packages/db/src/seed.ts`:
```ts
import { PrismaClient } from '@prisma/client';
import { createHash, randomBytes } from 'node:crypto';

export interface SeedResult {
  advertiserId: string;
  campaignId: string;
  adId: string;
  publisherId: string;
  rawApiKey: string;
}

export async function runSeed(prisma: PrismaClient): Promise<SeedResult> {
  const advertiser = await prisma.advertiser.upsert({
    where: { id: 'seed-advertiser-1' },
    update: {},
    create: { id: 'seed-advertiser-1', name: 'Acme Ads', signingSecret: 'seed-signing-secret' },
  });

  const rawApiKey = randomBytes(24).toString('hex');
  await prisma.apiKey.upsert({
    where: { id: 'seed-api-key-1' },
    update: {},
    create: {
      id: 'seed-api-key-1',
      advertiserId: advertiser.id,
      hashedKey: createHash('sha256').update(rawApiKey).digest('hex'),
    },
  });

  const campaign = await prisma.campaign.upsert({
    where: { id: 'seed-campaign-1' },
    update: {},
    create: { id: 'seed-campaign-1', advertiserId: advertiser.id, name: 'Summer Launch', status: 'ACTIVE' },
  });

  const ad = await prisma.ad.upsert({
    where: { id: 'seed-ad-1' },
    update: {},
    create: {
      id: 'seed-ad-1',
      campaignId: campaign.id,
      name: 'Banner A',
      landingUrl: 'https://advertiser.example.com/landing',
    },
  });

  const publisher = await prisma.publisher.upsert({
    where: { id: 'seed-publisher-1' },
    update: {},
    create: { id: 'seed-publisher-1', name: 'Example Publisher Network' },
  });

  return {
    advertiserId: advertiser.id,
    campaignId: campaign.id,
    adId: ad.id,
    publisherId: publisher.id,
    rawApiKey,
  };
}

async function main() {
  const prisma = new PrismaClient();
  const result = await runSeed(prisma);
  console.log('Seed complete. Demo API key (save this, it is not stored raw):', result.rawApiKey);
  await prisma.$disconnect();
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db test`
Expected: PASS — 3 tests (1 from Task 6, 2 new).

- [ ] **Step 5: Commit**

```bash
git add packages/db/src/seed.ts packages/db/src/seed.test.ts
git commit -m "feat: add idempotent control-plane seed script"
```

---

### Task 8: `packages/db` — `listActiveAdDirectory` + package barrel export

**Files:**
- Create: `packages/db/src/directory.ts`
- Test: `packages/db/src/directory.test.ts`
- Create: `packages/db/src/index.ts`

**Interfaces:**
- Consumes: `prisma` client shape from Task 6.
- Produces: `listActiveAdDirectory(client: PrismaClient): Promise<AdDirectoryEntry[]>` where `AdDirectoryEntry = { adId: string; campaignId: string; advertiserId: string; signingSecret: string }` — this is the exact query sub-project 2's redirect-service cache polls (spec §"Directory read path").

- [ ] **Step 1: Write the failing test**

`packages/db/src/directory.test.ts`:
```ts
import { describe, expect, it, afterAll } from 'vitest';
import { PrismaClient } from '@prisma/client';
import { listActiveAdDirectory } from './directory.js';

const prisma = new PrismaClient();

describe('listActiveAdDirectory', () => {
  afterAll(async () => {
    await prisma.ad.deleteMany({ where: { name: 'directory-test-ad' } });
    await prisma.campaign.deleteMany({ where: { name: 'directory-test-campaign' } });
    await prisma.advertiser.deleteMany({ where: { name: 'directory-test-advertiser' } });
    await prisma.$disconnect();
  });

  it('returns a flattened row for an ad on an active campaign', async () => {
    const advertiser = await prisma.advertiser.create({
      data: { name: 'directory-test-advertiser', signingSecret: 'shh' },
    });
    const campaign = await prisma.campaign.create({
      data: { name: 'directory-test-campaign', advertiserId: advertiser.id, status: 'ACTIVE' },
    });
    const ad = await prisma.ad.create({
      data: { name: 'directory-test-ad', campaignId: campaign.id, landingUrl: 'https://example.com' },
    });

    const rows = await listActiveAdDirectory(prisma);
    const row = rows.find((r) => r.adId === ad.id);

    expect(row).toEqual({
      adId: ad.id,
      campaignId: campaign.id,
      advertiserId: advertiser.id,
      signingSecret: 'shh',
    });
  });

  it('excludes ads on non-active campaigns', async () => {
    const advertiser = await prisma.advertiser.create({
      data: { name: 'directory-test-advertiser', signingSecret: 'shh' },
    });
    const campaign = await prisma.campaign.create({
      data: { name: 'directory-test-campaign', advertiserId: advertiser.id, status: 'PAUSED' },
    });
    const ad = await prisma.ad.create({
      data: { name: 'directory-test-ad', campaignId: campaign.id, landingUrl: 'https://example.com' },
    });

    const rows = await listActiveAdDirectory(prisma);
    expect(rows.some((r) => r.adId === ad.id)).toBe(false);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db test`
Expected: FAIL — `./directory.js` has no exported member `listActiveAdDirectory`.

- [ ] **Step 3: Write the implementation**

`packages/db/src/directory.ts`:
```ts
import type { PrismaClient } from '@prisma/client';

export interface AdDirectoryEntry {
  adId: string;
  campaignId: string;
  advertiserId: string;
  signingSecret: string;
}

export async function listActiveAdDirectory(client: PrismaClient): Promise<AdDirectoryEntry[]> {
  const ads = await client.ad.findMany({
    where: { campaign: { status: 'ACTIVE' } },
    select: {
      id: true,
      campaignId: true,
      campaign: { select: { advertiserId: true, advertiser: { select: { signingSecret: true } } } },
    },
  });

  return ads.map((ad) => ({
    adId: ad.id,
    campaignId: ad.campaignId,
    advertiserId: ad.campaign.advertiserId,
    signingSecret: ad.campaign.advertiser.signingSecret,
  }));
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db test`
Expected: PASS — 5 tests (3 from Tasks 6–7, 2 new).

- [ ] **Step 5: Add the package barrel export**

`packages/db/src/index.ts`:
```ts
export { prisma } from './client.js';
export { listActiveAdDirectory } from './directory.js';
export type { AdDirectoryEntry } from './directory.js';
export { runSeed } from './seed.js';
export type { SeedResult } from './seed.js';
export { PrismaClient } from '@prisma/client';
```

Run: `pnpm --filter @app/db typecheck`
Expected: exits 0.

- [ ] **Step 6: Commit**

```bash
git add packages/db/src/directory.ts packages/db/src/directory.test.ts packages/db/src/index.ts
git commit -m "feat: add ad directory query and db package barrel export"
```

---

## Self-Review

**Spec coverage:**
- Monorepo scaffold (pnpm, tsconfig, Node 24) → Task 1.
- Docker Compose (LocalStack/Postgres/Redis) + `AWS_ENDPOINT_URL` mechanism → Task 4.
- Prisma schema, all 5 models → Task 6.
- Directory read path contract (`listActiveAdDirectory`) → Task 8.
- Shared event schema (`ClickEventSchema`, `r` excluded) → Task 2.
- LocalStack Kinesis bootstrap (`ad-clicks-raw`) → Task 5.
- Seed script, idempotent → Task 7.
- Explicit deferrals (no admin API, no Turborepo, no DynamoDB/S3 tables) → correctly absent from this plan, called out in the spec instead of silently missing.

**Placeholder scan:** No TBD/TODO; every step has complete, runnable code.

**Type consistency:** `AdDirectoryEntry` (Task 8) matches the spec's `listActiveAdDirectory` return shape exactly (`adId`, `campaignId`, `advertiserId`, `signingSecret`). `SeedResult` (Task 7) is consumed only within this plan's own test, not referenced elsewhere yet — safe. `ClickEvent`/`ClickEventSchema` (Task 2) field names match the spec's event contract verbatim (`cid`, `ad_id`, `campaign_id`, `pub_id`, `ts`, `sig`).

---

**Plan complete and saved to `docs/superpowers/plans/2026-07-13-foundations-control-plane.md`.**

Per the original request to produce the full series of plans and specs before implementation begins, execution of this plan is intentionally not being kicked off yet — continuing on to sub-project 2's spec next. When you're ready to execute this (or any) plan, the two options will be Subagent-Driven (superpowers:subagent-driven-development) or Inline Execution (superpowers:executing-plans).

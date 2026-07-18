# Sub-project 1: Foundations & control plane

**Status:** approved (autonomous — see note below) · **Date:** 2026-07-13

> This spec was produced under an active `/goal continue the previous query` directive instructing continuous forward progress without pausing for per-decision approval. Implementation-level calls below (tooling, folder layout, framework picks) were made directly using the two prior architectural decisions as constraints: (1) full original AWS-shaped stack — DynamoDB/Kinesis/S3/Redis — with Prisma used only where a genuinely relational need exists, and (2) local-first via Docker Compose + LocalStack, no IaC in scope. Flag anything below you want changed; nothing downstream is built yet.

## Purpose

Stand up the repo skeleton, local infrastructure, and the one piece of data model the system design doc assumes exists but never designs: the relational "control plane" (advertisers, campaigns, ads, publishers, API keys). Every later sub-project builds on top of this — the ingestion APIs need it to validate `ad_id`/`campaign_id` and resolve HMAC signing secrets; the query API needs it to resolve an API key to its owning advertiser for tenant scoping.

## In scope

- Monorepo scaffold (pnpm workspaces, TypeScript, shared config).
- Docker Compose stack: LocalStack (Kinesis, DynamoDB, S3, Firehose), Redis, Postgres.
- Prisma schema + migrations + seed script for `Advertiser`, `ApiKey`, `Publisher`, `Campaign`, `Ad`.
- A flattened "directory" query shaped for in-memory caching by the hot-path redirect service (sub-project 2) — the query itself, not the cache.
- Shared `event-schema` package: Zod schema + inferred TS types for the click event contract (`cid`, `ad_id`, `campaign_id`, `pub_id`, `ts`, `sig`, `r`), used by every producer/consumer downstream so the shape can't drift between services.
- LocalStack bootstrap script that creates the `ad-clicks-raw` Kinesis stream (the one resource sub-project 2 needs immediately). Resources specific to later sub-projects (DynamoDB hot-aggregate table, S3 archive bucket) are created in those sub-projects' own specs, not here.

## Explicitly out of scope

- **Any HTTP service.** No admin/CRUD API for managing advertisers/campaigns/ads — the doc puts the advertiser-facing dashboard UI out of scope for the same reason (§01), and campaign management was never part of this system's job. A seed script is enough to get rows into Postgres for local dev and demos. `ponytail: seed script, not a service — add a real admin API if/when something other than a human running a script needs to create these rows.`
- Turborepo / Nx / any build-orchestration layer on top of pnpm. At five-or-so packages, `pnpm -r` is enough. `ponytail: add Turborepo when build/test times across packages actually hurt, not preemptively.`
- The DynamoDB hot-aggregate table, dedup backstop table, and S3 raw-archive bucket — each is created in the sub-project that first writes to it (3 and 5 respectively), so its schema spec lives next to its first consumer.

## Repo layout

```
/
├── pnpm-workspace.yaml
├── tsconfig.base.json
├── docker-compose.yml
├── .env.example
├── packages/
│   ├── event-schema/        # Zod schema + TS types for the click event contract
│   ├── db/                  # Prisma schema, migrations, generated client, seed script
│   └── config/               # typed env loading (zod-validated process.env)
├── services/                 # empty for now — populated starting sub-project 2
├── infra/
│   └── localstack/
│       └── bootstrap.ts      # idempotent: creates ad-clicks-raw stream against LocalStack
└── docs/superpowers/specs/   # this spec and the ones that follow
```

Package manager: pnpm (already present locally, no new install). Runtime target: Node.js 24 (current LTS as of this date). Language: TypeScript 5.x, strict mode, ESM (`"type": "module"` everywhere) — matches the doc's own Node.js code samples throughout §04/§6.6.

## Data model

```prisma
model Advertiser {
  id            String     @id @default(cuid())
  name          String
  signingSecret String     // HMAC key for this advertiser's `sig` params — see "Directory read path" below
  apiKeys       ApiKey[]
  campaigns     Campaign[]
  createdAt     DateTime   @default(now())
}

model ApiKey {
  id           String     @id @default(cuid())
  advertiserId String
  advertiser   Advertiser @relation(fields: [advertiserId], references: [id])
  hashedKey    String     @unique  // sha256 of the raw key; raw key is shown once at creation, never stored
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
  landingUrl String   // the redirect target of record — see "Directory read path" below
  createdAt  DateTime @default(now())
}
```

The signing secret lives on `Advertiser`, not per-campaign or per-ad: the doc's own tenant boundary is the advertiser ("scoped so one advertiser can never read another's click data," §01), so one secret per advertiser is the natural fit and keeps key rotation and lookup both O(1) on the same identifier.

`Publisher` has no foreign key from `Ad`/`Campaign` — `pub_id` on a click event identifies where the click came from, not what was clicked, and the doc treats it as an independent dimension. It's modeled here only so publisher names resolve to something instead of a bare opaque ID.

### Directory read path (constraint on this schema, built in sub-project 2)

The redirect service's hot path has a p99 <100ms budget with nothing allowed to add latency to it (§02, §04) — a synchronous Postgres round-trip per click to resolve the signing secret for `ad_id` would risk exactly that, especially under the 12,000/s burst target. So this package exposes one query shaped for periodic in-memory caching, not per-request lookup:

```ts
// packages/db — used by services/click-redirect's in-memory refresh cache, not called per-request
function listActiveAdDirectory(): Promise<{
  adId: string; campaignId: string; advertiserId: string; signingSecret: string; landingUrl: string;
}[]>
```

Sub-project 2 polls this on an interval (default 30s, configurable) into a `Map<adId, …>` in the redirect service's process memory — the same "everything about counting the click happens off the critical path" principle the doc applies to Kinesis enqueue (§04), applied one step earlier to the signature-verification lookup. This spec defines the query contract; the cache itself is sub-project 2's to build.

`landingUrl` is deliberately included in this contract even though the doc's own `GET /click` query string also carries a client-supplied `r` param for the same purpose. Trusting `r` at face value is an open redirect: `sig` (as sub-project 2 defines it) covers only the click-identity fields, not `r`, so nothing stops a caller from reusing a validly-signed click and swapping `r` for an arbitrary URL. Sub-project 2's redirect handler treats `landingUrl` from this directory as the target of record and requires `r` to match it exactly, rejecting the request otherwise — the doc's illustrative code sample doesn't show this check, but it's a correctness gap worth closing rather than reproducing.

## Shared event schema

`packages/event-schema` is the one place the click-event shape is defined — every ingestion endpoint, the aggregator, and the fraud consumer import from here instead of redeclaring the shape, so they can't silently drift apart.

```ts
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

`r` (landing URL) is deliberately not part of this shared schema — it's specific to the redirect endpoint's request, not part of the durable click-event record that flows through Kinesis (see the doc's own split between the query params in §04 and the `PutRecord` payload, which is `{ ...q, receivedAt: Date.now() }` plus everything already validated).

## Local infrastructure

`docker-compose.yml` brings up:

- **LocalStack** — `KINESIS`, `DYNAMODB`, `S3`, `FIREHOSE` services enabled. `infra/localstack/bootstrap.ts` runs after health-check passes and idempotently creates the `ad-clicks-raw` stream (2 shards locally — no need to provision toward the 20-shard burst target for a machine that will never see 12,000 clicks/s).
- **Postgres 16** — backs Prisma.
- **Redis 7** — provisioned here since it's trivial, though nothing uses it until sub-project 3's dedup fast-path.

All AWS clients throughout the project are configured from `packages/config` with an `AWS_ENDPOINT_URL` override — set to the LocalStack container for local dev, unset (falls through to real AWS endpoints) for a real deployment. This is the mechanism that makes "local-first, real SDKs" true: no code branches on environment, only an endpoint config value.

## Testing

- **Vitest** across the monorepo (ESM-native, fast, current default for new TS projects — no reason to pull in Jest's extra config weight here).
- `packages/event-schema`: schema parse/reject unit tests (valid event, missing field, malformed timestamp).
- `packages/db`: one integration test that runs against the Dockerized Postgres — seed, then `listActiveAdDirectory()` returns the expected flattened rows. This is the one test in this sub-project that needs real infra up, and it doubles as the smoke test that migrations + seed actually work.

## Migration & seed strategy

`prisma migrate dev` for local schema changes, committed migration files under `packages/db/prisma/migrations`. `packages/db/seed.ts` is idempotent (upsert on natural keys, not blind insert) so it's safe to re-run against a running LocalStack/Postgres stack without accumulating duplicate demo data — it seeds a small handful of advertisers/campaigns/ads/publishers, enough to drive the other sub-projects' local demos, not a scale simulation.

## What later sub-projects assume exists after this one

- `pnpm install && docker compose up` brings up Postgres + Redis + LocalStack with the Kinesis stream already created.
- `pnpm --filter db seed` populates enough control-plane data to generate a valid signed click URL by hand.
- `import { ClickEventSchema } from '@app/event-schema'` is the one true shape of a click event.
- `listActiveAdDirectory()` exists and is what sub-project 2's cache polls.

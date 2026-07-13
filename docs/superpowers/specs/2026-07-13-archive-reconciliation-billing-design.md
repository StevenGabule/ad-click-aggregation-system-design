# Sub-project 5: Archive + batch reconciliation + billing

**Status:** approved (autonomous — see sub-project 1 spec for why) · **Date:** 2026-07-13

## Purpose

The doc's "durable, exact path" (§05 HLD fork): every raw click, untouched, in S3; a reconciliation job that recomputes exact per-campaign billed counts from that archive; the statements table billing reads from; and the backfill mechanism (§6.7) that makes recomputing safe and boring instead of risky.

## In scope

- `packages/kinesis-consumer-loop` — the generic Kinesis polling loop, extracted out of sub-project 3's `services/aggregator/src/poll.ts` (see "Extraction and retrofit" below) and reused by the new archiver.
- `packages/parquet-archive` — DuckDB-backed Parquet writer (archiver) and reconciliation query (reconciler), both against S3/LocalStack.
- `services/archiver` — third Kinesis consumer: buffers raw events (pure, tested `BatchBuffer` core), flushes batches to S3 as Parquet.
- `packages/statements-store` — DynamoDB read/write for the statements table.
- `@app/db` addition: `getCampaignOwnerAdvertiserId`, mirroring sub-project 4's `getAdOwnerAdvertiserId`.
- `services/billing-api` — `GET /v1/campaigns/:campaignId/statement` and `POST /v1/reconciliation/:date/rerun`.
- `infra/localstack` additions: the raw-archive S3 bucket, the `click-statements` DynamoDB table.

## Explicitly out of scope

- Fraud exclusion's actual data source — sub-project 6 doesn't exist yet. Reconciliation accepts an `excludedCids` set and defaults it to empty; nothing here computes it. See "Fraud exclusion seam" below — this is a deliberate forward-compatible seam, not a gap silently ignored.
- A real nightly cron trigger for reconciliation — this plan builds `reconcileDate` as a callable function and exposes it via the manual rerun endpoint. Scheduling it to actually run nightly is a deployment concern (parallel to the ECS-autoscaling and IaC exclusions already made in sub-projects 1 and 3).
- Schema evolution handling for old Parquet partitions (doc §6.7 mentions this) — DuckDB's `read_parquet` already tolerates a superset/subset of columns across files by name, which is what the doc's own requirement asks for; there's no bespoke code to write here beyond not hardcoding a rigid column list.

## Extraction and retrofit: `packages/kinesis-consumer-loop`

Sub-project 3's `poll.ts` and this sub-project's new archiver need the identical shape: `GetShardIterator` once, then loop `GetRecords`, invoke a per-record callback, advance the iterator, sleep when idle. A third consumer with the same shape (fraud scoring) is already planned for sub-project 6 — that's concrete, known-in-advance duplication, not a hypothetical "might need it later," so this plan extracts the loop now rather than waiting for a third inline copy to justify it. The extraction also fixes a real gap in sub-project 3's original loop: it had no way to stop cleanly. The shared version takes an `AbortSignal`.

This plan includes a retrofit task that updates sub-project 3's `services/aggregator/src/poll.ts` to use the shared loop instead of its inline copy. Nothing in sub-project 3 has been implemented yet, so this is a plan correction, the same category as the `landingUrl` and `peekClosedWindows` fixes in earlier sub-projects — not scope creep into already-shipped code.

## Why the raw archive isn't deduped

The archiver writes **every** raw record it sees, including retries and redeliveries — no call to `@app/click-dedup`, no filtering at all. This is deliberate, not an oversight: the doc's own exactness argument (§6.7) is that `COUNT(DISTINCT cid)` at reconciliation query time is what makes replaying a partition safe, not anything upstream of it. Deduping at write time would make the archive redundant with the real-time path's own dedup state and would remove the one property reconciliation depends on — a raw, complete, unfiltered record of everything that happened.

## Components

### `packages/parquet-archive`

DuckDB (`duckdb-async` — a stable Promise wrapper around the official embedded DuckDB engine; picked over the plain `duckdb` package for async/await ergonomics, and over standing up a separate Athena-equivalent service because DuckDB reads/writes Parquet on S3 directly with no server to run, which fits local-first the same way LocalStack does) is configured once per process against LocalStack's S3:

```ts
export async function createArchiveDb(env: Env): Promise<Database> {
  const db = await Database.create(':memory:');
  await db.exec(`
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='${new URL(env.AWS_ENDPOINT_URL!).host}';
    SET s3_url_style='path';
    SET s3_use_ssl=false;
    SET s3_access_key_id='test';
    SET s3_secret_access_key='test';
  `);
  return db;
}
```

Write side:
```ts
export async function archiveBatch(db: Database, bucketPrefix: string, events: RawArchiveEvent[]): Promise<void>
```
Buffers arrive as a plain array (the archiver, below, decides batch size/timing); each call writes one uniquely-named Parquet file (`part-<timestamp>-<random>.parquet`) under `bucketPrefix` — many small part-files per date partition, exactly how the reconciliation read side already expects to glob them (`*.parquet`), and it sidesteps any read-modify-write complexity since files are never rewritten, only added.

Read side:
```ts
export async function reconcileDate(
  db: Database,
  bucketPrefix: string,
  excludedCids: ReadonlySet<string> = new Set()
): Promise<{ campaignId: string; billedClicks: number; excludedInvalidClicks: number }[]>
```
One query, conditional aggregation (`COUNT(DISTINCT cid) FILTER (WHERE ...)`), no branching on whether `excludedCids` is empty beyond substituting `false` for the exclusion predicate when it is — see "Fraud exclusion seam" below for why this shape, not a per-row callback.

`bucketPrefix` is built from a `date` string the caller must validate as `^\d{4}-\d{2}-\d{2}$` before it ever reaches this function — it's interpolated directly into the S3 path and SQL text, so the validation is the injection boundary, not this function's job to re-check.

### Fraud exclusion seam

The natural instinct — an `isInvalid(cid): Promise<boolean>` callback consulted per row — doesn't work here: reconciliation aggregates over potentially millions of raw rows in one SQL query, and awaiting a JS callback per row would defeat the entire point of doing this in SQL. Instead, `excludedCids` is a pre-fetched, bulk `Set<string>`, bound into the query as parameterized values and matched in one pass. Sub-project 6's job, when it exists, is to produce that set (the confirmed-invalid `cid`s for a given date, from its own verdict store) — not to change this function's shape.

### `services/archiver`

Third Kinesis consumer on `ad-clicks-raw`, built on `packages/kinesis-consumer-loop`. Buffers raw records in memory and flushes via `archiveBatch` either every 30s or every 500 records, whichever comes first — batched for Parquet's columnar write efficiency (one row at a time would defeat the format), bounded on both axes so a quiet stream still flushes promptly and a busy one doesn't grow the buffer unbounded.

Same pure-core/thin-shell split as every other consumer in this project (sub-project 3's `handleRecord`/`flushClosedWindows` vs `poll.ts`; sub-project 2's `app.ts` vs `server.ts`) — the batching decision is its own testable unit, not buried inside the untestable Kinesis-loop wiring:
```ts
export interface BatchBuffer<T> {
  add(item: T): void;
  shouldFlush(nowMs: number): boolean;
  drain(): T[];  // returns buffered items and resets the buffer + timer
}
export function createBatchBuffer<T>(options: { maxSize: number; maxAgeMs: number }): BatchBuffer<T>
```
`services/archiver/src/run.ts` (thin, not unit tested) is the only piece that touches `kinesis-consumer-loop` and `archiveBatch` directly: on each record, `buffer.add(parsed)`; on a timer tick, if `buffer.shouldFlush(Date.now())`, `archiveBatch(db, prefix, buffer.drain())` where `prefix` is computed once per flush from the flush wall-clock time (`dt=<today>/`), not per event. `ponytail: a batch flushing within ~30s of UTC midnight can file a few straggler events under the wrong day's partition — narrow enough at this project's traffic to accept; the real fix is grouping a drained batch by each event's own ts before writing, add it if reconciliation accuracy near day boundaries actually matters.`

### `packages/statements-store`

```ts
export async function putStatement(dynamo, statement: { campaignId, period, billedClicks, excludedInvalidClicks, reconciledAt, sourceArchive }): Promise<void>
export async function getStatement(dynamo, campaignId: string, period?: string): Promise<Statement | null>
```

`click-statements` table: PK `campaignId` (String), SK `period` (String, `YYYY-MM-DD`). `putStatement` is a plain `PutItem` — a full overwrite, not `UpdateItem`/`ADD`. This is deliberately the opposite of `hot-aggregate-store.flush`'s semantics: a statement is the output of a from-scratch recompute over the complete raw archive for that date (doc §6.7 — "never patches an existing number... recomputes... every time"), so replacing it wholesale on every reconciliation run is correct; accumulating deltas onto it would be wrong. `getStatement` without `period` queries `ScanIndexForward: false, Limit: 1` for the latest; with `period`, a direct key lookup.

### `@app/db` — `getCampaignOwnerAdvertiserId`

```ts
export async function getCampaignOwnerAdvertiserId(client: PrismaClient, campaignId: string): Promise<string | null>
```
Same shape and same reasoning as sub-project 4's `getAdOwnerAdvertiserId` (status-agnostic — a paused or ended campaign's statement is still legitimately queryable).

### `services/billing-api`

**`GET /v1/campaigns/:campaignId/statement`** — `?period=YYYY-MM-DD` optional, defaults to latest. Auth and tenant-scoping identical in shape to sub-project 4: `Authorization: Bearer <key>` → `resolveApiKey` → `getCampaignOwnerAdvertiserId` → mismatch or unknown campaign both return the same `404 { error: "not_found" }`, no distinguishing response, same enumeration-prevention reasoning as sub-projects 2 and 4. Response is `getStatement`'s row reshaped to the doc's exact example fields (`campaignId`, `period`, `billedClicks`, `excludedInvalidClicks`, `exact: true`, `reconciledAt`, `sourceArchive`). No statement found at all (never reconciled) → `404 { error: "not_found" }` too — indistinguishable from "wrong tenant" by design, and distinguishable from "campaign doesn't exist" only in that the ownership check already passed, which is fine since this is the caller's own campaign either way.

**`POST /v1/reconciliation/:date/rerun`** — doc calls this "ops-only." The doc never specifies a distinct auth mechanism for it, and advertiser API keys are the wrong trust level for a system-wide, all-campaigns reprocessing trigger — resolved here as a separate shared-secret check: `X-Ops-Token` header compared against an `OPS_TOKEN` environment value. `OPS_TOKEN` is read directly by `services/billing-api` (`process.env.OPS_TOKEN`, required at startup), not added to `@app/config`'s shared `EnvSchema` — the same reasoning as sub-project 3's `KINESIS_SHARD_ID`: no other service needs it, so adding it to the shared schema would force every other service to also require a variable it never uses. Missing/wrong token → `401 { error: "unauthorized" }`. On success: validates `:date` matches `^\d{4}-\d{2}-\d{2}$` (400 otherwise), calls `reconcileDate` with an empty `excludedCids` set (sub-project 5 has no fraud data to exclude yet), and `putStatement`s one row per campaign the query returned. Response: `{ date, campaignsReconciled: <count> }`.

## Testing

- `BatchBuffer` (archiver's pure core): unit tests, no I/O — `shouldFlush` is false below both thresholds; true once `maxSize` items have been added; true once `maxAgeMs` has passed since the first item after a drain, using explicit `nowMs` parameters the same way sub-project 3's `windowed-aggregator` avoided fake timers; `drain()` empties the buffer and resets its age so a subsequent `shouldFlush` is false again until new items arrive.
- `parquet-archive`: integration against LocalStack S3 — `archiveBatch` twice for the same date with different events, `reconcileDate` sees both batches' events via the glob and returns correct per-campaign distinct counts; a non-empty `excludedCids` set moves matching `cid`s from `billedClicks` into `excludedInvalidClicks` for the right campaign; a `cid` appearing twice in the archive (simulating a retried delivery reaching the archiver twice) still counts once, proving `COUNT(DISTINCT cid)` is doing the dedup work here, not any write-side filtering.
- `kinesis-consumer-loop`: integration against LocalStack Kinesis — publish records, run the loop with a spy `onRecord` and an `AbortController`, abort after a short delay, assert the spy saw the published records and the loop actually returned (didn't hang).
- `statements-store`: integration against LocalStack DynamoDB — `putStatement` then `getStatement(campaignId, period)` round-trips; a second `putStatement` for the same key fully replaces the first (proving overwrite, not accumulation — the opposite assertion from `hot-aggregate-store`'s ADD test); `getStatement` with no `period` returns the most recently-put one across two different periods.
- `getCampaignOwnerAdvertiserId`: integration against Postgres, same two cases as `getAdOwnerAdvertiserId` (works for a non-active campaign; `null` for unknown).
- `services/billing-api` app: dependency-injected fakes — statement endpoint's six cases mirror sub-project 4's query-api tests exactly (owned/found, owned/not-yet-reconciled, wrong tenant, nonexistent, missing auth, bad key); rerun endpoint's cases: valid ops token + valid date reconciles and returns a count, missing/wrong ops token → 401, malformed date → 400.

## What later sub-projects assume exists after this one

- `packages/kinesis-consumer-loop` is the one Kinesis-polling implementation in the codebase — sub-project 6's fraud consumer builds on it directly rather than writing a fourth copy.
- `parquet-archive`'s `reconcileDate(db, bucketPrefix, excludedCids)` signature is exactly what sub-project 6 will call with a real, non-empty `excludedCids` set once its verdict store exists — no signature change expected, only a real value flowing into the parameter that's been empty until now.

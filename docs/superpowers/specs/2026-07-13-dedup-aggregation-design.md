# Sub-project 3: Dedup + real-time aggregation pipeline

**Status:** approved (autonomous — see sub-project 1 spec for why) · **Date:** 2026-07-13

## Purpose

The Kinesis consumer that turns validated click events into hot, approximate, real-time aggregates: exactly-once counting despite at-least-once delivery (§6.1), watermarked windowing for late data (§6.2), and the local-pre-aggregation half of the hot-key mitigation (§6.3) — all as the one Node.js consumer process the doc's own `aggregator.js` sample sketches (§6.6).

## In scope

- `packages/click-dedup` — Redis fast-path + DynamoDB durable-backstop idempotency check.
- `packages/windowed-aggregator` — pure, infra-free in-memory windowing with watermark-gated flush, extracted from the doc's inline sample so it's unit-testable without Kinesis/DynamoDB.
- `packages/hot-aggregate-store` — DynamoDB atomic-increment writer for the hot rollup table.
- `services/aggregator` — the consumer process: polls Kinesis, dedupes, aggregates, flushes.
- Two new DynamoDB tables, bootstrapped in `infra/localstack` alongside sub-project 1's Kinesis stream: `click-dedup` and `click-aggregates`.

## Explicitly out of scope (deferred, not forgotten)

- **Adaptive hot-key counter sub-sharding** (back half of §6.3) — the doc itself says apply this only to a key that's still hot *after* local pre-aggregation, which this sub-project provides. Nothing to build until real traffic shows a key that needs it.
- **`worker_threads` CPU offloading** (§6.6) — the doc's rationale is HMAC verification and sketch/hash updates blocking the event loop. This consumer does neither: signature verification already happened at ingestion (sub-project 2), and click counting is exact, not sketch-based (§6.5). There's no CPU-bound per-record work here to offload. `ponytail: no worker pool — add one if a genuinely CPU-bound step lands in this consumer's hot loop.`
- **ECS auto-scaling on `IteratorAgeMilliseconds`** — a deployment/ops concern, out of scope per the local-first-only decision (sub-project 1).
- **Dynamic shard lease assignment/rebalancing** (what AWS's Kinesis Client Library does with a DynamoDB lease table in production) — real distributed-coordination complexity. See "Shard assignment" below for what replaces it here.

## One deliberate deviation from the doc's sample code

The doc's `aggregator.js` (§6.6) uses `SubscribeToShardCommand` (enhanced fan-out, push-based). This plan uses `GetShardIterator` + polling `GetRecords` instead. Reason: enhanced fan-out is an HTTP/2 streaming API, and LocalStack's Kinesis emulation has historically had partial-to-no support for it, which would make the consumer untestable locally — the one environment this whole project runs in (sub-project 1). Polling `GetRecords` is a strict subset of what enhanced fan-out offers, works identically against LocalStack and real AWS, and the only cost is a small fixed poll-interval latency instead of push delivery — irrelevant next to the 60s window size this consumer operates on. Switching to enhanced fan-out later is a self-contained change to `services/aggregator`'s polling loop only.

## Components

### `packages/click-dedup`

```ts
export interface DedupStore {
  isNew(cid: string): Promise<boolean>;  // true = accept and count it, false = duplicate, drop
}
export function createDedupStore(
  redis: RedisClientType,
  dynamo: DynamoDBClient,
  options?: { tableName?: string; windowSeconds?: number }
): DedupStore
```

`isNew` checks Redis first (`SET click:{cid} 1 NX EX {windowSeconds}`, default 600s per §03's capacity math). If Redis says duplicate, return `false` immediately — no DynamoDB call needed for the common case. If Redis says new, confirm durably with a DynamoDB conditional `PutItem` (`ConditionExpression: attribute_not_exists(cid)`, TTL attribute set): success means genuinely new, `ConditionalCheckFailedException` means Redis's state was lost (a restart) but DynamoDB still remembers — treat as duplicate. This is what makes DynamoDB an actual backstop rather than a second copy of the same fast-path check: it's only consulted on Redis's word that something is new, and it's the one that survives a Redis restart.

`click-dedup` table: PK `cid` (String), attribute `expiresAt` (Number, DynamoDB TTL).

### `packages/windowed-aggregator`

Pure, no I/O — the doc's `aggregator.js` windowing logic extracted into a standalone unit so it's testable with a fake clock instead of real Kinesis records and real wall-clock waits.

```ts
export interface ClosedWindow {
  windowStart: number;
  counts: Map<string, number>;  // adId -> count
}
export interface WindowedAggregator {
  record(adId: string, eventTimeMs: number): void;
  peekClosedWindows(nowMs: number): ClosedWindow[];      // read-only — does not modify state
  removeWindow(windowStart: number): void;                // caller removes only after successfully flushing it
}
export function createWindowedAggregator(options: { windowMs: number; watermarkMs: number }): WindowedAggregator
```

`services/aggregator` constructs this with `{ windowMs: 60_000, watermarkMs: 120_000 }` (doc §6.2's 1-minute windows, 2-minute allowed lateness). `peekClosedWindows`/`removeWindow` are deliberately two steps rather than one draining call: it's what lets `flushClosedWindows` (below) retry a window whose DynamoDB flush failed, instead of losing that delta the moment it's collected. A record whose window has already been removed by the time it arrives (a genuinely late straggler) simply opens a fresh single-entry bucket for that old `windowStart`, which the next flush tick immediately sees as already past its watermark — no special-casing needed, this falls out of the same peek/remove logic every other window uses. This is why the hot-aggregate store below must use an atomic increment rather than an overwrite: a stray late flush for an already-flushed window must add to the existing count, not replace it.

### `packages/hot-aggregate-store`

```ts
export function createHotAggregateStore(dynamo: DynamoDBClient, tableName: string): {
  flush(adId: string, windowStart: number, delta: number): Promise<void>;
}
```

`UpdateItem` with `ADD #c :delta` (an `ExpressionAttributeNames` alias is required — `COUNT` is a DynamoDB reserved word), never `PutItem`. `click-aggregates` table: PK `adId` (String), SK `windowStart` (Number), attributes `count` (Number), `expiresAt` (Number, TTL — 48h per §03, "self-trimming"). The composite key is what lets sub-project 4's query API ask "give me ad X's most recent window(s)" directly.

### `services/aggregator`

Split the same way sub-project 2 split HTTP routing from business logic: a pure, unit-tested core and a thin, untested polling shell.

```ts
// core.ts — no infra, fully unit-testable
export interface ConsumerDeps {
  dedupStore: Pick<DedupStore, 'isNew'>;
  aggregator: Pick<WindowedAggregator, 'record'>;
}
export async function handleRecord(deps: ConsumerDeps, event: { cid: string; ad_id: string; ts: string }): Promise<void>

export async function flushClosedWindows(
  aggregator: Pick<WindowedAggregator, 'peekClosedWindows' | 'removeWindow'>,
  hotStore: Pick<ReturnType<typeof createHotAggregateStore>, 'flush'>,
  nowMs: number
): Promise<void>
```

```ts
// poll.ts — thin composition: GetShardIterator + GetRecords loop, setInterval flush, not unit tested
```

**Shard assignment:** the consumer takes a required `KINESIS_SHARD_ID` value (its own small config concern, not added to `@app/config`'s shared schema since no other service needs it). Running the full stream locally means starting one process per shard with a different `KINESIS_SHARD_ID` — sub-project 1's bootstrap creates 2 shards, so local dev runs 2 aggregator processes. There is no automatic lease/rebalancing; that's the explicitly deferred piece above.

## Data flow (happy path)

```
Kinesis record → JSON.parse → handleRecord()
  → dedupStore.isNew(cid)?  no  → drop, done
                             yes → aggregator.record(ad_id, eventTime)  [in-memory, no I/O]

Every 5s (independent of record processing):
  flushClosedWindows() → aggregator.peekClosedWindows(now)
    → for each (windowStart, adId, count): hotStore.flush(adId, windowStart, count)  [DynamoDB UpdateItem ADD]
    → only once every (adId, count) pair in a windowStart has flushed successfully: aggregator.removeWindow(windowStart)
```

## Error handling

- `handleRecord` throwing (malformed record JSON, dedup store I/O error) is caught by `poll.ts` per-record, logged with the raw record's sequence number, and the poller advances past it rather than blocking the shard on one bad record — a poison-pill record must not stall every record behind it on the same shard.
- A `hotStore.flush` failure during the periodic flush is logged and `removeWindow` is **not** called for that `windowStart`, so the next flush tick retries the whole window — safe because retrying a *failed* flush is correct, while two *successful* flushes of the same delta would double-count. This is the reason `peekClosedWindows`/`removeWindow` are separate steps rather than one draining call: it gives `flushClosedWindows` a place to stop between "read" and "commit."

## Testing

- `click-dedup`: integration against LocalStack Redis + DynamoDB — first call for a `cid` returns `true`; immediate second call returns `false` (Redis fast path); manually delete the Redis key (simulating a Redis restart) and call again with the same `cid` — still returns `false` (DynamoDB backstop).
- `windowed-aggregator`: pure unit tests — records within an open window accumulate; `peekClosedWindows` returns nothing before the watermark passes and returns the window once it does, without removing it (a second `peekClosedWindows` call still sees it); `removeWindow` actually removes it; a late `record()` call for an already-removed `windowStart` produces a fresh single-entry window that the next `peekClosedWindows` call immediately returns.
- `hot-aggregate-store`: integration against LocalStack DynamoDB — two `flush()` calls for the same `(adId, windowStart)` sum, proving `ADD` semantics rather than overwrite.
- `services/aggregator` core (`handleRecord`, `flushClosedWindows`): unit tests with fake `dedupStore`/`aggregator`/`hotStore` — duplicate records never reach `aggregator.record`; `flushClosedWindows` calls `hotStore.flush` once per `(adId, windowStart)` pair from a fake set of closed windows and does not call it for windows that aren't closed; when `hotStore.flush` rejects for one window, `removeWindow` is not called for it but is still called for other, successfully-flushed windows in the same batch.
- `poll.ts` (the real Kinesis loop) is exercised only implicitly, by publishing real events via sub-project 2's `publishClickEvent` and asserting the resulting `click-aggregates` row — one end-to-end smoke test, not a substitute for the unit coverage above.

## What later sub-projects assume exists after this one

- `click-aggregates` rows, keyed `(adId, windowStart)`, are what sub-project 4's real-time query API reads.
- `click-dedup`'s table and Redis key convention (`click:{cid}`) are specific to this sub-project — the fraud consumer (sub-project 6) reads the same Kinesis stream independently and does **not** share this dedup state; it makes its own pass over every record regardless of counting-dedup status, since a duplicate delivery of a fraudulent click is still evidence about that `cid`, not something to be silently dropped before fraud sees it.

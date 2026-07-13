# Sub-project 2: Ingestion APIs

**Status:** approved (autonomous — see sub-project 1 spec for why) · **Date:** 2026-07-13

## Purpose

Build the two surfaces every click enters the system through (doc §04): the **click redirect** hot path (`GET /click`) and **batch ingestion** for offline-queued clients (`POST /v1/events/clicks`). Both terminate in the same place — a validated event on the `ad-clicks-raw` Kinesis stream — and share everything except how they receive the click.

## In scope

- `services/click-redirect` — `GET /click`, verify → redirect → async enqueue, p99 <100ms redirect latency.
- `services/batch-ingest` — `POST /v1/events/clicks`, per-event validation, partial accept/reject.
- `packages/click-signature` — HMAC compute/verify, shared by both services.
- `packages/directory-cache` — in-memory polling cache over sub-project 1's `listActiveAdDirectory()`, used for signature-key and landing-URL resolution without a synchronous DB call per request.
- `packages/kinesis-publisher` — the salted-partition-key `PutRecord` call (doc §6.3), shared by both services so the hot-key mitigation can't drift between them.

## Explicitly out of scope

- Fraud/velocity signals (§6.4) — sub-project 6. Neither service does anything with a click beyond validating and forwarding it.
- The real-time/exact query APIs (§04's other two endpoints) — sub-projects 4 and 5.
- Any consumer of the Kinesis stream — sub-project 3.

## Two resolved ambiguities from the original doc

The doc's illustrative code (§04, §6.6) leaves two things unspecified that this implementation has to pin down to actually run:

1. **What exactly `sig` is computed over.** Resolved as: `HMAC-SHA256(advertiser.signingSecret, cid|ad_id|campaign_id|pub_id|ts)` — precisely the fields in `@app/event-schema`'s `ClickEventSchema`, nothing else. This is what lets both ingestion surfaces share one verification function even though only the redirect path also carries `r` — `r` is never part of the signed payload (see sub-project 1's spec for why `r` is validated against the stored `landingUrl` instead of trusted directly).
2. **How `POST /v1/events/clicks` is "authenticated."** The doc doesn't specify a mechanism separate from per-click `sig`. Resolved as: no separate API-key/header auth on this endpoint — each event's own `sig` is both its integrity check and its authentication, identical to the redirect path. Adding a second, endpoint-level auth scheme on top would protect nothing `sig` doesn't already cover, since a caller without a valid `signingSecret` can't produce a valid `sig` regardless.

## Components

### `packages/click-signature`

```ts
export function computeSignature(secret: string, event: Pick<ClickEvent, 'cid'|'ad_id'|'campaign_id'|'pub_id'|'ts'>): string
export function verifySignature(secret: string, event: Pick<ClickEvent, 'cid'|'ad_id'|'campaign_id'|'pub_id'|'ts'>, sig: string): boolean
```

Pure functions, no I/O. `computeSignature` returns lowercase hex (`crypto.createHmac('sha256', secret).update(canonical).digest('hex')`) — URL-safe with no escaping needed, matching how `sig` shows up as a bare query-string value in the doc's own examples. `verifySignature` recomputes and compares with `crypto.timingSafeEqual` on equal-length buffers (mismatched-length input is rejected before the constant-time comparison, since `timingSafeEqual` throws on unequal lengths) — so response timing can't be used to brute-force a valid signature byte-by-byte.

### `packages/directory-cache`

```ts
export interface DirectoryCache {
  lookup(adId: string): AdDirectoryEntry | undefined;
  start(): Promise<void>;  // populates immediately, then refreshes on an interval
  stop(): void;
}
export function createDirectoryCache(
  loadDirectory: () => Promise<AdDirectoryEntry[]>,
  options?: { refreshIntervalMs?: number }
): DirectoryCache
```

Takes `loadDirectory` as a parameter (sub-project 1's `listActiveAdDirectory` bound to a `prisma` client) rather than importing `@app/db` directly, so it's testable with a fake loader and no database. Default refresh interval 30s, per sub-project 1's spec. `lookup` is synchronous and reads from memory — this is what keeps the redirect path off a database round-trip.

### `packages/kinesis-publisher`

```ts
export function publishClickEvent(client: KinesisClient, streamName: string, event: ClickEvent): Promise<void>
```

Salts the partition key as `${event.ad_id}#${Math.floor(Math.random() * 8)}` (doc §6.3) and sends `PutRecordCommand` with `Data: Buffer.from(JSON.stringify({ ...event, receivedAt: <ms> }))`. One implementation, so the hot-key mitigation is identical from both ingestion surfaces by construction, not by convention.

### `services/click-redirect` — `GET /click`

Request: `cid, ad_id, campaign_id, pub_id, ts, sig, r` (query string, per doc §04).

```
1. Parse query params into ClickEventSchema shape (+ r separately) → 400 invalid_request if the shape is wrong.
2. directoryCache.lookup(ad_id) → 400 invalid_request if missing (see note on error uniformity below).
3. verifySignature(entry.signingSecret, event, sig) → 400 invalid_request if false.
4. decodeURIComponent(r) === entry.landingUrl → 400 invalid_request if false.
5. reply.redirect(302, entry.landingUrl)   ← nothing before this point may be skipped, nothing after it may block it
6. setImmediate(() => publishClickEvent(kinesis, 'ad-clicks-raw', event).catch(err => log.error({err, cid}, 'click enqueue failed')))
```

Steps 2–4 all return the same `400 { error: "invalid_request" }` rather than distinguishing "unknown ad" from "bad signature" from "landing URL mismatch" in the response body — distinguishing them would let a caller enumerate valid `ad_id`s by observing which error they get back. The real reason is logged server-side (distinct log messages), just not returned. This mirrors the doc's own instinct on the query API ("never trusted from a client-supplied ID," §04) applied to the redirect path's failure modes.

Known gap, inherited from the doc's own design rather than introduced here: step 6 is fire-and-forget past the AWS SDK's built-in retry policy — a Kinesits outage after retries are exhausted logs an error and drops the click. The doc's NFR table wants zero silent drops (§02) but its own code sample (§04) is exactly this fire-and-forget shape. Closing that gap for real would mean a durable local outbox (write-ahead to disk or a local queue before returning) — real added complexity, and not what's being built here. `ponytail: fire-and-forget with SDK retries + error logging; add a durable outbox if enqueue failures show up in practice.`

### `services/batch-ingest` — `POST /v1/events/clicks`

Request: `{ events: ClickEvent[] }` (each event includes `sig`, matches `ClickEventSchema`).

```
For each event independently:
  1. ClickEventSchema.safeParse → reject on shape failure
  2. directoryCache.lookup(ad_id) → reject if missing
  3. verifySignature(...) → reject if false
  4. publishClickEvent(...) → reject if the Kinesis call itself throws (awaited here, unlike the redirect path — there's no
     redirect response to protect from latency, and the caller needs an accurate accepted/rejected count)
Response: 202 { accepted: <count>, rejected: <count> }
```

One bad event never fails the batch — this is the whole reason the endpoint reports counts instead of a single pass/fail.

## Error handling summary

| Condition | `click-redirect` | `batch-ingest` |
|---|---|---|
| Malformed/missing field | `400 invalid_request` | event counted in `rejected` |
| Unknown `ad_id` | `400 invalid_request` | event counted in `rejected` |
| Bad `sig` | `400 invalid_request` | event counted in `rejected` |
| `r` ≠ stored `landingUrl` | `400 invalid_request` | n/a (batch events carry no `r`) |
| Kinesis enqueue fails | logged, click still redirected (see gap above) | event counted in `rejected` |

## Testing

- `click-signature`: valid signature accepted; any single tampered field (`cid`, `ad_id`, `ts`, …) rejected; wrong secret rejected. Pure unit tests, no infra.
- `directory-cache`: given a fake `loadDirectory` and Vitest fake timers — populated after `start()`, `lookup()` returns entries, a second `loadDirectory` call happens after the refresh interval elapses and replaces stale entries.
- `kinesis-publisher`: integration test against sub-project 1's LocalStack stream — publish, `GetRecords` it back, assert the partition key matches `^ad_\w+#[0-7]$` and the payload round-trips through JSON.
- `click-redirect`: Fastify `.inject()` integration tests — valid request redirects to the correct `landingUrl` and a record lands on the stream; each of the four `400` conditions above gets its own test; confirm the `PutRecord` call happens *after* the redirect is already sent (assert on call order, not wall-clock timing — see note below).
- `batch-ingest`: `.inject()` with a mixed valid/invalid batch — asserts the exact `accepted`/`rejected` counts and that only the valid events land on the stream.

The p99 <100ms redirect latency target (doc §02) is a load-test concern, not a unit-test one — asserting wall-clock timing in an integration test is flaky by construction and isn't attempted here. It's called out as a gap: this plan doesn't include a load-testing task, and one should exist before this is treated as production-ready.

## What later sub-projects assume exists after this one

- A running `ad-clicks-raw` Kinesis record, once written, has the shape `ClickEvent & { receivedAt: number }`.
- `packages/directory-cache`'s `createDirectoryCache` is reused as-is by any other service that needs `ad_id → advertiser` resolution (the query API, sub-project 4, needs the inverse direction — API key → advertiser — which is a different lookup and not built here).
- `packages/kinesis-publisher`'s partition-key salting is the only place that logic lives — sub-project 3's consumers read it back but never re-derive it.

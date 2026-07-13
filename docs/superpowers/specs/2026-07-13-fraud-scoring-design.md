# Sub-project 6: Fraud scoring

**Status:** approved (autonomous — see sub-project 1 spec for why) · **Date:** 2026-07-13

## Purpose

§6.4 in full: the two minimal synchronous checks that sit in front of the redirect, and the async fourth Kinesis consumer that turns their flags into a verdict per `cid`, feeding sub-project 5's reconciliation exclusion seam. The last sub-project — after this, every "explicitly deferred/forward-reference" note left in sub-projects 3 and 5 is resolved.

## In scope

- `packages/fraud-signals` — `isPreviewBotRequest` (pure) and `createVelocityChecker` (Redis-backed).
- Retrofit: `packages/kinesis-publisher`'s `publishClickEvent` gains an optional enrichment parameter.
- Retrofit: `services/click-redirect` computes both sync signals and forwards them as enrichment.
- `packages/fraud-verdict-store` — `putVerdict`, `listExcludedCids`, and the `fraud-verdicts` DynamoDB table (`infra/localstack` addition).
- `services/fraud-scorer` — fourth Kinesis consumer, built on `packages/kinesis-consumer-loop` (sub-project 5).
- Retrofit: `services/billing-api`'s `reconcileAndStore` calls `listExcludedCids` and passes a real set into `reconcileDate`, fulfilling the seam sub-project 5 left open.

## Explicitly out of scope

- **Device fingerprinting.** The doc says "per IP/device fingerprint"; device fingerprinting is a client-side SDK/technology choice this backend implementation doesn't control or receive data for. Velocity checking here is IP-only. `ponytail: IP-only velocity; add a fingerprint dimension if a client-side fingerprinting SDK is ever wired up.`
- **The landing-page beacon** ("requiring a follow-up landing-page beacon before a click is eligible to be billable"). This implies a whole additional API surface — an endpoint the advertiser's landing page calls back to confirm a real page render happened — that doc §04's API design never lists. Building it means inventing a new authenticated public endpoint and a client-side integration contract from scratch, a materially bigger scope than reading two existing request headers. Preview-bot detection here is header/user-agent-based only; the beacon-confirmation half of the doc's own mitigation is a real gap, named rather than quietly built partially.
- **A suspicious-verdict review workflow.** The doc: "Trust-and-safety policy... is a business decision this pipeline makes available fast, not one it hardcodes." No review UI, no resolution API, no "unresolved by close" time logic — see "Exclusion policy" below for what ships instead and why.

## Why the sync checks retrofit `services/click-redirect` instead of being new code here

IP address and `User-Agent`/`Purpose` headers exist only at the HTTP request layer. Sub-project 2's `ClickEventSchema` and Kinesis payload never carried them — there was nothing for an async, Kinesis-only consumer to compute a velocity or preview-bot signal *from*. The signals have to be computed where the headers are (the redirect handler) and carried into the stream payload as enrichment; the async consumer (this sub-project) only ever reads flags that already exist by the time it sees a record. Nothing in sub-project 2 has been implemented yet, so extending its already-written plan is the same category of correction as the `landingUrl` and `peekClosedWindows` fixes, not scope creep into shipped code.

## Components

### `packages/fraud-signals`

```ts
export function isPreviewBotRequest(headers: { purpose?: string; userAgent?: string }): boolean
```
Pure — `purpose` matched case-insensitively against `prefetch`; `userAgent` matched against a fixed list of known preview-bot patterns (Slackbot, facebookexternalhit, Twitterbot, Discordbot, WhatsApp, iMessage's `Messages` prefetcher UA). No I/O, unit-tested directly.

```ts
export function createVelocityChecker(
  redis: RedisClientType,
  options?: { windowSeconds?: number; threshold?: number }
): { checkAndIncrement(ip: string): Promise<boolean> }
```
Fixed-window counter: `INCR velocity:{ip}`, `EXPIRE` set only on the first increment in a window (default 60s, threshold 20 — arbitrary but explicit starting values, the doc doesn't specify numbers and calls the check "coarse"). Returns `true` (flagged) once the count in the current window exceeds the threshold. Redis round-trip, same latency class as the existing dedup fast-path (sub-project 3, §6.1) already sitting in a hot path — this is not a new category of risk to the redirect's p99 budget.

### Retrofit: `packages/kinesis-publisher`

`publishClickEvent`'s signature grows an optional fourth-ish parameter (third positional):
```ts
export async function publishClickEvent(
  client: KinesisClient,
  streamName: string,
  event: ClickEvent,
  enrichment: { velocityFlag?: boolean; previewBot?: boolean } = {}
): Promise<void>
```
Backward compatible — `services/batch-ingest` (sub-project 2) never passes a fourth argument and is untouched; only `services/click-redirect` starts passing one.

### Retrofit: `services/click-redirect`

`ClickRedirectDeps.publish` changes shape from `(event) => Promise<void>` to `(event, enrichment) => Promise<void>`. The handler reads `req.headers['user-agent']` and `req.headers['purpose'] ?? req.headers['sec-purpose']`, calls `isPreviewBotRequest`, calls `velocityChecker.checkAndIncrement(req.ip)`, and passes both flags to `deps.publish` — after the redirect has already been sent, same as the existing enqueue call, so neither check can add latency to the response the user is waiting on. This is also the first thing in the whole project that gives `services/click-redirect` a Redis dependency it didn't have before.

### `packages/fraud-verdict-store`

```ts
export type Verdict = 'legitimate' | 'suspicious' | 'invalid';
export function scoreVerdict(enrichment: { velocityFlag?: boolean; previewBot?: boolean }): Verdict
export async function putVerdict(dynamo, params: { date: string; cid: string; verdict: Verdict }): Promise<void>
export async function listExcludedCids(dynamo, date: string): Promise<Set<string>>
```
`scoreVerdict` is pure and matches the doc's own table exactly: `previewBot` → `invalid` (a known-bot user agent is as close to "confirmed" as a rule-based check gets); `velocityFlag` alone → `suspicious`; neither → `legitimate`. `fraud-verdicts` table: PK `date` (String, `YYYY-MM-DD`), SK `cid` (String) — keyed by date, not just `cid`, specifically so `listExcludedCids` is a `Query` (cheap, indexed) instead of a table `Scan` (expensive, unbounded) — this is the same reasoning sub-project 5's tables use composite keys for. `date` is computed by the fraud-scorer at verdict-scoring wall-clock time, the same way sub-project 5's archiver computes its partition date — consistency between the two matters more than which specific convention was picked, since reconciling date D needs both the archive files and the excluded-cids set to agree on what "date D" means. `listExcludedCids` returns `cid`s where `verdict != 'legitimate'` — see "Exclusion policy" below for why both `suspicious` and `invalid` are excluded, not just `invalid`.

### `services/fraud-scorer`

Fourth Kinesis consumer on `ad-clicks-raw`, built on sub-project 5's `packages/kinesis-consumer-loop` — no dedup consultation (sub-project 3's spec already established why: a duplicate delivery of a fraudulent click is still evidence about that `cid`, and re-scoring the same `cid` twice with the same inputs is idempotent by construction, so there's nothing dedup would protect here). Per record: parse the enriched payload, `scoreVerdict(...)`, `putVerdict(dynamo, { date: todayDateString(), cid: event.cid, verdict })`.

### Exclusion policy

Reconciliation excludes both `suspicious` and `invalid` verdicts, not just `invalid`. The doc frames "suspicious, unresolved" as its own middle category with its own fate ("held pending review; excluded if unresolved by close") — but this pipeline builds no review mechanism to ever resolve a suspicious verdict into anything else, so "unresolved by close" is true of every suspicious verdict, unconditionally, by construction. Excluding it is the conservative default that follows from that, not a decision to treat suspicious as equivalent to confirmed-invalid in general. `ponytail: exclude suspicious + invalid, no resolution path exists yet; add a resolution mechanism (and a policy of which verdicts survive it) if suspicious clicks turn out to be common enough that blanket exclusion costs real revenue.`

## Testing

- `isPreviewBotRequest`: pure unit tests — `Purpose: prefetch` header flags true; each known bot user-agent pattern flags true; an ordinary browser user-agent with no `Purpose` header flags false.
- `createVelocityChecker`: integration against LocalStack Redis — under the threshold returns `false` each call; the call that pushes the count over threshold (and every one after, within the window) returns `true`; after the window elapses (test with a short window, e.g. `windowSeconds: 1`, and a real ~1.1s wait) the counter resets and a fresh call returns `false` again.
- `scoreVerdict`: pure unit tests — the three doc-table rows, plus both flags set simultaneously (previewBot wins — `invalid`, matching "if it's a confirmed bot, the velocity signal doesn't need to also apply").
- `fraud-verdict-store`: integration against LocalStack DynamoDB — `putVerdict` for a `legitimate`, a `suspicious`, and an `invalid` cid on the same date; `listExcludedCids` for that date returns exactly the suspicious and invalid ones, not the legitimate one; a different date returns an empty set.
- `services/click-redirect` retrofit: extends the existing dependency-injected `app.test.ts` — a request with a Slackbot user-agent still redirects (sync checks never block, per the doc) but `publish` is called with `{ previewBot: true }`; a request that pushes the fake velocity checker over threshold gets `{ velocityFlag: true }`; an ordinary request gets an enrichment object with neither flag set. This retrofit also breaks one existing sub-project 2 assertion: `publish` becomes a two-argument call (`event, enrichment`), and Vitest's `toHaveBeenCalledWith` matches the full argument list, not a prefix — the existing "redirects and publishes" test's `toHaveBeenCalledWith(expect.objectContaining({ cid, ad_id }))` needs a second matcher argument added (`expect.any(Object)` or the specific expected enrichment), not just new tests layered on top. Flagged explicitly so the plan fixes the existing assertion instead of leaving it to fail.
- `services/fraud-scorer`: no new integration test beyond `kinesis-consumer-loop`'s own (sub-project 5) and `scoreVerdict`/`fraud-verdict-store`'s unit/integration coverage — the same reasoning sub-project 3 used for its `poll.ts` and sub-project 5 used for its `run.ts`: the composition itself is thin wiring, proven correct by its parts.
- `services/billing-api` retrofit: one updated test — `reconcileAndStore` calls `listExcludedCids` before `reconcileDate` and passes its result through; a fake `listExcludedCids` returning a known set proves it reaches `reconcileDate` unmodified.

## What later sub-projects assume exists after this one

None — this is the last planned sub-project. Every forward-reference left open earlier is closed: sub-project 3's dedup-independence note, and sub-project 5's `excludedCids` seam, both resolve here.

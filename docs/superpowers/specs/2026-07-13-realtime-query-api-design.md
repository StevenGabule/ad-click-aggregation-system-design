# Sub-project 4: Real-time query API

**Status:** approved (autonomous — see sub-project 1 spec for why) · **Date:** 2026-07-13

## Purpose

`GET /v1/ads/:adId/aggregates` (doc §04) — the read side of the fast path. Resolves an API key to its owning advertiser, enforces that an advertiser can only ever read their own ads' data (§01: "scoped so one advertiser can never read another's click data"), and serves the latest window from sub-project 3's `click-aggregates` table.

## In scope

- Two additions to `@app/db`: `resolveApiKey` (bearer token → owning advertiser) and `getAdOwnerAdvertiserId` (ad → owning advertiser, for tenant-scoping — not the same query as sub-project 2's `listActiveAdDirectory`, see below).
- One addition to `@app/hot-aggregate-store`: `getLatestAggregate` (the read counterpart to `flush`).
- `services/query-api` — the Fastify service serving the one endpoint.

## Explicitly out of scope

- `GET /v1/campaigns/:campaignId/statement` and `POST /v1/reconciliation/:date/rerun` — sub-project 5, they read from the exact/batch side, not `click-aggregates`.
- Rate limiting, pagination, or a `?window=` param for querying historical (non-latest) windows — the doc's example shows exactly one window in the response; nothing here asks for more than the latest.

## Why this doesn't reuse sub-project 2's directory cache

`@app/directory-cache` (sub-project 2) wraps `listActiveAdDirectory`, which only returns ads on **active** campaigns — correct for the redirect hot path, where a paused campaign's ad genuinely shouldn't be clickable. This endpoint has the opposite requirement: an advertiser querying a *paused* campaign's recent numbers is a completely normal request, not an error. Reusing the active-only directory here would 404 a legitimate query. `getAdOwnerAdvertiserId` is a new, unfiltered lookup for exactly that reason — same shape of problem, different filter, so it's a different function rather than a shared one with a flag bolted on.

This endpoint also has no p99 <100ms budget (that's specific to the click redirect, doc §02) — the doc's real-time NFR is 5s end-to-end aggregate *visibility*, not this query's own response time. A direct Postgres read per request is fine here; there's no need for the redirect service's in-memory polling cache.

## Components

### `@app/db` additions

```ts
export async function getAdOwnerAdvertiserId(client: PrismaClient, adId: string): Promise<string | null>
export async function resolveApiKey(client: PrismaClient, rawKey: string): Promise<{ advertiserId: string } | null>
```

`resolveApiKey` hashes `rawKey` with `sha256` (matching the convention sub-project 1's seed script already uses for storage) and looks up `ApiKey` by `hashedKey`, returning `null` if not found or if `revokedAt` is set. The hash call is one stdlib line duplicated between the seed script and here rather than factored into a shared helper — there's no correctness subtlety in `sha256(x)` that the two call sites could drift apart on, unlike the HMAC canonicalization in `@app/click-signature`, which genuinely does need one shared implementation. `ponytail: inline sha256 call, not a shared package — extract only if a third call site or a hashing-scheme change makes duplication actually costly.`

### `@app/hot-aggregate-store` addition

```ts
export async function getLatestAggregate(
  dynamo: DynamoDBClient,
  adId: string,
  tableName?: string
): Promise<{ windowStart: number; clicks: number } | null>
```

`Query` on the table's existing composite key, `ScanIndexForward: false, Limit: 1` — the most recent `windowStart` for that `adId`. Returns `null` if the ad has no rows yet (a real, non-error state: an ad with no clicks in the retained window simply never had anything flushed for it).

### `services/query-api` — `GET /v1/ads/:adId/aggregates`

```
1. Authorization: Bearer <key> header missing or malformed → 401 { error: "unauthorized" }
2. resolveApiKey(rawKey) → null → 401 { error: "unauthorized" }
3. getAdOwnerAdvertiserId(adId) → null → 404 { error: "not_found" }
4. ownerAdvertiserId !== apiKey.advertiserId → 404 { error: "not_found" }   ← same response as step 3, deliberately
5. getLatestAggregate(adId) → row or null → build response
```

Steps 3 and 4 return the identical `404` for the same reason sub-project 2's redirect handler collapses its failure modes into one `400`: a distinct "403 forbidden, this ad exists but isn't yours" would let a caller enumerate other advertisers' ad IDs by the response code alone.

Response shape (matches the doc's example exactly):
```json
{
  "adId": "ad_881203",
  "windowStart": "2026-07-12T09:14:00Z",
  "clicks": 842,
  "exact": false,
  "asOf": "2026-07-12T09:14:58Z"
}
```

When `getLatestAggregate` returns `null` (no clicks recorded yet for this ad), the response substitutes the current window (`Math.floor(Date.now()/60000)*60000`, matching sub-project 3's `windowMs`) and `clicks: 0` — a real, honest "nothing yet" answer rather than a 404, since the ad itself is valid and owned by the caller.

## Testing

- `getAdOwnerAdvertiserId`: integration against Postgres — returns the advertiser ID for an ad on any campaign status (active or paused, unlike the directory cache), `null` for an unknown ad ID.
- `resolveApiKey`: integration against Postgres — a freshly-created key resolves to its advertiser; a revoked key (`revokedAt` set) and an unknown key both resolve to `null`.
- `getLatestAggregate`: integration against LocalStack DynamoDB — after a `flush()` (reusing sub-project 3's writer), returns that window; an `adId` with no rows returns `null`.
- `services/query-api` app: dependency-injected fakes, six cases — valid key + own ad with data (200, correct shape); valid key + own ad with no data yet (200, `clicks: 0`, current window); valid key + another advertiser's ad (404); valid key + nonexistent ad (404, identical body to the previous case); missing/malformed `Authorization` header (401); unknown/revoked key (401).

## What later sub-projects assume exists after this one

- Nothing downstream depends on this sub-project — it's a pure read leaf. Sub-project 5 (billing/statements) reads from a different table (the exact/batch side) and doesn't call anything defined here.

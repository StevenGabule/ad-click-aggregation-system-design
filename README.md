# Ad Click Aggregation

Turning a firehose of retried, delayed, and occasionally fraudulent ad-click pings into a number an advertiser can be **billed on — and trust**.

Every click a user makes must become **exactly one** row on an invoice — not zero (the network dropped it), not two (a retry duplicated it), and not one if the "user" turns out to be a bot. This repository contains both the **system design** for that pipeline and a **working, runnable implementation** of it — the whole thing runs on your laptop with no AWS account and no cloud cost.

---

## Two documents, one system

| File | What it is |
| --- | --- |
| **[`index.html`](index.html)** | The **system-design writeup** — requirements, capacity estimation, API design, deep dives (idempotency, late data, hot keys, fraud, sketches), trade-offs, cost model. Open it in a browser; read it like a technical blog post. |
| **[`guide.html`](guide.html)** | A **hands-on beginner's guide** — assumes no monorepo or AWS background. Explains every concept, walks the full local setup, and traces one real click through the entire pipeline. **Start here if you want to run it.** |
| **The code** | The design, actually built — under `packages/`, `services/`, and `infra/`. |

---

## Architecture at a glance

The core idea: **two parallel lanes off the same event stream**, because dashboards and invoices want incompatible things.

```
                          ┌──────────────────────────────────────────────┐
                FAST LANE │  aggregator: dedup → 1-min windows → counts   │  seconds old,
                (approx.) │  (allowed to be slightly wrong)               │  for dashboards
                          └───────▲───────────────────────┬──────────────┘
 user clicks ad                   │                       ▼
      │                           │                ┌────────────┐    ┌───────────┐
      ▼                           │                │  DynamoDB  │◀───│ query-api │◀── dashboard
┌────────────────┐       ┌────────┴────────┐       │ aggregates │    └───────────┘
│ click-redirect │──────▶│  Kinesis stream │       └────────────┘
│  302 first,    │       │  ad-clicks-raw  │
│  count later   │       └────────┬────────┘
└────────────────┘                │
                          ┌───────▼────────────────────────────────────────┐
               EXACT LANE │  archiver: EVERY raw event → S3 as Parquet      │  hours old,
               (billing)  │  (duplicates included — on purpose)             │  money-grade
                          └───────┬────────────────────────────────────────┘
                                  ▼
                   reconcile: DuckDB COUNT(DISTINCT cid) over S3
                                  ▼
                     statements table ──▶ billing-api ──▶ invoice
```

- **Fast lane** — dedups at ingest, counts into watermarked 1-minute windows, serves an *approximate* number in seconds (`"exact": false`). Dashboards and budget pacing read this.
- **Exact lane** — archives *every* raw event (duplicates and all), then recomputes billing with `COUNT(DISTINCT cid)` at query time, so replays are idempotent and auditable (`"exact": true`). Invoices read this.
- **Exactly-once** hinges on one decision: the click id (`cid`) is minted **client-side, before the first retryable hop**, so a retry resends the *same* id and downstream dedup can recognize it.

---

## Tech stack

**Runtime:** Node.js 24 · TypeScript (strict, ESM) · pnpm workspaces (monorepo)
**Web:** Fastify 5 · Zod (validation)
**Storage:** PostgreSQL + Prisma (control plane) · Redis (dedup fast-path) · DuckDB (Parquet reconciliation)
**AWS (emulated locally via [LocalStack](https://localstack.cloud)):** Kinesis Data Streams · DynamoDB · S3
**Local infra:** Docker Compose (via colima on macOS)
**Testing:** Vitest — 85 tests (pure unit + integration against the live containers)

> The code uses the real AWS SDK throughout. Local vs. real AWS is a single environment variable (`AWS_ENDPOINT_URL`) — there is deliberately no `if (isLocal)` branching anywhere.

---

## Quick start

**Prerequisites:** Node ≥ 24, pnpm ≥ 9, and Docker + colima (`brew install colima docker docker-compose`).

```bash
# 1. Start the container runtime and the local databases
colima start
cp .env.example .env
docker compose up -d          # LocalStack + Postgres + Redis; wait until all 3 are healthy

# 2. Install and build the monorepo
pnpm install                  # also generates the Prisma client (postinstall)
pnpm -r build                 # compile every package's dist/ (required before running services)

# 3. Create the local AWS resources (idempotent)
pnpm --filter @app/infra-localstack bootstrap

# 4. Apply the DB schema, then seed demo data (prints a demo API key — save it)
DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db exec prisma migrate deploy
DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db seed

# 5. Verify — run the full suite (unit + integration)
DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm -r test
```

To actually **fire a click and watch it flow** through Redis → S3 → DynamoDB → a billing statement, follow the step-by-step demo in **[`guide.html`](guide.html) §06**. Two macOS gotchas worth knowing up front are covered there too (the `docker compose` plugin symlink, and Homebrew Postgres squatting on port 5432).

---

## Repository layout

```
packages/                     ── libraries (imported, never "run") ──
  event-schema/          The one true shape of a click event (Zod). Everyone imports this.
  config/                loadEnv(): typed, validated environment variables.
  db/                    Prisma schema + queries (advertisers/campaigns/ads/API keys) + seed.
  click-signature/       HMAC sign/verify for click URLs (constant-time compare).
  directory-cache/       In-memory 30s-refresh cache: ad → signing secret + landing URL.
  kinesis-publisher/     publishClickEvent(): the one place shard-salting lives.
  click-dedup/           isNew(cid): Redis fast-path + DynamoDB backstop. Exactly-once's heart.
  windowed-aggregator/   Pure 1-min windowing with a 2-min watermark for late events.
  hot-aggregate-store/   DynamoDB atomic-ADD counter writes + latest-window read.
  kinesis-consumer-loop/ Shared, abortable polling loop all stream consumers use.
  parquet-archive/       DuckDB: write Parquet to S3 + the COUNT(DISTINCT) billing query.
  statements-store/      Billing statements table: full-overwrite writes (never increment).

services/                     ── runnable processes ──
  click-redirect/   :3000  GET /click — verify sig, 302 the user, THEN publish the event.
  batch-ingest/     :3001  POST /v1/events/clicks — bulk upload for offline mobile SDKs.
  aggregator/       (consumer) dedup → window → flush counts to DynamoDB. Fast lane.
  archiver/         (consumer) write EVERY raw event to S3 Parquet. Exact lane.
  query-api/        :3002  GET /v1/ads/:adId/aggregates — dashboard read (approximate).
  billing-api/      :3003  GET statement (exact) + POST reconciliation rerun (ops-only).

infra/localstack/         bootstrap: Kinesis stream, 3 DynamoDB tables, S3 bucket.
docs/superpowers/         Design specs and build plans, one pair per sub-project.
```

Each `service` splits into a pure, dependency-injected `buildApp(deps)` (fully unit-tested with fakes — no real infrastructure) and a thin `server.ts` that wires real clients (deliberately untested).

---

## Services & endpoints

| Service | Port | Endpoint / role |
| --- | --- | --- |
| `click-redirect` | 3000 | `GET /click?cid&ad_id&campaign_id&pub_id&ts&sig&r` — 302 redirect, then fire-and-forget publish |
| `batch-ingest` | 3001 | `POST /v1/events/clicks` — batch of signed events, partial accept/reject |
| `query-api` | 3002 | `GET /v1/ads/:adId/aggregates` — real-time approximate count (Bearer auth, tenant-scoped) |
| `billing-api` | 3003 | `GET /v1/campaigns/:campaignId/statement` (Bearer) · `POST /v1/reconciliation/:date/rerun` (`X-Ops-Token`) |
| `aggregator` | — | Kinesis consumer (one process per shard): dedup → window → flush |
| `archiver` | — | Kinesis consumer (one process per shard): raw → S3 Parquet |

Both read APIs enforce **tenant isolation**: an advertiser can never read — or even detect the existence of — another advertiser's data (nonexistent and not-yours both return a byte-identical `404` from a single code branch).

---

## Testing

```bash
DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm -r test        # everything (85 tests)
pnpm --filter @app/windowed-aggregator test                             # one package (unit, no infra)
DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db test   # one package (integration)
```

Integration tests require the Docker containers to be up (`docker compose ps`). Pure-unit packages (event-schema, click-signature, windowed-aggregator, archiver core) run with no infrastructure.

---

## Project status

The backend was built in six sub-projects. **Sub-projects 1–5 are implemented and merged** (this repository); sub-project 6 is designed and planned but not yet built.

| # | Sub-project | Status |
| --- | --- | --- |
| 1 | Foundations & control plane (monorepo, infra, Prisma) | ✅ Implemented |
| 2 | Ingestion APIs (click redirect + batch) | ✅ Implemented |
| 3 | Dedup + real-time aggregation | ✅ Implemented |
| 4 | Real-time query API | ✅ Implemented |
| 5 | Archive + reconciliation + billing | ✅ Implemented |
| 6 | Fraud scoring | 📐 Specced & planned, not built |

Known deferrals (tracked, out of scope for a local-first build): consumer resilience (Kinesis checkpointing / retry-with-backoff), the fire-and-forget publish outbox, and fraud exclusion (the reconciliation query already has a parameterized exclusion seam waiting for sub-project 6). See each sub-project's spec for the full rationale.

---

## Documentation

- **[`index.html`](index.html)** — the system-design document (the "why" and the numbers).
- **[`guide.html`](guide.html)** — the hands-on guide: concepts primer, full setup, a click's journey, troubleshooting, glossary.
- **[`CLAUDE.md`](CLAUDE.md)** — contributor notes and the bootstrap sequence.
- **`docs/superpowers/specs/`** — per-sub-project design specs (what to build and why).
- **`docs/superpowers/plans/`** — per-sub-project TDD implementation plans.

---

## Notes

This is a **reference implementation** for learning and demonstration. Capacity and cost figures in the design doc are illustrative — scale them to real traffic before treating any number as a target. Everything runs locally against emulated AWS; deploying to real AWS is a configuration change, not a rewrite, but productionizing (IaC, checkpointing, autoscaling) is intentionally out of scope.

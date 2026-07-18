# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single self-contained `index.html` file: an interactive system-design writeup ("Ad Click Aggregation") styled as a technical doc/blog post. There is no build system, package manager, linter, or test suite — everything (CSS, fonts, JS) is inlined into the one file so it works fully offline.

## Commands

None. Preview by opening `index.html` directly in a browser, or serve it locally:

```
python3 -m http.server
```

## Structure of `index.html`

- `<style>` block (top): all CSS, including `@font-face` rules with base64-encoded woff2 fonts. Theming uses CSS custom properties defined in `:root`, with `prefers-color-scheme: dark` as the default signal and `:root[data-theme="dark"]` / `:root[data-theme="light"]` overrides that win in both directions.
- `<header class="masthead">`: title, subhead, stat strip, and tech chips.
- `<nav class="toc">`: table of contents + the Light/Dark/Auto theme toggle buttons.
- `<main id="content">`: the numbered `<section id="...">` elements that hold the actual content.
- `<script>` block (bottom), one IIFE with three independent behaviors:
  - Theme toggle, persisted to `localStorage` under `ad-click-aggregation-doc-theme`.
  - Scroll-spy that highlights the active `nav.toc` link via `IntersectionObserver`, matched 1:1 against sections by array index.
  - The §6.1 "dedup ledger" demo — an in-memory `Set` simulating exactly-once click counting (fire/retry/reset buttons).

## Conventions to preserve when editing

- Every content `<section id="X">` must have a matching `<a href="#X">` entry in `nav.toc`, in the same document order — the scroll-spy JS pairs them by index, so an added/removed/reordered section needs the TOC updated in lockstep.
- Top-level sections are numbered `00`–`09` via `<span class="section-num">`; deep-dive subsections under §06 are numbered `6.1`–`6.7` via `<span class="sub-num">`. Keep numbering sequential when adding, removing, or reordering sections.
- No external resources — no CDN links, no `<link rel="stylesheet">`, no `<script src>`. Keep any additions inlined so the file stays a single portable artifact.

## Content

The doc follows a system-design-interview structure: overview → functional/non-functional requirements → capacity estimation → API design → high-level design → deep dives (idempotency, late data, hot keys, fraud, sketches, Node.js consumer architecture, backfill) → trade-off summary → cost model → closing notes. The stack described is Node.js on AWS (Kinesis Data Streams, DynamoDB, ElastiCache Redis, S3 + Athena). When editing content, keep the numbers in the masthead `stat-strip` consistent with whatever the capacity-estimation and cost-model sections claim.

## Backend monorepo (ad-click-aggregation pipeline)

This repo also contains a pnpm/TypeScript backend monorepo (separate from `index.html`) implementing the design under `packages/` (`@app/db` — Prisma/Postgres, `@app/config`, `@app/event-schema`) and `infra/` (`@app/infra-localstack` — LocalStack bootstrap), with more `services/` packages to follow as the pipeline is built out. Local bootstrap sequence:

1. `docker compose up -d` — starts LocalStack, Postgres, and Redis.
2. `pnpm install` — installs workspace deps; `@app/db` has a `postinstall` script that auto-runs `prisma generate`, so the Prisma client is ready immediately (no manual step needed, and no DB connection required for this).
3. `pnpm --filter @app/infra-localstack bootstrap` — creates the Kinesis stream in LocalStack.
4. Against a fresh database, apply migrations: `DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db exec prisma migrate deploy` (already applied on the standard dev DB).
5. Seed demo data: `DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db seed`.

`@app/db` tests need `DATABASE_URL` set inline on the command (e.g. `DATABASE_URL=postgresql://app:app@localhost:5432/app pnpm --filter @app/db test`) because Prisma Client reads it from `process.env` at runtime and does not auto-load `.env` files.

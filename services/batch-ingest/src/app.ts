import Fastify, { type FastifyInstance } from 'fastify';
import { ClickEventSchema, type ClickEvent } from '@app/event-schema';
import { verifySignature } from '@app/click-signature';
import type { AdDirectoryEntry } from '@app/db';

export interface BatchIngestDeps {
  directoryCache: { lookup(adId: string): AdDirectoryEntry | undefined };
  publish: (event: ClickEvent) => Promise<void>;
}

const MAX_BATCH_SIZE = 1000;
const PUBLISH_CONCURRENCY = 20;

async function processEvent(deps: BatchIngestDeps, raw: unknown): Promise<boolean> {
  const parsed = ClickEventSchema.safeParse(raw);
  if (!parsed.success) return false;

  const event = parsed.data;
  const entry = deps.directoryCache.lookup(event.ad_id);
  if (!entry) return false;
  if (!verifySignature(entry.signingSecret, event, event.sig)) return false;

  try {
    await deps.publish(event);
    return true;
  } catch {
    return false;
  }
}

// ponytail: chunked pool bounds in-flight PutRecords; swap for a sliding-window limiter if throughput demands
async function processAllBounded(deps: BatchIngestDeps, events: unknown[]): Promise<boolean[]> {
  const results: boolean[] = [];
  for (let i = 0; i < events.length; i += PUBLISH_CONCURRENCY) {
    const chunk = events.slice(i, i + PUBLISH_CONCURRENCY);
    results.push(...(await Promise.all(chunk.map((event) => processEvent(deps, event)))));
  }
  return results;
}

export function buildApp(deps: BatchIngestDeps): FastifyInstance {
  const app = Fastify({ logger: true });

  app.post('/v1/events/clicks', async (req, reply) => {
    const body = req.body as { events?: unknown[] } | undefined;
    const events = Array.isArray(body?.events) ? body.events : [];

    if (events.length > MAX_BATCH_SIZE) {
      return reply.code(413).send({ error: 'batch_too_large', maxBatchSize: MAX_BATCH_SIZE });
    }

    const results = await processAllBounded(deps, events);
    const accepted = results.filter(Boolean).length;

    reply.code(202).send({ accepted, rejected: results.length - accepted });
  });

  return app;
}

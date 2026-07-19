import Fastify, { type FastifyInstance } from 'fastify';
import { ClickEventSchema, type ClickEvent } from '@app/event-schema';
import { verifySignature } from '@app/click-signature';
import type { AdDirectoryEntry } from '@app/db';

export interface BatchIngestDeps {
  directoryCache: { lookup(adId: string): AdDirectoryEntry | undefined };
  publish: (event: ClickEvent) => Promise<void>;
}

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

export function buildApp(deps: BatchIngestDeps): FastifyInstance {
  const app = Fastify({ logger: true });

  app.post('/v1/events/clicks', async (req, reply) => {
    const body = req.body as { events?: unknown[] } | undefined;
    const events = Array.isArray(body?.events) ? body.events : [];

    const results = await Promise.all(events.map((event) => processEvent(deps, event)));
    const accepted = results.filter(Boolean).length;

    reply.code(202).send({ accepted, rejected: results.length - accepted });
  });

  return app;
}

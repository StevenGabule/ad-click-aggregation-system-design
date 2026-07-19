import Fastify, { type FastifyInstance } from 'fastify';
import { ClickEventSchema, type ClickEvent } from '@app/event-schema';
import { verifySignature } from '@app/click-signature';
import type { AdDirectoryEntry } from '@app/db';

export interface ClickRedirectDeps {
  directoryCache: { lookup(adId: string): AdDirectoryEntry | undefined };
  publish: (event: ClickEvent) => Promise<void>;
}

export function buildApp(deps: ClickRedirectDeps): FastifyInstance {
  const app = Fastify({ logger: true });

  app.get('/click', async (req, reply) => {
    const query = req.query as Record<string, unknown>;
    const parsed = ClickEventSchema.safeParse(query);
    const rawR = typeof query.r === 'string' ? query.r : undefined;

    if (!parsed.success || !rawR) {
      return reply.code(400).send({ error: 'invalid_request' });
    }

    const event = parsed.data;
    const entry = deps.directoryCache.lookup(event.ad_id);
    if (!entry) {
      req.log.warn({ adId: event.ad_id }, 'unknown ad_id');
      return reply.code(400).send({ error: 'invalid_request' });
    }

    if (!verifySignature(entry.signingSecret, event, event.sig)) {
      req.log.warn({ cid: event.cid }, 'signature verification failed');
      return reply.code(400).send({ error: 'invalid_request' });
    }

    let decodedR: string;
    try {
      decodedR = decodeURIComponent(rawR);
    } catch {
      return reply.code(400).send({ error: 'invalid_request' });
    }

    if (decodedR !== entry.landingUrl) {
      req.log.warn({ cid: event.cid }, 'landing url mismatch');
      return reply.code(400).send({ error: 'invalid_request' });
    }

    reply.redirect(entry.landingUrl, 302);

    setImmediate(() => {
      deps.publish(event).catch((err) => req.log.error({ err, cid: event.cid }, 'click enqueue failed'));
    });
  });

  return app;
}

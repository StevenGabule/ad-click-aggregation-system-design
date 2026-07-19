import Fastify, { type FastifyInstance } from 'fastify';

export interface QueryApiDeps {
  resolveApiKey(rawKey: string): Promise<{ advertiserId: string } | null>;
  getAdOwner(adId: string): Promise<string | null>;
  getLatestAggregate(adId: string): Promise<{ windowStart: number; clicks: number } | null>;
}

const WINDOW_MS = 60_000;

export function buildApp(deps: QueryApiDeps): FastifyInstance {
  const app = Fastify({ logger: true });

  app.get<{ Params: { adId: string } }>('/v1/ads/:adId/aggregates', async (req, reply) => {
    const authHeader = req.headers.authorization;
    const rawKey = authHeader?.startsWith('Bearer ') ? authHeader.slice('Bearer '.length) : undefined;
    if (!rawKey) {
      return reply.code(401).send({ error: 'unauthorized' });
    }

    const apiKey = await deps.resolveApiKey(rawKey);
    if (!apiKey) {
      return reply.code(401).send({ error: 'unauthorized' });
    }

    const { adId } = req.params;
    const ownerAdvertiserId = await deps.getAdOwner(adId);
    if (!ownerAdvertiserId || ownerAdvertiserId !== apiKey.advertiserId) {
      return reply.code(404).send({ error: 'not_found' });
    }

    const latest = await deps.getLatestAggregate(adId);
    const now = Date.now();
    const windowStart = latest?.windowStart ?? Math.floor(now / WINDOW_MS) * WINDOW_MS;

    reply.send({
      adId,
      windowStart: new Date(windowStart).toISOString(),
      clicks: latest?.clicks ?? 0,
      exact: false,
      asOf: new Date(now).toISOString(),
    });
  });

  return app;
}

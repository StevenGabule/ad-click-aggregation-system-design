import Fastify, { type FastifyInstance } from 'fastify';

export interface BillingApiDeps {
  opsToken: string;
  resolveApiKey(rawKey: string): Promise<{ advertiserId: string } | null>;
  getCampaignOwner(campaignId: string): Promise<string | null>;
  getStatement(campaignId: string, period?: string): Promise<{
    campaignId: string; period: string; billedClicks: number; excludedInvalidClicks: number;
    reconciledAt: string; sourceArchive: string;
  } | null>;
  reconcileAndStore(date: string): Promise<number>;
}

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

export function buildApp(deps: BillingApiDeps): FastifyInstance {
  const app = Fastify({ logger: true });

  app.get<{ Params: { campaignId: string }; Querystring: { period?: string } }>(
    '/v1/campaigns/:campaignId/statement',
    async (req, reply) => {
      const authHeader = req.headers.authorization;
      const rawKey = authHeader?.startsWith('Bearer ') ? authHeader.slice('Bearer '.length) : undefined;
      if (!rawKey) return reply.code(401).send({ error: 'unauthorized' });

      const apiKey = await deps.resolveApiKey(rawKey);
      if (!apiKey) return reply.code(401).send({ error: 'unauthorized' });

      const { campaignId } = req.params;
      const ownerAdvertiserId = await deps.getCampaignOwner(campaignId);
      if (!ownerAdvertiserId || ownerAdvertiserId !== apiKey.advertiserId) {
        return reply.code(404).send({ error: 'not_found' });
      }

      const statement = await deps.getStatement(campaignId, req.query.period);
      if (!statement) return reply.code(404).send({ error: 'not_found' });

      reply.send({ ...statement, exact: true });
    }
  );

  app.post<{ Params: { date: string } }>('/v1/reconciliation/:date/rerun', async (req, reply) => {
    const opsToken = req.headers['x-ops-token'];
    if (opsToken !== deps.opsToken) return reply.code(401).send({ error: 'unauthorized' });

    const { date } = req.params;
    if (!DATE_PATTERN.test(date)) return reply.code(400).send({ error: 'invalid_date' });

    const campaignsReconciled = await deps.reconcileAndStore(date);
    reply.send({ date, campaignsReconciled });
  });

  return app;
}

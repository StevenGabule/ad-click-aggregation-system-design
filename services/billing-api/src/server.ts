import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { prisma, getCampaignOwnerAdvertiserId, resolveApiKey } from '@app/db';
import { getStatement, putStatement } from '@app/statements-store';
import { createArchiveDb, reconcileDate, bucketPrefixForDate } from '@app/parquet-archive';
import { loadEnv } from '@app/config';
import { buildApp } from './app.js';

const env = loadEnv();
const opsToken = process.env.OPS_TOKEN;
if (!opsToken) throw new Error('OPS_TOKEN is required');

const dynamo = new DynamoDBClient({
  region: env.AWS_REGION,
  endpoint: env.AWS_ENDPOINT_URL,
  // ponytail: LocalStack dummy creds; swap for the default AWS credential provider chain when targeting real AWS
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});
const archiveDb = await createArchiveDb(env);

const app = buildApp({
  opsToken,
  resolveApiKey: (rawKey) => resolveApiKey(prisma, rawKey),
  getCampaignOwner: (campaignId) => getCampaignOwnerAdvertiserId(prisma, campaignId),
  getStatement: (campaignId, period) => getStatement(dynamo, campaignId, period),
  reconcileAndStore: async (date) => {
    const prefix = bucketPrefixForDate(date);
    const results = await reconcileDate(archiveDb, prefix);
    const reconciledAt = new Date().toISOString();
    for (const result of results) {
      await putStatement(dynamo, { ...result, period: date, reconciledAt, sourceArchive: prefix });
    }
    return results.length;
  },
});

await app.listen({ port: Number(process.env.PORT ?? 3003), host: '0.0.0.0' });

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { prisma, getCampaignOwnerAdvertiserId, resolveApiKey } from '@app/db';
import { getStatement, putStatement } from '@app/statements-store';
import { createArchiveDb, reconcileDate, bucketPrefixForDate } from '@app/parquet-archive';
import { listExcludedCids } from '@app/fraud-verdict-store';
import { loadEnv } from '@app/config';
import { buildApp } from './app.js';
import { reconcileAndStore } from './reconciliation.js';

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
  reconcileAndStore: (date) => reconcileAndStore({
    bucketPrefixForDate,
    listExcludedCids: (d) => listExcludedCids(dynamo, d),
    reconcileDate: (prefix, excludedCids) => reconcileDate(archiveDb, prefix, excludedCids),
    putStatement: (statement) => putStatement(dynamo, statement),
  }, date),
});

await app.listen({ port: Number(process.env.PORT ?? 3003), host: '0.0.0.0' });

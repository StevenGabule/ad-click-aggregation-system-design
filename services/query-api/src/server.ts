import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { prisma, getAdOwnerAdvertiserId, resolveApiKey } from '@app/db';
import { getLatestAggregate } from '@app/hot-aggregate-store';
import { loadEnv } from '@app/config';
import { buildApp } from './app.js';

const env = loadEnv();
const dynamo = new DynamoDBClient({
  region: env.AWS_REGION,
  endpoint: env.AWS_ENDPOINT_URL,
  // ponytail: LocalStack dummy creds; swap for the default AWS credential provider chain when targeting real AWS
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});

const app = buildApp({
  resolveApiKey: (rawKey) => resolveApiKey(prisma, rawKey),
  getAdOwner: (adId) => getAdOwnerAdvertiserId(prisma, adId),
  getLatestAggregate: (adId) => getLatestAggregate(dynamo, adId),
});

await app.listen({ port: Number(process.env.PORT ?? 3002), host: '0.0.0.0' });

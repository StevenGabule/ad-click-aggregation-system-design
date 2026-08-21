import { KinesisClient } from '@aws-sdk/client-kinesis';
import { createClient as createRedisClient } from 'redis';
import { prisma, listActiveAdDirectory } from '@app/db';
import { createDirectoryCache } from '@app/directory-cache';
import { publishClickEvent, CLICK_STREAM_NAME } from '@app/kinesis-publisher';
import { createVelocityChecker } from '@app/fraud-signals';
import { loadEnv } from '@app/config';
import { buildApp } from './app.js';

const env = loadEnv();
const kinesis = new KinesisClient({
  region: env.AWS_REGION,
  endpoint: env.AWS_ENDPOINT_URL,
  // ponytail: LocalStack dummy creds; swap for the default AWS credential provider chain when targeting real AWS
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});
const redis = createRedisClient({ url: env.REDIS_URL });
await redis.connect();

const directoryCache = createDirectoryCache(() => listActiveAdDirectory(prisma));
await directoryCache.start();
const velocityChecker = createVelocityChecker(redis);

const app = buildApp({
  directoryCache,
  velocityChecker,
  publish: (event, enrichment) => publishClickEvent(kinesis, CLICK_STREAM_NAME, event, enrichment),
});

await app.listen({ port: Number(process.env.PORT ?? 3000), host: '0.0.0.0' });

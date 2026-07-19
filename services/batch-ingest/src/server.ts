import { KinesisClient } from '@aws-sdk/client-kinesis';
import { prisma, listActiveAdDirectory } from '@app/db';
import { createDirectoryCache } from '@app/directory-cache';
import { publishClickEvent, CLICK_STREAM_NAME } from '@app/kinesis-publisher';
import { loadEnv } from '@app/config';
import { buildApp } from './app.js';

const env = loadEnv();
const kinesis = new KinesisClient({
  region: env.AWS_REGION,
  endpoint: env.AWS_ENDPOINT_URL,
  // ponytail: LocalStack dummy creds; swap for the default AWS credential provider chain when targeting real AWS
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});

const directoryCache = createDirectoryCache(() => listActiveAdDirectory(prisma));
await directoryCache.start();

const app = buildApp({
  directoryCache,
  publish: (event) => publishClickEvent(kinesis, CLICK_STREAM_NAME, event),
});

await app.listen({ port: Number(process.env.PORT ?? 3001), host: '0.0.0.0' });

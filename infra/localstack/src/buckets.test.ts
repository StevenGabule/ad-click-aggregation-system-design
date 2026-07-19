import { describe, expect, it } from 'vitest';
import { S3Client, HeadBucketCommand } from '@aws-sdk/client-s3';
import { ensureRawArchiveBucket, RAW_ARCHIVE_BUCKET_NAME } from './buckets.js';

function testClient(): S3Client {
  return new S3Client({
    region: 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    forcePathStyle: true,
  });
}

describe('ensureRawArchiveBucket', () => {
  it('creates the bucket, and is a no-op the second time', async () => {
    const client = testClient();
    await ensureRawArchiveBucket(client);
    await ensureRawArchiveBucket(client);

    await expect(client.send(new HeadBucketCommand({ Bucket: RAW_ARCHIVE_BUCKET_NAME }))).resolves.toBeDefined();
  }, 20_000);
});

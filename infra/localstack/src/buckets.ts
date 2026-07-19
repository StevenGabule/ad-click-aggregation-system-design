import { S3Client, CreateBucketCommand, HeadBucketCommand } from '@aws-sdk/client-s3';

export const RAW_ARCHIVE_BUCKET_NAME = 'ad-clicks-raw';

async function bucketExists(client: S3Client, bucket: string): Promise<boolean> {
  try {
    await client.send(new HeadBucketCommand({ Bucket: bucket }));
    return true;
  } catch {
    return false;
  }
}

export async function ensureRawArchiveBucket(client: S3Client): Promise<void> {
  if (await bucketExists(client, RAW_ARCHIVE_BUCKET_NAME)) return;
  await client.send(new CreateBucketCommand({ Bucket: RAW_ARCHIVE_BUCKET_NAME }));
}

import {
  KinesisClient,
  CreateStreamCommand,
  DescribeStreamSummaryCommand,
  ResourceInUseException,
  ResourceNotFoundException,
  waitUntilStreamExists,
} from '@aws-sdk/client-kinesis';

const STREAM_NAME = 'ad-clicks-raw';
const SHARD_COUNT = 2;

export async function ensureClickStream(client: KinesisClient): Promise<void> {
  if (!(await streamExists(client))) {
    try {
      await client.send(new CreateStreamCommand({ StreamName: STREAM_NAME, ShardCount: SHARD_COUNT }));
    } catch (err) {
      if (!(err instanceof ResourceInUseException)) throw err;
    }
  }

  await waitUntilStreamExists({ client, maxWaitTime: 30, minDelay: 1 }, { StreamName: STREAM_NAME });
}

async function streamExists(client: KinesisClient): Promise<boolean> {
  try {
    await client.send(new DescribeStreamSummaryCommand({ StreamName: STREAM_NAME }));
    return true;
  } catch (err) {
    if (err instanceof ResourceNotFoundException) return false;
    throw err;
  }
}

async function main() {
  const client = new KinesisClient({
    region: process.env.AWS_REGION ?? 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
  await ensureClickStream(client);
  console.log(`Kinesis stream "${STREAM_NAME}" is ready.`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}

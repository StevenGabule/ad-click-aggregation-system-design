import { describe, expect, it } from 'vitest';
import { KinesisClient, DescribeStreamSummaryCommand } from '@aws-sdk/client-kinesis';
import { ensureClickStream } from './bootstrap.js';

function testClient(): KinesisClient {
  return new KinesisClient({
    region: 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  });
}

describe('ensureClickStream', () => {
  it('creates the stream, and is a no-op the second time', async () => {
    const client = testClient();
    await ensureClickStream(client);
    await ensureClickStream(client); // must not throw on re-run

    const description = await client.send(
      new DescribeStreamSummaryCommand({ StreamName: 'ad-clicks-raw' })
    );
    expect(description.StreamDescriptionSummary?.StreamStatus).toBe('ACTIVE');
  }, 20_000);
});

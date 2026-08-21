import { KinesisClient } from '@aws-sdk/client-kinesis';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { runPollingConsumer } from '@app/kinesis-consumer-loop';
import { scoreVerdict, putVerdict } from '@app/fraud-verdict-store';
import { loadEnv } from '@app/config';

interface EnrichedRecord {
  cid: string;
  velocityFlag?: boolean;
  previewBot?: boolean;
}

// Must match the archiver's partition-date computation so reconciliation
// looks up verdicts under the same "today".
function todayDateString(): string {
  return new Date().toISOString().slice(0, 10);
}

async function main() {
  const env = loadEnv();
  const shardId = process.env.KINESIS_SHARD_ID;
  if (!shardId) throw new Error('KINESIS_SHARD_ID is required');

  const awsClientConfig = {
    region: env.AWS_REGION,
    endpoint: env.AWS_ENDPOINT_URL,
    // ponytail: LocalStack dummy creds; swap for the default AWS credential provider chain when targeting real AWS
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  };
  const kinesis = new KinesisClient(awsClientConfig);
  const dynamo = new DynamoDBClient(awsClientConfig);

  await runPollingConsumer({
    kinesis,
    streamName: 'ad-clicks-raw',
    shardId,
    onRecord: async (data) => {
      const event = JSON.parse(data.toString('utf-8')) as EnrichedRecord;
      const verdict = scoreVerdict(event);
      await putVerdict(dynamo, { date: todayDateString(), cid: event.cid, verdict });
    },
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

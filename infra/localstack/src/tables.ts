import {
  DynamoDBClient, CreateTableCommand, DescribeTableCommand, UpdateTimeToLiveCommand,
  ResourceInUseException, ResourceNotFoundException,
} from '@aws-sdk/client-dynamodb';

export const DEDUP_TABLE_NAME = 'click-dedup';
export const HOT_AGGREGATE_TABLE_NAME = 'click-aggregates';

async function tableExists(client: DynamoDBClient, tableName: string): Promise<boolean> {
  try {
    await client.send(new DescribeTableCommand({ TableName: tableName }));
    return true;
  } catch (err) {
    if (err instanceof ResourceNotFoundException) return false;
    throw err;
  }
}

export async function ensureDedupTable(client: DynamoDBClient): Promise<void> {
  if (await tableExists(client, DEDUP_TABLE_NAME)) return;

  try {
    await client.send(new CreateTableCommand({
      TableName: DEDUP_TABLE_NAME,
      AttributeDefinitions: [{ AttributeName: 'cid', AttributeType: 'S' }],
      KeySchema: [{ AttributeName: 'cid', KeyType: 'HASH' }],
      BillingMode: 'PAY_PER_REQUEST',
    }));
  } catch (err) {
    if (!(err instanceof ResourceInUseException)) throw err;
  }

  await client.send(new UpdateTimeToLiveCommand({
    TableName: DEDUP_TABLE_NAME,
    TimeToLiveSpecification: { AttributeName: 'expiresAt', Enabled: true },
  }));
}

export async function ensureHotAggregateTable(client: DynamoDBClient): Promise<void> {
  if (await tableExists(client, HOT_AGGREGATE_TABLE_NAME)) return;

  try {
    await client.send(new CreateTableCommand({
      TableName: HOT_AGGREGATE_TABLE_NAME,
      AttributeDefinitions: [
        { AttributeName: 'adId', AttributeType: 'S' },
        { AttributeName: 'windowStart', AttributeType: 'N' },
      ],
      KeySchema: [
        { AttributeName: 'adId', KeyType: 'HASH' },
        { AttributeName: 'windowStart', KeyType: 'RANGE' },
      ],
      BillingMode: 'PAY_PER_REQUEST',
    }));
  } catch (err) {
    if (!(err instanceof ResourceInUseException)) throw err;
  }

  await client.send(new UpdateTimeToLiveCommand({
    TableName: HOT_AGGREGATE_TABLE_NAME,
    TimeToLiveSpecification: { AttributeName: 'expiresAt', Enabled: true },
  }));
}

import { DynamoDBClient, PutItemCommand, QueryCommand } from '@aws-sdk/client-dynamodb';
import type { Verdict } from './verdict.js';

const FRAUD_VERDICTS_TABLE_NAME = 'fraud-verdicts';

export async function putVerdict(
  dynamo: DynamoDBClient,
  params: { date: string; cid: string; verdict: Verdict }
): Promise<void> {
  await dynamo.send(new PutItemCommand({
    TableName: FRAUD_VERDICTS_TABLE_NAME,
    Item: {
      date: { S: params.date },
      cid: { S: params.cid },
      verdict: { S: params.verdict },
    },
  }));
}

export async function listExcludedCids(dynamo: DynamoDBClient, date: string): Promise<Set<string>> {
  const { Items } = await dynamo.send(new QueryCommand({
    TableName: FRAUD_VERDICTS_TABLE_NAME,
    KeyConditionExpression: '#d = :date',
    ExpressionAttributeNames: { '#d': 'date' },
    ExpressionAttributeValues: { ':date': { S: date } },
  }));

  const excluded = new Set<string>();
  for (const item of Items ?? []) {
    if (item.verdict?.S !== 'legitimate') excluded.add(item.cid.S!);
  }
  return excluded;
}

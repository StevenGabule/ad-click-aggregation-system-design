import { DynamoDBClient, PutItemCommand, QueryCommand, GetItemCommand } from '@aws-sdk/client-dynamodb';

const STATEMENTS_TABLE_NAME = 'click-statements';

export interface Statement {
  campaignId: string;
  period: string;
  billedClicks: number;
  excludedInvalidClicks: number;
  reconciledAt: string;
  sourceArchive: string;
}

export async function putStatement(dynamo: DynamoDBClient, statement: Statement): Promise<void> {
  await dynamo.send(new PutItemCommand({
    TableName: STATEMENTS_TABLE_NAME,
    Item: {
      campaignId: { S: statement.campaignId },
      period: { S: statement.period },
      billedClicks: { N: String(statement.billedClicks) },
      excludedInvalidClicks: { N: String(statement.excludedInvalidClicks) },
      reconciledAt: { S: statement.reconciledAt },
      sourceArchive: { S: statement.sourceArchive },
    },
  }));
}

function toStatement(item: Record<string, { S?: string; N?: string }>): Statement {
  return {
    campaignId: item.campaignId.S!,
    period: item.period.S!,
    billedClicks: Number(item.billedClicks.N),
    excludedInvalidClicks: Number(item.excludedInvalidClicks.N),
    reconciledAt: item.reconciledAt.S!,
    sourceArchive: item.sourceArchive.S!,
  };
}

export async function getStatement(dynamo: DynamoDBClient, campaignId: string, period?: string): Promise<Statement | null> {
  if (period) {
    const { Item } = await dynamo.send(new GetItemCommand({
      TableName: STATEMENTS_TABLE_NAME,
      Key: { campaignId: { S: campaignId }, period: { S: period } },
    }));
    return Item ? toStatement(Item as Record<string, { S?: string; N?: string }>) : null;
  }

  const { Items } = await dynamo.send(new QueryCommand({
    TableName: STATEMENTS_TABLE_NAME,
    KeyConditionExpression: 'campaignId = :campaignId',
    ExpressionAttributeValues: { ':campaignId': { S: campaignId } },
    ScanIndexForward: false,
    Limit: 1,
  }));
  const item = Items?.[0];
  return item ? toStatement(item as Record<string, { S?: string; N?: string }>) : null;
}

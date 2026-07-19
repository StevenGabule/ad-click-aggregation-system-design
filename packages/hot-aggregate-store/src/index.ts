import { DynamoDBClient, UpdateItemCommand } from '@aws-sdk/client-dynamodb';

const HOT_AGGREGATE_TABLE_NAME = 'click-aggregates';
const TTL_SECONDS = 48 * 3600;

export interface HotAggregateStore {
  flush(adId: string, windowStart: number, delta: number): Promise<void>;
}

export function createHotAggregateStore(
  dynamo: DynamoDBClient,
  tableName: string = HOT_AGGREGATE_TABLE_NAME
): HotAggregateStore {
  return {
    async flush(adId, windowStart, delta) {
      await dynamo.send(new UpdateItemCommand({
        TableName: tableName,
        Key: {
          adId: { S: adId },
          windowStart: { N: String(windowStart) },
        },
        UpdateExpression: 'ADD #c :delta SET expiresAt = :expiresAt',
        ExpressionAttributeNames: { '#c': 'count' },
        ExpressionAttributeValues: {
          ':delta': { N: String(delta) },
          ':expiresAt': { N: String(Math.floor(Date.now() / 1000) + TTL_SECONDS) },
        },
      }));
    },
  };
}

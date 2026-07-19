import { DynamoDBClient, PutItemCommand, ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';

export interface DedupStore {
  isNew(cid: string): Promise<boolean>;
}

export interface DedupRedis {
  set(key: string, value: string, options: { NX: true; EX: number }): Promise<string | null>;
}

const DEDUP_TABLE_NAME = 'click-dedup';

export function createDedupStore(
  redis: DedupRedis,
  dynamo: DynamoDBClient,
  options: { tableName?: string; windowSeconds?: number } = {}
): DedupStore {
  const tableName = options.tableName ?? DEDUP_TABLE_NAME;
  const windowSeconds = options.windowSeconds ?? 600;

  return {
    async isNew(cid) {
      const redisResult = await redis.set(`click:${cid}`, '1', { NX: true, EX: windowSeconds });
      if (redisResult === null) return false;

      try {
        await dynamo.send(new PutItemCommand({
          TableName: tableName,
          Item: {
            cid: { S: cid },
            expiresAt: { N: String(Math.floor(Date.now() / 1000) + windowSeconds) },
          },
          ConditionExpression: 'attribute_not_exists(cid)',
        }));
        return true;
      } catch (err) {
        if (err instanceof ConditionalCheckFailedException) return false;
        throw err;
      }
    },
  };
}

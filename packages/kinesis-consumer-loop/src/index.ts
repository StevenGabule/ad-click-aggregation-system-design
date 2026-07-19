import { KinesisClient, GetShardIteratorCommand, GetRecordsCommand } from '@aws-sdk/client-kinesis';

export interface PollingConsumerOptions {
  kinesis: KinesisClient;
  streamName: string;
  shardId: string;
  onRecord: (data: Buffer, meta: { sequenceNumber: string }) => Promise<void>;
  pollIntervalMs?: number;
  shardIteratorType?: 'LATEST' | 'TRIM_HORIZON';
  signal?: AbortSignal;
}

export async function runPollingConsumer(options: PollingConsumerOptions): Promise<void> {
  const { kinesis, streamName, shardId, onRecord, signal } = options;
  const pollIntervalMs = options.pollIntervalMs ?? 1000;

  let { ShardIterator: iterator } = await kinesis.send(new GetShardIteratorCommand({
    StreamName: streamName,
    ShardId: shardId,
    ShardIteratorType: options.shardIteratorType ?? 'LATEST',
  }));

  while (iterator && !signal?.aborted) {
    const { Records, NextShardIterator } = await kinesis.send(new GetRecordsCommand({ ShardIterator: iterator }));

    for (const record of Records ?? []) {
      try {
        await onRecord(Buffer.from(record.Data!), { sequenceNumber: record.SequenceNumber! });
      } catch (err) {
        console.error('consumer callback failed for record, skipping', { sequenceNumber: record.SequenceNumber, err });
      }
    }

    iterator = NextShardIterator;
    if (!Records || Records.length === 0) {
      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    }
  }
}

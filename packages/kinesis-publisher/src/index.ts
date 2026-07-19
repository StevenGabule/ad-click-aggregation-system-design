import { KinesisClient, PutRecordCommand } from '@aws-sdk/client-kinesis';
import type { ClickEvent } from '@app/event-schema';

export const CLICK_STREAM_NAME = 'ad-clicks-raw';

export async function publishClickEvent(
  client: KinesisClient,
  streamName: string,
  event: ClickEvent
): Promise<void> {
  await client.send(new PutRecordCommand({
    StreamName: streamName,
    PartitionKey: `${event.ad_id}#${Math.floor(Math.random() * 8)}`,
    Data: Buffer.from(JSON.stringify({ ...event, receivedAt: Date.now() })),
  }));
}

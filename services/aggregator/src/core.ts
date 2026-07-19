import type { DedupStore } from '@app/click-dedup';
import type { WindowedAggregator } from '@app/windowed-aggregator';
import type { HotAggregateStore } from '@app/hot-aggregate-store';

export interface RawClickEvent {
  cid: string;
  ad_id: string;
  ts: string;
}

export interface ConsumerDeps {
  dedupStore: Pick<DedupStore, 'isNew'>;
  aggregator: Pick<WindowedAggregator, 'record'>;
}

export async function handleRecord(deps: ConsumerDeps, event: RawClickEvent): Promise<void> {
  const isNew = await deps.dedupStore.isNew(event.cid);
  if (!isNew) return;
  deps.aggregator.record(event.ad_id, new Date(event.ts).getTime());
}

export async function flushClosedWindows(
  aggregator: Pick<WindowedAggregator, 'peekClosedWindows' | 'commitFlushed'>,
  hotStore: Pick<HotAggregateStore, 'flush'>,
  nowMs: number
): Promise<void> {
  for (const { windowStart, counts } of aggregator.peekClosedWindows(nowMs)) {
    for (const [adId, count] of counts) {
      try {
        await hotStore.flush(adId, windowStart, count);
        aggregator.commitFlushed(windowStart, adId, count);
      } catch (err) {
        console.error('flush failed for adId, will retry next tick', { windowStart, adId, err });
      }
    }
  }
}

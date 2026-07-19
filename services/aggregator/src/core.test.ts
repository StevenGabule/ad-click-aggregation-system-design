import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import { createWindowedAggregator } from '@app/windowed-aggregator';
import { handleRecord, flushClosedWindows } from './core.js';

const WINDOW_MS = 60_000;
const WATERMARK_MS = 120_000;
const PAST_WATERMARK = WINDOW_MS + WATERMARK_MS + 1;

describe('handleRecord', () => {
  it('records the event when the dedup store says it is new', async () => {
    const record = vi.fn();
    const deps = { dedupStore: { isNew: vi.fn().mockResolvedValue(true) }, aggregator: { record } };

    await handleRecord(deps, { cid: 'clk_1', ad_id: 'ad_1', ts: '2026-07-12T09:14:32.118Z' });

    expect(record).toHaveBeenCalledWith('ad_1', new Date('2026-07-12T09:14:32.118Z').getTime());
  });

  it('does not record a duplicate', async () => {
    const record = vi.fn();
    const deps = { dedupStore: { isNew: vi.fn().mockResolvedValue(false) }, aggregator: { record } };

    await handleRecord(deps, { cid: 'clk_1', ad_id: 'ad_1', ts: '2026-07-12T09:14:32.118Z' });

    expect(record).not.toHaveBeenCalled();
  });
});

describe('flushClosedWindows', () => {
  let errorSpy: ReturnType<typeof vi.spyOn>;
  beforeEach(() => { errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {}); });
  afterEach(() => { errorSpy.mockRestore(); });

  // nowMs at which a window opened at `windowStart` is guaranteed closed.
  function pastWatermarkFor(windowStart: number): number {
    return windowStart + WINDOW_MS + WATERMARK_MS + 1;
  }

  it('flushes every (adId,count) pair and empties the window on success', async () => {
    const aggregator = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    aggregator.record('ad_A', 1_000);
    aggregator.record('ad_A', 2_000);
    aggregator.record('ad_B', 3_000);
    const flush = vi.fn().mockResolvedValue(undefined);

    await flushClosedWindows(aggregator, { flush }, pastWatermarkFor(0));

    expect(flush).toHaveBeenCalledWith('ad_A', 0, 2);
    expect(flush).toHaveBeenCalledWith('ad_B', 0, 1);
    expect(aggregator.peekClosedWindows(pastWatermarkFor(0))).toEqual([]);
  });

  it('does not double-count an already-flushed adId when a sibling fails then retries', async () => {
    const aggregator = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    aggregator.record('ad_A', 1_000);
    aggregator.record('ad_A', 2_000);
    aggregator.record('ad_B', 3_000);

    let failB = true;
    const flush = vi.fn().mockImplementation(async (adId: string) => {
      if (adId === 'ad_B' && failB) throw new Error('DynamoDB throttled');
    });

    // tick 1: ad_A flushes and commits; ad_B throws and is left in the window for retry.
    await flushClosedWindows(aggregator, { flush }, pastWatermarkFor(0));

    failB = false;
    // tick 2: ad_B retries and succeeds. ad_A must NOT be re-flushed.
    await flushClosedWindows(aggregator, { flush }, pastWatermarkFor(0));

    const adACalls = flush.mock.calls.filter((c) => c[0] === 'ad_A');
    expect(adACalls).toHaveLength(1);
    expect(adACalls).toEqual([['ad_A', 0, 2]]);
  });

  it('isolates failures across windows', async () => {
    const aggregator = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    aggregator.record('ad_fail', 1_000);
    aggregator.record('ad_ok', 60_000 + 1_000);

    const flush = vi.fn().mockImplementation(async (adId: string) => {
      if (adId === 'ad_fail') throw new Error('DynamoDB unavailable');
    });

    const now = pastWatermarkFor(60_000);
    await flushClosedWindows(aggregator, { flush }, now);

    expect(flush).toHaveBeenCalledWith('ad_ok', 60_000, 1);

    const closed = aggregator.peekClosedWindows(now);
    expect(closed).toHaveLength(1);
    expect(closed[0].windowStart).toBe(0);
    expect(closed[0].counts.get('ad_fail')).toBe(1);
  });
});

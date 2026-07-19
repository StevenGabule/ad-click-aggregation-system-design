import { describe, expect, it } from 'vitest';
import { createWindowedAggregator } from './index.js';

const WINDOW_MS = 60_000;
const WATERMARK_MS = 120_000;

describe('windowed-aggregator', () => {
  it('accumulates records within the same window across ads, not yet closed', () => {
    const agg = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    agg.record('ad_1', 1_000);
    agg.record('ad_1', 30_000);
    agg.record('ad_2', 15_000);

    expect(agg.peekClosedWindows(40_000)).toEqual([]);
  });

  it('returns a closed window once now has passed windowStart + windowMs + watermarkMs', () => {
    const agg = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    agg.record('ad_1', 1_000);
    agg.record('ad_1', 30_000);
    agg.record('ad_2', 15_000);

    const closed = agg.peekClosedWindows(WINDOW_MS + WATERMARK_MS + 1);

    expect(closed).toHaveLength(1);
    expect(closed[0].windowStart).toBe(0);
    expect(closed[0].counts.get('ad_1')).toBe(2);
    expect(closed[0].counts.get('ad_2')).toBe(1);
  });

  it('peek does not remove — a second peek still sees the window', () => {
    const agg = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    agg.record('ad_1', 1_000);
    const now = WINDOW_MS + WATERMARK_MS + 1;

    agg.peekClosedWindows(now);

    expect(agg.peekClosedWindows(now)).toHaveLength(1);
  });

  it('removeWindow removes it so a later peek no longer sees it', () => {
    const agg = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    agg.record('ad_1', 1_000);
    const now = WINDOW_MS + WATERMARK_MS + 1;

    agg.peekClosedWindows(now);
    agg.removeWindow(0);

    expect(agg.peekClosedWindows(now)).toEqual([]);
  });

  it('a late record for an already-removed window opens a fresh single-entry window', () => {
    const agg = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    agg.record('ad_1', 1_000);
    const now = WINDOW_MS + WATERMARK_MS + 1;
    agg.peekClosedWindows(now);
    agg.removeWindow(0);

    agg.record('ad_1', 500);

    const closed = agg.peekClosedWindows(now + 1);
    expect(closed).toHaveLength(1);
    expect(closed[0].counts.get('ad_1')).toBe(1);
  });

  it('commitFlushed subtracts the flushed amount and leaves a remainder', () => {
    const agg = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    agg.record('ad_A', 1_000);
    agg.record('ad_A', 2_000);
    const now = WINDOW_MS + WATERMARK_MS + 1;

    agg.commitFlushed(0, 'ad_A', 1);

    const closed = agg.peekClosedWindows(now);
    expect(closed).toHaveLength(1);
    expect(closed[0].counts.get('ad_A')).toBe(1);
  });

  it('commitFlushed removes the adId and the empty window once its count reaches zero', () => {
    const agg = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    agg.record('ad_A', 1_000);
    const now = WINDOW_MS + WATERMARK_MS + 1;

    agg.commitFlushed(0, 'ad_A', 1);

    expect(agg.peekClosedWindows(now)).toEqual([]);
  });

  it('peekClosedWindows returns a defensive copy — mutating it does not corrupt internal state', () => {
    const agg = createWindowedAggregator({ windowMs: WINDOW_MS, watermarkMs: WATERMARK_MS });
    agg.record('ad_A', 1_000);
    const now = WINDOW_MS + WATERMARK_MS + 1;

    const closed = agg.peekClosedWindows(now);
    closed[0].counts.set('ad_A', 999);

    expect(agg.peekClosedWindows(now)[0].counts.get('ad_A')).toBe(1);
  });
});

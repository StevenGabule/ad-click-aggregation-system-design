import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import { handleRecord, flushClosedWindows } from './core.js';

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

  it('flushes every (adId, count) pair per closed window, then removes the window', async () => {
    const closedWindows = [{ windowStart: 0, counts: new Map([['ad_1', 3], ['ad_2', 1]]) }];
    const peekClosedWindows = vi.fn().mockReturnValue(closedWindows);
    const removeWindow = vi.fn();
    const flush = vi.fn().mockResolvedValue(undefined);

    await flushClosedWindows({ peekClosedWindows, removeWindow }, { flush }, 999_999);

    expect(flush).toHaveBeenCalledWith('ad_1', 0, 3);
    expect(flush).toHaveBeenCalledWith('ad_2', 0, 1);
    expect(removeWindow).toHaveBeenCalledWith(0);
  });

  it('does not remove a window whose flush fails, so it can retry next tick', async () => {
    const closedWindows = [{ windowStart: 0, counts: new Map([['ad_1', 3]]) }];
    const peekClosedWindows = vi.fn().mockReturnValue(closedWindows);
    const removeWindow = vi.fn();
    const flush = vi.fn().mockRejectedValue(new Error('DynamoDB unavailable'));

    await flushClosedWindows({ peekClosedWindows, removeWindow }, { flush }, 999_999);

    expect(removeWindow).not.toHaveBeenCalled();
  });

  it('removes other windows even when one window fails, and does not remove the failed one', async () => {
    const closedWindows = [
      { windowStart: 0, counts: new Map([['ad_fail', 1]]) },
      { windowStart: 60_000, counts: new Map([['ad_ok', 2]]) },
    ];
    const peekClosedWindows = vi.fn().mockReturnValue(closedWindows);
    const removeWindow = vi.fn();
    const flush = vi.fn().mockImplementation(async (adId: string) => {
      if (adId === 'ad_fail') throw new Error('DynamoDB unavailable');
    });

    await flushClosedWindows({ peekClosedWindows, removeWindow }, { flush }, 999_999);

    // window 0 failed -> not removed (retried next tick); window 60_000 succeeded -> removed
    expect(removeWindow).toHaveBeenCalledWith(60_000);
    expect(removeWindow).not.toHaveBeenCalledWith(0);
    expect(removeWindow).toHaveBeenCalledTimes(1);
  });
});

import { describe, expect, it, vi } from 'vitest';
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
});

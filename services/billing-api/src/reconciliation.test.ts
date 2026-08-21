import { describe, expect, it, vi } from 'vitest';
import { reconcileAndStore } from './reconciliation.js';

describe('reconcileAndStore', () => {
  it('fetches excluded cids before reconciling, and passes them through unmodified', async () => {
    const excludedCids = new Set(['clk_bad_1', 'clk_bad_2']);
    const calls: string[] = [];

    const deps = {
      bucketPrefixForDate: (date: string) => `s3://ad-clicks-raw/dt=${date}/`,
      listExcludedCids: vi.fn(async () => {
        calls.push('listExcludedCids');
        return excludedCids;
      }),
      reconcileDate: vi.fn(async (_prefix: string, passedExcludedCids: Set<string>) => {
        calls.push('reconcileDate');
        expect(passedExcludedCids).toBe(excludedCids);
        return [{ campaignId: 'cmp_1', billedClicks: 10, excludedInvalidClicks: 2 }];
      }),
      putStatement: vi.fn().mockResolvedValue(undefined),
    };

    const count = await reconcileAndStore(deps, '2026-07-11');

    expect(calls).toEqual(['listExcludedCids', 'reconcileDate']);
    expect(count).toBe(1);
    expect(deps.putStatement).toHaveBeenCalledWith(expect.objectContaining({
      campaignId: 'cmp_1', billedClicks: 10, excludedInvalidClicks: 2, period: '2026-07-11',
      sourceArchive: 's3://ad-clicks-raw/dt=2026-07-11/',
    }));
  });

  it('stores one statement per campaign the reconciliation query returns', async () => {
    const deps = {
      bucketPrefixForDate: (date: string) => `s3://ad-clicks-raw/dt=${date}/`,
      listExcludedCids: vi.fn().mockResolvedValue(new Set()),
      reconcileDate: vi.fn().mockResolvedValue([
        { campaignId: 'cmp_1', billedClicks: 10, excludedInvalidClicks: 0 },
        { campaignId: 'cmp_2', billedClicks: 5, excludedInvalidClicks: 1 },
      ]),
      putStatement: vi.fn().mockResolvedValue(undefined),
    };

    const count = await reconcileAndStore(deps, '2026-07-11');

    expect(count).toBe(2);
    expect(deps.putStatement).toHaveBeenCalledTimes(2);
  });
});

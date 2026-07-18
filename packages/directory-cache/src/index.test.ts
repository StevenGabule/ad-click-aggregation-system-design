import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createDirectoryCache } from './index.js';
import type { AdDirectoryEntry } from '@app/db';

const entryV1: AdDirectoryEntry = {
  adId: 'ad_1', campaignId: 'cmp_1', advertiserId: 'adv_1',
  signingSecret: 'secret-v1', landingUrl: 'https://example.com/v1',
};
const entryV2: AdDirectoryEntry = { ...entryV1, signingSecret: 'secret-v2' };

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

describe('createDirectoryCache', () => {
  it('populates from loadDirectory on start()', async () => {
    const loadDirectory = vi.fn().mockResolvedValue([entryV1]);
    const cache = createDirectoryCache(loadDirectory, { refreshIntervalMs: 1000 });

    await cache.start();

    expect(cache.lookup('ad_1')).toEqual(entryV1);
    expect(loadDirectory).toHaveBeenCalledTimes(1);
    cache.stop();
  });

  it('refreshes on the configured interval', async () => {
    const loadDirectory = vi.fn().mockResolvedValueOnce([entryV1]).mockResolvedValueOnce([entryV2]);
    const cache = createDirectoryCache(loadDirectory, { refreshIntervalMs: 1000 });

    await cache.start();
    expect(cache.lookup('ad_1')?.signingSecret).toBe('secret-v1');

    await vi.advanceTimersByTimeAsync(1000);

    expect(cache.lookup('ad_1')?.signingSecret).toBe('secret-v2');
    cache.stop();
  });

  it('returns undefined for an unknown adId', async () => {
    const cache = createDirectoryCache(vi.fn().mockResolvedValue([entryV1]), { refreshIntervalMs: 1000 });
    await cache.start();
    expect(cache.lookup('ad_unknown')).toBeUndefined();
    cache.stop();
  });
});

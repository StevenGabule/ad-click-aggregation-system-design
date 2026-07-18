import { describe, expect, it } from 'vitest';
import { ClickEventSchema } from './index.js';

const validEvent = {
  cid: 'clk_9f2k4x',
  ad_id: 'ad_881203',
  campaign_id: 'cmp_44210',
  pub_id: 'pub_6612',
  ts: '2026-07-12T09:14:32.118Z',
  sig: 'deadbeef',
};

describe('ClickEventSchema', () => {
  it('accepts a valid click event', () => {
    const result = ClickEventSchema.safeParse(validEvent);
    expect(result.success).toBe(true);
  });

  it('rejects an event missing ad_id', () => {
    const { ad_id, ...withoutAdId } = validEvent;
    const result = ClickEventSchema.safeParse(withoutAdId);
    expect(result.success).toBe(false);
  });

  it('rejects a malformed timestamp', () => {
    const result = ClickEventSchema.safeParse({ ...validEvent, ts: 'not-a-date' });
    expect(result.success).toBe(false);
  });

  it('rejects an empty cid', () => {
    const result = ClickEventSchema.safeParse({ ...validEvent, cid: '' });
    expect(result.success).toBe(false);
  });
});

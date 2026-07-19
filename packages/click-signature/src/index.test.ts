import { describe, expect, it } from 'vitest';
import { computeSignature, verifySignature } from './index.js';

const secret = 'advertiser-secret';
const fields = {
  cid: 'clk_9f2k4x',
  ad_id: 'ad_881203',
  campaign_id: 'cmp_44210',
  pub_id: 'pub_6612',
  ts: '2026-07-12T09:14:32.118Z',
};

describe('click-signature', () => {
  it('verifies a signature computed with the same secret', () => {
    const sig = computeSignature(secret, fields);
    expect(verifySignature(secret, fields, sig)).toBe(true);
  });

  it('rejects a signature when any field is tampered', () => {
    const sig = computeSignature(secret, fields);
    expect(verifySignature(secret, { ...fields, ad_id: 'ad_other' }, sig)).toBe(false);
  });

  it('rejects a signature computed with a different secret', () => {
    const sig = computeSignature('other-secret', fields);
    expect(verifySignature(secret, fields, sig)).toBe(false);
  });

  it('rejects a garbage signature of the wrong length', () => {
    expect(verifySignature(secret, fields, 'not-a-real-signature')).toBe(false);
  });
});

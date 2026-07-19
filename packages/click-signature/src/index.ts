import { createHmac, timingSafeEqual } from 'node:crypto';

export interface SignableFields {
  cid: string;
  ad_id: string;
  campaign_id: string;
  pub_id: string;
  ts: string;
}

function canonicalize(fields: SignableFields): string {
  return [fields.cid, fields.ad_id, fields.campaign_id, fields.pub_id, fields.ts].join('|');
}

export function computeSignature(secret: string, fields: SignableFields): string {
  return createHmac('sha256', secret).update(canonicalize(fields)).digest('hex');
}

export function verifySignature(secret: string, fields: SignableFields, sig: string): boolean {
  const expected = Buffer.from(computeSignature(secret, fields), 'hex');
  const actual = Buffer.from(sig, 'hex');
  if (expected.length !== actual.length) return false;
  return timingSafeEqual(expected, actual);
}

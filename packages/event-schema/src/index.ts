import { z } from 'zod';

export const ClickEventSchema = z.object({
  cid: z.string().min(1),
  ad_id: z.string().min(1),
  campaign_id: z.string().min(1),
  pub_id: z.string().min(1),
  ts: z.string().datetime(),
  sig: z.string().min(1),
});

export type ClickEvent = z.infer<typeof ClickEventSchema>;

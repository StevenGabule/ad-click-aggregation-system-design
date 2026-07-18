import type { PrismaClient } from '@prisma/client';

export interface AdDirectoryEntry {
  adId: string;
  campaignId: string;
  advertiserId: string;
  signingSecret: string;
  landingUrl: string;
}

export async function listActiveAdDirectory(client: PrismaClient): Promise<AdDirectoryEntry[]> {
  const ads = await client.ad.findMany({
    where: { campaign: { status: 'ACTIVE' } },
    select: {
      id: true,
      campaignId: true,
      landingUrl: true,
      campaign: { select: { advertiserId: true, advertiser: { select: { signingSecret: true } } } },
    },
  });

  return ads.map((ad) => ({
    adId: ad.id,
    campaignId: ad.campaignId,
    advertiserId: ad.campaign.advertiserId,
    signingSecret: ad.campaign.advertiser.signingSecret,
    landingUrl: ad.landingUrl,
  }));
}

import { describe, expect, it, afterAll } from 'vitest';
import { PrismaClient } from '@prisma/client';
import { listActiveAdDirectory } from './directory.js';

const prisma = new PrismaClient();

describe('listActiveAdDirectory', () => {
  afterAll(async () => {
    await prisma.ad.deleteMany({ where: { name: 'directory-test-ad' } });
    await prisma.campaign.deleteMany({ where: { name: 'directory-test-campaign' } });
    await prisma.advertiser.deleteMany({ where: { name: 'directory-test-advertiser' } });
    await prisma.$disconnect();
  });

  it('returns a flattened row for an ad on an active campaign', async () => {
    const advertiser = await prisma.advertiser.create({
      data: { name: 'directory-test-advertiser', signingSecret: 'shh' },
    });
    const campaign = await prisma.campaign.create({
      data: { name: 'directory-test-campaign', advertiserId: advertiser.id, status: 'ACTIVE' },
    });
    const ad = await prisma.ad.create({
      data: { name: 'directory-test-ad', campaignId: campaign.id, landingUrl: 'https://example.com/landing' },
    });

    const rows = await listActiveAdDirectory(prisma);
    const row = rows.find((r) => r.adId === ad.id);

    expect(row).toEqual({
      adId: ad.id,
      campaignId: campaign.id,
      advertiserId: advertiser.id,
      signingSecret: 'shh',
      landingUrl: 'https://example.com/landing',
    });
  });

  it('excludes ads on non-active campaigns', async () => {
    const advertiser = await prisma.advertiser.create({
      data: { name: 'directory-test-advertiser', signingSecret: 'shh' },
    });
    const campaign = await prisma.campaign.create({
      data: { name: 'directory-test-campaign', advertiserId: advertiser.id, status: 'PAUSED' },
    });
    const ad = await prisma.ad.create({
      data: { name: 'directory-test-ad', campaignId: campaign.id, landingUrl: 'https://example.com' },
    });

    const rows = await listActiveAdDirectory(prisma);
    expect(rows.some((r) => r.adId === ad.id)).toBe(false);
  });
});

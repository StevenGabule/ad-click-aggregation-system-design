import { describe, expect, it, afterAll } from 'vitest';
import { PrismaClient } from '@prisma/client';
import { createHash, randomBytes } from 'node:crypto';
import { getAdOwnerAdvertiserId, resolveApiKey } from './ownership.js';

const prisma = new PrismaClient();

afterAll(async () => {
  await prisma.apiKey.deleteMany({
    where: { advertiser: { name: { in: ['ownership-test-advertiser', 'ownership-test-advertiser-2'] } } },
  });
  await prisma.ad.deleteMany({ where: { name: 'ownership-test-ad' } });
  await prisma.campaign.deleteMany({ where: { name: 'ownership-test-campaign' } });
  await prisma.advertiser.deleteMany({
    where: { name: { in: ['ownership-test-advertiser', 'ownership-test-advertiser-2'] } },
  });
  await prisma.$disconnect();
});

describe('getAdOwnerAdvertiserId', () => {
  it('returns the owning advertiser id regardless of campaign status', async () => {
    const advertiser = await prisma.advertiser.create({
      data: { name: 'ownership-test-advertiser', signingSecret: 'shh' },
    });
    const campaign = await prisma.campaign.create({
      data: { name: 'ownership-test-campaign', advertiserId: advertiser.id, status: 'PAUSED' },
    });
    const ad = await prisma.ad.create({
      data: { name: 'ownership-test-ad', campaignId: campaign.id, landingUrl: 'https://example.com' },
    });

    expect(await getAdOwnerAdvertiserId(prisma, ad.id)).toBe(advertiser.id);
  });

  it('returns null for an unknown ad id', async () => {
    expect(await getAdOwnerAdvertiserId(prisma, 'ad_does_not_exist')).toBeNull();
  });
});

describe('resolveApiKey', () => {
  it('resolves an active key to its advertiser', async () => {
    const advertiser = await prisma.advertiser.create({
      data: { name: 'ownership-test-advertiser-2', signingSecret: 'shh' },
    });
    const rawKey = randomBytes(16).toString('hex');
    await prisma.apiKey.create({
      data: { advertiserId: advertiser.id, hashedKey: createHash('sha256').update(rawKey).digest('hex') },
    });

    expect(await resolveApiKey(prisma, rawKey)).toEqual({ advertiserId: advertiser.id });
  });

  it('returns null for a revoked key', async () => {
    const advertiser = await prisma.advertiser.create({
      data: { name: 'ownership-test-advertiser-2', signingSecret: 'shh' },
    });
    const rawKey = randomBytes(16).toString('hex');
    await prisma.apiKey.create({
      data: {
        advertiserId: advertiser.id,
        hashedKey: createHash('sha256').update(rawKey).digest('hex'),
        revokedAt: new Date(),
      },
    });

    expect(await resolveApiKey(prisma, rawKey)).toBeNull();
  });

  it('returns null for an unknown key', async () => {
    expect(await resolveApiKey(prisma, 'not-a-real-key')).toBeNull();
  });
});

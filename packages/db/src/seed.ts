import { PrismaClient } from '@prisma/client';
import { createHash, randomBytes } from 'node:crypto';

export interface SeedResult {
  advertiserId: string;
  campaignId: string;
  adId: string;
  publisherId: string;
  rawApiKey: string;
}

export async function runSeed(prisma: PrismaClient): Promise<SeedResult> {
  const advertiser = await prisma.advertiser.upsert({
    where: { id: 'seed-advertiser-1' },
    update: {},
    create: { id: 'seed-advertiser-1', name: 'Acme Ads', signingSecret: 'seed-signing-secret' },
  });

  const rawApiKey = randomBytes(24).toString('hex');
  const hashedKey = createHash('sha256').update(rawApiKey).digest('hex');
  await prisma.apiKey.upsert({
    where: { id: 'seed-api-key-1' },
    update: { hashedKey },
    create: {
      id: 'seed-api-key-1',
      advertiserId: advertiser.id,
      hashedKey,
    },
  });

  const campaign = await prisma.campaign.upsert({
    where: { id: 'seed-campaign-1' },
    update: {},
    create: { id: 'seed-campaign-1', advertiserId: advertiser.id, name: 'Summer Launch', status: 'ACTIVE' },
  });

  const ad = await prisma.ad.upsert({
    where: { id: 'seed-ad-1' },
    update: {},
    create: {
      id: 'seed-ad-1',
      campaignId: campaign.id,
      name: 'Banner A',
      landingUrl: 'https://advertiser.example.com/landing',
    },
  });

  const publisher = await prisma.publisher.upsert({
    where: { id: 'seed-publisher-1' },
    update: {},
    create: { id: 'seed-publisher-1', name: 'Example Publisher Network' },
  });

  return {
    advertiserId: advertiser.id,
    campaignId: campaign.id,
    adId: ad.id,
    publisherId: publisher.id,
    rawApiKey,
  };
}

async function main() {
  const prisma = new PrismaClient();
  const result = await runSeed(prisma);
  console.log('Seed complete. Demo API key (save this, it is not stored raw):', result.rawApiKey);
  await prisma.$disconnect();
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}

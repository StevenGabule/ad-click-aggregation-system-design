import { describe, expect, it, afterAll } from 'vitest';
import { createHash } from 'node:crypto';
import { PrismaClient } from '@prisma/client';
import { runSeed } from './seed.js';

const prisma = new PrismaClient();

describe('runSeed', () => {
  afterAll(async () => {
    await prisma.$disconnect();
  });

  it('is idempotent: running twice does not duplicate rows', async () => {
    await runSeed(prisma);
    await runSeed(prisma);

    const advertisers = await prisma.advertiser.findMany({ where: { id: 'seed-advertiser-1' } });
    const ads = await prisma.ad.findMany({ where: { id: 'seed-ad-1' } });

    expect(advertisers).toHaveLength(1);
    expect(ads).toHaveLength(1);
  });

  it('returns a fresh raw API key each run (never stored raw)', async () => {
    const first = await runSeed(prisma);
    const second = await runSeed(prisma);
    expect(first.rawApiKey).not.toEqual(second.rawApiKey);
  });

  it('stores a hash that matches the freshly returned raw API key', async () => {
    const result = await runSeed(prisma);

    const apiKey = await prisma.apiKey.findUniqueOrThrow({ where: { id: 'seed-api-key-1' } });
    const expectedHash = createHash('sha256').update(result.rawApiKey).digest('hex');

    expect(apiKey.hashedKey).toEqual(expectedHash);
  });
});

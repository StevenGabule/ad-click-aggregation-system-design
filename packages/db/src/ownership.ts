import type { PrismaClient } from '@prisma/client';
import { createHash } from 'node:crypto';

export async function getAdOwnerAdvertiserId(client: PrismaClient, adId: string): Promise<string | null> {
  const ad = await client.ad.findUnique({
    where: { id: adId },
    select: { campaign: { select: { advertiserId: true } } },
  });
  return ad?.campaign.advertiserId ?? null;
}

export async function resolveApiKey(client: PrismaClient, rawKey: string): Promise<{ advertiserId: string } | null> {
  const hashedKey = createHash('sha256').update(rawKey).digest('hex');
  const apiKey = await client.apiKey.findUnique({
    where: { hashedKey },
    select: { advertiserId: true, revokedAt: true },
  });
  if (!apiKey || apiKey.revokedAt) return null;
  return { advertiserId: apiKey.advertiserId };
}

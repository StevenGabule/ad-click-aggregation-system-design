import { describe, expect, it, afterAll } from 'vitest';
import { prisma } from './client.js';

describe('prisma client', () => {
  afterAll(async () => {
    await prisma.$disconnect();
  });

  it('connects to Postgres', async () => {
    const result = await prisma.$queryRaw<{ ok: number }[]>`SELECT 1 as ok`;
    expect(result[0].ok).toBe(1);
  });
});

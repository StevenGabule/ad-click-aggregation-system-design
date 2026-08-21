import { describe, expect, it } from 'vitest';
import { createClient as createRedisClient } from 'redis';
import { createVelocityChecker } from './velocity.js';

describe('createVelocityChecker', () => {
  it('flags once the threshold is exceeded, then resets after the window elapses', async () => {
    const redis = createRedisClient({ url: process.env.REDIS_URL ?? 'redis://localhost:6379' });
    await redis.connect();
    const checker = createVelocityChecker(redis, { windowSeconds: 1, threshold: 2 });
    const ip = `10.${Date.now() % 255}.0.1`;

    expect(await checker.checkAndIncrement(ip)).toBe(false);
    expect(await checker.checkAndIncrement(ip)).toBe(false);
    expect(await checker.checkAndIncrement(ip)).toBe(true);

    await new Promise((resolve) => setTimeout(resolve, 1100));

    expect(await checker.checkAndIncrement(ip)).toBe(false);

    await redis.quit();
  }, 10_000);
});

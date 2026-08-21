// Structural interface instead of node-redis's RedisClientType — the generic
// defaults make nominally identical client types mutually unassignable across
// package boundaries (same fix as @app/click-dedup's DedupRedis).
export interface VelocityRedis {
  incr(key: string): Promise<number>;
  expire(key: string, seconds: number): Promise<boolean | number>;
}

export interface VelocityChecker {
  checkAndIncrement(ip: string): Promise<boolean>;
}

export function createVelocityChecker(
  redis: VelocityRedis,
  options: { windowSeconds?: number; threshold?: number } = {}
): VelocityChecker {
  const windowSeconds = options.windowSeconds ?? 60;
  const threshold = options.threshold ?? 20;

  return {
    async checkAndIncrement(ip) {
      const key = `velocity:${ip}`;
      const count = await redis.incr(key);
      if (count === 1) {
        await redis.expire(key, windowSeconds);
      }
      return count > threshold;
    },
  };
}

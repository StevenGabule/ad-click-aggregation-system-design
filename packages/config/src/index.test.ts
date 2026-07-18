import { describe, expect, it } from 'vitest';
import { loadEnv } from './index.js';

const validEnv = {
  DATABASE_URL: 'postgresql://user:pass@localhost:5432/app',
  REDIS_URL: 'redis://localhost:6379',
};

describe('loadEnv', () => {
  it('loads a valid environment and defaults AWS_REGION', () => {
    const env = loadEnv(validEnv);
    expect(env.AWS_REGION).toBe('us-east-1');
    expect(env.DATABASE_URL).toBe(validEnv.DATABASE_URL);
  });

  it('passes through AWS_ENDPOINT_URL when set (LocalStack)', () => {
    const env = loadEnv({ ...validEnv, AWS_ENDPOINT_URL: 'http://localhost:4566' });
    expect(env.AWS_ENDPOINT_URL).toBe('http://localhost:4566');
  });

  it('throws clearly when DATABASE_URL is missing', () => {
    const { DATABASE_URL, ...withoutDb } = validEnv;
    expect(() => loadEnv(withoutDb)).toThrow('Invalid environment configuration');
  });
});

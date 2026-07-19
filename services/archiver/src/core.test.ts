import { describe, expect, it } from 'vitest';
import { createBatchBuffer } from './core.js';

describe('createBatchBuffer', () => {
  it('does not flush below both thresholds', () => {
    const buffer = createBatchBuffer<number>({ maxSize: 3, maxAgeMs: 1000 });
    buffer.add(1, 0);

    expect(buffer.shouldFlush(500)).toBe(false);
  });

  it('flushes once maxSize items have been added', () => {
    const buffer = createBatchBuffer<number>({ maxSize: 3, maxAgeMs: 1000 });
    buffer.add(1, 0);
    buffer.add(2, 0);
    buffer.add(3, 0);

    expect(buffer.shouldFlush(0)).toBe(true);
  });

  it('flushes once maxAgeMs has passed since the first item after the last drain', () => {
    const buffer = createBatchBuffer<number>({ maxSize: 100, maxAgeMs: 1000 });
    buffer.add(1, 0);

    expect(buffer.shouldFlush(999)).toBe(false);
    expect(buffer.shouldFlush(1001)).toBe(true);
  });

  it('drain empties the buffer and resets its age', () => {
    const buffer = createBatchBuffer<number>({ maxSize: 3, maxAgeMs: 1000 });
    buffer.add(1, 0);
    buffer.add(2, 0);

    expect(buffer.drain()).toEqual([1, 2]);
    expect(buffer.shouldFlush(1001)).toBe(false);

    buffer.add(3, 1001);
    expect(buffer.shouldFlush(1001)).toBe(false);
  });
});

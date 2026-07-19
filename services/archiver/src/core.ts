export interface BatchBuffer<T> {
  add(item: T, nowMs?: number): void;
  shouldFlush(nowMs: number): boolean;
  drain(): T[];
}

export function createBatchBuffer<T>(options: { maxSize: number; maxAgeMs: number }): BatchBuffer<T> {
  let items: T[] = [];
  let firstItemAt: number | null = null;

  return {
    // ponytail: nowMs defaults to Date.now() so production callers (run.ts) need not pass it;
    // tests pin an explicit value instead of mocking the clock, matching the sub-project 3
    // windowed-aggregator convention of explicit time injection over internal wall-clock reads.
    add(item, nowMs = Date.now()) {
      if (items.length === 0) firstItemAt = nowMs;
      items.push(item);
    },
    shouldFlush(nowMs) {
      if (items.length >= options.maxSize) return true;
      if (firstItemAt !== null && nowMs - firstItemAt >= options.maxAgeMs) return true;
      return false;
    },
    drain() {
      const drained = items;
      items = [];
      firstItemAt = null;
      return drained;
    },
  };
}

export interface ClosedWindow {
  windowStart: number;
  counts: Map<string, number>;
}

export interface WindowedAggregator {
  record(adId: string, eventTimeMs: number): void;
  peekClosedWindows(nowMs: number): ClosedWindow[];
  removeWindow(windowStart: number): void;
}

export function createWindowedAggregator(options: { windowMs: number; watermarkMs: number }): WindowedAggregator {
  const { windowMs, watermarkMs } = options;
  const windows = new Map<number, Map<string, number>>();

  function bucketFor(eventTimeMs: number): number {
    return Math.floor(eventTimeMs / windowMs) * windowMs;
  }

  return {
    record(adId, eventTimeMs) {
      const windowStart = bucketFor(eventTimeMs);
      const counts = windows.get(windowStart) ?? windows.set(windowStart, new Map()).get(windowStart)!;
      counts.set(adId, (counts.get(adId) ?? 0) + 1);
    },
    peekClosedWindows(nowMs) {
      const closeBefore = nowMs - watermarkMs;
      const closed: ClosedWindow[] = [];
      for (const [windowStart, counts] of windows) {
        if (windowStart + windowMs > closeBefore) continue;
        closed.push({ windowStart, counts });
      }
      return closed;
    },
    removeWindow(windowStart) {
      windows.delete(windowStart);
    },
  };
}

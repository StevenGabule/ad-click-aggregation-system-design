import type { AdDirectoryEntry } from '@app/db';

export interface DirectoryCache {
  lookup(adId: string): AdDirectoryEntry | undefined;
  start(): Promise<void>;
  stop(): void;
}

export function createDirectoryCache(
  loadDirectory: () => Promise<AdDirectoryEntry[]>,
  options: { refreshIntervalMs?: number } = {}
): DirectoryCache {
  const refreshIntervalMs = options.refreshIntervalMs ?? 30_000;
  let entries = new Map<string, AdDirectoryEntry>();
  let timer: NodeJS.Timeout | undefined;

  async function refresh(): Promise<void> {
    const rows = await loadDirectory();
    entries = new Map(rows.map((row) => [row.adId, row]));
  }

  return {
    lookup(adId) {
      return entries.get(adId);
    },
    async start() {
      await refresh();
      timer = setInterval(() => {
        refresh().catch((err) => console.error('directory cache refresh failed', err));
      }, refreshIntervalMs);
      timer.unref();
    },
    stop() {
      clearInterval(timer);
    },
  };
}

export interface ReconciliationResult {
  campaignId: string;
  billedClicks: number;
  excludedInvalidClicks: number;
}

export interface ReconciliationDeps {
  bucketPrefixForDate(date: string): string;
  listExcludedCids(date: string): Promise<Set<string>>;
  reconcileDate(bucketPrefix: string, excludedCids: Set<string>): Promise<ReconciliationResult[]>;
  putStatement(statement: ReconciliationResult & {
    period: string; reconciledAt: string; sourceArchive: string;
  }): Promise<void>;
}

export async function reconcileAndStore(deps: ReconciliationDeps, date: string): Promise<number> {
  const prefix = deps.bucketPrefixForDate(date);
  const excludedCids = await deps.listExcludedCids(date);
  const results = await deps.reconcileDate(prefix, excludedCids);

  const reconciledAt = new Date().toISOString();
  for (const result of results) {
    await deps.putStatement({ ...result, period: date, reconciledAt, sourceArchive: prefix });
  }

  return results.length;
}

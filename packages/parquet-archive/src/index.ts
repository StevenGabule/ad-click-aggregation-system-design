import { Database } from 'duckdb-async';

export interface RawArchiveEvent {
  cid: string;
  ad_id: string;
  campaign_id: string;
  pub_id: string;
  ts: string;
  sig: string;
  receivedAt: number;
}

interface ArchiveDbEnv {
  AWS_ENDPOINT_URL?: string;
}

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

export function bucketPrefixForDate(date: string): string {
  if (!DATE_PATTERN.test(date)) throw new Error(`invalid date: ${date}`);
  return `s3://ad-clicks-raw/dt=${date}/`;
}

export async function createArchiveDb(env: ArchiveDbEnv): Promise<Database> {
  const db = await Database.create(':memory:');
  const endpointHost = new URL(env.AWS_ENDPOINT_URL!).host;
  await db.exec(`
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='${endpointHost}';
    SET s3_url_style='path';
    SET s3_use_ssl=false;
    SET s3_access_key_id='test';
    SET s3_secret_access_key='test';
  `);
  return db;
}

export async function archiveBatch(db: Database, bucketPrefix: string, events: RawArchiveEvent[]): Promise<void> {
  if (events.length === 0) return;

  await db.run(
    'CREATE OR REPLACE TEMP TABLE batch(cid VARCHAR, ad_id VARCHAR, campaign_id VARCHAR, pub_id VARCHAR, ts VARCHAR, sig VARCHAR, receivedAt BIGINT)'
  );
  const placeholders = events.map(() => '(?, ?, ?, ?, ?, ?, ?)').join(', ');
  const params = events.flatMap((e) => [e.cid, e.ad_id, e.campaign_id, e.pub_id, e.ts, e.sig, e.receivedAt]);
  await db.run(`INSERT INTO batch VALUES ${placeholders}`, ...params);

  const key = `part-${Date.now()}-${Math.random().toString(36).slice(2)}.parquet`;
  await db.run(`COPY batch TO '${bucketPrefix}${key}' (FORMAT PARQUET)`);
}

export async function reconcileDate(
  db: Database,
  bucketPrefix: string,
  excludedCids: ReadonlySet<string> = new Set()
): Promise<{ campaignId: string; billedClicks: number; excludedInvalidClicks: number }[]> {
  const excludedList = [...excludedCids];
  const isExcludedExpr = excludedList.length > 0
    ? `cid IN (${excludedList.map(() => '?').join(', ')})`
    : 'false';

  const rows = await db.all(
    `SELECT
       campaign_id AS campaignId,
       COUNT(DISTINCT CASE WHEN NOT (${isExcludedExpr}) THEN cid END) AS billedClicks,
       COUNT(DISTINCT CASE WHEN (${isExcludedExpr}) THEN cid END) AS excludedInvalidClicks
     FROM read_parquet('${bucketPrefix}*.parquet')
     GROUP BY campaign_id`,
    ...excludedList,
    ...excludedList
  );

  return rows.map((row: Record<string, unknown>) => ({
    campaignId: String(row.campaignId),
    billedClicks: Number(row.billedClicks),
    excludedInvalidClicks: Number(row.excludedInvalidClicks),
  }));
}

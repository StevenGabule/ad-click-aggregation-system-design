export type Verdict = 'legitimate' | 'suspicious' | 'invalid';

export function scoreVerdict(enrichment: { velocityFlag?: boolean; previewBot?: boolean }): Verdict {
  if (enrichment.previewBot) return 'invalid';
  if (enrichment.velocityFlag) return 'suspicious';
  return 'legitimate';
}

import { describe, expect, it } from 'vitest';
import { isPreviewBotRequest } from './previewBot.js';

describe('isPreviewBotRequest', () => {
  it('flags a Purpose: prefetch header', () => {
    expect(isPreviewBotRequest({ purpose: 'prefetch' })).toBe(true);
  });

  it('flags known preview-bot user agents', () => {
    expect(isPreviewBotRequest({ userAgent: 'Slackbot-LinkExpanding 1.0' })).toBe(true);
    expect(isPreviewBotRequest({ userAgent: 'facebookexternalhit/1.1' })).toBe(true);
    expect(isPreviewBotRequest({ userAgent: 'Discordbot/2.0' })).toBe(true);
  });

  it('does not flag an ordinary browser request', () => {
    expect(isPreviewBotRequest({ userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)' })).toBe(false);
  });

  it('does not flag a request with neither header', () => {
    expect(isPreviewBotRequest({})).toBe(false);
  });
});

import { describe, expect, it } from 'vitest';
import { scoreVerdict } from './verdict.js';

describe('scoreVerdict', () => {
  it('is legitimate when neither flag is set', () => {
    expect(scoreVerdict({})).toBe('legitimate');
  });

  it('is suspicious when only velocityFlag is set', () => {
    expect(scoreVerdict({ velocityFlag: true })).toBe('suspicious');
  });

  it('is invalid when previewBot is set', () => {
    expect(scoreVerdict({ previewBot: true })).toBe('invalid');
  });

  it('is invalid when both flags are set — previewBot wins', () => {
    expect(scoreVerdict({ velocityFlag: true, previewBot: true })).toBe('invalid');
  });
});

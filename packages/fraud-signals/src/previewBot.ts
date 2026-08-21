const PREVIEW_BOT_USER_AGENT_PATTERNS = [
  /Slackbot/i,
  /facebookexternalhit/i,
  /Twitterbot/i,
  /Discordbot/i,
  /WhatsApp/i,
  /iMessage/i,
];

export function isPreviewBotRequest(headers: { purpose?: string; userAgent?: string }): boolean {
  if (headers.purpose?.toLowerCase() === 'prefetch') return true;
  if (headers.userAgent && PREVIEW_BOT_USER_AGENT_PATTERNS.some((pattern) => pattern.test(headers.userAgent!))) {
    return true;
  }
  return false;
}

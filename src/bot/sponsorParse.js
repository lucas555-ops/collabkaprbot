export function sponsorToChatId(s) {
  const x = String(s || '').trim();
  if (!x) return null;

  // Accept:
  // - https://t.me/username
  // - http://t.me/username
  // - t.me/username
  // - @username
  // - username
  let u = x;

  const m1 = u.match(/^(?:https?:\/\/)?t\.me\/([a-zA-Z0-9_]{5,})/i);
  if (m1) return '@' + m1[1];

  const m2 = u.match(/^@([a-zA-Z0-9_]{5,})$/);
  if (m2) return '@' + m2[1];

  const m3 = u.match(/^([a-zA-Z0-9_]{5,})$/);
  if (m3) return '@' + m3[1];

  return null;
}

export function parseSponsorsFromText(text) {
  const t = String(text || '').trim();
  if (!t) return [];

  // split by newline or spaces, keep only reasonable tokens
  const raw = t.split(/\s+/g).map((x) => x.trim()).filter(Boolean);
  const out = [];
  for (const token of raw) {
    const chat = sponsorToChatId(token);
    if (chat) out.push(chat);
  }

  // unique preserve order
  return [...new Set(out)];
}

import crypto from 'crypto';

export function escapeHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

export function fmtTs(ts) {
  if (!ts) return '—';
  const d = new Date(ts);
  const dd = String(d.getUTCDate()).padStart(2, '0');
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mi = String(d.getUTCMinutes()).padStart(2, '0');
  return `${dd}.${mm} ${hh}:${mi} (UTC)`;
}

export function randomToken(nBytes = 12) {
  return crypto.randomBytes(nBytes).toString('hex');
}

export function parseCb(data) {
  // example: a:gw_export|i:12|t:all
  const out = { raw: data };
  const parts = String(data || '').split('|');
  out.a = parts[0] || '';
  for (let i = 1; i < parts.length; i++) {
    const [k, v] = parts[i].split(':');
    if (k) out[k] = v;
  }
  return out;
}

export function parseStartPayload(text) {
  const t = String(text || '');
  let m = t.match(/\/start\s+gw_(\d+)/);
  if (m) return { type: 'gw', id: Number(m[1]) };
  m = t.match(/\/start\s+gwo_(\d+)/);
  if (m) return { type: 'gwo', id: Number(m[1]) };
  m = t.match(/\/start\s+cur_(\d+)_(\w+)/);
  if (m) return { type: 'cur', wsId: Number(m[1]), token: m[2] };
  m = t.match(/\/start\s+fed_(\d+)_(\w+)/);
  if (m) return { type: 'fed', wsId: Number(m[1]), token: m[2] };
  m = t.match(/\/start\s+bxo_(\d+)/);
  if (m) return { type: 'bxo', id: Number(m[1]) };

  m = t.match(/\/start\s+bxth_(\d+)/);
  if (m) return { type: 'bxth', id: Number(m[1]) };
  return null;
}

export function nowIso() {
  return new Date().toISOString();
}

export function addMinutes(date, minutes) {
  return new Date(date.getTime() + minutes * 60000);
}

export function parseMoscowDateTime(input) {
  // expects "DD.MM HH:MM" Moscow time. Converts to Date (UTC) by subtracting 3 hours.
  const s = String(input || '').trim();
  const m = s.match(/^(\d{1,2})\.(\d{1,2})\s+(\d{1,2}):(\d{2})$/);
  if (!m) return null;
  const dd = Number(m[1]);
  const mm = Number(m[2]);
  const hh = Number(m[3]);
  const mi = Number(m[4]);
  const year = new Date().getUTCFullYear();
  // Moscow UTC+3
  const utc = new Date(Date.UTC(year, mm - 1, dd, hh - 3, mi, 0));
  return isNaN(utc.getTime()) ? null : utc;
}


export function computeThreadReplyStatus(thread, viewerUserId, opts = {}) {
  const afterHours = Number(opts.afterHours ?? 24);
  const retryEnabled = !!opts.retryEnabled;
  const now = opts.now instanceof Date ? opts.now : new Date();

  const buyerId = Number(thread?.buyer_user_id || 0);
  const sellerId = Number(thread?.seller_user_id || 0);
  const isBuyer = Number(viewerUserId || 0) === buyerId;
  const isSeller = Number(viewerUserId || 0) === sellerId;

  const buyerFirstMsgAt = thread?.buyer_first_msg_at ? new Date(thread.buyer_first_msg_at) : null;
  const sellerFirstReplyAt = thread?.seller_first_reply_at ? new Date(thread.seller_first_reply_at) : null;
  const retryIssuedAt = thread?.retry_issued_at ? new Date(thread.retry_issued_at) : null;

  let base = '';
  if (sellerFirstReplyAt) {
    base = '✅ replied';
  } else if (buyerFirstMsgAt) {
    base = isBuyer ? '⏳ waiting reply…' : '⏳ waiting your reply…';
  } else {
    base = isBuyer ? '✍️ write first msg' : '⏳ waiting first msg…';
  }

  // Retry status (only meaningful for buyer/brand side)
  let retry = '';
  if (retryEnabled && isBuyer && buyerFirstMsgAt && !sellerFirstReplyAt) {
    if (retryIssuedAt) {
      retry = '🎟 retry issued';
    } else if (Number.isFinite(afterHours) && afterHours > 0) {
      const elapsedH = (now.getTime() - buyerFirstMsgAt.getTime()) / 3600000;
      const left = Math.ceil(afterHours - elapsedH);
      if (elapsedH >= afterHours) retry = '♻️ retry eligible';
      else if (left > 0) retry = `⏳ retry ~${left}h`;
    }
  }

  return { base, retry, isBuyer, isSeller };
}

export function formatBxChargeLine(thread) {
  const src = String(thread?.intro_charge_source || '').toUpperCase();
  const cost = Number(thread?.intro_cost || 0);
  const chargedAt = thread?.intro_charged_at;

  if (!chargedAt && !src) return '';
  if (!cost && src !== 'RETRY') return '';

  let label = '';
  if (src === 'RETRY') label = 'Retry credit';
  else if (src === 'CREDITS') label = 'Brand Pass';
  else if (src) label = src;

  if (label) return `💳 Списано: ${cost} · ${label}`;
  return `💳 Списано: ${cost}`;
}

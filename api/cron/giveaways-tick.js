import { giveawaysTick } from '../../src/bot/cron.js';
import { CFG, assertEnv } from '../../src/lib/config.js';

function getBearerToken(req) {
  const h = req.headers?.authorization || req.headers?.Authorization || '';
  const s = String(h || '').trim();
  const m = s.match(/^Bearer\s+(.+)$/i);
  return m ? m[1].trim() : '';
}

export default async function handler(req, res) {
  try {
    // NOTE: Vercel Cron invokes endpoints via HTTP GET (docs),
    // but we also allow POST for manual runs (curl / external schedulers).
    const m = String(req.method || 'GET').toUpperCase();
    if (m !== 'GET' && m !== 'POST') {
      res.status(405).end('Method Not Allowed');
      return;
    }

    // Cron secret is REQUIRED in prod.
    if (!CFG.CRON_SECRET) {
      res.status(500).json({ ok: false, error: 'cron_secret_missing' });
      return;
    }

    const token = getBearerToken(req);
    if (!token || token !== String(CFG.CRON_SECRET)) {
      res.status(401).json({ ok: false, error: 'unauthorized' });
      return;
    }

    assertEnv();

    const r = await giveawaysTick();
    res.status(200).json({ ok: true, ...r });
  } catch (e) {
    console.error('[CRON] error', e);
    res.status(500).json({ ok: false, error: 'internal_error' });
  }
}

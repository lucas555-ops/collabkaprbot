import { getBot } from '../src/bot/bot.js';
import { assertEnv, CFG } from '../src/lib/config.js';

export default async function handler(req, res) {
  try {
    if (req.method !== 'POST') {
      res.status(405).end('Method Not Allowed');
      return;
    }

    // Telegram webhook secret token is REQUIRED for prod safety.
    if (!CFG.WEBHOOK_SECRET_TOKEN) {
      res.status(500).json({ ok: false, error: 'webhook_secret_missing' });
      return;
    }

    const token = req.headers['x-telegram-bot-api-secret-token'];
    if (!token || String(token) !== String(CFG.WEBHOOK_SECRET_TOKEN)) {
      res.status(401).json({ ok: false, error: 'unauthorized' });
      return;
    }

    assertEnv();
    const bot = getBot();

    const update = req.body;
    if (!update) {
      res.status(400).json({ ok: false, error: 'no_body' });
      return;
    }

    await bot.handleUpdate(update);
    res.status(200).json({ ok: true });
  } catch (e) {
    console.error('[WEBHOOK] error', e);
    // Non-2xx makes Telegram retry: better for reliability.
    res.status(500).json({ ok: false, error: 'internal_error' });
  }
}

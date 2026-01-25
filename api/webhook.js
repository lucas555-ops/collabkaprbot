import { getBot } from '../src/bot/bot.js';
import { assertEnv, CFG } from '../src/lib/config.js';

let botInitPromise = null;

// Prevent leaking secrets (bot token can appear in grammy BotError ctx)
const TOKEN_RE = /\b\d{6,}:[A-Za-z0-9_-]{20,}\b/g;
function redact(v) {
  const s = typeof v === 'string' ? v : (() => {
    try { return JSON.stringify(v); } catch { return String(v); }
  })();
  return s.replace(TOKEN_RE, '[REDACTED_BOT_TOKEN]');
}

function updateMeta(update) {
  const m = update?.message;
  const cb = update?.callback_query;
  return {
    update_id: update?.update_id,
    kind: m ? 'message' : cb ? 'callback' : 'other',
    from_id: m?.from?.id ?? cb?.from?.id ?? null,
    chat_id: m?.chat?.id ?? cb?.message?.chat?.id ?? null,
    text: typeof m?.text === 'string' ? m.text.slice(0, 80) : null,
    cb_data: typeof cb?.data === 'string' ? cb.data.slice(0, 80) : null,
  };
}

function safeError(e) {
  const ctx = e?.ctx;
  const u = ctx?.update;
  return {
    name: e?.name || e?.error?.name,
    message: redact(e?.message || e?.error?.message || e),
    update_id: u?.update_id ?? null,
    chat_id: ctx?.chat?.id ?? null,
    from_id: ctx?.from?.id ?? null,
  };
}

async function ensureBotInit(bot) {
  if (!botInitPromise) {
    botInitPromise = bot.init().catch((e) => {
      // если init упал на холодном старте — разрешаем повторить на следующем запросе
      botInitPromise = null;
      throw e;
    });
  }
  await botInitPromise;
}

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
    await ensureBotInit(bot);

    const update = req.body;
    if (!update) {
      res.status(400).json({ ok: false, error: 'no_body' });
      return;
    }

    // Helpful, short, safe webhook log (visible in Vercel "Messages" column)
    console.log('[WEBHOOK] in', updateMeta(update));

    await bot.handleUpdate(update);
    res.status(200).json({ ok: true });
  } catch (e) {
    console.error('[WEBHOOK] error', safeError(e));
    res.status(500).json({ ok: false, error: 'internal' });
  }
}

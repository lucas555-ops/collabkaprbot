import { getBot } from '../src/bot/bot.js';
import { assertEnv, CFG } from '../src/lib/config.js';

let botInitPromise = null;

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

function summarizeUpdate(update) {
  const u = update?.message?.from || update?.callback_query?.from || null;
  const chat = update?.message?.chat || update?.callback_query?.message?.chat || null;
  const text = update?.message?.text ? String(update.message.text) : '';
  const cb = update?.callback_query?.data ? String(update.callback_query.data) : '';
  let kind = 'other';
  if (update?.callback_query) kind = 'callback_query';
  else if (update?.message?.text) kind = 'message:text';
  else if (update?.message) kind = 'message';

  return {
    update_id: update?.update_id,
    kind,
    user_id: u?.id,
    username: u?.username,
    chat_id: chat?.id,
    text: text ? text.slice(0, 120) : undefined,
    cb_data: cb ? cb.slice(0, 120) : undefined,
  };
}

function safeErr(e) {
  const inner = e?.error || null;
  return {
    name: String(inner?.name || e?.name || 'Error'),
    message: String(inner?.message || e?.message || e || ''),
  };
}

export default async function handler(req, res) {
  try {
    const t0 = Date.now();
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

    // Success logs were missing before (Vercel "Messages" column was empty).
    // Keep it short + no secrets.
    console.log('[WEBHOOK] in ' + JSON.stringify(summarizeUpdate(update)));

    await bot.handleUpdate(update);

    console.log('[WEBHOOK] ok ' + JSON.stringify({ ms: Date.now() - t0, update_id: update?.update_id }));
    res.status(200).json({ ok: true });
  } catch (e) {
    // IMPORTANT: don't log the whole object (grammy BotError may include ctx.api.token).
    console.error('[WEBHOOK] error ' + JSON.stringify(safeErr(e)));
    res.status(500).json({ ok: false, error: 'internal' });
  }
}

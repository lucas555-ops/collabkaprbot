import { InlineKeyboard } from 'grammy';
import { CFG } from '../lib/config.js';
import { sponsorToChatId } from './sponsorParse.js';

function acc2Key(chat) {
  return ['mg', CFG.APP_ENV, 'acc2', chat].join(':');
}

async function mapLimit(items, limit, fn) {
  const res = new Array(items.length);
  let i = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (true) {
      const idx = i++;
      if (idx >= items.length) break;
      res[idx] = await fn(items[idx], idx);
    }
  });
  await Promise.all(workers);
  return res;
}

async function checkBotAdminCached(redis, api, chat, botId) {
  const key = acc2Key(chat);
  const cached = await redis.get(key);
  if (cached) {
    try {
      return typeof cached === 'string' ? JSON.parse(cached) : cached;
    } catch {}
  }

  try {
    const cm = await api.getChatMember(chat, botId);
    const st = String(cm.status || '');
    let res;
    if (st === 'administrator' || st === 'creator') res = { state: 'admin', status: st };
    else if (st === 'member') res = { state: 'member', status: st };
    else if (st === 'left' || st === 'kicked') res = { state: 'no', status: st };
    else res = { state: 'no', status: st || 'unknown' };
    await redis.set(key, JSON.stringify(res), { ex: 10 * 60 });
    return res;
  } catch (e) {
    const res = { state: 'no', status: 'error', reason: String(e?.message || e) };
    await redis.set(key, JSON.stringify(res), { ex: 5 * 60 });
    return res;
  }
}

function fmtLine(chat, a) {
  if (a.state === 'admin') return `✅ ${chat} — bot: <b>admin</b>`;
  if (a.state === 'member') return `🟦 ${chat} — bot: <b>member</b>`;
  return `❌ ${chat} — bot: <b>no access</b>`;
}

function accessHelpText(botUsername) {
  return (
`<b>Как исправить ❌</b>
Открой канал-спонсор → Управление → Администраторы → Добавить → @${botUsername}

<b>Почему это важно:</b>
без доступа бот не сможет подтвердить подписки участников (будет ❔/не подтверждено).`
  );
}

export async function renderGwAccess({ ctx, gwId, ownerUserId, redis, db, forceRecheck = false }) {
  const g = await db.getGiveawayForOwner(gwId, ownerUserId);
  if (!g) {
    await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
    return null;
  }

  const sponsorsRaw = await db.listGiveawaySponsors(gwId);
  const chats = sponsorsRaw.map((s) => sponsorToChatId(s.sponsor_text)).filter(Boolean);

  const botId = CFG.BOT_ID;
  const botUsername = CFG.BOT_USERNAME || 'YourBotUsername';

  if (!botId) {
    await ctx.answerCallbackQuery();
    await ctx.editMessageText(
      `⚠️ BOT_ID не задан.\n\nСделай /whoami в боте, возьми BOT_ID и добавь в env.`,
      { reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:gw_open|i:${gwId}`) }
    );
    return null;
  }

  if (!chats.length) {
    await ctx.answerCallbackQuery();
    await ctx.editMessageText(
      '🧩 Проверка доступа\n\nСпонсоров нет — проверять нечего ✅',
      { reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:gw_open|i:${gwId}`) }
    );
    return { adminCount: 0, memberCount: 0, noCount: 0, total: 0 };
  }

  if (forceRecheck) {
    for (const chat of chats) await redis.del(acc2Key(chat));
  }

  const limit = CFG.TG_ACCESS_CHECK_CONCURRENCY || 4;
  const results = await mapLimit(chats, limit, async (chat) => {
    const a = await checkBotAdminCached(redis, ctx.api, chat, botId);
    return { chat, a };
  });

  let adminCount = 0, memberCount = 0;
  const lines = [];

  for (const r of results) {
    if (r.a.state === 'admin') adminCount++;
    if (r.a.state === 'member') memberCount++;
    lines.push(fmtLine(r.chat, r.a));
  }

  const noCount = chats.length - adminCount - memberCount;

  const text =
`🧩 <b>Проверка доступа (бот в каналах-спонсорах)</b>

✅ admin: <b>${adminCount}</b>
🟦 member: <b>${memberCount}</b>
❌ no access: <b>${noCount}</b>

${lines.join('\n')}

${accessHelpText(botUsername)}`;

  const kb = new InlineKeyboard()
    .text('🔄 Обновить', `a:gw_access_recheck|i:${gwId}`)
    .row()
    .text('⬅️ Назад', `a:gw_open|i:${gwId}`);

  await ctx.answerCallbackQuery();
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });

  await db.auditGiveaway(gwId, g.workspace_id, ownerUserId, 'gw.access_checked', {
    adminCount, memberCount, noCount, total: chats.length
  });

  return { adminCount, memberCount, noCount, total: chats.length };
}

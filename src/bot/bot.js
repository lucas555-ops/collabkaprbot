import { Bot, InlineKeyboard } from 'grammy';
import { CFG, assertEnv } from '../lib/config.js';
import { redis, k, rateLimit, consumeOnce } from '../lib/redis.js';
import * as db from '../db/queries.js';
import { escapeHtml, fmtTs, parseCb, parseStartPayload, randomToken, addMinutes, parseMoscowDateTime, computeThreadReplyStatus, formatBxChargeLine } from './helpers.js';
import { parseSponsorsFromText, sponsorToChatId } from './sponsorParse.js';
import { setExpectText, getExpectText, clearExpectText, setDraft, getDraft, clearDraft } from './draft.js';
import { renderGwAccess } from './gwAccess.js';
import { makeSeed, makeXorShift32, sampleWithoutReplacement } from './prng.js';

let BOT;

// Brand Pass: brands pay credits for first contact (opening a new inbox thread)
const BRAND_PACKS = [
  { id: 'S', credits: 10, stars: 199, title: 'Brand Pass S' },
  { id: 'M', credits: 30, stars: 499, title: 'Brand Pass M' },
  { id: 'L', credits: 100, stars: 1299, title: 'Brand Pass L' }
];

function getBrandPack(packId) {
  return BRAND_PACKS.find(p => p.id === String(packId)) || null;
}

// Brand tools subscriptions (Brand Plan)
const BRAND_PLANS = [
  { id: 'basic', title: 'Brand Plan Basic', stars: CFG.BRAND_PLAN_BASIC_PRICE },
  { id: 'max', title: 'Brand Plan Max', stars: CFG.BRAND_PLAN_MAX_PRICE }
];

const MATCH_TIERS = [
  { id: 'S', title: 'Match S', stars: CFG.MATCH_S_PRICE, count: CFG.MATCH_S_COUNT },
  { id: 'M', title: 'Match M', stars: CFG.MATCH_M_PRICE, count: CFG.MATCH_M_COUNT },
  { id: 'L', title: 'Match L', stars: CFG.MATCH_L_PRICE, count: CFG.MATCH_L_COUNT }
];

const FEATURED_DURATIONS = [
  { id: '1d', days: 1, title: '24ч', stars: CFG.FEATURED_1D_PRICE },
  { id: '7d', days: 7, title: '7 дней', stars: CFG.FEATURED_7D_PRICE },
  { id: '30d', days: 30, title: '30 дней', stars: CFG.FEATURED_30D_PRICE }
];
const OFFICIAL_DURATIONS = [
  { id: "1d", days: 1, label: "24ч", price: CFG.OFFICIAL_1D_PRICE },
  { id: "7d", days: 7, label: "7 дней", price: CFG.OFFICIAL_7D_PRICE },
  { id: "30d", days: 30, label: "30 дней", price: CFG.OFFICIAL_30D_PRICE }
];


const CRM_STAGES = [
  { id: 'new', title: '🆕 New' },
  { id: 'talk', title: '💬 Talk' },
  { id: 'deal', title: '🤝 Deal' },
  { id: 'paid', title: '💳 Paid' },
  { id: 'done', title: '✅ Done' }
];

function isSuperAdminTg(tgId) {
  return CFG.SUPER_ADMIN_TG_IDS.includes(Number(tgId));
}

function fmtWait(sec) {
  const s = Math.max(0, Number(sec || 0));
  if (!Number.isFinite(s) || s <= 0) return 'несколько секунд';
  if (s < 60) return `${Math.ceil(s)} сек.`;
  if (s < 3600) return `${Math.ceil(s / 60)} мин.`;
  return `${Math.ceil(s / 3600)} ч.`;
}

// Sponsors helpers (Jobs-style clarity)
function normalizeSponsorsList(raw) {
  if (!Array.isArray(raw)) return [];
  const out = [];
  for (const item of raw) {
    if (!item) continue;
    if (typeof item === 'string') {
      const s = item.trim();
      if (s) out.push(s);
      continue;
    }
    if (typeof item === 'object') {
      const s = String(item.sponsor_text ?? item.sponsorText ?? item.sponsor ?? item.text ?? item.handle ?? item.username ?? '').trim();
      if (s) out.push(s);
      continue;
    }
    const s = String(item).trim();
    if (s) out.push(s);
  }
  // unique preserve order
  return [...new Set(out)];
}

function fmtSponsorHandle(raw) {
  const item = raw && typeof raw === 'object'
    ? (raw.sponsor_text ?? raw.sponsorText ?? raw.sponsor ?? raw.text ?? raw.handle ?? raw.username)
    : raw;
  const handle = sponsorToChatId(item);
  return handle || String(item || '').trim();
}

function sponsorUrlFromHandle(handle) {
  const h = String(handle || '').trim();
  if (!h) return null;
  if (h.startsWith('@') && h.length > 1) return `https://t.me/${h.slice(1)}`;
  if (/^https?:\/\//i.test(h)) return h;
  return null;
}

function sponsorsInlineText(rawSponsors, max = 3) {
  const handles = normalizeSponsorsList(rawSponsors).map(fmtSponsorHandle).filter(Boolean);
  if (!handles.length) return '';
  const shown = handles.slice(0, max);
  const rest = handles.length - shown.length;
  const inline = shown.join(', ');
  return rest > 0 ? `${inline} +${rest}` : inline;
}

function ruPlural(n, one, few, many) {
  const x = Math.abs(Number(n) || 0);
  const mod10 = x % 10;
  const mod100 = x % 100;
  if (mod10 === 1 && mod100 !== 11) return one;
  if (mod10 >= 2 && mod10 <= 4 && !(mod100 >= 12 && mod100 <= 14)) return few;
  return many;
}

function sponsorsBulletText(rawSponsors, max = 5) {
  const handles = normalizeSponsorsList(rawSponsors).map(fmtSponsorHandle).filter(Boolean);
  if (!handles.length) return '';
  const shown = handles.slice(0, max);
  const rest = handles.length - shown.length;
  const bullets = shown.map((h) => `• ${escapeHtml(h)}`).join(' ');
  return rest > 0 ? `${bullets} +${rest}` : bullets;
}

function sponsorsCountText(rawSponsors) {
  const handles = normalizeSponsorsList(rawSponsors).map(fmtSponsorHandle).filter(Boolean);
  const n = handles.length;
  if (!n) return '';
  const word = ruPlural(n, 'канал', 'канала', 'каналов');
  return `подписка на <b>${n}</b> ${word}`;
}

function sponsorStateIcon(state) {
  if (state === 'ok') return '✅';
  if (state === 'no') return '❌';
  if (state === 'unknown') return '⚠️';
  return '⚪';
}


// Runtime toggles (stored in Redis, editable from Admin)
const SYS_KEYS = {
  pay_accept: k(['sys', 'pay_accept']),
  pay_auto_apply: k(['sys', 'pay_auto_apply'])
};


// UI banners (optional): send banner image no more than once per N hours per slot per user
async function maybeSendBanner(ctx, slot, fileId) {
  try {
    const fid = String(fileId || '').trim();
    if (!fid) return;
    const uid = ctx?.from?.id ? Number(ctx.from.id) : 0;
    if (!uid) return;

    const hours = Number(CFG.BANNER_COOLDOWN_HOURS || 24);
    const ttlSec = (Number.isFinite(hours) && hours > 0 ? hours : 24) * 3600;

    const key = k(['ui_banner', slot || 'default', uid]);
    const seen = await redis.get(key);
    if (seen) return;

    await redis.set(key, '1', { ex: ttlSec });
    await ctx.replyWithPhoto(fid);
  } catch (_) {
    // ignore banner failures
  }
}

async function getSysBool(key, defaultValue = false) {
  try {
    const v = await redis.get(key);
    if (v === null || v === undefined) return Boolean(defaultValue);
    const s = String(v).toLowerCase();
    if (s === '1' || s === 'true' || s === 'on' || s === 'yes') return true;
    if (s === '0' || s === 'false' || s === 'off' || s === 'no') return false;
    return Boolean(defaultValue);
  } catch {
    return Boolean(defaultValue);
  }
}

async function setSysBool(key, value) {
  try {
    await redis.set(key, value ? '1' : '0');
    return true;
  } catch {
    return false;
  }
}

async function getPaymentsRuntimeFlags() {
  const accept = await getSysBool(SYS_KEYS.pay_accept, CFG.PAYMENTS_ACCEPT_DEFAULT);
  const autoApply = await getSysBool(SYS_KEYS.pay_auto_apply, CFG.PAYMENTS_AUTO_APPLY_DEFAULT);
  return { accept, autoApply };
}
async function sendStarsInvoice(ctx, { title, description, payload, amount, backCb }) {
  // Stars payments: currency XTR, prices must contain exactly one item.
  const chatId = ctx?.chat?.id;
  const userId = ctx?.from?.id;

  // Put the "cancel/help" hint into the invoice description to avoid sending a second message.
  const fullDescription = `${description}

Если передумал — жми «📋 Меню».`;

  // Prices must contain exactly one item for Stars.
  const prices = [{ label: 'СЧЁТ', amount: Number(amount) }];

  try {
    // IMPORTANT: For sendInvoice, if reply_markup is present and non-empty,
    // the FIRST button MUST be a Pay button (otherwise Telegram returns REPLY_MARKUP_BUY_EMPTY).
    // We'll keep everything in ONE invoice message:
    //   row1: Pay
    //   row2: Back/Menu (regular callback buttons)
    const navRow = [];
    if (backCb) navRow.push({ text: '⬅️ Назад', callback_data: backCb });
    navRow.push({ text: '📋 Меню', callback_data: 'a:menu' });

    const invoiceMarkup = {
      inline_keyboard: [
        [{ text: `⭐️ Оплатить (${Number(amount)} Stars)`, pay: true }],
        navRow,
      ],
    };

    // Stars: currency XTR, provider_token must be empty string
    await ctx.api.raw.sendInvoice({
      chat_id: chatId,
      title,
      description: fullDescription,
      payload,
      provider_token: '',
      currency: 'XTR',
      prices,
      reply_markup: invoiceMarkup,
    });

    return true;
  } catch (e) {
    const desc = String(e?.description || e?.error?.description || e?.message || e);
    console.error('[PAY] sendInvoice(stars) failed', {
      chat_id: chatId ?? null,
      from_id: userId ?? null,
      payload: String(payload || '').slice(0, 64),
      error: desc,
    });

    const isAdmin = isSuperAdminTg(userId);
    const text = isAdmin
      ? `❌ Не удалось отправить Stars-инвойс.
Причина: ${desc}

Проверь:
• Telegram клиент обновлён
• Тестируешь НЕ с аккаунта владельца бота
• Валидный Stars прайс (целое число Stars)
`
      : 'Не удалось отправить инвойс. Проверь, что Telegram обновлён и Stars доступны.';
    try {
      await ctx.reply(text, backCb ? { reply_markup: new InlineKeyboard().text('⬅️ Назад', backCb) } : undefined);
    } catch {}
    return false;
  }
}

async function renderGwNewWorkspacePicker(ctx, ownerUserId, backCb = 'a:gw_list') {
  const wss = await db.listWorkspaces(ownerUserId);
  const kb = new InlineKeyboard();
  if (!wss.length) {
    kb.text('⬅️ Меню', 'a:menu');
    await ctx.editMessageText('Сначала подключи канал: нажми «🚀 Подключить канал» в меню.', { reply_markup: kb });
    return;
  }

  for (const ws of wss) {
    const label = `📣 ${String(ws.title || ws.channel_username || ws.id).slice(0, 32)}`;
    kb.text(label, `a:gw_new|ws:${ws.id}`).row();
  }
  kb.text('⬅️ Назад', backCb).row().text('🏠 Меню', 'a:menu');

  await ctx.editMessageText(
    `Выбери канал, где создать новый конкурс:`,
    { reply_markup: kb }
  );
}


async function getRoleFlags(userRow, tgId) {
  const isAdmin = isSuperAdminTg(tgId);
  const isModerator = isAdmin || (userRow ? await db.isNetworkModerator(userRow.id) : false);
  const isFolderEditor = userRow ? await db.hasAnyWorkspaceEditorRole(userRow.id) : false;
  const isCurator = userRow ? await db.hasAnyCuratorRole(userRow.id) : false;
  return { isAdmin, isModerator, isFolderEditor, isCurator };
}

async function isModerator(userRow, tgId) {
  return isSuperAdminTg(tgId) || (userRow ? await db.isNetworkModerator(userRow.id) : false);
}

function isMissingRelationError(err, relation) {
  if (!err) return false;
  if (err.code === '42P01') {
    return relation ? String(err.message || '').includes(relation) : true;
  }
  const msg = String(err.message || '');
  if (!msg) return false;
  if (relation) return msg.includes('does not exist') && msg.includes(relation);
  return msg.includes('does not exist');
}

async function safeUserVerifications(primaryFn, fallbackFn) {
  try {
    return await primaryFn();
  } catch (e) {
    if (isMissingRelationError(e, 'user_verifications')) {
      return await fallbackFn();
    }
    throw e;
  }
}



async function safeBrandProfiles(primaryFn, fallbackFn) {
  try {
    return await primaryFn();
  } catch (e) {
    if (isMissingRelationError(e, 'brand_profiles')) {
      return await fallbackFn();
    }
    throw e;
  }
}

async function safeBrandApplications(primaryFn, fallbackFn) {
  try {
    return await primaryFn();
  } catch (e) {
    if (isMissingRelationError(e, 'brand_applications')) {
      return await fallbackFn();
    }
    throw e;
  }
}


function mainMenuKb(flags = {}) {
  const { isModerator = false, isAdmin = false, isFolderEditor = false, isCurator = false } = flags;

  const kb = new InlineKeyboard()
    .text('🚀 Подключить канал', 'a:setup')
    .text('📣 Мои каналы', 'a:ws_list')
    .row()
    .text('🎁 Розыгрыши', 'a:gw_list')
    .text('🎬 UGC / Офферы', 'a:bx_home')
    .row();

  if (isFolderEditor) {
    kb.text('📁 Папки', 'a:folders_my').text('🏷 Для брендов', 'a:bx_open|ws:0').row();
  } else {
    kb.text('🏷 Для брендов', 'a:bx_open|ws:0').row();
  }

  if (CFG.OFFICIAL_CHANNEL_USERNAME) {
    const uname = String(CFG.OFFICIAL_CHANNEL_USERNAME || '').replace(/^@/, '').trim();
    if (uname) kb.url('📢 Официальный канал', `https://t.me/${uname}`).row();
  }

  kb.text('🧭 Быстрый старт', 'a:guide').text('💬 Поддержка', 'a:support').row();
  kb.text('🔄 Обновить', 'a:menu').row();

  const extra = [];
  if (CFG.VERIFICATION_ENABLED) extra.push(['✅ Верификация', 'a:verify_home']);
  if (isCurator) extra.push(['👤 Куратор', 'a:cur_home']);
  if (isModerator) extra.push(['🛡 Модерация', 'a:mod_home']);
  if (isAdmin) extra.push(['👑 Админка', 'a:admin_home']);

  for (let i = 0; i < extra.length; i += 2) {
    const a = extra[i];
    const b = extra[i + 1];
    kb.text(a[0], a[1]);
    if (b) kb.text(b[0], b[1]);
    kb.row();
  }

  return kb;
}


function mainMenuCreatorKb(flags = {}, opts = {}) {
  const { isModerator = false, isAdmin = false, isFolderEditor = false, isCurator = false } = flags;

  const kb = new InlineKeyboard()
    .text('🚀 Подключить канал', 'a:setup')
    .text('📣 Мои каналы', 'a:ws_list')
    .row()
    .text('🎬 UGC / Офферы', 'a:bx_home')
    .text('🏷 Бренды', 'a:brands_home|p:0')
    .row()
    .text('🎁 Розыгрыши', 'a:gw_list')
    .text('🧭 Быстрый старт', 'a:guide')
    .row()
    .text('💬 Поддержка', 'a:support')
    .row();

  if (opts.canManager) kb.text('🧑‍💼 Кабинет менеджера', 'a:bm_home');
  kb.text('🏷 Я бренд', 'a:ui_mode_set|m:brand|ret:menu');

  const extra = [];
  if (CFG.VERIFICATION_ENABLED) extra.push(['✅ Верификация', 'a:verify_home']);
  if (isCurator) extra.push(['👤 Куратор', 'a:cur_home']);
  if (isModerator) extra.push(['🛡 Модерация', 'a:mod_home']);
  if (isAdmin) extra.push(['👑 Админка', 'a:admin_home']);

  for (let i = 0; i < extra.length; i += 2) {
    const a = extra[i];
    const b = extra[i + 1];
    kb.row().text(a[0], a[1]);
    if (b) kb.text(b[0], b[1]);
  }

  if (isFolderEditor) kb.row().text('📁 Папки', 'a:folders_my');

  return kb;
}

function mainMenuBrandKb(flags = {}, opts = {}) {
  const { isModerator = false, isAdmin = false } = flags;
  const { isManager = false, hasMultipleBrands = false, canManager = false, teamLocked = false } = opts;

  const kb = new InlineKeyboard()
    .text('🛍 Лента', 'a:bx_feed|ws:0|p:0')
    .text('🔎 Поиск креаторов', 'a:pm_home|ws:0')
    .row()
    .text('📨 Inbox', 'a:bx_inbox|ws:0|p:0')
	    .text('📝 Заявки', 'a:brand_apps|ws:0|s:new|p:0')
	    .row()
	    .text('📌 Сделки', 'a:brand_deals|ws:0|st:negotiation|p:0');

  if (!isManager) {
    kb.text('🎫 Brand Pass', 'a:brand_pass|ws:0')
      .row()
      .text('🏷 Профиль бренда', 'a:brand_profile|ws:0|ret:brand')
      .text('⭐️ Подписка', 'a:brand_plan|ws:0')
      .row()
      .text(teamLocked ? '👥 Команда бренда 🔒' : '👥 Команда бренда', 'a:brand_team|ws:0');
  } else {
    kb.text('ℹ️ Права менеджера', 'a:bm_help')
      .row();
    if (hasMultipleBrands) {
      kb.text('🔁 Сменить бренд', 'a:bm_pick_brand|ret:menu')
        .row();
    }
    kb.text('🔓 Режим владельца', 'a:bm_mode_set|v:0|ret:menu');
  }

  kb.row()
    .text('🧭 Быстрый старт', 'a:guide')
    .text('💬 Поддержка', 'a:support')
    .row()
    .text('✨ Я Creator / канал', 'a:ui_mode_set|m:creator|ret:menu');

  const extra = [];
  if (CFG.VERIFICATION_ENABLED) extra.push(['✅ Верификация', 'a:verify_home']);
  if (isModerator) extra.push(['🛡 Модерация', 'a:mod_home']);
  if (isAdmin) extra.push(['👑 Админка', 'a:admin_home']);

  for (let i = 0; i < extra.length; i += 2) {
    const a = extra[i];
    const b = extra[i + 1];
    kb.row().text(a[0], a[1]);
    if (b) kb.text(b[0], b[1]);
  }

  
  if (!isManager && canManager) {
    kb.row().text('🧑‍💼 Режим менеджера', 'a:bm_mode_set|v:1|ret:menu');
  }
return kb;
}

function bmModeKey(tgId) {
  return k(['bm_mode', Number(tgId || 0)]);
}
function bmActiveBrandKey(tgId) {
  return k(['bm_active_brand', Number(tgId || 0)]);
}
async function getBrandManagerMode(tgId) {
  const v = await redis.get(bmModeKey(tgId));
  return v === '1' || v === 1 || v === true;
}
async function setBrandManagerMode(tgId, enabled) {
  if (enabled) await redis.set(bmModeKey(tgId), '1');
  else await redis.del(bmModeKey(tgId));
}
async function getBmActiveBrand(tgId) {
  const v = await redis.get(bmActiveBrandKey(tgId));
  const n = Number(v || 0);
  return n || 0;
}
async function setBmActiveBrand(tgId, brandUserId) {
  const n = Number(brandUserId || 0);
  if (!n) return;
  await redis.set(bmActiveBrandKey(tgId), String(n));
}


function bmBrandLabelFromRow(row) {
  const id = Number(row?.user_id || 0);
  const name = row?.brand_name ? String(row.brand_name).trim() : '';
  const uname = row?.tg_username ? String(row.tg_username).trim() : '';
  if (name) return name;
  if (uname) return `@${uname}`;
  if (id) return `Бренд #${id}`;
  return 'Бренд';
}

async function clearBmActiveBrand(tgId) {
  await redis.del(bmActiveBrandKey(tgId));
}

async function disableBrandManagerState(tgId) {
  await setBrandManagerMode(tgId, false);
  await clearBmActiveBrand(tgId);
}

async function renderBmPickBrand(ctx, u, params = {}) {
  const edit = params.edit !== false;
  const ret = String(params.ret || 'menu');
  const wsId = Number(params.wsId || 0);
  const page = Number(params.page || 0);

  const bm = await resolveBmBrandContext(ctx, u, { requirePickWhenMissingActive: true });

  if (bm.dbMissing) {
    const kb = new InlineKeyboard().text('🏠 Меню', 'a:menu');
    const text = `⚠️ <b>Нужна миграция 026_brand_managers</b>

В Neon должна быть таблица <code>brand_managers</code>.`;
    if (edit) await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
    else await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb });
    return;
  }

  if (bm.revoked || !(bm.brands || []).length) {
    await disableBrandManagerState(ctx.from.id);
    const kb = new InlineKeyboard().text('🏠 Меню', 'a:menu');
    const text = `⛔ <b>Доступ менеджера отозван</b>

Если это ошибка — попроси владельца бренда добавить тебя в «👥 Команда бренда».`;
    if (edit) await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
    else await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb });
    return;
  }

  const brands = bm.brands || [];
  const active = await getBmActiveBrand(ctx.from.id);

  const kb = new InlineKeyboard();
  for (const b of brands) {
    const id = Number(b.user_id);
    const label = bmBrandLabelFromRow(b).slice(0, 32);
    const prefix = (id && id === Number(active)) ? '✅ ' : '';
    kb.text(`${prefix}${label}`, `a:bm_set_brand|bu:${id}|ret:${ret}|ws:${wsId}|p:${page}`).row();
  }

  const backCb = (
    ret === 'bx_inbox' ? `a:bx_inbox|ws:${wsId}|p:${page}` :
    ret === 'bx_feed' ? `a:bx_feed|ws:${wsId}|p:${page}` :
    ret === 'bx_open' ? `a:bx_open|ws:${wsId}` :
    ret === 'pm_home' ? `a:pm_home|ws:${wsId}` :
    ret === 'brand_apps' ? `a:brand_apps|ws:0|s:new|p:${page}` :
	    ret === 'brand_deals' ? `a:brand_deals|ws:0|st:negotiation|p:${page}` :
    'a:menu'
  );

  kb.row().text('⬅️ Назад', backCb).text('🏠 Меню', 'a:menu');

  const text = `🧑‍💼 <b>Выбери бренд для работы</b>

Я запомню выбор и открою кабинет выбранного бренда.`;
  if (edit) await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
  else await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function bmResolveAssert(ctx, u, wsId, ret = 'menu', page = 0) {
  const wsNum = Number(wsId);
  if (!Number.isFinite(wsNum) || wsNum !== 0) return { bm: { enabled: false }, userId: u.id };

  const bm = await resolveBmBrandContext(ctx, u, { requirePickWhenMissingActive: true });

  if (bm.dbMissing) {
    const kb = new InlineKeyboard().text('🏠 Меню', 'a:menu');
    const text = `⚠️ <b>Нужна миграция 026_brand_managers</b>

В Neon должна быть таблица <code>brand_managers</code>.`;
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
    return null;
  }

  if (bm.revoked) {
    await disableBrandManagerState(ctx.from.id);
    const kb = new InlineKeyboard().text('🏠 Меню', 'a:menu');
    const text = `⛔ <b>Доступ менеджера отозван</b>

Если это ошибка — попроси владельца бренда добавить тебя в «👥 Команда бренда».`;
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
    return null;
  }

  if (bm.enabled && bm.needsPick) {
    await renderBmPickBrand(ctx, u, { ret, wsId: wsNum, page, edit: true });
    return null;
  }

  return { bm, userId: (bm.enabled ? bm.brandUserId : u.id) };
}

async function resolveBmBrandContext(ctx, u, opts = {}) {
  const tgId = Number(ctx.from?.id || 0);
  const enabled = await getBrandManagerMode(tgId);
  if (!enabled) return { enabled: false, brands: [] };

  let brands = [];
  try {
    brands = await db.listBrandsForManager(u.id);
  } catch (e) {
    if (isMissingRelationError(e, 'brand_managers')) {
      return { enabled: false, brands: [], dbMissing: true };
    }
    return { enabled: false, brands: [] };
  }

  if (!brands.length) {
    return { enabled: false, brands: [], revoked: true };
  }

  const requirePickWhenMissingActive = !!opts.requirePickWhenMissingActive;
  const hasMultiple = brands.length > 1;

  let active = await getBmActiveBrand(tgId);
  const has = (id) => brands.some((b) => Number(b.user_id) === Number(id));

  if (!active || !has(active)) {
    if (hasMultiple && requirePickWhenMissingActive) {
      return { enabled: true, brands, needsPick: true, brandUserId: 0, brandLabel: '' };
    }
    active = Number(brands[0].user_id);
    await setBmActiveBrand(tgId, active);
  }

  const cur = brands.find((b) => Number(b.user_id) === Number(active)) || brands[0];
  const label = bmBrandLabelFromRow(cur);

  return { enabled: true, brandUserId: Number(cur.user_id), brandLabel: label, brands };
}

async function renderMainMenu(ctx, flags, params = {}) {
  const edit = params.edit !== false; // default true
  const u = params.user || (ctx.from ? await db.upsertUser(ctx.from.id, ctx.from.username ?? null) : null);

  const mode = await resolveUiMode(ctx.from?.id);
  let modeHuman = uiModeHuman(mode);
  let text;
  let kb;

  if (mode === UI_MODES.BRAND && u) {
    const bm = await resolveBmBrandContext(ctx, u, { requirePickWhenMissingActive: true });

    if (bm.dbMissing) {
      modeHuman = 'Brand Manager';
      text = `⚠️ <b>Нужна миграция 026_brand_managers</b>

В Neon должна быть таблица <code>brand_managers</code>.`;
      kb = new InlineKeyboard().text('🏠 Меню', 'a:menu');
    } else if (bm.revoked) {
      await disableBrandManagerState(ctx.from.id);
      modeHuman = 'Brand Manager';
      text = `⛔ <b>Доступ менеджера отозван</b>

Если это ошибка — попроси владельца бренда добавить тебя в «👥 Команда бренда».`;
      kb = new InlineKeyboard().text('🏠 Меню', 'a:menu');
    } else if (bm.enabled && bm.needsPick) {
      await renderBmPickBrand(ctx, u, { ret: 'menu', wsId: 0, page: 0, edit });
      return;
    } else if (bm.enabled) {
      modeHuman = 'Brand Manager';
      const base = `🏠 <b>Главное меню</b>

<b>Ты сейчас в режиме:</b> <b>${modeHuman}</b>
<b>Бренд:</b> <b>${escapeHtml(bm.brandLabel)}</b>
`;
      text = base + `
Для команды бренда — Inbox и поиск креаторов.

Выбери действие:`;
      kb = mainMenuBrandKb(flags, { isManager: true, hasMultipleBrands: (bm.brands || []).length > 1 });
    } else {
      const base = `🏠 <b>Главное меню</b>

<b>Ты сейчас в режиме:</b> <b>${modeHuman}</b>
`;
      text = base + `
Для брендов — поиск креаторов, лента офферов и Inbox.

Выбери действие:`;
      let canManager = false;
      try { canManager = (await db.listBrandsForManager(u.id)).length > 0; } catch { canManager = false; }

      // UX: show lock icon on Brand Team button until profile+purchase requirements met
      let teamLocked = false;
      try {
        const prof = await safeBrandProfiles(() => db.getBrandProfile(u.id), async () => null);
        const basicOk = isBrandBasicComplete(prof);
        let teamPaid = false;
        if (basicOk) {
          try { teamPaid = await db.hasBrandTeamUnlockPurchase(u.id); } catch { teamPaid = false; }
        }
        teamLocked = !(basicOk && teamPaid);
      } catch {
        teamLocked = false;
      }

      kb = mainMenuBrandKb(flags, { isManager: false, canManager, teamLocked });
    }
  } else if (mode === UI_MODES.BRAND) {
    const base = `🏠 <b>Главное меню</b>

<b>Ты сейчас в режиме:</b> <b>${modeHuman}</b>
`;
    text = base + `
Для брендов — поиск креаторов, лента офферов и Inbox.

Выбери действие:`;
    kb = mainMenuBrandKb(flags, { isManager: false });
  } else {
    const base = `🏠 <b>Главное меню</b>

<b>Ты сейчас в режиме:</b> <b>${modeHuman}</b>
`;
    text = base + `
Для Creator/UGC — подключение канала, витрина, лента и розыгрыши.

Выбери действие:`;
    let canManager = false;
    if (u) {
      try { canManager = (await db.listBrandsForManager(u.id)).length > 0; } catch { canManager = false; }
    }
    kb = mainMenuCreatorKb(flags, { canManager });
  }

  const opts = { parse_mode: 'HTML', reply_markup: kb };
  if (edit && ctx.callbackQuery?.message) {
    await ctx.editMessageText(text, opts);
  } else {
    await ctx.reply(text, opts);
  }
}


function curatorModeMenuKb(flags = {}) {
  const { isModerator = false, isAdmin = false } = flags;
  const kb = new InlineKeyboard()
    .text('👤 Кабинет куратора', 'a:cur_home')
    .row()
    .text('🧭 Быстрый старт', 'a:guide')
    .text('💬 Поддержка', 'a:support')
    .row()
    .text('🔓 Обычный режим', 'a:cur_mode_set|v:0|ret:menu')
    .row()
    .text('🔄 Обновить', 'a:menu');

  const extra = [];
  if (isModerator) extra.push(['🛡 Модерация', 'a:mod_home']);
  if (isAdmin) extra.push(['👑 Админка', 'a:admin_home']);
  for (let i = 0; i < extra.length; i += 2) {
    const a = extra[i];
    const b = extra[i + 1];
    kb.row().text(a[0], a[1]);
    if (b) kb.text(b[0], b[1]);
  }
  return kb;
}


function onboardingKb(flags = {}) {
  const { isModerator = false, isAdmin = false } = flags;
  const kb = new InlineKeyboard()
    .text('✨ Я канал / Creator', 'a:onb_creator')
    .row()
    .text('🏷 Я бренд', 'a:onb_brand')
    .row()
    .text('📋 Открыть меню', 'a:menu');
  // keep quick access for staff even in onboarding
  if (CFG.VERIFICATION_ENABLED) kb.row().text('✅ Верификация', 'a:verify_home');
  if (isModerator) kb.row().text('🛡 Модерация', 'a:mod_home');
  if (isAdmin) kb.row().text('👑 Админка', 'a:admin_home');
  return kb;
}

function navKb(backCb) {
  const kb = new InlineKeyboard();
  if (backCb) kb.text('⬅️ Назад', backCb);
  kb.text('🏠 Меню', 'a:menu');
  return kb;
}

function expectBackCb(exp) {
  if (!exp || !exp.type) return 'a:menu';
  if (exp.backCb) return String(exp.backCb);
  if (exp.back) return String(exp.back);
  const t = String(exp.type || '');
  const wsId = exp.wsId ? Number(exp.wsId) : null;
  const gwId = exp.gwId ? Number(exp.gwId) : null;

  if (t === 'curator_username') return wsId ? `a:cur_manage|ws:${wsId}` : 'a:menu';
  if (t === 'bm_username') return 'a:brand_team|ws:0';
  if (t === 'curator_note') return (wsId && gwId) ? `a:cur_gw_open|ws:${wsId}|i:${gwId}` : 'a:cur_home';

  if (t.startsWith('folder_')) return wsId ? `a:folders_home|ws:${wsId}` : 'a:menu';

  if (t.startsWith('brand_')) return 'a:bx_open|ws:0';
  if (t.startsWith('verify_')) return 'a:verify_home';

  if (t.startsWith('wsp_')) return wsId ? `a:wsp_open|ws:${wsId}` : 'a:ws_list';
  if (t.startsWith('gw_')) return wsId ? `a:gw_list_ws|ws:${wsId}` : 'a:gw_list';
  if (t.startsWith('bx_')) return exp.back ? String(exp.back) : 'a:bx_open|ws:0';

  if (t.startsWith('mod_verif_')) return 'a:mod_home';

  return 'a:menu';
}

async function setActiveWorkspace(tgId, wsId) {
  await redis.set(k(['active_ws', tgId]), String(wsId), { ex: 30 * 24 * 3600 });
}
async function getActiveWorkspace(tgId) {
  const v = await redis.get(k(['active_ws', tgId]));
  const n = Number(v);
  return n > 0 ? n : null;
}

// Curator UI mode (hide non-curator actions to reduce confusion)
async function setCuratorMode(tgId, enabled) {
  await redis.set(k(['cur_mode', tgId]), enabled ? '1' : '0', { ex: 365 * 24 * 3600 });
}

async function getCuratorMode(tgId) {
  const v = await redis.get(k(['cur_mode', tgId]));
  return String(v || '') === '1';
}

// UI mode: Creator vs Brand (reduce main menu overload)
const UI_MODES = { CREATOR: 'creator', BRAND: 'brand' };

function normalizeUiMode(mode) {
  const m = String(mode || '').toLowerCase().trim();
  if (m === 'brand') return UI_MODES.BRAND;
  return UI_MODES.CREATOR;
}

async function setUiMode(tgId, mode) {
  await redis.set(k(['ui_mode', tgId]), normalizeUiMode(mode), { ex: 365 * 24 * 3600 });
}

async function getUiMode(tgId) {
  const v = await redis.get(k(['ui_mode', tgId]));
  return normalizeUiMode(v || '');
}

async function resolveUiMode(tgId) {
  // Default: Creator. Onboarding / explicit switch sets Brand.
  const v = await redis.get(k(['ui_mode', tgId]));
  if (v) return normalizeUiMode(v);
  return UI_MODES.CREATOR;
}

function uiModeHuman(mode) {
  const m = normalizeUiMode(mode);
  return m === UI_MODES.BRAND ? 'Brand' : 'Creator';
}


// Curator meta for a giveaway (safe helpers): "checked" mark + notes history (last 3)
const CUR_GW_META_TTL_SEC = 180 * 24 * 3600; // ~180 days

function clipText(s, maxLen = 140) {
  const t = String(s ?? '').trim();
  const n = Number(maxLen) || 0;
  if (!n || t.length <= n) return t;
  return t.slice(0, Math.max(1, n - 1)) + '…';
}

function curatorLabelFromTg(from) {
  const uname = from?.username ? `@${from.username}` : '';
  const name = [from?.first_name, from?.last_name].filter(Boolean).join(' ').trim();
  if (uname && name) return `${uname} (${name})`;
  return uname || name || `tg:${from?.id}`;
}

function curatorLabelFromMeta(meta) {
  if (!meta) return '—';
  const uname = meta.by_username ? `@${meta.by_username}` : '';
  const name = String(meta.by_name || '').trim();
  if (uname && name) return `${uname} (${name})`;
  return uname || name || (meta.by_tg_id ? `tg:${meta.by_tg_id}` : '—');
}

function curatorNotesBlock(notes) {
  if (!Array.isArray(notes) || notes.length === 0) return '📝 Заметки: —';
  const shown = notes.slice(0, 3);
  const lines = shown
    .map((n) => {
      const txt = clipText(String(n?.text || ''), 140);
      const who = curatorLabelFromMeta(n);
      const when = n?.at ? fmtTs(n.at) : '—';
      return `• ${escapeHtml(txt)}\n  — <b>${escapeHtml(who)}</b> · ${escapeHtml(when)}`;
    })
    .join('\n\n');
  return `📝 <b>Заметки</b> (последние ${shown.length}):\n${lines}`;
}










async function getCurGwChecked(gwId) {
  try { return await redis.get(k(['cur_gw_checked', gwId])); } catch { return null; }
}
async function setCurGwChecked(gwId, meta) {
  try { await redis.set(k(['cur_gw_checked', gwId]), meta, { ex: CUR_GW_META_TTL_SEC }); } catch {}
}

async function getCurGwNotes(gwId, limit = 3) {
  const lim = Math.max(1, Math.min(10, Number(limit) || 3));
  const listKey = k(['cur_gw_notes', gwId]);

  // Prefer list history (new)
  try {
    if (typeof redis.lrange === 'function') {
      const raw = await redis.lrange(listKey, 0, lim - 1);
      const out = [];
      if (Array.isArray(raw)) {
        for (const item of raw) {
          if (item == null) continue;
          if (typeof item === 'object') {
            out.push(item);
          } else if (typeof item === 'string') {
            try { out.push(JSON.parse(item)); } catch { out.push({ text: item, at: Date.now() }); }
          } else {
            out.push({ text: String(item), at: Date.now() });
          }
        }
      }
      if (out.length) return out;
    }
  } catch {
    // ignore
  }

  // Fallback: legacy single note (old)
  try {
    const legacy = await redis.get(k(['cur_gw_note', gwId]));
    if (legacy) return [legacy].slice(0, lim);
  } catch {
    // ignore
  }


// Fallback: if Redis was flushed, try restore from DB audit (no migrations).
try {
  const rows = await db.listGiveawayCuratorNotesAudit(gwId, lim);
  if (Array.isArray(rows) && rows.length) {
    const out = [];
    for (const r of rows) {
      const p = r?.payload || {};
      const t = String(p.text || '').trim();
      if (!t) continue;
      out.push({
        text: t,
        by_tg_id: p.by_tg_id || null,
        by_username: p.by_username || null,
        by_name: p.by_name || null,
        at: r.created_at || null,
      });
    }
    if (out.length) return out;
  }
} catch {
  // ignore
}
  return [];
}

async function getCurGwNote(gwId) {
  const notes = await getCurGwNotes(gwId, 1);
  return notes && notes.length ? notes[0] : null;
}

async function setCurGwNote(gwId, meta) {
  // Push into history list (new) + keep legacy "last note" key (compat)
  const listKey = k(['cur_gw_notes', gwId]);
  try {
    const payload = typeof meta === 'string' ? meta : JSON.stringify(meta);
    if (typeof redis.lpush === 'function') {
      await redis.lpush(listKey, payload);
      if (typeof redis.ltrim === 'function') await redis.ltrim(listKey, 0, 2);
      if (typeof redis.expire === 'function') await redis.expire(listKey, CUR_GW_META_TTL_SEC);
    }
  } catch {
    // ignore
  }

  try { await redis.set(k(['cur_gw_note', gwId]), meta, { ex: CUR_GW_META_TTL_SEC }); } catch {}
}

function wsMenuKb(wsId) {
  return new InlineKeyboard()
    .text('➕ Новый розыгрыш', `a:gw_new|ws:${wsId}`)
    .text('🎁 Розыгрыши', `a:gw_list_ws|ws:${wsId}`)
    .row()
    .text('🎬 UGC / Офферы', `a:bx_open|ws:${wsId}`)
    .text('📁 Папки', `a:folders_home|ws:${wsId}`)
    .row()
    .text('👤 Профиль', `a:ws_profile|ws:${wsId}`)
    .text('⭐️ PRO', `a:ws_pro|ws:${wsId}`)
    .row()
    .text('👥 Кураторы', `a:ws_settings|ws:${wsId}`)
    .text('🧾 История', `a:ws_history|ws:${wsId}`)
    .row()
    .text('⬅️ Назад', 'a:ws_list');
}


function wsSettingsKb(wsId, s) {
  const net = s.network_enabled ? '🌐 Сеть: ✅ ВКЛ' : '🌐 Сеть: ❌ ВЫКЛ';
  const cur = s.curator_enabled ? '👤 Куратор: ВКЛ' : '👤 Куратор: ВЫКЛ';
  return new InlineKeyboard()
    .text(net, `a:net_q|ws:${wsId}|ret:ws`)
    .row()
    .text(cur, `a:ws_toggle_cur|ws:${wsId}`)
    .row()
    .text('👥 Управление кураторами', `a:cur_manage|ws:${wsId}`)
    .row()
    .text('⬅️ Назад', `a:ws_open|ws:${wsId}`);
}

function curManageKb(wsId) {
  return new InlineKeyboard()
    .text('👤 Пригласить ссылкой', `a:cur_invite|ws:${wsId}`)
    .row()
    .text('➕ Добавить по @username', `a:cur_add_username|ws:${wsId}`)
    .row()
    .text('👥 Список кураторов', `a:cur_list|ws:${wsId}`)
    .row()
    .text('⬅️ Назад', `a:ws_settings|ws:${wsId}`)
    .text('🏠 Меню', 'a:menu');
}



function brandTeamKb() {
  return new InlineKeyboard()
    .text('👤 Пригласить ссылкой', 'a:bm_invite|ws:0')
    .row()
    .text('➕ Добавить по @username', 'a:bm_add_username|ws:0')
    .row()
    .text('👥 Список менеджеров', 'a:bm_list|ws:0')
    .row()
    .text('⬅️ Назад', 'a:menu');
}


// Brand Team access: unlock after basic profile + Brand Pass / Brand Plan
function brandTeamLockedKb() {
  return new InlineKeyboard()
    .text('🏷 Профиль бренда', 'a:brand_profile|ws:0|ret:brand')
    .row()
    .text('🎫 Brand Pass', 'a:brand_pass|ws:0')
    .text('⭐️ Brand Plan', 'a:brand_plan|ws:0')
    .row()
    .text('⬅️ Назад', 'a:menu');
}

async function getBrandTeamGateState(ownerUserId) {
  const prof = await safeBrandProfiles(
    () => db.getBrandProfile(ownerUserId),
    async () => ({ __missing_relation: true })
  );

  if (prof && prof.__missing_relation) {
    return {
      ok: false,
      missingRelation: true,
      basicDone: 0,
      missingBasic: ['Название', 'Ниша', 'Контакт', 'Ссылка'],
      teamPaid: false
    };
  }

  const p = prof || {};
  const basic = [
    { key: 'brand_name', label: 'Название' },
    { key: 'niche', label: 'Ниша' },
    { key: 'contact', label: 'Контакт' },
    { key: 'brand_link', label: 'Ссылка' }
  ];

  const basicDone = basic.filter((x) => String(p[x.key] || '').trim()).length;
  const missingBasic = basic.filter((x) => !String(p[x.key] || '').trim()).map((x) => x.label);

  let teamPaid = false;
  try {
    teamPaid = await db.hasBrandTeamUnlockPurchase(ownerUserId);
  } catch {
    teamPaid = false;
  }
  if (!teamPaid) {
    try {
      teamPaid = await db.isBrandPlanActive(ownerUserId);
    } catch {
      teamPaid = false;
    }
  }

  const ok = isBrandBasicComplete(p) && teamPaid;
  return { ok, p, basicDone, missingBasic, teamPaid };
}

async function ensureBrandTeamUnlocked(ctx, u, { edit = true } = {}) {
  // Owner-only: managers cannot manage team
  const bm = await resolveBmBrandContext(ctx, u, { requirePickWhenMissingActive: false });

  if (bm.dbMissing) {
    const kb = new InlineKeyboard().text('🏠 Меню', 'a:menu');
    const text = `⚠️ <b>Нужна миграция 026_brand_managers</b>\n\nВ Neon должна быть таблица <code>brand_managers</code>.`;
    if (edit) await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
    else await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb });
    return null;
  }

  if (bm.enabled && bm.brandUserId !== u.id) {
    const kb = new InlineKeyboard().text('⬅️ Назад', 'a:menu').text('🏠 Меню', 'a:menu');
    const text = `⛔ <b>Только владелец бренда</b>\n\nМенеджер не может управлять «👥 Команда бренда».`;
    if (edit) await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
    else await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb });
    return null;
  }

  const st = await getBrandTeamGateState(u.id);

  if (st.missingRelation) {
    const kb = new InlineKeyboard().text('🏠 Меню', 'a:menu');
    const text = `⚠️ <b>Нужна миграция 024_brand_profiles</b>\n\nВ Neon должна быть таблица <code>brand_profiles</code>.`;
    if (edit) await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
    else await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb });
    return null;
  }

  if (!st.ok) {
    const statusProfile = st.missingBasic && st.missingBasic.length
      ? `• Профиль: ${st.basicDone}/4 (не хватает: <b>${escapeHtml(st.missingBasic.join(', '))}</b>)`
      : `• Профиль: ${st.basicDone}/4`;

    const statusPay = st.teamPaid
      ? '• Покупка: ✅ найдена'
      : '• Покупка: ❌ нет (нужен Brand Pass / Brand Plan)';

    const text = `👥 <b>Команда бренда</b>\n\nДобавь менеджеров, чтобы быстрее отвечать на заявки и закрывать сделки.\n\n<b>Условия доступа:</b>\n1) Заполнить профиль бренда (4 поля: Название, Ниша, Контакт, Ссылка)\n2) Купить <b>Brand Pass</b> или <b>Brand Plan</b>\n\n<b>Статус:</b>\n${statusProfile}\n${statusPay}\n\n<i>Зачем:</i> защита от спама и ценность брендовой покупки.`;

    const kb = brandTeamLockedKb();
    if (edit) await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
    else await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb });
    return null;
  }

  return st;
}

function brandManagersListKb(managers) {
  const kb = new InlineKeyboard();
  for (const m of managers) {
    const label = m.tg_username ? `@${m.tg_username}` : `id:${m.tg_id}`;
    kb.text(`🗑 ${label}`, `a:bm_rm_q|ws:0|u:${m.user_id}`).row();
  }
  kb.text('⬅️ Назад', 'a:brand_team|ws:0').text('🏠 Меню', 'a:menu');
  return kb;
}

function brandManagerRemoveConfirmKb(managerUserId) {
  return new InlineKeyboard()
    .text('✅ Удалить', `a:bm_rm_ok|ws:0|u:${managerUserId}`)
    .row()
    .text('⬅️ Отмена', 'a:bm_list|ws:0')
    .text('🏠 Меню', 'a:menu');
}

function netConfirmKb(wsId, enabled, ret) {
  const actionLabel = enabled ? '❌ Выключить сеть' : '✅ Включить сеть';
  const v = enabled ? 0 : 1;
  const cancelCb = String(ret) === 'bx' ? `a:bx_open|ws:${wsId}` : `a:ws_settings|ws:${wsId}`;
  return new InlineKeyboard()
    .text(actionLabel, `a:net_set|ws:${wsId}|v:${v}|ret:${String(ret) === 'bx' ? 'bx' : 'ws'}`)
    .row()
    .text('⬅️ Отмена', cancelCb);
}

async function renderNetConfirm(ctx, ownerUserId, wsId, ret = 'ws') {
  const ws = await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const enabled = !!ws.network_enabled;
  const state = enabled ? '🌐 Сеть: ✅ ВКЛ' : '🌐 Сеть: ❌ ВЫКЛ';
  const hint = enabled
    ? 'Если выключить, твой канал пропадёт из ленты и не сможет публиковать новые офферы в сети.'
    : 'Если включить, твой канал появится в сети и сможет видеть ленту и публиковать офферы.';

  await ctx.answerCallbackQuery();
  await ctx.editMessageText(`🌐 <b>Сеть</b>\n\nСейчас: <b>${escapeHtml(state)}</b>\n\n${escapeHtml(hint)}`, {
    parse_mode: 'HTML',
    reply_markup: netConfirmKb(wsId, enabled, ret)
  });
}

function curListKb(wsId, curators) {
  const kb = new InlineKeyboard();
  for (const c of curators) {
    const label = c.tg_username ? `@${c.tg_username}` : `id:${c.tg_id}`;
    kb.text(`🗑 ${label}`, `a:cur_rm_q|ws:${wsId}|u:${c.user_id}`).row();
  }
  kb.text('⬅️ Назад', `a:cur_manage|ws:${wsId}`).text('🏠 Меню', 'a:menu');
  return kb;
}

// -----------------------------
// Barters Marketplace (v0.9.1)
// -----------------------------

function bxMenuKb(wsId, networkEnabled = true) {
  const net = networkEnabled ? '🌐 Сеть: ✅ ВКЛ' : '🌐 Сеть: ❌ ВЫКЛ';
  const kb = new InlineKeyboard()
    .text('🛍 Лента', `a:bx_feed|ws:${wsId}|p:0`)
    .text('🎛 Фильтры', `a:bx_filters|ws:${wsId}`)
    .row()
    .text('📨 Inbox', `a:bx_inbox|ws:${wsId}|p:0`)
    .text('📦 Мои офферы', `a:bx_my|ws:${wsId}|p:0`)
    .row()
    .text('➕ Разместить оффер', `a:bx_new|ws:${wsId}`)
    .text('🏷 Для брендов', 'a:bx_open|ws:0');

  if (CFG.VERIFICATION_ENABLED) kb.row().text('✅ Верификация', 'a:verify_home');

  kb.row().text(net, `a:net_q|ws:${wsId}|ret:bx`);
  kb.row().text('⬅️ Назад', `a:ws_open|ws:${wsId}`);
  return kb;
}



function bxBrandMenuKb(wsId, credits, plan, retry = 0) {
  const planLabel = plan?.active ? (plan.name === 'max' ? 'Max ✅' : 'Basic ✅') : 'OFF';
  const kb = new InlineKeyboard()
    .text('🛍 Лента', `a:bx_feed|ws:${wsId}|p:0`)
    .text('🎛 Фильтры', `a:bx_filters|ws:${wsId}`)
    .row()
    .text('📨 Inbox', `a:bx_inbox|ws:${wsId}|p:0`)
    .text('📝 Заявки', `a:brand_apps|ws:${wsId}|s:new|p:0`)
    .row()
    .text(`🎫 Brand Pass: ${credits}${retry ? ' · 🎟' + retry : ''}`, `a:brand_pass|ws:${wsId}`)
    .row()
    .text('🏷 Профиль бренда', `a:brand_profile|ws:${wsId}|ret:brand`)
    .row()
    .text(`⭐️ Подписка: ${planLabel}`, `a:brand_plan|ws:${wsId}`)
    .text('🔎 Поиск креаторов', `a:pm_home|ws:${wsId}`)
    .row()
    .text('🎯 Smart-подбор', `a:match_home|ws:${wsId}`)
    .text('🔥 Featured', `a:feat_home|ws:${wsId}`);

  if (CFG.VERIFICATION_ENABLED) kb.row().text('✅ Верификация', 'a:verify_home');

  kb.row().text('⬅️ Назад', 'a:menu');
  return kb;
}



function isBrandBasicComplete(p) {
  if (!p) return false;
  return !!(
    String(p.brand_name || '').trim() &&
    String(p.niche || '').trim() &&
    String(p.contact || '').trim() &&
    String(p.brand_link || '').trim()
  );
}

function isBrandExtendedComplete(p) {
  if (!p) return false;
  return isBrandBasicComplete(p) && !!(
    String(p.geo || '').trim() &&
    String(p.collab_types || '').trim()
  );
}

// Brand profile: structured collaboration types (stored as CSV in brand_profiles.collab_types).
// No migrations: we reuse the existing text field and keep backward-compat with old free-form values.
const BRAND_COLLAB_TYPES = [
  { key: 'stories', title: '📲 Сторис' },
  { key: 'reels', title: '🎞 Reels' },
  { key: 'post', title: '🧾 Пост' },
  { key: 'review', title: '🎥 Обзор' },
  { key: 'unboxing', title: '📦 Распаковка' },
  { key: 'ugc', title: '🧩 UGC (контент)' },
  { key: 'integration', title: '📣 Реклама/упоминание' },
  { key: 'giveaway', title: '🎁 Розыгрыш' },
  { key: 'ambassador', title: '🧿 Амбассадорство' },
  { key: 'barter', title: '🤝 Бартер' },
  { key: 'cert', title: '🎟 Сертификат' },
  { key: 'paid', title: '💸 ₽ (оплата)' },
  { key: 'mixed', title: '🔁 Смешано' },
  { key: 'other', title: '✨ Другое' }
];

const BRAND_COLLAB_KEYS = new Set(BRAND_COLLAB_TYPES.map(x => x.key));
const BRAND_COLLAB_ALIASES = {
  // ru
  'сторис': 'stories',
  'story': 'stories',
  'stories': 'stories',
  'рилс': 'reels',
  'reels': 'reels',
  'reel': 'reels',
  'пост': 'post',
  'post': 'post',
  'обзор': 'review',
  'review': 'review',
  'распаковка': 'unboxing',
  'unboxing': 'unboxing',
  'ugc': 'ugc',
  'югс': 'ugc',
  'интеграция': 'integration',
  'реклама': 'integration',
  'упоминание': 'integration',
  'integration': 'integration',
  'розыгрыш': 'giveaway',
  'giveaway': 'giveaway',
  'give away': 'giveaway',
  'амбассадор': 'ambassador',
  'амбассадорство': 'ambassador',
  'ambassador': 'ambassador',
  'бартер': 'barter',
  'barter': 'barter',
  'сертификат': 'cert',
  'cert': 'cert',
  'руб': 'paid',
  '₽': 'paid',
  'деньги': 'paid',
  'оплата': 'paid',
  'paid': 'paid',
  'смешано': 'mixed',
  'mixed': 'mixed',
  'другое': 'other',
  'other': 'other'
};

function parseBrandCollabTypes(raw) {
  const s = String(raw || '').trim();
  if (!s) return [];

  const parts = s
    .split(/[,;\n]+/g)
    .map((x) => String(x || '').trim())
    .filter(Boolean);

  const out = [];
  const seen = new Set();

  for (let p of parts) {
    // strip leading emojis / bullets
    p = p.replace(/^[^0-9a-zA-Zа-яА-Я]+/g, '').trim();
    if (!p) continue;
    const low = p.toLowerCase();
    const key = BRAND_COLLAB_ALIASES[low] || (BRAND_COLLAB_KEYS.has(low) ? low : null);
    if (!key) continue;
    if (!seen.has(key)) {
      seen.add(key);
      out.push(key);
    }
  }

  return out;
}

function brandCollabTypesToCsv(keys) {
  const arr = Array.isArray(keys) ? keys.map(String).filter((k) => BRAND_COLLAB_KEYS.has(k)) : [];
  const seen = new Set();
  const out = [];
  for (const k of arr) {
    if (!seen.has(k)) {
      seen.add(k);
      out.push(k);
    }
  }
  return out.length ? out.join(',') : null;
}

function brandCollabTypesDisplay(raw) {
  const s = String(raw || '').trim();
  if (!s) return '—';
  const keys = parseBrandCollabTypes(s);
  if (keys.length) return fmtMatrix(keys, BRAND_COLLAB_TYPES);
  // legacy free-form value (keep as-is)
  return s;
}

async function renderBrandCollabTypesPicker(ctx, ownerUserId, params = {}) {
  const wsId = Number(params.wsId || 0);
  const ret = String(params.ret || 'brand');
  const bo = params.backOfferId ? Number(params.backOfferId) : null;
  const bp = params.backPage ? Number(params.backPage) : 0;

  const prof = await safeBrandProfiles(() => db.getBrandProfile(ownerUserId), async () => null);
  const raw = String(prof?.collab_types || '').trim();
  const selected = parseBrandCollabTypes(raw);

  const nowTxt = selected.length ? fmtMatrix(selected, BRAND_COLLAB_TYPES) : (raw ? raw : '—');
  const legacyHint = (!selected.length && raw)
    ? `\n\nℹ️ У тебя был текстовый список. Выбери пункты ниже — я переведу в структурированный формат.`
    : '';

  const thread = Array.isArray(app?.meta?.thread) ? app.meta.thread : [];

let text =
  `📌 <b>Сделка</b>
` +
  `Бренд: <b>${escapeHtml(brandName)}</b>
` +
  `Креатор: <b>${escapeHtml(who)}</b>
` +
  `Обновлено: <b>${escapeHtml(when)}</b>

` +
  `Стадия: <b>${escapeHtml(dealStageTitle(stage))}</b>

` +
  `<b>Сообщение:</b>
<code>${escapeHtml(msg || '—')}</code>`;

if (app.reply_text) {
  text += `

<b>Последний ответ бренда:</b>
<code>${escapeHtml(String(app.reply_text))}</code>`;
}

const threadBlock = formatBrandAppThread(thread, 8);
if (threadBlock) {
  text += `

<b>Диалог:</b>
${threadBlock}`;
}

const kb = new InlineKeyboard()
    .text('✏️ Название', `a:brand_prof_set${suf}|f:bn`)
    .text('🎯 Ниша', `a:brand_prof_set${suf}|f:ni`)
    .row()
    .text('☎️ Контакт', `a:brand_prof_set${suf}|f:ct`)
    .text('🔗 Ссылка', `a:brand_prof_set${suf}|f:bl`)
    .row()
    .text('➕ Расширить', `a:brand_prof_more${suf}`)
    .row();

  if (CFG.VERIFICATION_ENABLED) kb.text('✅ Верификация', 'a:verify_home').row();

  if (params.ret === 'lead' && isBrandBasicComplete(p)) {
    kb.text('✅ Продолжить → Заявка', `a:brand_continue${suf}`).row();
  }

  kb.text('🧹 Сбросить', `a:brand_prof_reset${suf}`).row();

  kb.text('⬅️ Назад', brandBackCb(params));

  const opts = { parse_mode: 'HTML', reply_markup: kb };
  if (params.edit && ctx.callbackQuery?.message) {
    await ctx.editMessageText(txt, opts);
  } else {
    await ctx.reply(txt, opts);
  }
}

async function renderBrandProfileMore(ctx, ownerUserId, params = {}) {
  const prof = await safeBrandProfiles(() => db.getBrandProfile(ownerUserId), async () => null);
  const p = prof || {};

  const txt =
    `➕ <b>Расширенный профиль бренда</b>

` +
    `Заполни детали — это повышает доверие (и помогает в Brand-верификации).

` +
    `• Гео: <b>${escapeHtml(p.geo || '—')}</b>
` +
    `• Форматы: <b>${escapeHtml(brandCollabTypesDisplay(p.collab_types))}</b>
` +
    `• Бюджет: <b>${escapeHtml(p.budget || '—')}</b>
` +
    `• Цели: <b>${escapeHtml(p.goals || '—')}</b>
` +
    `• Требования: <b>${escapeHtml(p.requirements || '—')}</b>`;

  const suf = brandCbSuffix(params);
  const kb = new InlineKeyboard()
    .text('🌍 Гео', `a:brand_prof_set${suf}|f:ge`)
    .text('🧩 Форматы', `a:brand_prof_set${suf}|f:ty`)
    .row()
    .text('💰 Бюджет', `a:brand_prof_set${suf}|f:bu`)
    .text('🎬 Цели', `a:brand_prof_set${suf}|f:go`)
    .row()
    .text('📎 Требования', `a:brand_prof_set${suf}|f:rq`)
    .row()
    .text('⬅️ Назад', `a:brand_profile${suf}`);

  const opts = { parse_mode: 'HTML', reply_markup: kb };
  if (params.edit && ctx.callbackQuery?.message) {
    await ctx.editMessageText(txt, opts);
  } else {
    await ctx.reply(txt, opts);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Brand Directory (Creator): list brands with basic profile + paid (pass/plan)
// Commit 16
// ─────────────────────────────────────────────────────────────────────────────

function brandContactUrl(contactRaw) {
  const c = String(contactRaw || '').trim();
  if (!c) return null;
  if (/^https?:\/\//i.test(c)) return c;
  if (c.startsWith('@') && c.length > 1) return `https://t.me/${c.slice(1)}`;
  // common inputs: t.me/xxx or telegram.me/xxx
  if (/^(t\.me|telegram\.me)\//i.test(c)) return `https://${c}`;
  if (/^https?:\/\/(t\.me|telegram\.me)\//i.test(c)) return c;
  return null;
}

// Split badges by semantics: formats vs payment
// Examples:
//  - Formats: integration · review
//  - Pay: barter · cert

// Human labels for brand collab tags (stored as keys, shown as emoji+labels)
// short: compact (used in list buttons), long: readable (used in brand card)
const BRAND_COLLAB_TAG_LABELS = {
  // formats
  integration: { short: '📣Реклама', long: '📣 Реклама' },
  review: { short: '🔎Обзор', long: '🔎 Обзор' },
  unboxing: { short: '📦Распак', long: '📦 Распаковка' },
  giveaway: { short: '🎁Розыг', long: '🎁 Розыгрыш' },
  ugc: { short: '🎬UGC', long: '🎬 UGC' },
  other: { short: '✨Другое', long: '✨ Другое' },
  // payments
  barter: { short: '🤝Бартер', long: '🤝 Бартер' },
  cert: { short: '🎟Серт', long: '🎟 Сертификат' },
  paid: { short: '💰Оплата', long: '💰 Оплата' },
  mixed: { short: '🔀Микс', long: '🔀 Микс' }
};

function brandCollabTagLabel(key, mode = 'long') {
  const k = String(key || '').trim();
  if (!k) return '';
  const m = BRAND_COLLAB_TAG_LABELS[k];
  if (!m) return k;
  return mode === 'short' ? m.short : m.long;
}

function brandCollabTagBadgesSplit(raw, opts = {}) {
  const maxFormats = Math.max(0, Math.min(12, Number(opts.maxFormats ?? 4)));
  const maxPay = Math.max(0, Math.min(12, Number(opts.maxPay ?? 3)));
  const filter = opts.filter || null;

  const keys = parseBrandCollabTypes(String(raw || '').trim());
  if (!keys.length) return { formatsArr: [], payArr: [], formats: '', pay: '' };

  const payKeys = new Set(['barter', 'cert', 'paid', 'mixed']);
  const offerPref = {
    ad: ['integration'],
    review: ['review', 'unboxing'],
    giveaway: ['giveaway'],
    other: ['other']
  };
  const compPref = {
    barter: ['barter'],
    cert: ['cert'],
    rub: ['paid'],
    mixed: ['mixed']
  };

  const pushUnique = (arr, seen, k) => {
    if (!k || seen.has(k)) return;
    seen.add(k);
    arr.push(k);
  };

  const formatsArr = [];
  const payArr = [];
  const seenF = new Set();
  const seenP = new Set();

  // Prefer tags that match active filters (so it's obvious why brand is shown)
  if (filter && filter.offerType && offerPref[filter.offerType]) {
    for (const k of offerPref[filter.offerType]) {
      if (keys.includes(k) && !payKeys.has(k)) pushUnique(formatsArr, seenF, k);
    }
  }
  if (filter && filter.compensationType && compPref[filter.compensationType]) {
    for (const k of compPref[filter.compensationType]) {
      if (keys.includes(k) && payKeys.has(k)) pushUnique(payArr, seenP, k);
    }
  }

  // Then fill the rest (formats first, then payments)
  for (const k of keys) {
    if (!payKeys.has(k)) pushUnique(formatsArr, seenF, k);
    if (maxFormats && formatsArr.length >= maxFormats) break;
  }
  for (const k of keys) {
    if (payKeys.has(k)) pushUnique(payArr, seenP, k);
    if (maxPay && payArr.length >= maxPay) break;
  }

  const formats = maxFormats ? formatsArr.slice(0, maxFormats).map(k => brandCollabTagLabel(k, 'long')).join(' · ') : '';
  const pay = maxPay ? payArr.slice(0, maxPay).map(k => brandCollabTagLabel(k, 'long')).join(' · ') : '';

  return { formatsArr, payArr, formats, pay };
}

// Back-compat single-line badges (older screens)
function brandCollabTagBadges(raw, opts = {}) {
  const max = Math.max(1, Math.min(12, Number(opts.max || 6)));
  const filter = opts.filter || null;

  // roughly split max across two groups
  const maxFormats = Math.max(1, Math.ceil(max * 0.6));
  const maxPay = Math.max(1, max - maxFormats);

  const split = brandCollabTagBadgesSplit(raw, { maxFormats, maxPay, filter });
  const parts = [];
  if (split.formats) parts.push(split.formats);
  if (split.pay) parts.push(split.pay);
  return parts.join(' · ');
}

function brandDirectoryButtonLabel(bp, filter = null) {
  const name = String(bp?.brand_name || 'Бренд').trim() || 'Бренд';
  const niche = String(bp?.niche || '').trim();

  const split = brandCollabTagBadgesSplit(bp?.collab_types, { maxFormats: 2, maxPay: 2, filter });
  const fmtMini = split.formatsArr.slice(0, 2).map(k => brandCollabTagLabel(k, 'short')).join('·');
  const payMini = split.payArr.slice(0, 2).map(k => brandCollabTagLabel(k, 'short')).join('·');

  const parts = [name];
  if (niche) parts.push(niche);
  if (fmtMini) parts.push(`Форматы:${fmtMini}`);
  if (payMini) parts.push(`Оплата:${payMini}`);

  // Telegram inline button text limit is 64 chars; keep some margin.
  return clipText(parts.join(' · '), 56);
}


// Brand Directory filters (Creator): stored in Redis per viewer (tgId)
// Shape: { category, offerType, compensationType }
async function getBrandDirFilter(tgId) {
  const key = k(['brand_dir_filter', tgId]);
  const v = await redis.get(key);
  const base = v || {};

  const f = {
    category: base.category ?? null,
    offerType: base.offerType ?? null,
    compensationType: base.compensationType ?? null
  };

  const norm = (x) => {
    if (x == null) return null;
    const s = String(x);
    if (!s || s === 'all' || s === 'undefined' || s === 'null') return null;
    return s;
  };

  // Back-compat (if any)
  if (f.category == null && base.cat != null) f.category = norm(base.cat);
  if (f.offerType == null && base.type != null) f.offerType = norm(base.type);
  if (f.compensationType == null && base.comp != null) f.compensationType = norm(base.comp);

  const needsPersist = (!base.category && !base.offerType && !base.compensationType) && (base.cat || base.type || base.comp);
  if (needsPersist) {
    try {
      await redis.set(key, f, { ex: 30 * 24 * 3600 });
    } catch {}
  }

  return f;
}

async function setBrandDirFilter(tgId, patch) {
  const key = k(['brand_dir_filter', tgId]);
  const cur = await getBrandDirFilter(tgId);
  const next = { ...cur, ...patch };
  await redis.set(key, next, { ex: 30 * 24 * 3600 });
  return next;
}

function brandDirFilterSummary(f) {
  return [
    `Категория: ${bxAnyLabel(f.category, 'cat')}`,
    `Формат: ${bxAnyLabel(f.offerType, 'type')}`,
    `Оплата: ${bxAnyLabel(f.compensationType, 'comp')}`
  ].join(' · ');
}

function brandDirFiltersKb(f, page = 0) {
  const kb = new InlineKeyboard()
    .text(`Категория: ${bxAnyLabel(f.category, 'cat')}`, `a:bd_fpick|k:cat|p:${page}`)
    .row()
    .text(`Формат: ${bxAnyLabel(f.offerType, 'type')}`, `a:bd_fpick|k:type|p:${page}`)
    .row()
    .text(`Оплата: ${bxAnyLabel(f.compensationType, 'comp')}`, `a:bd_fpick|k:comp|p:${page}`)
    .row()
    .text('♻️ Сбросить', `a:bd_freset|p:${page}`)
    .text('⬅️ Назад', `a:brands_home|p:${page}`);
  return kb;
}

function brandDirPickKb(key, page = 0) {
  const kb = new InlineKeyboard();
  if (key === 'cat') {
    kb.text('Все', `a:bd_fset|k:cat|v:all|p:${page}`).row();
    for (const c of BX_CATEGORIES) {
      kb.text(c.label, `a:bd_fset|k:cat|v:${c.key}|p:${page}`).row();
    }
  }
  if (key === 'type') {
    kb.text('Все', `a:bd_fset|k:type|v:all|p:${page}`).row();
    kb.text('📣 Реклама', `a:bd_fset|k:type|v:ad|p:${page}`).row();
    kb.text('🎥 Обзор', `a:bd_fset|k:type|v:review|p:${page}`).row();
    kb.text('🎁 Розыгрыш', `a:bd_fset|k:type|v:giveaway|p:${page}`).row();
    kb.text('✍️ Другое', `a:bd_fset|k:type|v:other|p:${page}`).row();
  }
  if (key === 'comp') {
    kb.text('Все', `a:bd_fset|k:comp|v:all|p:${page}`).row();
    kb.text('🤝 Бартер', `a:bd_fset|k:comp|v:barter|p:${page}`).row();
    kb.text('🎟 Сертификат', `a:bd_fset|k:comp|v:cert|p:${page}`).row();
    kb.text('💸 ₽', `a:bd_fset|k:comp|v:rub|p:${page}`).row();
    kb.text('🔁 Смешано', `a:bd_fset|k:comp|v:mixed|p:${page}`).row();
  }
  kb.text('⬅️ Назад', `a:brands_filters|p:${page}`);
  return kb;
}

async function renderBrandDirFilters(ctx, viewerUserId, params = {}) {
  const page = Math.max(0, Number(params.page || 0));
  const f = await getBrandDirFilter(ctx.from.id);
  const text = `🎛 <b>Фильтры каталога</b>

${escapeHtml(brandDirFilterSummary(f))}

Выбери, какие бренды показывать.`;
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: brandDirFiltersKb(f, page) });
}

async function renderBrandDirFilterPick(ctx, viewerUserId, params = {}) {
  const page = Math.max(0, Number(params.page || 0));
  const key = String(params.key || 'cat');
  const title = key === 'cat' ? 'Категория' : (key === 'type' ? 'Формат' : 'Оплата');
  await ctx.editMessageText(`🎛 <b>${title}</b>

Выбери значение:`, { parse_mode: 'HTML', reply_markup: brandDirPickKb(key, page) });
}

async function renderBrandsDirectory(ctx, viewerUserId, params = {}) {
  const page = Math.max(0, Number(params.page || 0));
  const edit = !!params.edit;
  const PAGE_SIZE = 8;
  const offset = page * PAGE_SIZE;

  const f = await getBrandDirFilter(ctx.from.id);

  const rows = await safeBrandProfiles(
    () => db.listBrandsDirectoryFiltered(PAGE_SIZE + 1, offset, f),
    async () => ({ __missing_relation: true })
  );
  if (rows && rows.__missing_relation) {
    const msg = '⚠️ В базе нет таблицы brand_profiles. Применяй миграцию migrations/024_brand_profiles.sql в Neon и повтори.';
    if (edit && ctx.callbackQuery?.message) await ctx.editMessageText(msg, { reply_markup: navKb('a:menu') });
    else await ctx.reply(msg, { reply_markup: navKb('a:menu') });
    return;
  }

  const list = Array.isArray(rows) ? rows : [];
  const hasMore = list.length > PAGE_SIZE;
  const items = list.slice(0, PAGE_SIZE);

  const hasActiveFilters = !!(f.category || f.offerType || f.compensationType);
  let text = `🏷 <b>Каталог брендов</b>\n\n` +
    `Фильтры: <b>${escapeHtml(brandDirFilterSummary(f))}</b>\n\n` +
    `Показываю бренды с заполненным профилем (4/4) и активной покупкой <b>Brand Pass</b> или <b>Brand Plan</b>.\n\n`;

  if (!items.length) {
    text += `Пока брендов нет.\n\n` +
      `Если ты бренд — заполни профиль и купи Brand Pass/Plan, тогда ты появишься в каталоге.`;
  } else {
    text += `Выбери бренд:`;
  }

  const kb = new InlineKeyboard();
  kb.text('🎛 Фильтры', `a:brands_filters|p:${page}`);
  if (hasActiveFilters) kb.text('♻️ Сброс', `a:bd_freset|p:${page}`);
  kb.row();
  for (const bp of items) {
    kb.text(brandDirectoryButtonLabel(bp, f), `a:brand_dir_open|u:${Number(bp.user_id)}|p:${page}`).row();
  }

  if (page > 0 || hasMore) {
    if (page > 0) kb.text('⬅️ Назад', `a:brands_home|p:${page - 1}`);
    if (hasMore) kb.text('➡️ Далее', `a:brands_home|p:${page + 1}`);
    kb.row();
  }

  kb.text('⬅️ Меню', 'a:menu');

  const extra = { parse_mode: 'HTML', reply_markup: kb };
  if (edit && ctx.callbackQuery?.message) await ctx.editMessageText(text, extra);
  else await ctx.reply(text, extra);
}

async function renderBrandDirectoryCard(ctx, viewerUserId, params = {}) {
  const brandUserId = Number(params.brandUserId || 0);
  const backPage = Math.max(0, Number(params.backPage || 0));
  const edit = !!params.edit;
  if (!brandUserId) return;

  const prof = await safeBrandProfiles(
    () => db.getBrandProfile(brandUserId),
    async () => ({ __missing_relation: true })
  );
  if (prof && prof.__missing_relation) {
    const msg = '⚠️ В базе нет таблицы brand_profiles. Применяй миграцию migrations/024_brand_profiles.sql в Neon и повтори.';
    if (edit && ctx.callbackQuery?.message) await ctx.editMessageText(msg, { reply_markup: navKb('a:menu') });
    else await ctx.reply(msg, { reply_markup: navKb('a:menu') });
    return;
  }
  if (!prof) {
    const kb = new InlineKeyboard().text('⬅️ Назад', `a:brands_home|p:${backPage}`).row().text('⬅️ Меню', 'a:menu');
    if (edit && ctx.callbackQuery?.message) await ctx.editMessageText('⚠️ Бренд не найден.', { reply_markup: kb });
    else await ctx.reply('⚠️ Бренд не найден.', { reply_markup: kb });
    return;
  }

  const name = String(prof.brand_name || 'Бренд').trim() || 'Бренд';
  const niche = String(prof.niche || '').trim();
  const geo = String(prof.geo || '').trim();
  const formats = brandCollabTypesDisplay(String(prof.collab_types || '').trim());

  let viewerFilter = null;
  try {
    viewerFilter = await getBrandDirFilter(ctx.from.id);
  } catch (_) {
    viewerFilter = null;
  }
  const splitTags = brandCollabTagBadgesSplit(String(prof.collab_types || '').trim(), { maxFormats: 8, maxPay: 6, filter: viewerFilter });
  const budget = String(prof.budget || '').trim();
  const goals = String(prof.goals || '').trim();
  const req = String(prof.requirements || '').trim();
  const link = String(prof.brand_link || '').trim();
  const contact = String(prof.contact || '').trim();

  let text = `🏷 <b>${escapeHtml(name)}</b>\n\n`;
  if (niche) text += `🎯 Ниша: <b>${escapeHtml(niche)}</b>\n`;
  if (geo) text += `🌍 Гео: <b>${escapeHtml(geo)}</b>\n`;
  if (formats && formats !== '—') text += `🧩 Форматы/условия: <b>${escapeHtml(formats)}</b>\n`;
  if (splitTags.formats) text += `🎬 Форматы: <code>${escapeHtml(splitTags.formats)}</code>
`;
  if (splitTags.pay) text += `💳 Оплата: <code>${escapeHtml(splitTags.pay)}</code>
`;
  if (budget) text += `💰 Бюджет: ${escapeHtml(clipText(budget, 240))}\n`;
  if (goals) text += `🎬 Цели: ${escapeHtml(clipText(goals, 240))}\n`;
  if (req) text += `📎 Требования: ${escapeHtml(clipText(req, 240))}\n`;

  text += `\n`;

  const kb = new InlineKeyboard();
  const brandUrl = link && (/^https?:\/\//i.test(link) ? link : null);
  if (brandUrl) kb.url('🔗 Ссылка бренда', brandUrl).row();

  const cUrl = brandContactUrl(contact);
  if (cUrl) kb.url('✍️ Контакт', cUrl).row();

  kb.text('📝 Оставить заявку', `a:brand_apply|u:${brandUserId}|p:${backPage}`).row();

  kb.text('⬅️ Назад к списку', `a:brands_home|p:${backPage}`).row();
  kb.text('⬅️ Меню', 'a:menu');

  const extra = { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true };
  if (edit && ctx.callbackQuery?.message) await ctx.editMessageText(text, extra);
  else await ctx.reply(text, extra);
}


function bxNeedNetworkKb(wsId) {
  return new InlineKeyboard()
    .text('🌐 Сеть: ❌ ВЫКЛ', `a:net_q|ws:${wsId}|ret:bx`)
    .row()
    .text('⬅️ Назад', `a:ws_open|ws:${wsId}`);
}


const BX_CATEGORIES = [
  { key: 'cosmetics', label: '💄 Косметика' },
  { key: 'fashion', label: '👗 Одежда' },
  { key: 'unboxing', label: '📦 Распаковка' },
  { key: 'other', label: '✨ Другое' }
];

function bxCategoryLabel(c) {
  return BX_CATEGORIES.find((x) => x.key === c)?.label || '✨ Другое';
}

function bxCategoryKb(wsId) {
  const kb = new InlineKeyboard();
  for (const c of BX_CATEGORIES) {
    kb.text(c.label, `a:bx_cat|ws:${wsId}|c:${c.key}`).row();
  }
  kb.text('🧩 Шаблоны', `a:bx_preset_home|ws:${wsId}`).row();
  kb.text('⬅️ Отмена', `a:bx_open|ws:${wsId}`);
  return kb;
}

function bxKindKb(wsId) {
  return new InlineKeyboard()
    .text('🎬 UGC', `a:bx_kind|ws:${wsId}|k:ugc`).row()
    .text('📣 Интеграция', `a:bx_kind|ws:${wsId}|k:integration`).row()
    .text('⬅️ Отмена', `a:bx_open|ws:${wsId}`);
}

const BX_PRESETS = [
  {
    id: 'review_barter_unboxing',
    title: '📦 Распаковка за бартер (любой бренд)',
    category: 'unboxing',
    offer_type: 'review',
    compensation_type: 'barter',
    example:
      'Заголовок: Ищу бренд для распаковки/обзора\n\nУсловия: обзор + 3 сторис. Аудитория: 500–2k. Гео: РФ. Хочу: бартер (товары для обзора). Контакт: @myname'
  },
  {
    id: 'ad_cert_cosmetics',
    title: '📣 Упоминание/реклама за сертификат (косметика)',
    category: 'cosmetics',
    offer_type: 'ad',
    compensation_type: 'cert',
    example:
      'Заголовок: Возьму рекламный интеграл за сертификат\n\nФормат: пост/сторис (обсуждаемо). Аудитория: 1k+. Гео: ваш город/РФ. Хочу: сертификат/скидка. Контакт: @myname'
  },
  {
    id: 'giveaway_mixed_other',
    title: '🎁 Розыгрыш с магазином (смешано)',
    category: 'other',
    offer_type: 'giveaway',
    compensation_type: 'mixed',
    example:
      'Заголовок: Розыгрыш совместно с брендом\n\nФормат: конкурс в канале + отметки. Аудитория: 1k+. Нужен приз от бренда, готова помочь с механикой. Хочу: приз+сертификат/бартер. Контакт: @myname'
  }
];

function bxPresetKb(wsId) {
  return new InlineKeyboard()
    .text(BX_PRESETS[0].title, `a:bx_preset_apply|ws:${wsId}|id:${BX_PRESETS[0].id}`)
    .row()
    .text(BX_PRESETS[1].title, `a:bx_preset_apply|ws:${wsId}|id:${BX_PRESETS[1].id}`)
    .row()
    .text(BX_PRESETS[2].title, `a:bx_preset_apply|ws:${wsId}|id:${BX_PRESETS[2].id}`)
    .row()
    .text('⬅️ Назад', `a:bx_new|ws:${wsId}`);
  }

function bxTypeKb(wsId) {
  return new InlineKeyboard()
    .text('📣 Реклама/упоминание', `a:bx_type|ws:${wsId}|t:ad`)
    .row()
    .text('🎥 Обзор/распаковка', `a:bx_type|ws:${wsId}|t:review`)
    .row()
    .text('🎁 Розыгрыш с магазином', `a:bx_type|ws:${wsId}|t:giveaway`)
    .row()
    .text('✍️ Другое', `a:bx_type|ws:${wsId}|t:other`)
    .row()
    .text('⬅️ Назад', `a:bx_new|ws:${wsId}`);
}

function bxCompKb(wsId) {
  return new InlineKeyboard()
    .text('🤝 Бартер', `a:bx_comp|ws:${wsId}|p:barter`)
    .row()
    .text('🎟 Сертификат', `a:bx_comp|ws:${wsId}|p:cert`)
    .row()
    .text('💸 ₽', `a:bx_comp|ws:${wsId}|p:rub`)
    .row()
    .text('🔁 Смешано', `a:bx_comp|ws:${wsId}|p:mixed`)
    .row()
    .text('⬅️ Назад', `a:bx_new|ws:${wsId}`);
}

function bxFiltersKb(wsId, f, page = 0) {
  const kb = new InlineKeyboard()
    .text(`Категория: ${bxAnyLabel(f.category, 'cat')}`, `a:bx_fpick|ws:${wsId}|k:cat|p:${page}`)
    .row()
    .text(`Формат: ${bxAnyLabel(f.offerType, 'type')}`, `a:bx_fpick|ws:${wsId}|k:type|p:${page}`)
    .row()
    .text(`Оплата: ${bxAnyLabel(f.compensationType, 'comp')}`, `a:bx_fpick|ws:${wsId}|k:comp|p:${page}`)
    .row()
    .text('♻️ Сбросить', `a:bx_freset|ws:${wsId}|p:${page}`)
    .text('⬅️ Назад', `a:bx_feed|ws:${wsId}|p:${page}`);
  return kb;
}

function bxPickKb(wsId, key, page = 0) {
  const kb = new InlineKeyboard();
  if (key === 'cat') {
    kb.text('Все', `a:bx_fset|ws:${wsId}|k:cat|v:all|p:${page}`).row();
    for (const c of BX_CATEGORIES) {
      kb.text(c.label, `a:bx_fset|ws:${wsId}|k:cat|v:${c.key}|p:${page}`).row();
    }
  }
  if (key === 'type') {
    kb.text('Все', `a:bx_fset|ws:${wsId}|k:type|v:all|p:${page}`).row();
    kb.text('📣 Реклама', `a:bx_fset|ws:${wsId}|k:type|v:ad|p:${page}`).row();
    kb.text('🎥 Обзор', `a:bx_fset|ws:${wsId}|k:type|v:review|p:${page}`).row();
    kb.text('🎁 Розыгрыш', `a:bx_fset|ws:${wsId}|k:type|v:giveaway|p:${page}`).row();
    kb.text('✍️ Другое', `a:bx_fset|ws:${wsId}|k:type|v:other|p:${page}`).row();
  }
  if (key === 'comp') {
    kb.text('Все', `a:bx_fset|ws:${wsId}|k:comp|v:all|p:${page}`).row();
    kb.text('🤝 Бартер', `a:bx_fset|ws:${wsId}|k:comp|v:barter|p:${page}`).row();
    kb.text('🎟 Сертификат', `a:bx_fset|ws:${wsId}|k:comp|v:cert|p:${page}`).row();
    kb.text('💸 ₽', `a:bx_fset|ws:${wsId}|k:comp|v:rub|p:${page}`).row();
    kb.text('🔁 Смешано', `a:bx_fset|ws:${wsId}|k:comp|v:mixed|p:${page}`).row();
  }
  kb.text('⬅️ Назад', `a:bx_filters|ws:${wsId}|p:${page}`);
  return kb;
}

function bxInboxNavKb(wsId, page, hasPrev, hasNext) {
  const kb = new InlineKeyboard();
  if (hasPrev) kb.text('⬅️', `a:bx_inbox|ws:${wsId}|p:${page - 1}`);
  if (hasNext) kb.text('➡️', `a:bx_inbox|ws:${wsId}|p:${page + 1}`);
    kb.row().text('⬅️ Назад', `a:bx_open|ws:${wsId}`);
  return kb;
}

function bxThreadKb(wsId, threadId, opts = {}) {
  const back = opts.back || 'inbox';
  const page = Number(opts.page || 0);
  const offerId = opts.offerId ? Number(opts.offerId) : null;
  const canStage = !!opts.canStage;
  const curStage = opts.stage ? String(opts.stage) : null;
  const proofsCount = Number.isFinite(Number(opts.proofsCount)) ? Number(opts.proofsCount) : null;

  const kb = new InlineKeyboard();

  if (canStage) {
    for (const st of CRM_STAGES) {
      const active = curStage && curStage === st.id;
      kb.text(active ? `✅ ${st.title}` : st.title, `a:bx_stage|ws:${wsId}|t:${threadId}|s:${st.id}|p:${page}|b:${back}${offerId ? `|o:${offerId}` : ''}`);
    }
    kb.row();
  }

  kb.text('✍️ Ответить', `a:bx_thread_reply|ws:${wsId}|t:${threadId}|p:${page}`)
    .text(proofsCount !== null ? `🧾 Proofs: ${proofsCount}` : '🧾 Proofs', `a:bx_proofs|ws:${wsId}|t:${threadId}|p:${page}|b:${back}${offerId ? `|o:${offerId}` : ''}`)
    .row()
    .text('✅ Закрыть', `a:bx_thread_close_q|ws:${wsId}|t:${threadId}|p:${page}`);

  if (opts.showRetryInfo) {
    const cbTail = `${offerId ? `|o:${offerId}` : ''}|b:${back}|p:${page}`;
    kb.row().text('ℹ️ Retry', `a:bx_retry_help|ws:${wsId}|t:${threadId}${cbTail}`);
  }

  if (offerId) kb.row().text('🔎 Оффер', `a:bx_pub|ws:${wsId}|o:${offerId}|p:${page}`);
    kb.row().text('🚩 Жалоба', `a:bx_report_thread|ws:${wsId}|t:${threadId}|p:${page}`);
    kb.row().text('⬅️ Назад', back === 'offer' && offerId ? `a:bx_pub|ws:${wsId}|o:${offerId}|p:${page}` : `a:bx_inbox|ws:${wsId}|p:${page}`);
  return kb;
}

function bxFeedNavKb(wsId, page, hasPrev, hasNext) {
  const kb = new InlineKeyboard();
  if (hasPrev) kb.text('⬅️', `a:bx_feed|ws:${wsId}|p:${page - 1}`);
  if (hasNext) kb.text('➡️', `a:bx_feed|ws:${wsId}|p:${page + 1}`);
    kb.row()
    .text('🎛 Фильтры', `a:bx_filters|ws:${wsId}|p:${page}`)
    .text('📨 Inbox', `a:bx_inbox|ws:${wsId}|p:0`);
    kb.row().text('⬅️ Назад', `a:bx_open|ws:${wsId}`);
  return kb;
}

function gwNewStepPrizeKb(wsId) {
  return new InlineKeyboard()
    .text('Бартер', `a:gw_prize|ws:${wsId}|t:barter`)
    .text('Сертификат', `a:gw_prize|ws:${wsId}|t:cert`)
    .row()
    .text('Деньги ₽', `a:gw_prize|ws:${wsId}|t:rub`)
    .text('Stars', `a:gw_prize|ws:${wsId}|t:stars`)
    .row()
    .text('Другое', `a:gw_prize|ws:${wsId}|t:other`)
    .row()
    .text('Пресеты', `a:gw_preset_home|ws:${wsId}`)
    .row()
    .text('⬅️ Отмена', `a:ws_open|ws:${wsId}`);
}

const GW_PRESETS = [
  {
    id: 'product_barter',
    title: 'Розыгрыш продукта (бартер)',
    prize_type: 'barter',
    prize_value_text: 'Розыгрыш продукта от спонсора (бартер). Доставка/условия — уточняем в треде.'
  },
  {
    id: 'cert_discount',
    title: 'Сертификат / скидка',
    prize_type: 'cert',
    prize_value_text: 'Сертификат/скидка от магазина — условия и номинал в описании/в треде.'
  },
  {
    id: 'cash_rub',
    title: 'Денежный приз в ₽',
    prize_type: 'rub',
    prize_value_text: 'Денежный приз в ₽. Сумма и способ выплаты — указать в описании.'
  }
];

function gwPrizePrompt(prizeType) {
  switch (String(prizeType)) {
    case 'barter':
      return `<b>Бартер</b>

Опиши, что именно разыгрываем + условия/доставку.
Пример: <i>"Бьюти-бокс (1 победитель), доставка по РФ"</i>`;
    case 'cert':
      return `<b>Сертификат / скидка</b>

Укажи номинал и условия.
Пример: <i>"Сертификат 3 000₽ в @shopname (1 победитель)"</i>`;
    case 'rub':
      return `<b>Денежный приз в ₽</b>

Укажи сумму и способ выплаты.
Пример: <i>"2 000₽ на карту/СБП (1 победитель)"</i>`;
    case 'stars':
      return `<b>Приз в Stars</b>

Укажи количество Stars и условия.
Пример: <i>"500 Stars (1 победитель)"</i>`;
    case 'other':
    default:
      return `Опиши приз одним сообщением (коротко и понятно).
Пример: <i>"Подарок + доставка"</i>`;
  }
}

function gwPresetKb(wsId) {
  return new InlineKeyboard()
    .text(GW_PRESETS[0].title, `a:gw_preset_apply|ws:${wsId}|id:${GW_PRESETS[0].id}`)
    .row()
    .text(GW_PRESETS[1].title, `a:gw_preset_apply|ws:${wsId}|id:${GW_PRESETS[1].id}`)
    .row()
    .text(GW_PRESETS[2].title, `a:gw_preset_apply|ws:${wsId}|id:${GW_PRESETS[2].id}`)
    .row()
    .text('⬅️ Назад', `a:gw_new|ws:${wsId}`);
  }

function gwNewStepWinnersKb(wsId) {
  return new InlineKeyboard()
    .text('1', `a:gw_winners|ws:${wsId}|n:1`)
    .text('2', `a:gw_winners|ws:${wsId}|n:2`)
    .text('3', `a:gw_winners|ws:${wsId}|n:3`)
    .text('5', `a:gw_winners|ws:${wsId}|n:5`)
    .row()
    .text('✍️ Ввести число', `a:gw_winners_custom|ws:${wsId}`)
    .row()
    .text('⬅️ Назад', `a:gw_new|ws:${wsId}`);
}

function gwNewStepDeadlineKb(wsId) {
  return new InlineKeyboard()
    .text('⏳ 1 час', `a:gw_deadline|ws:${wsId}|m:60`)
    .text('⏳ 6 часов', `a:gw_deadline|ws:${wsId}|m:360`)
    .row()
    .text('⏳ 24 часа', `a:gw_deadline|ws:${wsId}|m:1440`)
    .text('⏳ 3 дня', `a:gw_deadline|ws:${wsId}|m:4320`)
    .row()
    .text('✍️ Ввести (DD.MM HH:MM МСК)', `a:gw_deadline_custom|ws:${wsId}`)
    .row()
    .text('⬅️ Назад', `a:gw_step_sponsors|ws:${wsId}`);
}

function gwSponsorsOptionalKb(wsId) {
  return new InlineKeyboard()
    .text('✅ Без спонсоров (соло)', `a:gw_sponsors_skip|ws:${wsId}`)
    .row()
    .text('✍️ Ввести списком', `a:gw_sponsors_enter|ws:${wsId}`)
    .row()
    .text('📁 Из папки', `a:gw_sponsors_from_folder|ws:${wsId}`)
    .row()
    .text('🧭 Что такое спонсоры?', `a:gw_sponsors_help|ws:${wsId}`)
    .row()
    .text('⬅️ Назад', `a:gw_new|ws:${wsId}`);
}



function gwSponsorsReviewKb(wsId) {
  return new InlineKeyboard()
    .text('✍️ Изменить', `a:gw_sponsors_edit|ws:${wsId}`)
    .text('🧹 Очистить', `a:gw_sponsors_clear|ws:${wsId}`)
    .row()
    .text('➡️ Дальше', `a:gw_sponsors_next|ws:${wsId}`)
    .row()
    .text('⬅️ Назад', `a:gw_step_sponsors|ws:${wsId}`);
}

function gwConfirmKb(wsId) {
  return new InlineKeyboard()
    .text('👁 Превью', `a:gw_preview|ws:${wsId}`)
    .text('📣 Опубликовать', `a:gw_publish|ws:${wsId}`)
    .row()
    .text('🖼 Медиа', `a:gw_media_step|ws:${wsId}`)
    .text('⬅️ Назад', `a:gw_step_deadline|ws:${wsId}`);
}

function gwMediaKb(wsId, hasMedia = false) {
  const kb = new InlineKeyboard()
    .text('🖼 Фото', `a:gw_media_photo|ws:${wsId}`)
    .text('🎞 GIF', `a:gw_media_gif|ws:${wsId}`)
    .row()
    .text('🎥 Видео', `a:gw_media_video|ws:${wsId}`)
    .text('👁 Превью', `a:gw_preview|ws:${wsId}`)
    .row();

  if (hasMedia) {
    kb.text('🗑 Убрать', `a:gw_media_clear|ws:${wsId}`)
      .text('✅ Дальше', `a:gw_media_skip|ws:${wsId}`);
  } else {
    kb.text('⏭ Пропустить', `a:gw_media_skip|ws:${wsId}`);
  }

  kb.row().text('⬅️ Назад', `a:gw_step_deadline|ws:${wsId}`);
  return kb;
}

async function renderGwConfirm(ctx, wsId, opts = {}) {
  const { edit = true } = opts;
  const draft = (await getDraft(ctx.from.id)) || {};

  const prize = (draft.prize_value_text || '').trim() || '—';
  const winners = Number(draft.winners_count || 0) || 1;
  const sponsors = Array.isArray(draft.sponsors) ? draft.sponsors : [];
  const ends = draft.ends_at ? fmtTs(draft.ends_at) : '—';

  const mediaLabel = draft.media_file_id
    ? (draft.media_type === 'photo' ? '🖼 Фото' : (draft.media_type === 'video' ? '🎥 Видео' : '🎞 GIF'))
    : '—';

  const sponsorLines = sponsors.length
    ? sponsors.map(x => `• ${escapeHtml(String(x))}`).join('\n')
    : '—';

  const text = `✅ <b>Черновик конкурса</b>

🎁 Приз: <b>${escapeHtml(prize)}</b>
🏆 Мест: <b>${winners}</b>
⏳ Итоги: <b>${escapeHtml(String(ends))}</b>
🖼 Медиа: <b>${escapeHtml(mediaLabel)}</b>

Спонсоры:
${sponsorLines}

Если всё ок — жми “📣 Опубликовать”.`;

  const extra = { parse_mode: 'HTML', reply_markup: gwConfirmKb(wsId) };
  if (edit) return ctx.editMessageText(text, extra);
  return ctx.reply(text, extra);
}

async function renderGwMediaStep(ctx, wsId, opts = {}) {
  const { edit = true } = opts;
  const draft = (await getDraft(ctx.from.id)) || {};
  const hasMedia = !!draft.media_file_id;

  const current = hasMedia
    ? (draft.media_type === 'photo' ? '🖼 Фото' : (draft.media_type === 'video' ? '🎥 Видео' : '🎞 GIF'))
    : '—';

  const text = `🖼 <b>Медиа для поста</b> (необязательно)

Можно прикрепить фото, GIF или видео — так пост в канале выглядит “живее”.

Сейчас: <b>${escapeHtml(current)}</b>

Выбери действие:`;

  const extra = { parse_mode: 'HTML', reply_markup: gwMediaKb(wsId, hasMedia) };
  if (edit) return ctx.editMessageText(text, extra);
  return ctx.reply(text, extra);
}


function gwOpenKb(g, flags = {}) {
  const { isAdmin = false } = flags;
  const gwId = g.id;
  const kb = new InlineKeyboard()
    .text('📊 Статистика', `a:gw_stats|i:${gwId}`)
    .text('🧾 Лог', `a:gw_log|i:${gwId}`)
    .row();
  if (isAdmin) kb.text('🧩 Проверка доступа', `a:gw_access|i:${gwId}`).row();
  kb.text('📣 Напомнить проверить', `a:gw_remind_q|i:${gwId}`)
    .row()
    .text('👤 Кураторы', `a:ws_settings|ws:${g.workspace_id}`)
    .row();

  if (String(g.status || '').toUpperCase() === 'WINNERS_DRAWN' && !g.results_message_id && g.published_chat_id) {
    kb.text('📣 Опубликовать итоги', `a:gw_publish_results|i:${gwId}`).row();
  }

  kb
    .text('🏁 Завершить сейчас', `a:gw_end_now|i:${gwId}`)
    .row()
    .text('🗑 Удалить', `a:gw_del_q|i:${gwId}|ws:${g.workspace_id}`)
    .row()
    .text('⬅️ Назад', 'a:gw_list');
  return kb;
}

function participantKb(gwId, entry, opts = {}) {
  const pub = opts.pub ? '|pub:1' : '';
  const kb = new InlineKeyboard();

  // Optional: direct user to the missing sponsor channel
  const blocker = opts.blocker;
  const blockerHandle =
    opts.firstBlockerHandle ||
    (blocker && typeof blocker.chat === 'string' && blocker.chat.startsWith('@') ? blocker.chat : null);
  if (blockerHandle) {
    const url = sponsorUrlFromHandle(blockerHandle);
    if (url) kb.url(`🔗 Открыть ${blockerHandle}`, url).row();
  }

  // Primary actions
  if (!entry) {
    kb.text('🎟 Участвовать', `a:gw_join|i:${gwId}${pub}`).row();
  }

  const checkLabel = entry?.is_eligible ? '🔄 Проверить ещё раз' : '🔄 Проверить';
  kb.text(checkLabel, `a:gw_check|i:${gwId}${pub}`).row();

  if (opts.backTo?.text && opts.backTo?.cb) {
    kb.row().text(opts.backTo.text, opts.backTo.cb);
  }
  return kb;
}

function renderParticipantScreen(g, entry, opts = {}) {
  const statusEligible = Boolean(entry?.is_eligible);
  const isEnded = g.status === 'ended';
  // Status block (Jobs-style: one screen, no extra messages)
  let stLine;
  if (opts.checking) {
    stLine = '⏳ <b>проверяю подписки...</b>';
  } else if (statusEligible) {
    stLine = '✅ <b>участие подтверждено</b>';
  } else if (entry) {
    stLine = '✅ <b>участие записано</b> · нужно подтвердить подписки';
  } else {
    stLine = '🕒 <b>нажми “🎟 Участвовать”</b> чтобы записаться';
  }

  // Explain the blocker (first missing / unknown), if we know it
  let blockerLine = '';
  const blocker = opts.blocker || opts.elig?.firstBlocker;
  if (!opts.checking && !statusEligible && blocker) {
    const chat = blocker.chat;
    const handle = opts.elig?.firstBlockerHandle || (typeof chat === 'string' && chat.startsWith('@') ? chat : null);
    if (blocker.state === 'no') {
      blockerLine = handle
        ? `\n\n❌ Не хватает подписки: <b>${escapeHtml(handle)}</b>`
        : `\n\n❌ Не хватает подписки на один из каналов.`;
    } else if (blocker.state === 'unknown') {
      blockerLine = handle
        ? `\n\n⚠️ Не могу проверить: <b>${escapeHtml(handle)}</b> (бот не добавлен в канал)`
        : `\n\n⚠️ Бот не может проверить один из каналов-спонсоров.`;
    }
  }

  // Sponsors list (with simple status icons)
  const sponsors = normalizeSponsorsList(opts.sponsors);
  const stateMap = {};
  if (opts.elig?.results) {
    for (const r of opts.elig.results) stateMap[String(r.chat)] = r.state;
  }

  const iconFor = (st) => {
    if (st === 'ok') return '✅';
    if (st === 'no') return '❌';
    if (st === 'unknown') return '⚠️';
    return '⚪';
  };

  let sponsorsBlock = '';
  if (sponsors.length) {
    const lines = sponsors
      .map((s) => {
        const handle = fmtSponsorHandle(s);
        const st = stateMap[handle] || 'pending';
        return `${iconFor(st)} ${escapeHtml(handle)}`;
      })
      .join('\n');

    sponsorsBlock = `\n\n👥 <b>Спонсоры</b>\n${lines}`;

    const hasUnknown = Object.values(stateMap).includes('unknown');
    if (hasUnknown) {
      sponsorsBlock += `\n\n💡 Если бот не может проверить канал — попроси админа добавить бота в канал-спонсор.`;
    }
  }
  // Action hint (super short)
  const actionHint = opts.checking
    ? ''
    : !entry
      ? `\n\nНажми “🎟 Участвовать”, чтобы записаться.`
      : !statusEligible
        ? `\n\nНажми “🔄 Проверить”, чтобы подтвердить подписки.`
        : '';

  const waitHint = opts.checking ? `\n\n⏳ Это может занять 2–5 сек. Подожди…` : '';

  const tipLine = opts.hint ? `\n\n💡 1) Участвовать  2) Проверить` : '';

  return (
    `🎁 <b>Конкурс #${g.id}</b>\n\n` +
    `🎁 Приз: ${escapeHtml(g.prize_value_text || '—')}\n` +
    `🏆 Мест: ${g.winners_count || 1}\n` +
    `⏰ Итоги: ${escapeHtml(fmtTs(g.ends_at))}\n\n` +
    `Статус: ${stLine}\n` +
    `Статус конкурса: ${isEnded ? '🔴 Завершён' : '🟢 Идёт'}` +
    blockerLine +
    sponsorsBlock +
    actionHint +
    waitHint +
    tipLine
  );
}


async function ensureWorkspaceForOwner(ctx, ownerUserId) {
  const wsList = await db.listWorkspaces(ownerUserId);
  if (!wsList.length) {
    const u = await db.upsertUser(ctx.from.id, ctx.from.username ?? null);
    try { await clearExpectText(ctx.from.id); } catch {}
    const flags = await getRoleFlags(u, ctx.from.id);
    await ctx.reply('Сначала подключи канал: нажми “🚀 Подключить канал”.', { reply_markup: mainMenuKb(flags) });
    return null;
  }
  const active = await getActiveWorkspace(ctx.from.id);
  if (active) {
    const ws = await db.getWorkspace(ownerUserId, active);
    if (ws) return ws;
  }
  // pick first
  await setActiveWorkspace(ctx.from.id, wsList[0].id);
  return await db.getWorkspace(ownerUserId, wsList[0].id);
}

async function renderWsList(ctx, ownerUserId) {
  const items = await db.listWorkspaces(ownerUserId);
  if (!items.length) {
    await ctx.editMessageText(`У тебя пока нет подключенных каналов.

Нажми “🚀 Подключить канал”.`, { reply_markup: mainMenuKb(await getRoleFlags(await db.upsertUser(ctx.from.id, ctx.from.username ?? null), ctx.from.id)) });
    return;
  }
  const kb = new InlineKeyboard();
  for (const w of items) {
    const label = w.channel_username ? `@${w.channel_username}` : w.title;
    kb.text(label, `a:ws_open|ws:${w.id}`).row();
  }
  kb.text('🚀 Подключить ещё', 'a:setup').text('⬅️ В меню', 'a:menu');
  await ctx.editMessageText(`📣 <b>Мои каналы</b>

Это каналы, которые ты подключил к боту (workspace).

Выбери канал — дальше можно:
• ➕ создать новый конкурс
• 🎁 смотреть активные/прошлые конкурсы
• 🤝 бартер‑биржа и Inbox
• 👤 профиль/витрина и настройки

💡 Хочешь добавить ещё канал — жми «🚀 Подключить ещё».`, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderWsOpen(ctx, ownerUserId, wsId) {
  const ws = await db.getWorkspace(ownerUserId, wsId);
  if (!ws) {
    await ctx.answerCallbackQuery({ text: 'Канал не найден.' });
    return;
  }
  await setActiveWorkspace(ctx.from.id, wsId);
  const title = ws.channel_username ? `@${ws.channel_username}` : ws.title;
  await ctx.editMessageText(`📣 <b>${escapeHtml(title)}</b>

Выбери действие:`, { parse_mode: 'HTML', reply_markup: wsMenuKb(wsId) });
}

async function renderWsSettings(ctx, ownerUserId, wsId) {
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  const ws = isAdmin ? await db.getWorkspaceAny(wsId) : await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  if (!isAdmin && Number(ws.owner_user_id) !== Number(ownerUserId)) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  await db.ensureWorkspaceSettings(wsId);
  const s = await db.getWorkspace(ownerUserId, wsId);
  const settings = {
    network_enabled: s.network_enabled,
    curator_enabled: s.curator_enabled
  };
  await ctx.editMessageText(`👥 <b>Кураторы и сеть</b>

Канал: <b>${escapeHtml(ws.channel_username ? '@' + ws.channel_username : ws.title)}</b>`, {
    parse_mode: 'HTML',
    reply_markup: wsSettingsKb(wsId, settings)
  });
}

async function renderWsHistory(ctx, ownerUserId, wsId) {
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  const ws = isAdmin ? await db.getWorkspaceAny(wsId) : await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  if (!isAdmin && Number(ws.owner_user_id) !== Number(ownerUserId)) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  const items = await db.listWorkspaceAudit(wsId, 20);
  const lines = items.map(i => `• <b>${escapeHtml(i.action)}</b> — ${fmtTs(i.created_at)}`);
  const text = `🧾 <b>История действий</b>

${lines.length ? lines.join('\n') : 'Пока пусто.'}`;
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:ws_open|ws:${wsId}`) });
}



// Workspace Profile Matrix (IG leads → TG deals)
// Stored in workspace_settings (per-channel profile)
const PROFILE_VERTICALS = [
  { key: 'beauty', title: '💄 Косметика / уход' },
  { key: 'fashion', title: '👗 Одежда / обувь' },
  { key: 'jewelry', title: '💍 Украшения / аксессуары' },
  { key: 'home', title: '🏠 Дом / декор' },
  { key: 'food', title: '🍽️ Еда / кафе / FMCG' },
  { key: 'kids', title: '🧸 Дети / семья' },
  { key: 'fitness', title: '🧘 Фитнес / здоровье' },
  { key: 'tech', title: '📱 Тех / гаджеты' },
  { key: 'services', title: '🎓 Сервисы / обучение' }
];

const PROFILE_FORMATS = [
  { key: 'reels', title: '🎬 Вертикальное видео (Reels/TikTok)' },
  { key: 'stories', title: '📲 Stories / вставки' },
  { key: 'post', title: '🖼️ Фото / карусель' },
  { key: 'unboxing', title: '📦 Unboxing / распаковка' },
  { key: 'tryon', title: '🧥 Try-on / примерка' },
  { key: 'review', title: '⭐ Review / честный обзор' },
  { key: 'howto', title: '🧠 How‑to / инструкция' },
  { key: 'talking', title: '🗣️ Говорящая голова / отзыв' },
  { key: 'routine', title: '🧴 Routine / «как использую»' },
  { key: 'voice', title: '🎙️ Voice-over / без лица' },
  { key: 'ugc_ads', title: '🎯 UGC для рекламы (файлы)' },
  { key: 'giveaway', title: '🎁 Розыгрыш (опционально)' }
];

const PROFILE_MODE_LABELS = {
  channel: 'Канал (интеграции)',
  ugc: 'UGC (контент без аудитории)',
  both: 'Оба (канал + UGC)'
};

// ─────────────────────────────────────────────────────────────────────────────
// №4 Матчинг профилей (каталог витрин по нишам/форматам) — минимальный UX
// Brand → выбирает фильтры → получает список → открывает витрину → оставляет заявку
// ─────────────────────────────────────────────────────────────────────────────
const PM_LIMITS = { verticals: 3, formats: 5 };
const PM_PAGE_SIZE = 5;

function pmStateKey(tgId, wsId) {
  return k(['pm_state', tgId, Number(wsId || 0)]);
}

async function pmGetState(tgId, wsId) {
  const raw = await redis.get(pmStateKey(tgId, wsId));
  const s = raw && typeof raw === 'object' ? raw : {};
  return {
    v: Array.isArray(s.v) ? s.v.filter(Boolean) : [],
    f: Array.isArray(s.f) ? s.f.filter(Boolean) : []
  };
}

async function pmSetState(tgId, wsId, state) {
  await redis.set(pmStateKey(tgId, wsId), state, { ex: 60 * 60 }); // 1 час
}

async function pmResetState(tgId, wsId) {
  await redis.del(pmStateKey(tgId, wsId));
}

function pmHumanList(keys, dict) {
  if (!Array.isArray(keys) || !keys.length) return '—';
  const map = new Map(dict.map(d => [d.key, d.title]));
  return keys.map(k => map.get(k) || k).join(', ');
}

function pmHumanBullets(keys, dict) {
  if (!Array.isArray(keys) || !keys.length) return '—';
  const map = new Map(dict.map(d => [d.key, d.title]));
  return keys.map(k => `• ${map.get(k) || k}`).join('\n');
}


function contactUrlFromRaw(contactRaw) {
  const c = contactRaw ? String(contactRaw).trim() : '';
  if (!c) return null;
  const tg = wsTgUrlFromContact(c);
  if (tg) return tg;
  if (/^https?:\/\//i.test(c)) return c;
  if (/^t\.me\//i.test(c)) return 'https://' + c;
  if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(c)) return 'mailto:' + c;
  return null;
}

async function pmAssertAccess(ctx, ownerUserId, wsId) {
  const wsNum = Number(wsId || 0);
  if (wsNum === 0) return true;
  const ws = await db.getWorkspace(ownerUserId, wsNum);
  if (!ws) {
    await ctx.answerCallbackQuery({ text: 'Нет доступа к этому workspace.', show_alert: true });
    return false;
  }
  return true;
}

async function renderProfileMatchingHome(ctx, ownerUserId, wsId) {
  if (!(await pmAssertAccess(ctx, ownerUserId, wsId))) return;

  const st = await pmGetState(ctx.from.id, wsId);

  const text =
    `🔎 <b>Поиск креаторов</b>\n\n` +
    `Выбираешь ниши и форматы — бот показывает подходящие витрины.\n\n` +
    `🏷 Ниши:
${escapeHtml(pmHumanBullets(st.v, PROFILE_VERTICALS))}
`
    + `🎬 Форматы:
${escapeHtml(pmHumanBullets(st.f, PROFILE_FORMATS))}

` +
    `Нажми «🔎 Найти», чтобы открыть список.\n` +
    `Подсказка: 1–2 ниши + 2–3 формата обычно дают лучший результат.`;

  const kb = new InlineKeyboard()
    .text(`🏷 Ниши (${st.v.length}/${PM_LIMITS.verticals})`, `a:pm_pick|ws:${wsId}|t:v`)
    .text(`🎬 Форматы (${st.f.length}/${PM_LIMITS.formats})`, `a:pm_pick|ws:${wsId}|t:f`)
    .row()
    .text('🔎 Найти', `a:pm_run|ws:${wsId}|p:0`)
    .text('🗑 Сброс', `a:pm_reset|ws:${wsId}`)
    .row()
    .text('⬅️ Назад', `a:bx_open|ws:${wsId}`);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
}

async function renderProfileMatchingPick(ctx, ownerUserId, wsId, type) {
  if (!(await pmAssertAccess(ctx, ownerUserId, wsId))) return;

  const st = await pmGetState(ctx.from.id, wsId);
  const isV = type === 'v';
  const dict = isV ? PROFILE_VERTICALS : PROFILE_FORMATS;
  const sel = isV ? st.v : st.f;
  const max = isV ? PM_LIMITS.verticals : PM_LIMITS.formats;
  const title = isV ? '🏷 Выбор ниш' : '🎬 Выбор форматов';

  const kb = new InlineKeyboard();
  for (const it of dict) {
    const chosen = sel.includes(it.key);
    kb.text(`${chosen ? '✅ ' : ''}${it.title}`, `a:pm_tog|ws:${wsId}|t:${type}|k:${it.key}`).row();
  }
  kb.text('✅ Готово', `a:pm_home|ws:${wsId}`).text('🗑 Сброс', `a:pm_reset|ws:${wsId}`);

  const text =
    `${title}\n\n` +
    `Выбрано: <b>${sel.length}/${max}</b>\n` +
    `Нажимай по пунктам, чтобы включать/выключать ✅.`;

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
}

async function renderProfileMatchingResults(ctx, ownerUserId, wsId, page = 0) {
  if (!(await pmAssertAccess(ctx, ownerUserId, wsId))) return;

  const st = await pmGetState(ctx.from.id, wsId);
  const p = Math.max(0, Number(page || 0));
  const offset = p * PM_PAGE_SIZE;

  const rows = await db.searchWorkspaceProfilesByMatrix(st.v, st.f, offset, PM_PAGE_SIZE + 1);
  const hasNext = rows.length > PM_PAGE_SIZE;
  const items = rows.slice(0, PM_PAGE_SIZE);

  const head =
    `🔎 <b>Результаты поиска</b>\n\n` +
    `🏷 Ниши:
${escapeHtml(pmHumanBullets(st.v, PROFILE_VERTICALS))}
`
    + `🎬 Форматы:
${escapeHtml(pmHumanBullets(st.f, PROFILE_FORMATS))}

`;

  if (!items.length) {
    const kb = new InlineKeyboard()
      .text('⚙️ Изменить фильтры', `a:pm_home|ws:${wsId}`)
      .row()
      .text('⬅️ Назад', `a:bx_open|ws:${wsId}`);
    return ctx.editMessageText(
      head + '😶 Ничего не нашёл по фильтрам.\n\nПопробуй упростить фильтр (меньше ниш/форматов).',
      { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true }
    );
  }

  const lines = items
    .map((r, i) => {
      const channel = r.channel_username ? '@' + String(r.channel_username).replace(/^@/, '') : (r.profile_title || r.ws_title || 'канал');
      const name = r.profile_title || channel;
      const mode = PROFILE_MODE_LABELS[String(r.profile_mode || 'both')] || PROFILE_MODE_LABELS.both;
      const geo = r.profile_geo || '—';
      return `${offset + i + 1}) <b>${escapeHtml(String(name))}</b> · ${escapeHtml(String(mode))} · ${escapeHtml(String(geo))}`;
    })
    .join('\n');

  const text = head + lines + `\n\nНажми «👤 …», чтобы открыть витрину.`;

  const kb = new InlineKeyboard();
  for (const r of items) {
    const channel = r.channel_username ? '@' + String(r.channel_username).replace(/^@/, '') : (r.profile_title || r.ws_title || 'канал');
    const name = r.profile_title || channel;
    const short = String(name).slice(0, 28);
    const contactUrl = contactUrlFromRaw(r.profile_contact);

    kb.text(`👤 ${short}`, `a:pm_view|ws:${wsId}|id:${r.id}|p:${p}`);
    if (contactUrl) kb.url('💬', contactUrl);
    kb.row();
  }

  if (p > 0 || hasNext) {
    if (p > 0) kb.text('⬅️', `a:pm_run|ws:${wsId}|p:${p - 1}`);
    if (hasNext) kb.text('➡️', `a:pm_run|ws:${wsId}|p:${p + 1}`);
    kb.row();
  }

  kb.text('⚙️ Фильтры', `a:pm_home|ws:${wsId}`).text('⬅️ Назад', `a:bx_open|ws:${wsId}`);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
}



const LEAD_STATUSES = {
  new: { key: 'new', title: '🆕 Новые', icon: '🆕' },
  in_progress: { key: 'in_progress', title: '💬 В работе', icon: '💬' },
  closed: { key: 'closed', title: '✅ Закрытые', icon: '✅' },
  spam: { key: 'spam', title: '🗑 Спам', icon: '🗑' }
};

function normLeadStatus(s) {
  const v = String(s || '').toLowerCase();
  if (v === 'new' || v === 'in_progress' || v === 'closed' || v === 'spam') return v;
  return 'new';
}

function leadStatusIcon(s) {
  return (LEAD_STATUSES[normLeadStatus(s)] || LEAD_STATUSES.new).icon;
}


function wsBrandLink(wsId) {
  const un = String(CFG.BOT_USERNAME || '').replace(/^@/, '');
  if (!un) return null;
  return `https://t.me/${un}?start=wsp_${wsId}`;
}

function shortUrl(u) {
  const s = String(u || '').replace(/^https?:\/\//i, '');
  return s.length > 48 ? s.slice(0, 45) + '…' : s;
}

function fmtMatrix(keys, dict, empty = '—') {
  const arr = Array.isArray(keys) ? keys.map(String) : [];
  const set = new Set(arr);
  const titles = dict.filter(x => set.has(x.key)).map(x => x.title);
  return titles.length ? titles.join(', ') : empty;
}

function fmtMatrixList(ids, dict, empty = '—') {
  const arr = Array.isArray(ids) ? ids : [];

  // Aliases for legacy / short keys (some DB rows store simplified enums like 'kids', 'jewelry', 'reels').
  const ALIAS_VERTICALS = {
    kids: '🧸 Дети / семья',
    jewelry: '💍 Украшения / аксессуары',
    accessories: '💍 Украшения / аксессуары',
    beauty: '💄 Косметика / уход',
    fitness: '🧘 Фитнес / здоровье',
    health: '🧘 Фитнес / здоровье',
    tech: '📱 Тех / гаджеты',
    food: '🍽 Еда / кафе / FMCG',
    cafe: '🍽 Еда / кафе / FMCG',
    home: '🏠 Дом / декор',
    decor: '🏠 Дом / декор',
    services: '🎓 Сервисы / обучение',
    education: '🎓 Сервисы / обучение',
    fashion: '👗 Одежда / обувь',
    clothes: '👗 Одежда / обувь',
    shoes: '👗 Одежда / обувь',
  };

  const ALIAS_FORMATS = {
    reels: '🎬 Вертикальное видео (Reels/TikTok)',
    tiktok: '🎬 Вертикальное видео (Reels/TikTok)',
    vertical_video: '🎬 Вертикальное видео (Reels/TikTok)',
    howto: '🧠 How‑to / инструкция',
    how_to: '🧠 How‑to / инструкция',
    instruction: '🧠 How‑to / инструкция',
    voice: '🎙 Voice-over / без лица',
    voice_over: '🎙 Voice-over / без лица',
    talking: '🗣 Говорящая голова / отзыв',
    ugc_ads: '🎯 UGC для рекламы (файлы)',
    ugc: '🎯 UGC для рекламы (файлы)',
    giveaway: '🎁 Розыгрыш (опционально)',
    unboxing: '📦 Unboxing / распаковка',
    photo: '🖼 Фото / карусель',
    tryon: '👗 Try-on / примерка',
    review: '⭐ Review / честный обзор',
    stories: '📰 Stories / вставки',
  };

  const useAlias = (x) => {
    if (!x) return null;
    const key = String(x).trim();
    const isVert = (typeof PROFILE_VERTICALS !== 'undefined') && (dict === PROFILE_VERTICALS);
    const isFmt  = (typeof PROFILE_FORMATS !== 'undefined') && (dict === PROFILE_FORMATS);
    if (isVert) return ALIAS_VERTICALS[key] || null;
    if (isFmt)  return ALIAS_FORMATS[key] || null;
    return null;
  };

  const items = arr
    .map((x) => (dict && dict[x]) ? dict[x] : (useAlias(x) || x))
    .filter((x) => typeof x === 'string' && x.trim().length);

  if (!items.length) return empty;
  return items.map((x) => `• ${x}`).join('\n');
}




function wsIgHandleFromWs(ws) {
  const h = ws?.profile_ig ? String(ws.profile_ig).replace(/^@/, '') : '';
  return h ? h : null;
}

function wsIgUrlFromWs(ws) {
  const h = wsIgHandleFromWs(ws);
  return h ? `https://instagram.com/${h}` : null;
}

function wsTgUsernameFromContact(contact) {
  const raw = String(contact || '').trim();
  const m = raw.match(/^@([a-zA-Z0-9_]{5,})$/);
  return m ? m[1] : null;
}

function wsTgUrlFromContact(contact) {
  const un = wsTgUsernameFromContact(contact);
  return un ? `https://t.me/${un}` : null;
}

function formatWsContactCard(ws, wsId) {
  const channel = ws.channel_username ? '@' + String(ws.channel_username).replace(/^@/, '') : (ws.title || 'канал');
  const channelUrl = ws.channel_username ? `https://t.me/${String(ws.channel_username).replace(/^@/, '')}` : null;

  const ig = wsIgHandleFromWs(ws);
  const igUrl = wsIgUrlFromWs(ws);

  const contact = ws.profile_contact ? String(ws.profile_contact) : null;
  const contactTgUrl = wsTgUrlFromContact(contact);

  const link = wsBrandLink(wsId);

  const lines = [];
  lines.push(`👤 <b>${escapeHtml(String(ws.profile_title || channel))}</b>`);
  if (channelUrl) lines.push(`📣 TG канал: <a href="${escapeHtml(channelUrl)}">${escapeHtml(channel)}</a>`);
  else lines.push(`📣 TG канал: <b>${escapeHtml(channel)}</b>`);
  if (igUrl) lines.push(`📸 IG: <a href="${escapeHtml(igUrl)}">${escapeHtml(shortUrl(igUrl))}</a> <code>@${escapeHtml(ig)}</code>`);
  if (contactTgUrl) lines.push(`✉️ Контакт: <a href="${escapeHtml(contactTgUrl)}">${escapeHtml(contact)}</a>`);
  else if (contact) lines.push(`✉️ Контакт: <b>${escapeHtml(contact)}</b>`);
  if (link) lines.push(`🔗 Витрина: <a href="${escapeHtml(link)}">${escapeHtml(shortUrl(link))}</a>`);

  const ports = Array.isArray(ws.profile_portfolio_urls) ? ws.profile_portfolio_urls.filter(Boolean).slice(0, 3) : [];
  if (ports.length) {
    lines.push(`🗂 Портфолио:`);
    for (const u of ports) {
      lines.push(`• <a href="${escapeHtml(String(u))}">${escapeHtml(shortUrl(String(u)))}</a>`);
    }
  }

  return lines.join('\n');
}

function buildWsShareText(ws, wsId, variant = 'short') {
const link = wsBrandLink(wsId);

  // UI text shown inside bot (short/long preview).
  const v = String(variant || 'short');
  const fallbackTitle = ws.channel_username ? ('@' + String(ws.channel_username).replace(/^@/, '')) : (ws.title || 'Creator');
  const titleRaw = String(ws.profile_title || fallbackTitle || 'Creator');
  const title = titleRaw.replace(/^@/, '').trim();

  const verticals = fmtMatrixList(ws.profile_verticals, PROFILE_VERTICALS, '—');
  const formats = fmtMatrixList(ws.profile_formats, PROFILE_FORMATS, '—');
  const about = String(ws.profile_about || '').trim();

  if (v === 'long') {
    let t =
      `👋 Привет! Я делаю коллабы / UGC.\n\n`+
      (link ? `🔗 Витрина: ${link}\n\n` : '\n') +
      `🏷 Ниши:\n${verticals}\n` +
      `🎬 Форматы:\n${formats}\n` +
      (about ? `\nКоротко:\n${about}\n` : '') +
      `\nЧтобы оставить заявку: открой витрину и нажми «📝 Оставить заявку».`;
    return t;
  }

  // short
  let t =
    `👋 Привет! Я делаю коллабы / UGC.\n` +
    (link ? `🔗 Витрина: ${link}\n\n` : '\n') +
    `Оставь заявку: открой витрину и нажми «📝 Оставить заявку».`;
  return t;
}

function buildWsSharePlain(ws, wsId, variant = 'short') {
const link = wsBrandLink(wsId);

  const v = String(variant || 'short');
  const fallbackTitle = ws.channel_username ? ('@' + String(ws.channel_username).replace(/^@/, '')) : (ws.title || 'Creator');
  const titleRaw = String(ws.profile_title || fallbackTitle || 'Creator');
  const title = titleRaw.replace(/^@/, '').trim();

  const verticals = fmtMatrixList(ws.profile_verticals, PROFILE_VERTICALS, '—');
  const formats = fmtMatrixList(ws.profile_formats, PROFILE_FORMATS, '—');
  const about = String(ws.profile_about || '').trim();

  if (v === 'long') {
    let t =
      `👋 Привет! Я делаю коллабы / UGC.\n\n`+
      (link ? `🔗 Витрина: ${link}\n\n` : '\n') +
      `🏷 Ниши:\n${verticals}\n` +
      `🎬 Форматы:\n${formats}\n` +
      (about ? `\nКоротко:\n${about}\n` : '') +
      `\nЧтобы оставить заявку: открой витрину и нажми «📝 Оставить заявку».`;
    return t;
  }

  let t =
    `👋 Привет! Я делаю коллабы / UGC.\n` +
    (link ? `🔗 Витрина: ${link}\n\n` : '\n') +
    `Оставь заявку: открой витрину и нажми «📝 Оставить заявку».`;
  return t;
}


function buildLeadTemplateText(ws, lead, key = 'thanks') {
  const channel = ws.channel_username ? '@' + String(ws.channel_username).replace(/^@/, '') : ws.title;
  const to = String(ws.profile_title || channel);

  const wants = fmtMatrix(ws.profile_formats, PROFILE_FORMATS, 'UGC/интеграция');
  const formatsShort = wants;

  switch (String(key)) {
    case 'brief':
    case 'need_tz':
      return `Привет! Спасибо за заявку. Пришли, пожалуйста, бриф/ТЗ, референсы и дедлайн — я отвечу с форматом и ценой.`;
    case 'price':
    case 'budget':
      return `Привет! Чтобы назвать прайс, уточни: 🎬 UGC или 📣 интеграция, длительность/формат, дедлайн и бюджет (или диапазон).`;
    case 'timing':
      return `Привет! Подскажи дедлайн и объём (1/3/5 видео или другой пакет). Я скажу сроки производства и варианты.`;
    case 'delivery':
      return `Привет! Подскажи город/доставка и что за продукт — это влияет на сроки.`;
    case 'format':
      return `Привет! Уточни, пожалуйста, что нужно: 🎬 UGC или 📣 интеграция? По форматам у меня: ${formatsShort}.`;
    case 'discuss':
    case 'thanks':
    default:
      return `Привет! Спасибо за заявку. Давай обсудим детали: что за продукт, дедлайн, 🎬 UGC или 📣 интеграция и условия (бартер/бюджет).`;
  }
}
function normalizeIgHandle(input) {
  const raw = String(input || '').trim();
  if (!raw) return null;
  let s = raw.replace(/\s+/g, '');
  s = s.replace(/^@/, '');

  // instagram.com/<handle>
  const m = s.match(/instagram\.com\/([^\/\?\#]+)/i);
  if (m) {
    const seg = String(m[1] || '').trim();
    const bad = ['reel', 'p', 'tv', 'stories', 'explore'].includes(seg.toLowerCase());
    if (bad) return null;
    const hm = seg.replace(/^@/, '').match(/^([A-Za-z0-9._]{2,30})$/);
    return hm ? hm[1] : null;
  }

  // If it's some other URL — reject
  if (/^https?:\/\//i.test(s)) return null;

  // plain handle
  const hm = s.match(/^([A-Za-z0-9._]{2,30})$/);
  if (!hm) return null;
  const bad = ['reel', 'p', 'tv', 'stories', 'explore'].includes(hm[1].toLowerCase());
  if (bad) return null;
  return hm[1];
}

function parseUrlsFromText(input, max = 3) {
  const text = String(input || '');
  const re = /(https?:\/\/[^\s<>"']+)/gi;
  const out = [];
  let m;
  while ((m = re.exec(text))) {
    let u = String(m[1] || '').trim();
    // strip trailing punctuation
    u = u.replace(/[)\],.!?]+$/g, '');
    if (!u) continue;
    if (!out.includes(u)) out.push(u);
    if (out.length >= max) break;
  }
  return out;
}

function wsProfileKb(wsId, ws) {
  const vCount = Array.isArray(ws.profile_verticals) ? ws.profile_verticals.length : 0;
  const fCount = Array.isArray(ws.profile_formats) ? ws.profile_formats.length : 0;

  // UX: "Витрина" — главный CTA, дальше парные кнопки по смыслу.
  const kb = new InlineKeyboard()
    .text('🪟 Витрина', `a:wsp_preview|ws:${wsId}`)
    .row()
    .text(`🏷 Ниши (${vCount}/3)`, `a:ws_prof_verticals|ws:${wsId}`)
    .text(`🎬 Форматы (${fCount}/5)`, `a:ws_prof_formats|ws:${wsId}`)
    .row()
    .text('✏️ Название', `a:ws_prof_edit|ws:${wsId}|f:title`)
    .text('✏️ Контакт', `a:ws_prof_edit|ws:${wsId}|f:contact`)
    .row()
    .text('📸 Instagram', `a:ws_prof_edit|ws:${wsId}|f:ig`)
    .text('🔗 Портфолио', `a:ws_prof_edit|ws:${wsId}|f:portfolio`)
    .row()
    .text('✏️ Гео', `a:ws_prof_edit|ws:${wsId}|f:geo`)
    .text('📝 Описание', `a:ws_prof_edit|ws:${wsId}|f:about`)
    .row()
    .text('📨 Заявки', `a:ws_leads|ws:${wsId}|s:new|p:0`)
    .text('🔗 Поделиться', `a:ws_share|ws:${wsId}`)
    .row()
    .text('🧩 Режим', `a:ws_prof_mode|ws:${wsId}`)
    .text('📌 IG шаблоны', `a:ws_ig_templates|ws:${wsId}`)
    .row()
    .text('⬅️ Назад', `a:ws_open|ws:${wsId}`);

  return kb;
}


function hasText(v) {
  return v !== null && v !== undefined && String(v).trim().length > 0 && String(v).trim() !== '—';
}

function calcWsProfileProgress(ws) {
  // Core fields that most сильно влияют на конверсию
  const igOk = hasText(ws.profile_ig);
  const contactOk = hasText(ws.profile_contact);
  const verticalsOk = Array.isArray(ws.profile_verticals) && ws.profile_verticals.length > 0;
  const formatsOk = Array.isArray(ws.profile_formats) && ws.profile_formats.length > 0;
  const ports = Array.isArray(ws.profile_portfolio_urls) ? ws.profile_portfolio_urls : [];
  const portfolioOk = ports.length > 0;
  const aboutOk = hasText(ws.profile_about);

  const checks = [
    { key: 'ig', ok: igOk },
    { key: 'contact', ok: contactOk },
    { key: 'verticals', ok: verticalsOk },
    { key: 'formats', ok: formatsOk },
    { key: 'portfolio', ok: portfolioOk },
    { key: 'about', ok: aboutOk },
  ];

  const total = checks.length;
  const done = checks.filter(x => x.ok).length;
  const percent = Math.round((done / total) * 100);

  const missing = [];
  if (!portfolioOk) missing.push('🔗 Портфолио: добавь 1–3 ссылки — <b>самый сильный буст конверсии</b>');
  if (!formatsOk) missing.push('🎬 Форматы: выбери 3–5 (брендам проще выбрать)');
  if (!verticalsOk) missing.push('🏷 Ниши: выбери до 3 (точнее матчи)');
  if (!igOk) missing.push('📸 Instagram: укажи @ или ссылку (доверие)');
  if (!contactOk) missing.push('✉️ Контакт: @username / t.me/... (быстро договориться)');
  if (!aboutOk) missing.push('📝 Описание: 1–2 строки, что именно ты снимаешь');

  const nextHint = !portfolioOk
    ? '💡 Добавь 1 ссылку портфолио — это обычно сильнее всего повышает конверсию.'
    : '💡 Держи 1–3 лучших ссылок в портфолио — бренд решает по примерам.';

  return { total, done, percent, missing, portfolioOk, igOk, contactOk, verticalsOk, formatsOk, aboutOk, nextHint };
}


async function renderWsProfile(ctx, ownerUserId, wsId) {
  const ws0 = await db.getWorkspace(ownerUserId, wsId);
  if (!ws0) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  await db.ensureWorkspaceSettings(wsId);
  const ws = await db.getWorkspace(ownerUserId, wsId);
  const isPro = await db.isWorkspacePro(wsId);

  const channel = ws.channel_username ? '@' + ws.channel_username : ws.title;
  const name = ws.profile_title || channel;
  const mode = String(ws.profile_mode || 'both');
  const ig = ws.profile_ig ? String(ws.profile_ig) : null;

  const verticalsTxt = fmtMatrix(ws.profile_verticals, PROFILE_VERTICALS);
  const formatsTxt = fmtMatrix(ws.profile_formats, PROFILE_FORMATS);

  const geo = ws.profile_geo || '—';
  const contact = ws.profile_contact || '—';
  const about = ws.profile_about || '—';

  const link = wsBrandLink(wsId);

  let igLine = '—';
  if (ig) {
    igLine =
      `<a href="https://instagram.com/${escapeHtml(ig)}">instagram.com/${escapeHtml(ig)}</a>\n` +
      `<code>@${escapeHtml(ig)}</code>`;
  }

  let portLine = '—';
  const ports = Array.isArray(ws.profile_portfolio_urls) ? ws.profile_portfolio_urls : [];
  if (ports.length) {
    portLine = ports
      .slice(0, 3)
      .map(u => `• <a href="${escapeHtml(String(u))}">${escapeHtml(shortUrl(u))}</a>`)
      .join('\n');
  }

  const proLine = isPro ? '⭐️ PRO: <b>активен</b>' : '⭐️ PRO: <b>free</b>';
  const modeLine = PROFILE_MODE_LABELS[mode] || PROFILE_MODE_LABELS.both;

  const prog = calcWsProfileProgress(ws);
  const progressLine = `📈 Заполнено: <b>${prog.percent}%</b> (${prog.done}/${prog.total})`;
  const improveBlock = prog.missing.length
    ? (`\n\n⚡️ <b>Что добавить, чтобы заявки шли чаще</b>\n` + prog.missing.map(x => `• ${x}`).join('\n'))
    : `\n\n✅ Профиль выглядит 🔥 — можно лить трафик из IG.`;

  const text =
    `👤 <b>Профиль (витрина)</b>\n\n` +
    `<b>IG leads → TG deals</b>\n` +
    `Бренды находят тебя в Instagram → по ссылке открывают этот профиль → дальше всё в Telegram.\n\n` +
    `🪟 Витрина: открой кнопку ниже — там находится «📝 Оставить заявку».\n\n` +
    `Канал: <b>${escapeHtml(channel)}</b>\n` +
    `${proLine}\n${progressLine}${improveBlock}\n\n` +
    `Название/витрина: <b>${escapeHtml(name)}</b>\n` +
    `🧩 Режим: <b>${escapeHtml(modeLine)}</b>\n` +
    `📸 Instagram:\n${igLine}\n` +
    `🏷 Ниши: <b>${escapeHtml(verticalsTxt)}</b>\n` +
    `🎬 Форматы: <b>${escapeHtml(formatsTxt)}</b>\n` +
    `🔗 Портфолио:\n${portLine}\n` +
    `📝 Описание: <b>${escapeHtml(about)}</b>\n` +
    `✉️ Контакт: <b>${escapeHtml(contact)}</b>\n` +
    `📍 Гео: <b>${escapeHtml(geo)}</b>\n\n` +
    (link
      ? `🔗 <b>Ссылка для брендов</b> (вставь в IG bio / сторис):\n<code>${escapeHtml(link)}</code>`
      : `⚠️ Не задан BOT_USERNAME — ссылка для брендов недоступна.`);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: wsProfileKb(wsId, ws), disable_web_page_preview: true });
}


async function renderWsShareMenu(ctx, ownerUserId, wsId) {
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  const ws = isAdmin ? await db.getWorkspaceAny(wsId) : await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  if (!isAdmin && Number(ws.owner_user_id) !== Number(ownerUserId)) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const link = wsBrandLink(wsId);

  const text =
    `🔗 <b>Поделиться витриной</b>\n\n` +
    `Покажу готовый текст в этом сообщении — ты сможешь скопировать и переслать бренду.\n\n` +
    (link ? `Витрина: <a href="${escapeHtml(link)}">${escapeHtml(link)}</a>\n\n` : '') +
    `Выбери вариант:`;

  const kb = new InlineKeyboard()
    .text('📄 Коротко', `a:ws_share_send|ws:${wsId}|v:short`)
    .text('📄 Подробно', `a:ws_share_send|ws:${wsId}|v:long`)
    .row()
    .text('⬅️ Назад', `a:ws_profile|ws:${wsId}`);

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}

async function sendWsShareTextMessage(ctx, ownerUserId, wsId, variant = 'short') {
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  const ws = isAdmin ? await db.getWorkspaceAny(wsId) : await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  if (!isAdmin && Number(ws.owner_user_id) !== Number(ownerUserId)) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const text = buildWsShareText(ws, wsId, variant);

  // Показываем текст в этом же сообщении (чтобы не оставлять "висящие" сообщения без кнопок)
  const link = wsBrandLink(wsId) || '';
  const channel = ws.channel_username ? '@' + String(ws.channel_username).replace(/^@/, '') : (ws.title || 'канал');
  const channelUrl = ws.channel_username ? `https://t.me/${String(ws.channel_username).replace(/^@/, '')}` : '';
  const ig = wsIgHandleFromWs(ws);
  const igUrl = wsIgUrlFromWs(ws);
  const plain = (() => {
  const fallbackTitle = ws.channel_username ? ('@' + String(ws.channel_username).replace(/^@/, '')) : (ws.title || 'Creator');
  const titleRaw = String(ws.profile_title || fallbackTitle || 'Creator');
    const title = titleRaw.replace(/^@/, '').trim();
    const verticals = fmtMatrixList(ws.profile_verticals, PROFILE_VERTICALS, '—');
    const formats = fmtMatrixList(ws.profile_formats, PROFILE_FORMATS, '—');
    const about = String(ws.profile_about || '').trim();

    if (String(variant) === 'long') {
      let t =
        `👋 Привет! Я делаю коллабы / UGC.\n\n`+
        (link ? `🔗 Витрина: ${link}\n\n` : '\n') +
        `🏷 Ниши:\n${verticals}\n` +
        `🎬 Форматы:\n${formats}\n` +
        (about ? `\nКоротко:\n${about}\n` : '') +
        `\nЧтобы оставить заявку: открой витрину и нажми «📝 Оставить заявку».`;
      return t;
    }

    // short
    let t =
      `👋 Привет! Я делаю коллабы / UGC.\n` +
      (link ? `🔗 Витрина: ${link}\n\n` : '\n') +
      `Оставь заявку: открой витрину и нажми «📝 Оставить заявку».`;
    return t;
  })();;;
  const shareUrl = `https://t.me/share/url?url=${encodeURIComponent('⁠')}&text=${encodeURIComponent(plain)}`;

  const kb = new InlineKeyboard()
    .url('📨 Отправить', shareUrl)
    .row()
    .text('⬅️ Назад', `a:ws_share|ws:${wsId}`)
    .text('👤 Профиль', `a:ws_profile|ws:${wsId}`);

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }

  try { await ctx.answerCallbackQuery({ text: '✅ Текст открыт' }); } catch {}
}


async function renderWsIgTemplatesMenu(ctx, ownerUserId, wsId) {
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  const ws = isAdmin ? await db.getWorkspaceAny(wsId) : await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  if (!isAdmin && Number(ws.owner_user_id) !== Number(ownerUserId)) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const link = wsBrandLink(wsId);
  const channel = ws.channel_username ? '@' + ws.channel_username : ws.title;
  const to = String(ws.profile_title || channel);

  const text =
    `📌 <b>Шаблоны для Instagram</b>\n\n` +
    `Скопируй текст ниже (покажу в этом сообщении) и вставь в Stories/пост/DM.\n` +
    `Ссылка ведёт бренда прямо в Telegram-воронку (витрина → заявка → сделка).\n\n` +
    `Канал: <b>${escapeHtml(channel)}</b>\n` +
    `Профиль: <b>${escapeHtml(to)}</b>\n` +
    (link ? `Витрина: <a href="${escapeHtml(link)}">${escapeHtml(link)}</a>\n\n` : '\n') +
    `Выбери формат:`;

  const kb = new InlineKeyboard()
    .text('📲 Stories', `a:ws_ig_templates_send|ws:${wsId}|t:story`)
    .text('🖼️ Пост', `a:ws_ig_templates_send|ws:${wsId}|t:post`)
    .row()
    .text('💬 DM бренду', `a:ws_ig_templates_send|ws:${wsId}|t:dm`)
    .text('🔖 Bio', `a:ws_ig_templates_send|ws:${wsId}|t:bio`)
    .row()
    .text('⬅️ Назад', `a:ws_profile|ws:${wsId}`);

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}

function buildWsIgTemplate(ws, wsId, type = 'story') {
  const link = wsBrandLink(wsId) || '';
  const channel = ws.channel_username ? '@' + ws.channel_username : ws.title;
  const title = String(ws.profile_title || channel);

  const mode = String(ws.profile_mode || 'both');
  const modeLine = PROFILE_MODE_LABELS[mode] || PROFILE_MODE_LABELS.both;

  const verticalsTxt = fmtMatrix(ws.profile_verticals, PROFILE_VERTICALS);
  const formatsTxt = fmtMatrix(ws.profile_formats, PROFILE_FORMATS);

  const ig = ws.profile_ig ? String(ws.profile_ig).trim() : '';
  const igCode = ig ? `@${ig.replace(/^@/, '')}` : '';
  const igLink = ig ? `https://instagram.com/${ig.replace(/^@/, '')}` : '';

  const ports = Array.isArray(ws.profile_portfolio_urls) ? ws.profile_portfolio_urls : [];
  const port1 = ports[0] ? String(ports[0]) : '';

  const contact = ws.profile_contact ? String(ws.profile_contact).trim() : '';

  // Decide best "offer line" depending on mode
  const offerLine = (() => {
    if (mode === 'ugc') return 'UGC-контент для брендов (видео/сторис/распаковки) + материалы для рекламы.';
    if (mode === 'channel') return 'Интеграции в Telegram-канале + конкурсы/розыгрыши.';
    return 'UGC + интеграции в Telegram-канале + конкурсы/розыгрыши.';
  })();

  const common = {
    title,
    channel,
    modeLine,
    verticalsTxt,
    formatsTxt,
    link,
    igCode,
    igLink,
    port1,
    contact,
    offerLine
  };

  const templates = {
    story: [
      `Бренды 🤝 открыта к коллабам`,
      `${offerLine}`,
      `Ниши: ${verticalsTxt}`,
      `Форматы: ${formatsTxt}`,
      link ? `ТЗ/заявка в TG: ${link}` : `ТЗ/заявка в TG: (ссылка из профиля)`,
    ].join('\n'),
    post: [
      `Бренды, привет! Я ${title}.`,
      offerLine,
      `Ниши: ${verticalsTxt}`,
      `Форматы: ${formatsTxt}`,
      port1 ? `Портфолио: ${port1}` : `Портфолио: (ссылка в TG-профиле)`,
      link ? `Чтобы быстро обсудить — заполните заявку в Telegram: ${link}` : `Заявка в Telegram: (ссылка из профиля)`,
      igCode ? `IG: ${igCode}` : '',
    ].filter(Boolean).join('\n'),
    dm: [
      `Привет! Я ${title}.`,
      `Делаю: ${offerLine}`,
      `Ниши: ${verticalsTxt}. Форматы: ${formatsTxt}.`,
      port1 ? `Портфолио: ${port1}` : '',
      link ? `Если актуально — оставьте заявку/ТЗ в TG (1 мин): ${link}` : `Если актуально — напишите, пришлю ссылку в TG.`,
    ].filter(Boolean).join('\n'),
    bio: [
      `UGC + Collabs`,
      `Ниши: ${verticalsTxt}`,
      link ? `Заявка/ТЗ (TG): ${link}` : `Заявка/ТЗ (TG): (ссылка из профиля)`,
    ].join(' | ')
  };

  const raw = templates[type] || templates.story;

  // Wrapper message (HTML) with <pre> for easy copy
  const typeTitle = ({ story: 'Stories', post: 'Пост (подпись)', dm: 'DM бренду', bio: 'Bio строка' }[type] || 'Stories');

  const hint =
    type === 'story'
      ? `💡 В Stories добавь <b>стикер-ссылку</b> на витрину (Telegram).`
      : type === 'bio'
        ? `💡 Можно поставить в bio или в link-in-bio.`
        : `💡 Скопируй и вставь, потом при желании подправь 1–2 строки под себя.`;

  const extra =
    (igLink || contact)
      ? `\n\nКонтакты: ` +
        [igLink ? `<a href="${escapeHtml(igLink)}">${escapeHtml(igCode || igLink)}</a>` : null,
         contact ? escapeHtml(contact) : null]
        .filter(Boolean).join(' • ')
      : '';

  return (
    `📌 <b>Шаблон IG — ${escapeHtml(typeTitle)}</b>\n` +
    `${hint}\n\n` +
    `<pre>${escapeHtml(raw)}</pre>` +
    extra
  );
}

function buildWsIgDmRaw(ws, wsId, tone = 'soft', variantIndex = 0) {
  const link = wsBrandLink(wsId) || '';
  const channel = ws.channel_username ? '@' + ws.channel_username : ws.title;
  const title = String(ws.profile_title || channel);

  const mode = String(ws.profile_mode || 'both');
  const verticalsTxt = fmtMatrix(ws.profile_verticals, PROFILE_VERTICALS);
  const formatsTxt = fmtMatrix(ws.profile_formats, PROFILE_FORMATS);

  const igHandle = normalizeIgHandle(ws.profile_ig);
  const igCode = igHandle ? `@${igHandle}` : '';
  const ports = Array.isArray(ws.profile_portfolio_urls) ? ws.profile_portfolio_urls : [];
  const port1 = ports[0] ? String(ports[0]) : '';

  const offerLine = (() => {
    if (mode === 'ugc') return 'UGC-контент для брендов (видео/сторис/распаковки) + материалы для рекламы.';
    if (mode === 'channel') return 'Интеграции в Telegram-канале + конкурсы/розыгрыши.';
    return 'UGC + интеграции в Telegram-канале + конкурсы/розыгрыши.';
  })();

  const soft = [
    [
      `Привет! Я ${title} 👋`,
      `Увидела ваш бренд и хочу предложить коллаб: ${offerLine}`,
      `Ниши: ${verticalsTxt}. Форматы: ${formatsTxt}.`,
      port1 ? `Портфолио: ${port1}` : '',
      link ? `Если ок — можно быстро оставить ТЗ/заявку в TG (1 мин): ${link}` : '',
      igCode ? `Мой IG: ${igCode}` : '',
    ].filter(Boolean).join('\n'),
    [
      `Здравствуйте! Я ${title}.`,
      `Делаю ${offerLine}`,
      `Могу снять: ${formatsTxt} (ниши: ${verticalsTxt}).`,
      port1 ? `Примеры: ${port1}` : '',
      link ? `Чтобы не теряться — оставьте заявку в TG: ${link}` : '',
    ].filter(Boolean).join('\n'),
    [
      `Добрый день! Я ${title}.`,
      `Ищу коллабы с брендами в нишах: ${verticalsTxt}.`,
      `Форматы: ${formatsTxt}. ${offerLine}`,
      port1 ? `Портфолио: ${port1}` : '',
      link ? `Если интересно — вот витрина/заявка в TG: ${link}` : '',
    ].filter(Boolean).join('\n'),
  ];

  const hard = [
    [
      `Привет! Я ${title}.`,
      `Снимаю ${formatsTxt} для брендов (ниши: ${verticalsTxt}).`,
      `Могу сделать ${offerLine}`,
      port1 ? `Портфолио: ${port1}` : '',
      link ? `Если хотите обсудить быстро — ТЗ/заявка в TG: ${link}` : '',
    ].filter(Boolean).join('\n'),
    [
      `Привет 👋 ${title} на связи.`,
      `Нужно UGC/интеграция без долгих переписок?`,
      `${offerLine}`,
      `Ниши: ${verticalsTxt}. Форматы: ${formatsTxt}.`,
      link ? `Киньте ТЗ сюда (TG, 1 мин): ${link}` : '',
    ].filter(Boolean).join('\n'),
    [
      `Привет! Я ${title}.`,
      `Делаю контент “под рекламу” + быстрые согласования.`,
      `Форматы: ${formatsTxt}. Ниши: ${verticalsTxt}.`,
      port1 ? `Примеры: ${port1}` : '',
      link ? `Если актуально — заполните короткую заявку в TG: ${link}` : '',
    ].filter(Boolean).join('\n'),
  ];

  const t = String(tone || 'soft').toLowerCase();
  const pool = t === 'hard' ? hard : soft;
  const idx = Math.abs(Number(variantIndex || 0)) % pool.length;
  return { raw: pool[idx], idx, total: pool.length, tone: (t === 'hard' ? 'hard' : 'soft') };
}

function buildWsIgDmMessage(ws, wsId, tone = 'soft', variantIndex = 0) {
  const t = String(tone || 'soft').toLowerCase();
  const toneLabel = t === 'hard' ? '⚡ Директ' : '🤝 Мягкий';
  const { raw, idx, total } = buildWsIgDmRaw(ws, wsId, t, variantIndex);

  const hint =
    `💡 Это варианты для аккуратного аутрича/АБ-теста. Персонализируй 1 строку под бренд — конверсия выше.`;

  return (
    `📌 <b>DM бренду — ${escapeHtml(toneLabel)}</b> (${idx + 1}/${total})\n` +
    `${hint}\n\n` +
    `<pre>${escapeHtml(raw)}</pre>`
  );
}

async function renderWsIgDmTemplate(ctx, ownerUserId, wsId, tone = 'soft', variantIndex = 0) {
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  const ws = isAdmin ? await db.getWorkspaceAny(wsId) : await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  if (!isAdmin && Number(ws.owner_user_id) !== Number(ownerUserId)) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const t = String(tone || 'soft').toLowerCase();
  const toneNorm = (t === 'hard' ? 'hard' : 'soft');
  const i = Math.max(0, Number(variantIndex || 0));

  const text = buildWsIgDmMessage(ws, wsId, toneNorm, i);

  const kb = new InlineKeyboard()
    .text(`${toneNorm === 'soft' ? '✅ ' : ''}🤝 Мягкий`, `a:ws_ig_dm|ws:${wsId}|tone:soft|i:${toneNorm === 'soft' ? i : 0}`)
    .text(`${toneNorm === 'hard' ? '✅ ' : ''}⚡ Директ`, `a:ws_ig_dm|ws:${wsId}|tone:hard|i:${toneNorm === 'hard' ? i : 0}`)
    .row()
    .text('📤 Ещё вариант', `a:ws_ig_dm|ws:${wsId}|tone:${toneNorm}|i:${i + 1}`)
    .row()
    .text('⬅️ Назад', `a:ws_ig_templates|ws:${wsId}`);

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}



async function sendWsIgTemplateMessage(ctx, ownerUserId, wsId, type = 'story') {
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  const ws = isAdmin ? await db.getWorkspaceAny(wsId) : await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  if (!isAdmin && Number(ws.owner_user_id) !== Number(ownerUserId)) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const t = String(type || 'story');
  const allowed = ['story', 'post', 'dm', 'bio'];
  const tt = allowed.includes(t) ? t : 'story';

  // DM templates are interactive (tone + variants) to avoid sending many messages.
  if (tt === 'dm') {
    await renderWsIgDmTemplate(ctx, ownerUserId, wsId, 'soft', 0);
    try { await ctx.answerCallbackQuery({ text: '✅ DM шаблон открыт' }); } catch {}
    return;
  }

  const msg = buildWsIgTemplate(ws, wsId, tt);

  // Показываем шаблон в этом же сообщении (без лишнего спама в чате)
  const kb = new InlineKeyboard()
    .text('⬅️ Назад', `a:ws_ig_templates|ws:${wsId}`)
    .text('👤 Профиль', `a:ws_profile|ws:${wsId}`);

  try {
    await ctx.editMessageText(msg, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(msg, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }

  try { await ctx.answerCallbackQuery({ text: '✅ Шаблон открыт' }); } catch {}
}
async function renderWsProfileMode(ctx, ownerUserId, wsId) {
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  const ws = isAdmin ? await db.getWorkspaceAny(wsId) : await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  if (!isAdmin && Number(ws.owner_user_id) !== Number(ownerUserId)) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  const cur = String(ws.profile_mode || 'both');

  const kb = new InlineKeyboard()
    .text(`${cur === 'channel' ? '✅ ' : ''}Канал`, `a:ws_prof_mode_set|ws:${wsId}|m:channel`)
    .text(`${cur === 'ugc' ? '✅ ' : ''}UGC`, `a:ws_prof_mode_set|ws:${wsId}|m:ugc`)
    .row()
    .text(`${cur === 'both' ? '✅ ' : ''}Оба`, `a:ws_prof_mode_set|ws:${wsId}|m:both`)
    .row()
    .text('⬅️ Назад', `a:ws_profile|ws:${wsId}`);

  const text =
    `🧩 <b>Режим профиля</b>\n\n` +
    `• <b>Канал</b> — интеграции/посты в TG\n` +
    `• <b>UGC</b> — контент без аудитории (файлы)\n` +
    `• <b>Оба</b> — лучше по РФ-рынку\n\n` +
    `Сейчас: <b>${escapeHtml(PROFILE_MODE_LABELS[cur] || PROFILE_MODE_LABELS.both)}</b>`;

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderWsProfileVerticals(ctx, ownerUserId, wsId) {
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  const ws = isAdmin ? await db.getWorkspaceAny(wsId) : await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  if (!isAdmin && Number(ws.owner_user_id) !== Number(ownerUserId)) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const selected = Array.isArray(ws.profile_verticals) ? ws.profile_verticals.map(String) : [];
  const kb = new InlineKeyboard();

  PROFILE_VERTICALS.forEach((it, i) => {
    const on = selected.includes(it.key);
    kb.text(`${on ? '✅' : '▫️'} ${it.title}`, `a:ws_prof_vert_t|ws:${wsId}|v:${it.key}`);
    if (i % 2 === 1) kb.row();
  });

  kb.row()
    .text('🧹 Сброс', `a:ws_prof_vert_clear|ws:${wsId}`)
    .text('⬅️ Назад', `a:ws_profile|ws:${wsId}`);

  const text =
    `🏷 <b>Ниши</b> (максимум 3)\n\n` +
    `Выбери до 3 ниш — так брендам проще понять, ты про что.\n\n` +
    `Сейчас: <b>${escapeHtml(fmtMatrix(selected, PROFILE_VERTICALS))}</b>`;

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderWsProfileFormats(ctx, ownerUserId, wsId) {
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  const ws = isAdmin ? await db.getWorkspaceAny(wsId) : await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  if (!isAdmin && Number(ws.owner_user_id) !== Number(ownerUserId)) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const selected = Array.isArray(ws.profile_formats) ? ws.profile_formats.map(String) : [];
  const kb = new InlineKeyboard();

  PROFILE_FORMATS.forEach((it, i) => {
    const on = selected.includes(it.key);
    kb.text(`${on ? '✅' : '▫️'} ${it.title}`, `a:ws_prof_fmt_t|ws:${wsId}|f:${it.key}`);
    if (i % 2 === 1) kb.row();
  });

  kb.row()
    .text('🧹 Сброс', `a:ws_prof_fmt_clear|ws:${wsId}`)
    .text('⬅️ Назад', `a:ws_profile|ws:${wsId}`);

  const text =
    `🎬 <b>Форматы</b> (максимум 5)\n\n` +
    `Выбери форматы — так брендам проще сделать быстрый заказ.\n\n` +
    `Сейчас: <b>${escapeHtml(fmtMatrix(selected, PROFILE_FORMATS))}</b>`;

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderWsPublicProfile(ctx, wsId, opts = {}) {
  const ws = await db.getWorkspaceAny(wsId);
  if (!ws) return ctx.reply('Профиль не найден.');

  const viewer = ctx?.from ? await db.upsertUser(ctx.from.id, ctx.from.username ?? null) : null;
  const isOwner = viewer && Number(viewer.id) === Number(ws.owner_user_id);

  const channel = ws.channel_username ? '@' + ws.channel_username : ws.title;
  const name = ws.profile_title || channel;
  const mode = String(ws.profile_mode || 'both');
  const ig = ws.profile_ig ? String(ws.profile_ig) : null;

  const verticalsTxt = fmtMatrix(ws.profile_verticals, PROFILE_VERTICALS);
  const formatsTxt = fmtMatrix(ws.profile_formats, PROFILE_FORMATS);
  const geo = ws.profile_geo || '—';
  const contact = ws.profile_contact || '—';
  const about = ws.profile_about || '—';

  let igLine = '—';
  if (ig) {
    igLine =
      `<a href="https://instagram.com/${escapeHtml(ig)}">instagram.com/${escapeHtml(ig)}</a>\n` +
      `<code>@${escapeHtml(ig)}</code>`;
  }

  let portLine = '—';
  const ports = Array.isArray(ws.profile_portfolio_urls) ? ws.profile_portfolio_urls : [];
  if (ports.length) {
    portLine = ports
      .slice(0, 3)
      .map(u => `• <a href="${escapeHtml(String(u))}">${escapeHtml(shortUrl(u))}</a>`)
      .join('\n');
  }

  const modeLine = PROFILE_MODE_LABELS[mode] || PROFILE_MODE_LABELS.both;
  const prog = isOwner ? calcWsProfileProgress(ws) : null;

  const text =
    `✨ <b>${escapeHtml(name)}</b>\n\n` +
    `IG leads → TG deals: бренд находит в Instagram → сделка закрывается в Telegram.\n\n` +
    `🪟 Витрина: открой кнопку ниже — там находится «📝 Оставить заявку».\n\n` +
    `Канал: <b>${escapeHtml(channel)}</b>\n` +
    `🧩 Режим: <b>${escapeHtml(modeLine)}</b>\n` +
    `📸 Instagram:\n${igLine}\n` +
    `🏷 Ниши: <b>${escapeHtml(verticalsTxt)}</b>\n` +
    `🎬 Форматы: <b>${escapeHtml(formatsTxt)}</b>\n` +
    `🔗 Портфолио:\n${portLine}\n` +
    `📝 Описание: <b>${escapeHtml(about)}</b>\n` +
    `✉️ Контакт: <b>${escapeHtml(contact)}</b>\n` +
    `📍 Гео: <b>${escapeHtml(geo)}</b>\n\n` +
    `Если хочешь UGC/интеграцию — нажми «📝 Оставить заявку» или «💬 Написать».` +
    (isOwner && prog ? `\n\n📈 <b>Твой профиль</b>: <b>${prog.percent}%</b>. ${prog.nextHint}` : '');

  const contactRaw = ws.profile_contact ? String(ws.profile_contact).trim() : '';
  const contactUrl = (() => {
    if (!contactRaw) return null;
    const tg = wsTgUrlFromContact(contactRaw);
    if (tg) return tg;
    if (/^https?:\/\//i.test(contactRaw)) return contactRaw;
    if (/^t\.me\//i.test(contactRaw)) return 'https://' + contactRaw;
    if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(contactRaw)) return 'mailto:' + contactRaw;
    return null;
  })();

  const kb = new InlineKeyboard();

  // CTA row
  kb.text('📝 Оставить заявку', `a:wsp_lead_new|ws:${wsId}`);
  if (contactUrl) kb.url('💬 Написать', contactUrl);
  kb.row();

  // Owner-only CTA
  if (isOwner) {
    kb.text('🔗 Поделиться', `a:ws_share|ws:${wsId}`).row();
  }

  // Links
  if (ws.channel_username) kb.url('📣 Telegram канал', `https://t.me/${String(ws.channel_username).replace(/^@/, '')}`);
  if (ig) kb.url('📸 Instagram', `https://instagram.com/${ig}`);
  const backCb = opts?.backCb || (isOwner ? `a:ws_profile|ws:${wsId}` : null);
  if (backCb) kb.row().text('⬅️ Назад', backCb);
  kb.row().text('📋 Меню', 'a:menu');

  const extra = { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true };
  if (ctx.callbackQuery) await ctx.editMessageText(text, extra);
  else await ctx.reply(text, extra);

}

async function renderWsLeadCompose(ctx, wsId, step = 1, draft = {}) {
  const ws = await db.getWorkspaceAny(wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Профиль не найден.' });

  const channel = ws.channel_username ? '@' + ws.channel_username : ws.title;
  const link = wsBrandLink(wsId);

  const to = String(ws.profile_title || channel);

  let text =
    `📩 <b>Запрос бренда</b>\n\n` +
    `Кому: <b>${escapeHtml(to)}</b>\n` +
    `Канал: <b>${escapeHtml(channel)}</b>\n` +
    (link ? `Витрина: <a href="${escapeHtml(link)}">${escapeHtml(link)}</a>\n\n` : `\n`);

  if (Number(step) === 2) {
    const contact = String(draft?.contact || '').trim();
    text +=
      `✅ <b>Шаг 2/2</b>\n` +
      (contact ? `Контакт бренда: <b>${escapeHtml(contact)}</b>\n\n` : `\n`) +
      `Опиши запрос коротко:\n` +
      `• тип: UGC или интеграция\n` +
      `• объём (1/3/5 видео, серия, пак)\n` +
      `• бюджет или бартер\n` +
      `• сроки/дедлайн\n` +
      `• 1 строка про продукт/бренд\n\n` +
      `После отправки я мгновенно уведомлю владельца канала.`;
  } else {
    text +=
      `🧩 <b>Шаг 1/2</b>\n` +
      `Пришли контакт бренда (IG / @username / ссылка / сайт).\n` +
      `Пример: <code>@brand</code> или <code>https://instagram.com/brand</code>\n\n` +
      `Дальше я спрошу детали (что нужно + условия + дедлайн).`;
  }

  const kb = new InlineKeyboard()
    .text('⬅️ Назад', `a:wsp_open|ws:${wsId}`)
    .text('📋 Меню', 'a:menu');

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}
function leadListTabsKb(wsId, counts, active) {
  const kb = new InlineKeyboard()
    .text(`${LEAD_STATUSES.new.icon} ${counts.new ?? 0}`, `a:ws_leads|ws:${wsId}|s:new|p:0`)
    .text(`${LEAD_STATUSES.in_progress.icon} ${counts.in_progress ?? 0}`, `a:ws_leads|ws:${wsId}|s:in_progress|p:0`)
    .row()
    .text(`${LEAD_STATUSES.closed.icon} ${counts.closed ?? 0}`, `a:ws_leads|ws:${wsId}|s:closed|p:0`)
    .text(`${LEAD_STATUSES.spam.icon} ${counts.spam ?? 0}`, `a:ws_leads|ws:${wsId}|s:spam|p:0`);
  return kb;
}

async function renderWsLeadsList(ctx, ownerUserId, wsId, status = 'new', page = 0) {
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  const ws = isAdmin ? await db.getWorkspaceAny(wsId) : await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  if (!isAdmin && Number(ws.owner_user_id) !== Number(ownerUserId)) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const st = normLeadStatus(status);
  const p = Math.max(0, Number(page) || 0);
  const limit = 10;
  const offset = p * limit;

  const counts = await db.countBrandLeadsByStatus(wsId);
  const leads = await db.listBrandLeads(wsId, st, limit, offset);

  const channel = ws.channel_username ? '@' + ws.channel_username : ws.title;
  const textHeader =
    `📨 <b>Запросы брендов</b>\n\n` +
    `Канал: <b>${escapeHtml(channel)}</b>\n` +
    `Статус: <b>${escapeHtml((LEAD_STATUSES[st] || LEAD_STATUSES.new).title)}</b>\n\n`;

  const lines = leads.map((l) => {
    const who = l.brand_username ? '@' + String(l.brand_username).replace(/^@/, '') : (l.brand_name || 'brand');
    const snippet = String(l.message || '').replace(/\s+/g, ' ').slice(0, 60);
    return `${leadStatusIcon(l.status)} <b>#${l.id}</b> — ${escapeHtml(who)} — <i>${escapeHtml(snippet)}${String(l.message || '').length > 60 ? '…' : ''}</i>`;
  });

  const body = lines.length ? lines.join('\n') : 'Пока пусто. Запросы появятся, когда бренд нажмёт кнопку на витрине.';

  const kb = leadListTabsKb(wsId, counts, st);

  // quick open buttons (max 8 to avoid huge kb)
  for (const l of leads.slice(0, 8)) {
    kb.row().text(`${leadStatusIcon(l.status)} #${l.id}`, `a:lead_view|id:${l.id}|ws:${wsId}|s:${st}|p:${p}`);
  }

  // pagination
  if (p > 0) {
    kb.row().text('⬅️', `a:ws_leads|ws:${wsId}|s:${st}|p:${p - 1}`);
  }
  if (leads.length === limit) {
    if (p > 0) kb.text('➡️', `a:ws_leads|ws:${wsId}|s:${st}|p:${p + 1}`);
    else kb.row().text('➡️', `a:ws_leads|ws:${wsId}|s:${st}|p:${p + 1}`);
  }

  kb.row().text('⬅️ Назад', `a:ws_profile|ws:${wsId}`);

  try {
    await ctx.editMessageText(textHeader + body, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(textHeader + body, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}

async function renderLeadView(ctx, actorUserId, leadId, back = { wsId: null, status: 'new', page: 0 }) {
  const lead = await db.getBrandLeadById(leadId);
  if (!lead) return ctx.answerCallbackQuery({ text: 'Заявка не найдена.' });

  const wsId = Number(lead.workspace_id);
  const ws = await db.getWorkspaceAny(wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });

  const isOwner = Number(ws.owner_user_id) === Number(actorUserId);
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  if (!isOwner && !isAdmin) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const channel = ws.channel_username ? '@' + ws.channel_username : ws.title;
  const who = lead.brand_username ? '@' + String(lead.brand_username).replace(/^@/, '') : (lead.brand_name || 'brand');
  const when = lead.created_at ? fmtTs(lead.created_at) : '—';

  const link = wsBrandLink(wsId);

  let text =
    `✉️ <b>Заявка #${lead.id}</b> ${leadStatusIcon(lead.status)}\n\n` +
    `Канал: <b>${escapeHtml(channel)}</b>\n` +
    (link ? `Витрина: <a href="${escapeHtml(link)}">${escapeHtml(link)}</a>\n` : '') +
    `От: <b>${escapeHtml(who)}</b>\n` +
    `Когда: <b>${escapeHtml(when)}</b>\n\n` +
    `<b>Текст:</b>\n${escapeHtml(String(lead.message || '—'))}`;

  if (lead.reply_text) {
    text += `\n\n<b>Ответ:</b>\n${escapeHtml(String(lead.reply_text))}`;
  }

  const st = normLeadStatus(lead.status);

  const kb = new InlineKeyboard()
    .text('✍️ Ответить', `a:lead_reply|id:${lead.id}|ws:${wsId}|s:${back.status}|p:${back.page}`)
    .text('⚡ Шаблоны', `a:lead_tpls|id:${lead.id}|ws:${wsId}|s:${back.status}|p:${back.page}`)
    .row()
    .text('💬 В работу', `a:lead_set|id:${lead.id}|st:in_progress|ws:${wsId}|s:${back.status}|p:${back.page}`)
    .text('✅ Закрыть', `a:lead_set|id:${lead.id}|st:closed|ws:${wsId}|s:${back.status}|p:${back.page}`)
    .row()
    .text('🗑 Спам', `a:lead_set|id:${lead.id}|st:spam|ws:${wsId}|s:${back.status}|p:${back.page}`)
    .row()
    .text('⬅️ Назад', `a:ws_leads|ws:${wsId}|s:${back.status}|p:${back.page}`);

  try {
    try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}



// --- Brand Applications Inbox (Creator → Brand) ---

// Deal stages (no migrations; stored in brand_applications.meta.deal_stage)
const DEAL_STAGES = {
  negotiation: { id: 'negotiation', icon: '💬', label: 'Переговоры' },
  deal: { id: 'deal', icon: '🤝', label: 'Договорились' },
  paid: { id: 'paid', icon: '💳', label: 'Оплата' },
  done: { id: 'done', icon: '✅', label: 'Завершено' },
  lost: { id: 'lost', icon: '🗑', label: 'Потеряно' },
  all: { id: 'all', icon: '📌', label: 'Все' },
};

function normDealStage(s) {
  const k = String(s || '').toLowerCase().trim();
  return DEAL_STAGES[k] ? k : 'negotiation';
}

function dealStageTitle(s) {
  const k = String(s || '').toLowerCase().trim();
  const d = DEAL_STAGES[k];
  if (!d) return '📌 Сделка';
  return `${d.icon} ${d.label}`;
}

function getAppDealStage(app) {
  const s = app?.meta?.deal_stage;
  const k = String(s || '').toLowerCase().trim();
  return k && DEAL_STAGES[k] ? k : '';
}

function formatBrandAppThread(threadArr, limit = 6) {
  const rows = (Array.isArray(threadArr) ? threadArr : [])
    .filter(x => x && typeof x === 'object' && String(x.text || '').trim());

  if (!rows.length) return '';

  const tail = rows.slice(-Math.max(1, Number(limit) || 6));
  const lines = tail.map(m => {
    const from = String(m.from || '').toLowerCase();
    const icon = from === 'brand' ? '🏷️' : (from === 'creator' ? '🧑‍🎨' : (from === 'system' ? '⚙️' : '💬'));
    const t = m.at ? fmtTs(String(m.at)) : '';
    const body = String(m.text || '').trim().replace(/\s+/g, ' ');
    const short = body.length > 110 ? body.slice(0, 110).trim() + '…' : body;
    return `${icon} ${t ? `<code>${escapeHtml(t)}</code> ` : ''}${escapeHtml(short)}`;
  });

  return lines.join('\n');
}


function brandAppsTabsKb(counts = {}, active = 'new') {
  const a = normLeadStatus(active);
  const kb = new InlineKeyboard()
    .text(`${LEAD_STATUSES.new.icon} ${counts.new ?? 0}`, `a:brand_apps|ws:0|s:new|p:0`)
    .text(`${LEAD_STATUSES.in_progress.icon} ${counts.in_progress ?? 0}`, `a:brand_apps|ws:0|s:in_progress|p:0`)
    .row()
    .text(`${LEAD_STATUSES.closed.icon} ${counts.closed ?? 0}`, `a:brand_apps|ws:0|s:closed|p:0`)
    .text(`${LEAD_STATUSES.spam.icon} ${counts.spam ?? 0}`, `a:brand_apps|ws:0|s:spam|p:0`);

  // Mark active with a dot (cheap but readable)
  for (const row of kb.inline_keyboard) {
    for (const btn of row) {
      const d = String(btn.callback_data || '');
      if (d.includes(`|s:${a}|`)) btn.text = '• ' + btn.text;
    }
  }
  return kb;
}

function brandDealsTabsKb(counts = {}, active = 'negotiation') {
  const a = normDealStage(active);
  const kb = new InlineKeyboard()
    .text(`${DEAL_STAGES.negotiation.icon} ${counts.negotiation ?? 0}`, `a:brand_deals|ws:0|st:negotiation|p:0`)
    .text(`${DEAL_STAGES.deal.icon} ${counts.deal ?? 0}`, `a:brand_deals|ws:0|st:deal|p:0`)
    .text(`${DEAL_STAGES.paid.icon} ${counts.paid ?? 0}`, `a:brand_deals|ws:0|st:paid|p:0`)
    .row()
    .text(`${DEAL_STAGES.done.icon} ${counts.done ?? 0}`, `a:brand_deals|ws:0|st:done|p:0`)
    .text(`${DEAL_STAGES.lost.icon} ${counts.lost ?? 0}`, `a:brand_deals|ws:0|st:lost|p:0`)
    .text(`${DEAL_STAGES.all.icon} ${counts.all ?? 0}`, `a:brand_deals|ws:0|st:all|p:0`);

  // Mark active with a dot
  const rows = kb.inline_keyboard;
  for (const r of rows) {
    for (const b of r) {
      const cd = String(b.callback_data || '');
      const m = cd.match(/\bst:([^|]+)/);
      if (!m) continue;
      const st = String(m[1] || '').toLowerCase();
      if (st === a && !String(b.text).startsWith('• ')) {
        b.text = `• ${b.text}`;
      }
    }
  }
  return kb;
}

async function getBrandDealsSearch(tgId, brandUserId) {
  try {
    const v = await redis.get(k(['brandDealsSearch', tgId, Number(brandUserId)]));
    const s = String(v || '').trim();
    return s || '';
  } catch {
    return '';
  }
}

async function setBrandDealsSearch(tgId, brandUserId, query, ttlSec = 24 * 60 * 60) {
  try {
    const q = String(query || '').trim();
    if (!q) return;
    await redis.set(k(['brandDealsSearch', tgId, Number(brandUserId)]), q, { ex: ttlSec });
  } catch {}
}

async function clearBrandDealsSearch(tgId, brandUserId) {
  try {
    await redis.del(k(['brandDealsSearch', tgId, Number(brandUserId)]));
  } catch {}
}


async function getBrandDealsMineOnly(tgId, brandUserId) {
  try {
    const v = await redis.get(k(['brandDealsMineOnly', tgId, Number(brandUserId)]));
    return String(v || '') === '1';
  } catch {
    return false;
  }
}

async function setBrandDealsMineOnly(tgId, brandUserId, on = true, ttlSec = 24 * 60 * 60) {
  try {
    if (!on) {
      await redis.del(k(['brandDealsMineOnly', tgId, Number(brandUserId)]));
      return;
    }
    await redis.set(k(['brandDealsMineOnly', tgId, Number(brandUserId)]), '1', { ex: ttlSec });
  } catch {}
}

async function clearBrandDealsMineOnly(tgId, brandUserId) {
  try {
    await redis.del(k(['brandDealsMineOnly', tgId, Number(brandUserId)]));
  } catch {}
}

async function assertBrandAppsAccess(ctx, actorUserId, brandUserId) {
  const isOwner = Number(actorUserId) === Number(brandUserId);
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  if (isOwner || isAdmin) return { ok: true, isOwner, isAdmin, isManager: false };

  const isManager = await safeBrandApplications(() => db.isBrandManager(brandUserId, actorUserId), async () => false);
  if (!isManager) {
    await ctx.answerCallbackQuery({ text: 'Доступ отозван.' });
    return { ok: false, isOwner: false, isAdmin, isManager: false };
  }

  // Auto-enter manager mode for better UX when opening from notifications
  await setBrandManagerMode(ctx.from.id, true);
  await setUiMode(ctx.from.id, 'Brand');
  await setActiveBrand(ctx.from.id, brandUserId);

  return { ok: true, isOwner: false, isAdmin, isManager: true };
}

async function renderBrandAppsList(ctx, actorUserId, brandUserId, status = 'new', page = 0) {
  const access = await assertBrandAppsAccess(ctx, actorUserId, brandUserId);
  if (!access.ok) return;

  const st = normLeadStatus(status);
  const p = Math.max(0, Number(page) || 0);
  const limit = 8;
  const offset = p * limit;

  const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
  const brandName = String(prof?.brand_name || '').trim() || 'Бренд';

  const counts = await safeBrandApplications(() => db.countBrandApplicationsByStatus(brandUserId), async () => ({
    new: 0, in_progress: 0, closed: 0, spam: 0
  }));

  const apps = await safeBrandApplications(() => db.listBrandApplications(brandUserId, st, limit, offset), async () => []);

  const header =
    `📨 <b>Заявки от креаторов</b>\n` +
    `Бренд: <b>${escapeHtml(brandName)}</b>\n` +
    `Статус: <b>${escapeHtml(LEAD_STATUSES[st]?.label || st)}</b>\n`;

  let body = '';
  if (!apps.length) {
    body = '\nПока пусто. Заявки появятся, когда креаторы нажимают “📝 Оставить заявку” в каталоге.';
  } else {
    const lines = apps.map((a, i) => {
      const who = a.creator_username
        ? '@' + String(a.creator_username).replace(/^@/, '')
        : (a.creator_tg_id ? `id:${a.creator_tg_id}` : 'creator');
      const when = a.created_at ? fmtTs(a.created_at) : '—';
      const msg = String(a.message || '').replace(/\s+/g, ' ').trim();
      const short = msg.length > 60 ? msg.slice(0, 60) + '…' : (msg || '—');
      return `${offset + i + 1}. <b>${escapeHtml(who)}</b> · ${escapeHtml(when)}\n<code>${escapeHtml(short)}</code>`;
    });
    body = '\n\n' + lines.join('\n\n');
  }

  const kb = brandAppsTabsKb(counts, st);

  if (access.isManager) {
    kb.row().text('🔁 Сменить бренд', 'a:bm_pick_brand|ret:brand_apps|ws:0|p:0');
  }

  if (apps.length) {
    kb.row();
    for (const a of apps) {
      kb.text(`#${a.id}`, `a:brand_app_view|id:${a.id}|s:${st}|p:${p}`);
    }
  }

  // Pagination
  const total = (counts[st] ?? 0) || 0;
  const hasPrev = p > 0;
  const hasNext = (offset + apps.length) < total;

  if (hasPrev || hasNext) kb.row();
  if (hasPrev) kb.text('⬅️', `a:brand_apps|ws:0|s:${st}|p:${p - 1}`);
  if (hasNext) kb.text('➡️', `a:brand_apps|ws:0|s:${st}|p:${p + 1}`);

  kb.row().text('⬅️ Назад', 'a:bx_open|ws:0');

  const text = header + body;

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}

async function renderBrandDealsList(ctx, actorUserId, brandUserId, stage = 'negotiation', page = 0) {
  const access = await assertBrandAppsAccess(ctx, actorUserId, brandUserId);
  if (!access.ok) return;

  const st = normDealStage(stage);
  const p = Math.max(0, Number(page) || 0);
  const limit = 8;
  const offset = p * limit;

  const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
  const brandName = String(prof?.brand_name || '').trim() || 'Бренд';

  const counts = await safeBrandApplications(() => db.countBrandDealsByStage(brandUserId, mineOnly ? actorUserId : null), async () => ({
    negotiation: 0, deal: 0, paid: 0, done: 0, lost: 0, all: 0
  }));

  const search = await getBrandDealsSearch(ctx.from.id, brandUserId);

  const mineOnly = access.isManager ? await getBrandDealsMineOnly(ctx.from.id, brandUserId) : false;

  const items = await safeBrandApplications(
    () => search ? db.listBrandDealsFiltered(brandUserId, st, search, limit, offset, mineOnly ? actorUserId : null) : db.listBrandDeals(brandUserId, st, limit, offset, mineOnly ? actorUserId : null),
    async () => []
  );

  let header =
    `📌 <b>Сделки</b>\n` +
    `Бренд: <b>${escapeHtml(brandName)}</b>\n` +
    `Стадия: <b>${escapeHtml(dealStageTitle(st))}</b>\n`;

  if (mineOnly) header += `Фильтр: <b>только мои</b>\n`;


  if (search) header += `Поиск: <code>${escapeHtml(String(search))}</code>\n`;

  let body = '';
  if (!items.length) {
    body = '\nПока пусто. Сюда попадают заявки после “✅ Принять”.';
  } else {
    const lines = items.map((a, i) => {
      const who = a.creator_username
        ? '@' + String(a.creator_username).replace(/^@/, '')
        : (a.creator_tg_id ? `id:${a.creator_tg_id}` : 'creator');
      const when = a.updated_at ? fmtTs(a.updated_at) : (a.created_at ? fmtTs(a.created_at) : '—');
      const dealStage = getAppDealStage(a) || 'negotiation';
      const msg = String(a.message || '').replace(/\s+/g, ' ').trim();
      const short = msg.length > 60 ? msg.slice(0, 60) + '…' : (msg || '—');
      return `${offset + i + 1}. <b>${escapeHtml(who)}</b> · ${escapeHtml(when)}\n${escapeHtml(dealStageTitle(dealStage))}\n<code>${escapeHtml(short)}</code>`;
    });
    body = '\n\n' + lines.join('\n\n');
  }

  const kb = brandDealsTabsKb(counts, st);

  if (access.isManager) {
    kb.row().text('🔁 Сменить бренд', 'a:bm_pick_brand|ret:brand_deals|ws:0|p:0');
  }


  if (access.isManager) {
    kb.row().text(mineOnly ? '👥 Показать все' : '👤 Только мои', `a:brand_deals_mine_toggle|ws:0|st:${st}|p:${p}`);
  }

// Search/filter by creator (username or tg id)
kb.row().text('🔎 Поиск', `a:brand_deals_search|ws:0|st:${st}|p:${p}`);
if (search) kb.text('❌ Сброс', `a:brand_deals_search_clear|ws:0|st:${st}|p:${p}`);

  if (items.length) {
    kb.row();
    for (const a of items) {
      kb.text(`#${a.id}`, `a:brand_deal_view|id:${a.id}|st:${st}|p:${p}`);
    }
  }

  // Pagination
  const total = search
    ? await safeBrandApplications(() => db.countBrandDealsFiltered(brandUserId, st, search, mineOnly ? actorUserId : null), async () => (offset + items.length))
    : ((st === 'all' ? (counts.all ?? 0) : (counts[st] ?? 0)) || 0);
  const hasPrev = p > 0;
  const hasNext = (offset + items.length) < total;
  if (hasPrev || hasNext) kb.row();
  if (hasPrev) kb.text('⬅️', `a:brand_deals|ws:0|st:${st}|p:${p - 1}`);
  if (hasNext) kb.text('➡️', `a:brand_deals|ws:0|st:${st}|p:${p + 1}`);

  kb.row().text('⬅️ Назад', 'a:bx_open|ws:0');

  const text = header + body;
  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}

async function renderBrandDealView(ctx, actorUserId, appId, back = { stage: 'negotiation', page: 0 }) {
  const app = await safeBrandApplications(() => db.getBrandApplicationById(appId), async () => null);
  if (!app) return ctx.answerCallbackQuery({ text: 'Сделка не найдена.' });

  const brandUserId = Number(app.brand_user_id);
  const access = await assertBrandAppsAccess(ctx, actorUserId, brandUserId);
  if (!access.ok) return;

  const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
  const brandName = String(prof?.brand_name || '').trim() || 'Бренд';

  const stage = getAppDealStage(app) || 'negotiation';

  const who = app.creator_username
    ? '@' + String(app.creator_username).replace(/^@/, '')
    : (app.creator_tg_id ? `id:${app.creator_tg_id}` : 'creator');
  const when = app.updated_at ? fmtTs(app.updated_at) : (app.created_at ? fmtTs(app.created_at) : '—');
  const msg = String(app.message || '').trim();

const thread = Array.isArray(app?.meta?.thread) ? app.meta.thread : [];

let text =
  `📌 <b>Сделка</b>\n` +
  `Бренд: <b>${escapeHtml(brandName)}</b>\n` +
  `Креатор: <b>${escapeHtml(who)}</b>\n` +
  `Обновлено: <b>${escapeHtml(when)}</b>\n\n` +
  `Стадия: <b>${escapeHtml(dealStageTitle(stage))}</b>\n\n` +
  `<b>Сообщение:</b>\n<code>${escapeHtml(msg || '—')}</code>`;

if (app.reply_text) {
  text += `\n\n<b>Последний ответ бренда:</b>\n<code>${escapeHtml(String(app.reply_text))}</code>`;
}

const threadBlock = formatBrandAppThread(thread, 8);
if (threadBlock) {
  text += `\n\n<b>Диалог:</b>\n${threadBlock}`;
}

  const kb = new InlineKeyboard()
    .text(dealStageTitle('negotiation'), `a:brand_deal_set|id:${app.id}|st:negotiation|b:${back.stage}|p:${back.page}`)
    .text(dealStageTitle('deal'), `a:brand_deal_set|id:${app.id}|st:deal|b:${back.stage}|p:${back.page}`)
    .row()
    .text(dealStageTitle('paid'), `a:brand_deal_set|id:${app.id}|st:paid|b:${back.stage}|p:${back.page}`)
    .text(dealStageTitle('done'), `a:brand_deal_set|id:${app.id}|st:done|b:${back.stage}|p:${back.page}`)
    .row()
    .text(dealStageTitle('lost'), `a:brand_deal_set|id:${app.id}|st:lost|b:${back.stage}|p:${back.page}`)
    .row()
    .text('✍️ Ответить', `a:brand_deal_reply|id:${app.id}|b:${back.stage}|p:${back.page}`)
    .text('⚡ Шаблоны', `a:brand_deal_tpls|id:${app.id}|b:${back.stage}|p:${back.page}`)
    .row()
    .text('✉️ Открыть заявку', `a:brand_app_view|id:${app.id}|s:in_progress|p:0`)
    .row();

  if (access.isManager) {
    kb.text('🔁 Сменить бренд', 'a:bm_pick_brand|ret:brand_deals|ws:0|p:0').row();
  }

  const bStage = normDealStage(back.stage);
  const bPage = Math.max(0, Number(back.page) || 0);
  kb.text('⬅️ Назад', `a:brand_deals|ws:0|st:${bStage}|p:${bPage}`);

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}

async function renderBrandAppView(ctx, actorUserId, appId, back = { status: 'new', page: 0 }) {
  const app = await safeBrandApplications(() => db.getBrandApplicationById(appId), async () => null);
  if (!app) return ctx.answerCallbackQuery({ text: 'Заявка не найдена.' });

  const brandUserId = Number(app.brand_user_id);
  const access = await assertBrandAppsAccess(ctx, actorUserId, brandUserId);
  if (!access.ok) return;

  const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
  const brandName = String(prof?.brand_name || '').trim() || 'Бренд';

  const who = app.creator_username ? '@' + String(app.creator_username).replace(/^@/, '') : (app.creator_tg_id ? `id:${app.creator_tg_id}` : 'creator');
  const when = app.created_at ? fmtTs(app.created_at) : '—';
  const st = normLeadStatus(app.status);

  // Micro-CRM thread (stored in meta.thread[])
  const thread = Array.isArray(app?.meta?.thread) ? app.meta.thread : [];

  let text =
    `✉️ <b>Заявка #${app.id}</b> ${leadStatusIcon(st)}

` +
    `Бренд: <b>${escapeHtml(brandName)}</b>
` +
    `От: <b>${escapeHtml(who)}</b>
` +
    `Когда: <b>${escapeHtml(when)}</b>

` +
    `<b>Текст:</b>
${escapeHtml(String(app.message || '—'))}`;

  const dealStage = getAppDealStage(app);
  if (dealStage) {
    text += `

📌 <b>Сделка:</b> ${escapeHtml(dealStageTitle(dealStage))}`;
  }

  if (app.reply_text) {
    text += `

<b>Ответ:</b>
${escapeHtml(String(app.reply_text))}`;
  }

  const threadBlock = formatBrandAppThread(thread, 6);
  if (threadBlock) {
    text += `

<b>Диалог:</b>
${threadBlock}`;
  }

  if (st === 'new') {
    text += `

💡 Нажми <b>✅ Принять</b>, чтобы открыть диалог: креатор получит кнопку “💬 Написать бренду”.`;
  }

  const kb = new InlineKeyboard();
  if (st === 'new') kb.text('✅ Принять', `a:brand_app_accept|id:${app.id}|s:${back.status}|p:${back.page}`).row();

  if (dealStage) {
    kb.text('📌 В сделках', `a:brand_deal_view|id:${app.id}|st:${dealStage}|p:0`).row();
  }

  kb
    .text('✍️ Ответить', `a:brand_app_reply|id:${app.id}|s:${back.status}|p:${back.page}`)
    .text('⚡ Шаблоны', `a:brand_app_tpls|id:${app.id}|s:${back.status}|p:${back.page}`)
    .row()
    .text('💬 В работу', `a:brand_app_set|id:${app.id}|st:in_progress|s:${back.status}|p:${back.page}`)
    .text('✅ Закрыть', `a:brand_app_set|id:${app.id}|st:closed|s:${back.status}|p:${back.page}`)
    .row()
    .text('🗑 Спам', `a:brand_app_set|id:${app.id}|st:spam|s:${back.status}|p:${back.page}`)
    .row()
    .text('⬅️ Назад', `a:brand_apps|ws:0|s:${back.status}|p:${back.page}`);

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}

async function startBrandAppReply(ctx, actorUserId, appId, back) {
  const app = await safeBrandApplications(() => db.getBrandApplicationById(appId), async () => null);
  if (!app) return ctx.answerCallbackQuery({ text: 'Заявка не найдена.' });

  const brandUserId = Number(app.brand_user_id);
  const access = await assertBrandAppsAccess(ctx, actorUserId, brandUserId);
  if (!access.ok) return;

  const who = app.creator_username ? '@' + String(app.creator_username).replace(/^@/, '') : (app.creator_tg_id ? `id:${app.creator_tg_id}` : 'creator');

  await setExpectText(ctx.from.id, {
    type: 'brand_app_reply',
    appId: Number(app.id),
    brandUserId,
    creatorTgId: Number(app.creator_tg_id || 0),
    creatorUsername: app.creator_username ? String(app.creator_username).replace(/^@/, '') : null,
    backCb: `a:brand_app_view|id:${app.id}|s:${back.status}|p:${back.page}`,
    backStatus: back.status,
    backPage: back.page
  });

  const kb = new InlineKeyboard()
    .text('⬅️ Назад', `a:brand_app_view|id:${app.id}|s:${back.status}|p:${back.page}`)
    .text('🏠 Меню', 'a:menu');

  const text =
    `✍️ <b>Ответ креатору</b>

` +
    `Заявка #${app.id} от <b>${escapeHtml(who)}</b>

` +
    `Напиши ответ одним сообщением — я отправлю его креатору.`;

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}

async function startBrandDealReply(ctx, actorUserId, appId, back = { stage: 'negotiation', page: 0 }) {
  const app = await safeBrandApplications(() => db.getBrandApplicationById(appId), async () => null);
  if (!app) return ctx.answerCallbackQuery({ text: 'Сделка не найдена.' });

  const brandUserId = Number(app.brand_user_id);
  const access = await assertBrandAppsAccess(ctx, actorUserId, brandUserId);
  if (!access.ok) return;

  const creatorTgId = Number(app.creator_tg_id || 0);
  if (!creatorTgId) return ctx.reply('⚠️ У креатора нет TG id.');

  const who = app.creator_username ? '@' + String(app.creator_username).replace(/^@/, '') : (app.creator_tg_id ? `id:${app.creator_tg_id}` : 'creator');

  const backCb = `a:brand_deal_view|id:${app.id}|st:${normDealStage(back.stage)}|p:${Math.max(0, Number(back.page) || 0)}`;

  await setExpectText(ctx.from.id, {
    type: 'brand_app_reply',
    appId: Number(app.id),
    brandUserId: Number(brandUserId),
    creatorTgId: Number(creatorTgId),
    creatorUsername: app.creator_username || null,
    backCb
  });

  const kb = new InlineKeyboard().text('⬅️ Назад', backCb);

  const text =
    `✍️ <b>Ответ креатору</b>

` +
    `Сделка #${app.id} · <b>${escapeHtml(String(who))}</b>

` +
    `Напиши ответ одним сообщением — я отправлю его креатору.`;

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}

async function renderBrandDealTemplates(ctx, actorUserId, appId, back = { stage: 'negotiation', page: 0 }) {
  const app = await safeBrandApplications(() => db.getBrandApplicationById(appId), async () => null);
  if (!app) return ctx.answerCallbackQuery({ text: 'Сделка не найдена.' });

  const brandUserId = Number(app.brand_user_id);
  const access = await assertBrandAppsAccess(ctx, actorUserId, brandUserId);
  if (!access.ok) return;

  const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
  const brandName = String(prof?.brand_name || '').trim() || 'Бренд';
  const who = app.creator_username ? '@' + String(app.creator_username).replace(/^@/, '') : (app.creator_tg_id ? `id:${app.creator_tg_id}` : 'creator');

  const text =
    `⚡ <b>Быстрые ответы</b>

` +
    `Сделка #${app.id} от <b>${escapeHtml(String(who))}</b>

` +
    `Нажми кнопку — я отправлю креатору готовый ответ. После отправки у креатора появится кнопка “💬 Написать бренду”.`;

  const backCb = `a:brand_deal_view|id:${app.id}|st:${normDealStage(back.stage)}|p:${Math.max(0, Number(back.page) || 0)}`;

  const kb = new InlineKeyboard()
    .text('✅ Приняли — дальше', `a:brand_deal_tpl|id:${app.id}|k:next|b:${back.stage}|p:${back.page}`)
    .row()
    .text('📎 Прайс / медиа‑кит', `a:brand_deal_tpl|id:${app.id}|k:price|b:${back.stage}|p:${back.page}`)
    .row()
    .text('🧾 Уточнить детали', `a:brand_deal_tpl|id:${app.id}|k:brief|b:${back.stage}|p:${back.page}`)
    .row()
    .text('🤝 Бартер', `a:brand_deal_tpl|id:${app.id}|k:barter|b:${back.stage}|p:${back.page}`)
    .row()
    .text('⏱ Сроки', `a:brand_deal_tpl|id:${app.id}|k:timing|b:${back.stage}|p:${back.page}`)
    .row()
    .text('⬅️ Назад', backCb);

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}

async function sendBrandDealTemplateReply(ctx, actorUserId, appId, key, back = { stage: 'negotiation', page: 0 }) {
  const app = await safeBrandApplications(() => db.getBrandApplicationById(appId), async () => null);
  if (!app) return ctx.answerCallbackQuery({ text: 'Сделка не найдена.' });

  const brandUserId = Number(app.brand_user_id);
  const access = await assertBrandAppsAccess(ctx, actorUserId, brandUserId);
  if (!access.ok) return;

  const creatorTgId = Number(app.creator_tg_id || 0);
  if (!creatorTgId) return ctx.answerCallbackQuery({ text: 'У креатора нет TG id.' });

  const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
  const brandName = String(prof?.brand_name || '').trim() || 'Бренд';

  const replyText = buildBrandAppTemplateText(brandName, key);

  const cUrl = prof?.contact ? brandContactUrl(prof.contact) : null;
  const linkLine = prof?.link ? `\n🔗 Сайт/ссылка: ${escapeHtml(String(prof.link))}` : '';
  const contactLine = cUrl ? `\n✍️ Контакт: ${escapeHtml(String(prof.contact))}` : '';

  const outText =
    `📩 <b>Ответ бренда</b>\n\n` +
    `Бренд: <b>${escapeHtml(brandName)}</b>` +
    linkLine +
    contactLine +
    `\n\n<b>Сообщение:</b>\n${escapeHtml(replyText)}`;

  const outKb = new InlineKeyboard()
    .text('💬 Написать бренду', `a:brand_app_chat|id:${app.id}`)
    .row()
    .text('🪟 Открыть бренд', `a:brand_dir_open|u:${brandUserId}|p:0`);

  try {
    await bot.api.sendMessage(creatorTgId, outText, { parse_mode: 'HTML', reply_markup: outKb, disable_web_page_preview: true });
  } catch (e) {
    const backCb = `a:brand_deal_view|id:${app.id}|st:${normDealStage(back.stage)}|p:${Math.max(0, Number(back.page) || 0)}`;
    await ctx.reply('❌ Не удалось отправить сообщение креатору. Возможно, он ещё не нажимал /start.', {
      reply_markup: new InlineKeyboard().text('⬅️ Назад', backCb)
    });
    return;
  }

  // Persist
  await safeBrandApplications(() => db.markBrandApplicationReplied(appId, replyText, actorUserId), async () => null);
  await safeBrandApplications(() => db.appendBrandApplicationThreadMessage(appId, {
    from: 'brand',
    text: replyText,
    at: new Date().toISOString(),
    by_user_id: Number(actorUserId),
    by_tg_id: Number(ctx.from?.id || 0),
    by_username: ctx.from?.username || null
  }), async () => null);
  if (normLeadStatus(app.status) === 'new') {
    await safeBrandApplications(() => db.updateBrandApplicationStatus(appId, 'in_progress'), async () => null);
  }

  try { await ctx.answerCallbackQuery({ text: '✅ Отправлено' }); } catch {}
  await renderBrandDealView(ctx, actorUserId, appId, back);
}

function buildBrandAppTemplateText(brandName, key) {
  const k = String(key || '').toLowerCase();
  if (k === 'discuss') {
    return `Спасибо за заявку! ✅ Давайте обсудим детали.\nПришли, пожалуйста, прайс/медиа‑кит и примеры прошлых интеграций.`;
  }
  if (k === 'brief') {
    return `Супер. Чтобы быстро согласовать — пришли кратко: канал/ссылка, аудитория, форматы, сроки, примерные условия.`;
  }
  if (k === 'price') {
    return `Ок. Пришли, пожалуйста, прайс/пакеты + статистику (охваты/ER) и примеры публикаций.`;
  }
  if (k === 'barter') {
    return `Рассмотрим бартер 🤝 Напиши, какие форматы бартеришь и что тебе обычно нужно от бренда (товар/доставка/сроки).`;
  }
  if (k === 'timing') {
    return `Уточни по срокам: когда можешь подготовить контент и когда готов(а) к публикации?`;
  }
  if (k === 'next') {
    return `Приняли ✅ Давай дальше: пришли 2–3 варианта формата и ориентир по бюджету/условиям — выберем лучший.`;
  }
  return `Спасибо за заявку! ✅ Напиши, пожалуйста, чуть подробнее про формат и условия — и продолжим.`;
}

async function renderBrandAppTemplates(ctx, actorUserId, appId, back) {
  const app = await safeBrandApplications(() => db.getBrandApplicationById(appId), async () => null);
  if (!app) return ctx.answerCallbackQuery({ text: 'Заявка не найдена.' });

  const brandUserId = Number(app.brand_user_id);
  const access = await assertBrandAppsAccess(ctx, actorUserId, brandUserId);
  if (!access.ok) return;

  const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
  const brandName = String(prof?.brand_name || '').trim() || 'Бренд';
  const who = app.creator_username ? '@' + String(app.creator_username).replace(/^@/, '') : (app.creator_tg_id ? `id:${app.creator_tg_id}` : 'creator');

  const text =
    `⚡ <b>Быстрые ответы</b>\n\n` +
    `Заявка #${app.id} от <b>${escapeHtml(String(who))}</b>\n\n` +
    `Нажми кнопку — я отправлю креатору готовый ответ. После отправки у креатора появится кнопка “💬 Написать бренду”.`;

  const kb = new InlineKeyboard()
    .text('✅ Приняли — дальше', `a:brand_app_tpl|id:${app.id}|k:next|s:${back.status}|p:${back.page}`)
    .row()
    .text('📎 Прайс / медиа‑кит', `a:brand_app_tpl|id:${app.id}|k:price|s:${back.status}|p:${back.page}`)
    .row()
    .text('🧾 Уточнить детали', `a:brand_app_tpl|id:${app.id}|k:brief|s:${back.status}|p:${back.page}`)
    .row()
    .text('🤝 Бартер', `a:brand_app_tpl|id:${app.id}|k:barter|s:${back.status}|p:${back.page}`)
    .row()
    .text('⏱ Сроки', `a:brand_app_tpl|id:${app.id}|k:timing|s:${back.status}|p:${back.page}`)
    .row()
    .text('⬅️ Назад', `a:brand_app_view|id:${app.id}|s:${back.status}|p:${back.page}`);

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}

async function sendBrandAppTemplateReply(ctx, actorUserId, appId, key, back) {
  const app = await safeBrandApplications(() => db.getBrandApplicationById(appId), async () => null);
  if (!app) return ctx.answerCallbackQuery({ text: 'Заявка не найдена.' });

  const brandUserId = Number(app.brand_user_id);
  const access = await assertBrandAppsAccess(ctx, actorUserId, brandUserId);
  if (!access.ok) return;

  const creatorTgId = Number(app.creator_tg_id || 0);
  if (!creatorTgId) return ctx.answerCallbackQuery({ text: 'У креатора нет TG id.' });

  const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
  const brandName = String(prof?.brand_name || '').trim() || 'Бренд';

  const replyText = buildBrandAppTemplateText(brandName, key);

  const cUrl = prof?.contact ? brandContactUrl(prof.contact) : null;
  const linkLine = prof?.link ? `\n🔗 Сайт/ссылка: ${escapeHtml(String(prof.link))}` : '';
  const contactLine = cUrl ? `\n✍️ Контакт: ${escapeHtml(String(prof.contact))}` : '';

  const outText =
    `📩 <b>Ответ бренда</b>\n\n` +
    `Бренд: <b>${escapeHtml(brandName)}</b>` +
    linkLine +
    contactLine +
    `\n\n<b>Сообщение:</b>\n${escapeHtml(replyText)}`;

  const outKb = new InlineKeyboard()
    .text('💬 Написать бренду', `a:brand_app_chat|id:${app.id}`)
    .row()
    .text('🪟 Открыть бренд', `a:brand_dir_open|u:${brandUserId}|p:0`);

  try {
    await bot.api.sendMessage(creatorTgId, outText, { parse_mode: 'HTML', reply_markup: outKb, disable_web_page_preview: true });
  } catch (e) {
    await ctx.reply('❌ Не удалось отправить сообщение креатору. Возможно, он ещё не нажимал /start.', {
      reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:brand_app_view|id:${app.id}|s:${back.status}|p:${back.page}`)
    });
    return;
  }

  // Persist
  await safeBrandApplications(() => db.markBrandApplicationReplied(appId, replyText, actorUserId), async () => null);
  await safeBrandApplications(() => db.appendBrandApplicationThreadMessage(appId, {
    from: 'brand',
    text: replyText,
    at: new Date().toISOString(),
    by_user_id: Number(actorUserId),
    by_tg_id: Number(ctx.from?.id || 0),
    by_username: ctx.from?.username || null
  }), async () => null);
  if (normLeadStatus(app.status) === 'new') {
    await safeBrandApplications(() => db.updateBrandApplicationStatus(appId, 'in_progress'), async () => null);
  }

  try { await ctx.answerCallbackQuery({ text: '✅ Отправлено' }); } catch {}
  await renderBrandAppView(ctx, actorUserId, appId, back);
}

async function acceptBrandApplication(ctx, actorUserId, appId, back) {
  const app = await safeBrandApplications(() => db.getBrandApplicationById(appId), async () => null);
  if (!app) return ctx.answerCallbackQuery({ text: 'Заявка не найдена.' });

  const brandUserId = Number(app.brand_user_id);
  const access = await assertBrandAppsAccess(ctx, actorUserId, brandUserId);
  if (!access.ok) return;

  // mark accepted (status=in_progress + meta.deal)
  await safeBrandApplications(() => db.markBrandApplicationAccepted(appId, actorUserId), async () => null);

  // notify creator
  const creatorTgId = Number(app.creator_tg_id || 0);
  if (creatorTgId) {
    const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
    const brandName = String(prof?.brand_name || '').trim() || 'Бренд';
    const outText =
      `✅ <b>Заявка принята</b>\n\n` +
      `Бренд <b>${escapeHtml(brandName)}</b> принял твою заявку.\n` +
      `Теперь можно продолжить диалог прямо в боте.`;

    const outKb = new InlineKeyboard()
      .text('💬 Написать бренду', `a:brand_app_chat|id:${app.id}`)
      .row()
      .text('🪟 Открыть бренд', `a:brand_dir_open|u:${brandUserId}|p:0`);
    try {
      await bot.api.sendMessage(creatorTgId, outText, { parse_mode: 'HTML', reply_markup: outKb, disable_web_page_preview: true });
    } catch {}
  }

  await safeBrandApplications(() => db.appendBrandApplicationThreadMessage(appId, {
    from: 'system',
    text: 'Заявка принята ✅',
    at: new Date().toISOString(),
    by_user_id: Number(actorUserId)
  }), async () => null);

  try { await ctx.answerCallbackQuery({ text: '✅ Принято' }); } catch {}
  await renderBrandAppView(ctx, actorUserId, appId, back);
}

async function startBrandAppChatForCreator(ctx, actorUserId, appId) {
  const app = await safeBrandApplications(() => db.getBrandApplicationById(appId), async () => null);
  if (!app) return ctx.answerCallbackQuery({ text: 'Заявка не найдена.' });

  if (Number(app.creator_user_id) !== Number(actorUserId)) {
    return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  }

  const brandUserId = Number(app.brand_user_id);
  const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
  const brandName = String(prof?.brand_name || '').trim() || 'Бренд';

  // allow chat if accepted OR already in progress (brand replied / accepted)
  const st = normLeadStatus(app.status);
  if (st === 'new') {
    return ctx.answerCallbackQuery({ text: 'Бренд ещё не принял заявку.' });
  }

  await setExpectText(ctx.from.id, { type: 'brand_app_chat_send', appId: Number(app.id) });

  const kb = new InlineKeyboard()
    .text('🪟 Открыть бренд', `a:brand_dir_open|u:${brandUserId}|p:0`)
    .text('🏠 Меню', 'a:menu');

  const text =
    `💬 <b>Сообщение бренду</b>\n\n` +
    `Бренд: <b>${escapeHtml(brandName)}</b>\n` +
    `Заявка: #${app.id}\n\n` +
    `Напиши сообщение одним текстом — я доставлю его в Inbox бренда.`;

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}



async function renderLeadTemplates(ctx, actorUserId, leadId, back) {
  const lead = await db.getBrandLeadById(leadId);
  if (!lead) return ctx.answerCallbackQuery({ text: 'Заявка не найдена.' });

  const wsId = Number(lead.workspace_id);
  const ws = await db.getWorkspaceAny(wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });

  const isOwner = Number(ws.owner_user_id) === Number(actorUserId);
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  if (!isOwner && !isAdmin) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const who = lead.brand_username ? '@' + String(lead.brand_username).replace(/^@/, '') : (lead.brand_name || 'brand');

  const text =
    `⚡ <b>Быстрые ответы</b>\n\n` +
    `Заявка #${lead.id} от <b>${escapeHtml(String(who))}</b>\n\n` +
    `Нажми кнопку — я отправлю бренду готовый ответ + добавлю твою контакт‑карточку (IG / TG / витрина).`;

  const kb = new InlineKeyboard()
    .text('✅ Спасибо, обсудим', `a:lead_tpl|id:${lead.id}|k:discuss|ws:${wsId}|s:${back.status}|p:${back.page}`)
    .row()
    .text('💰 Прайс / бюджет', `a:lead_tpl|id:${lead.id}|k:price|ws:${wsId}|s:${back.status}|p:${back.page}`)
    .row()
    .text('🧾 Пришли бриф', `a:lead_tpl|id:${lead.id}|k:brief|ws:${wsId}|s:${back.status}|p:${back.page}`)
    .row()
    .text('⏱ Сроки / дедлайн', `a:lead_tpl|id:${lead.id}|k:timing|ws:${wsId}|s:${back.status}|p:${back.page}`)
    .row()
    .text('🧩 UGC или интеграция?', `a:lead_tpl|id:${lead.id}|k:format|ws:${wsId}|s:${back.status}|p:${back.page}`)
    .row()
    .text('✍️ Ответить вручную', `a:lead_reply|id:${lead.id}|ws:${wsId}|s:${back.status}|p:${back.page}`)
    .row()
    .text('⬅️ Назад', `a:lead_view|id:${lead.id}|ws:${wsId}|s:${back.status}|p:${back.page}`);

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
}

async function sendLeadTemplateReply(ctx, actorUserId, leadId, key, back) {
  const lead = await db.getBrandLeadById(leadId);
  if (!lead) return ctx.answerCallbackQuery({ text: 'Заявка не найдена.' });

  const wsId = Number(lead.workspace_id);
  const ws = await db.getWorkspaceAny(wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });

  const isOwner = Number(ws.owner_user_id) === Number(actorUserId);
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  if (!isOwner && !isAdmin) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const brandTgId = Number(lead.brand_tg_id || 0);
  if (!brandTgId) return ctx.answerCallbackQuery({ text: 'У бренда нет TG id.' });

  const replyText = buildLeadTemplateText(ws, lead, key);
  const card = formatWsContactCard(ws, wsId);

  const out =
    `💬 <b>Ответ от ${escapeHtml(String(ws.profile_title || (ws.channel_username ? '@' + ws.channel_username : ws.title)))}</b>\n\n` +
    `${escapeHtml(String(replyText))}\n\n` +
    `<b>Контакты:</b>\n${card}`;

  try {
    await ctx.api.sendMessage(brandTgId, out, { parse_mode: 'HTML', disable_web_page_preview: true });
  } catch (e) {
    await ctx.reply('❌ Не удалось отправить сообщение бренду. Возможно, он не писал боту первым.', { reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:lead_view|id:${leadId}|ws:${wsId}|s:${back.status}|p:${back.page}`) });
    return;
  }

  await db.markBrandLeadReplied(leadId, replyText, Number(actorUserId));

  // auto move status to in_progress if it was new
  if (normLeadStatus(lead.status) === 'new') {
    await db.updateBrandLeadStatus(leadId, 'in_progress');
  }

  try { await ctx.answerCallbackQuery({ text: '✅ Отправлено' }); } catch {}
  await renderLeadView(ctx, actorUserId, leadId, back);
}
async function renderWsPro(ctx, ownerUserId, wsId) {
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  const ws = isAdmin ? await db.getWorkspaceAny(wsId) : await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  if (!isAdmin && Number(ws.owner_user_id) !== Number(ownerUserId)) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  await db.ensureWorkspaceSettings(wsId);
  const s = await db.getWorkspace(ownerUserId, wsId);
  const isPro = await db.isWorkspacePro(wsId);
  const until = s.pro_until ? fmtTs(s.pro_until) : '—';

  const free = `Free: конкурсы + базовая биржа`;
  const pro = `PRO: bump чаще / больше офферов / pin в ленте / расширенная аналитика`;

  const text = `⭐️ <b>PRO</b>

Канал: <b>${escapeHtml(ws.channel_username ? '@' + ws.channel_username : ws.title)}</b>
План: <b>${escapeHtml(String(s.plan || 'free').toUpperCase())}</b>
PRO до: <b>${escapeHtml(until)}</b>

${escapeHtml(free)}
${escapeHtml(pro)}

Лимиты:
• Офферы: <b>${CFG.BARTER_MAX_ACTIVE_OFFERS_FREE}</b> (Free) / <b>${CFG.BARTER_MAX_ACTIVE_OFFERS_PRO}</b> (PRO)
• Bump: <b>${CFG.BARTER_BUMP_COOLDOWN_HOURS_FREE}ч</b> (Free) / <b>${CFG.BARTER_BUMP_COOLDOWN_HOURS_PRO}ч</b> (PRO)

Оплата: Telegram Stars или ссылкой.`;

  const kb = new InlineKeyboard();
  if (!isPro) {
    kb.text(`⭐️ Купить PRO (${CFG.PRO_STARS_PRICE} Stars)`, `a:ws_pro_buy|ws:${wsId}`).row();
    if (CFG.PRO_PAYMENT_URL) kb.url('🔗 Оплатить ссылкой', CFG.PRO_PAYMENT_URL).row();
  } else {
    kb.text('📌 Пин в ленте', `a:ws_pro_pin|ws:${wsId}`).row();
  }
  kb.text('⬅️ Назад', `a:ws_open|ws:${wsId}`);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderWsProPinPick(ctx, ownerUserId, wsId) {
  const isAdmin = isSuperAdminTg(ctx.from?.id);
  const ws = isAdmin ? await db.getWorkspaceAny(wsId) : await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  if (!isAdmin && Number(ws.owner_user_id) !== Number(ownerUserId)) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  const isPro = await db.isWorkspacePro(wsId);
  if (!isPro) return ctx.answerCallbackQuery({ text: 'Доступно в PRO.' });

  const rows = await db.listBarterOffersForWorkspace(ownerUserId, wsId, 30, 0);
  const active = rows.filter(r => String(r.status).toUpperCase() === 'ACTIVE');
  const current = (await db.getWorkspace(ownerUserId, wsId)).pro_pinned_offer_id;

  const kb = new InlineKeyboard();
  for (const o of active.slice(0, 10)) {
    const isPinned = Number(current) === Number(o.id);
    const label = `${isPinned ? '📌' : '▫️'} #${o.id} ${o.title}`.slice(0, 60);
    kb.text(label, `a:ws_pro_pin_set|ws:${wsId}|o:${o.id}`).row();
  }
  kb.text('❌ Снять пин', `a:ws_pro_pin_clear|ws:${wsId}`).row();
  kb.text('⬅️ Назад', `a:ws_pro|ws:${wsId}`);

  await ctx.editMessageText(`📌 <b>Пин в ленте</b>

Выбери оффер, который будет закреплен в ленте (только для PRO).`, {
    parse_mode: 'HTML',
    reply_markup: kb
  });
}


// --- Workspace channel folders (shared lists of @channels) ---
async function getFolderAccess(userId, wsId) {
  const wsOwned = await db.getWorkspace(userId, Number(wsId));
  if (wsOwned) return { ws: wsOwned, isOwner: true, canEdit: true };
  const isEd = await db.isWorkspaceEditor(Number(wsId), userId);
  if (!isEd) return null;
  const ws = await db.getWorkspaceById(Number(wsId));
  if (!ws) return null;
  return { ws, isOwner: false, canEdit: true };
}

function foldersHomeKb(access, folders) {
  const wsId = Number(access.ws.id);
  const kb = new InlineKeyboard();
  if (access.canEdit) kb.text('➕ Новая папка', `a:folder_new|ws:${wsId}`).row();
  if (access.isOwner) kb.text('👥 Editors', `a:ws_editors|ws:${wsId}`).row();

  for (const f of folders) {
    const cnt = Number(f.items_count || 0);
    const title = String(f.title || 'Папка').slice(0, 40);
    kb.text(`📁 ${title} (${cnt})`, `a:folder_open|ws:${wsId}|f:${f.id}`).row();
  }

  if (access.isOwner) kb.text('⬅️ Назад', `a:ws_open|ws:${wsId}`);
  else kb.text('⬅️ Назад', 'a:folders_my');
  return kb;
}

async function renderFoldersMy(ctx, userId) {
  const rows = await db.listWorkspaceEditorWorkspaces(userId);
  const kb = new InlineKeyboard();
  if (rows.length) {
    for (const w of rows.slice(0, 20)) {
      const name = w.channel_username ? '@' + w.channel_username : (w.title || `ws:${w.id}`);
      kb.text(`📁 ${String(name).slice(0, 48)}`, `a:folders_home|ws:${w.id}`)
        .row();
    }
  }
  kb.text('⬅️ Меню', 'a:menu');

  const text = rows.length
    ? `📁 <b>Папки</b>\n\nВыбери канал, где ты редактор:`
    : `📁 <b>Папки</b>\n\nПока тебя не назначили редактором папок ни в одном Workspace.`;

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderFoldersHome(ctx, userId, wsId) {
  const access = await getFolderAccess(userId, wsId);
  if (!access) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const folders = await db.listChannelFolders(Number(wsId));
  const isPro = await db.isWorkspacePro(Number(wsId));
  const max = isPro ? CFG.WORKSPACE_FOLDER_MAX_ITEMS_PRO : CFG.WORKSPACE_FOLDER_MAX_ITEMS_FREE;

  const title = access.ws.channel_username ? '@' + access.ws.channel_username : (access.ws.title || `ws:${wsId}`);
  const text = `📁 <b>Папки</b>\n\nКанал: <b>${escapeHtml(String(title))}</b>\nЛимит каналов в папке: <b>${max}</b>\n\nСоздай папку и добавь @каналы для совместных конкурсов/офферов.`;

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: foldersHomeKb(access, folders) });
}

function folderViewKb(access, wsId, folderId) {
  const kb = new InlineKeyboard();

  if (access.canEdit) {
    kb.text('➕ Добавить каналы', `a:folder_add|ws:${wsId}|f:${folderId}`)
      .row()
      .text('➖ Удалить каналы', `a:folder_remove|ws:${wsId}|f:${folderId}`)
      .row()
      .text('✏️ Переименовать', `a:folder_rename|ws:${wsId}|f:${folderId}`)
      .row()
      .text('🧹 Очистить', `a:folder_clear_q|ws:${wsId}|f:${folderId}`)
      .row();
  }

  kb.text('📤 Выгрузить списком', `a:folder_export|ws:${wsId}|f:${folderId}`)
    .row();

  if (access.isOwner) {
    kb.text('🗑 Удалить папку', `a:folder_delete_q|ws:${wsId}|f:${folderId}`)
      .row();
  }

  kb.text('⬅️ Назад', `a:folders_home|ws:${wsId}`);
  return kb;
}

async function renderFolderView(ctx, userId, wsId, folderId) {
  const access = await getFolderAccess(userId, wsId);
  if (!access) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const folder = await db.getChannelFolder(Number(folderId));
  if (!folder || Number(folder.workspace_id) !== Number(wsId)) {
    return ctx.answerCallbackQuery({ text: 'Папка не найдена.' });
  }

  const items = await db.listChannelFolderItems(Number(folderId));
  const isPro = await db.isWorkspacePro(Number(wsId));
  const max = isPro ? CFG.WORKSPACE_FOLDER_MAX_ITEMS_PRO : CFG.WORKSPACE_FOLDER_MAX_ITEMS_FREE;

  const shown = items.slice(0, 25).map(i => `• ${escapeHtml(i.channel_username)}`);
  const more = items.length > 25 ? `\n…и ещё <b>${items.length - 25}</b>` : '';

  const title = access.ws.channel_username ? '@' + access.ws.channel_username : (access.ws.title || `ws:${wsId}`);

  const text = `📁 <b>${escapeHtml(String(folder.title || 'Папка'))}</b>\n` +
    `Канал: <b>${escapeHtml(String(title))}</b>\n` +
    `Каналы: <b>${items.length}</b> / <b>${max}</b>\n\n` +
    (shown.length ? shown.join('\n') : 'Пока пусто.') +
    more;

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: folderViewKb(access, Number(wsId), Number(folderId)) });
}

async function renderWsEditors(ctx, ownerUserId, wsId) {
  const ws = await db.getWorkspace(ownerUserId, Number(wsId));
  if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const editors = await db.listWorkspaceEditors(Number(wsId));

  const kb = new InlineKeyboard()
    .text('➕ Invite link', `a:ws_editor_invite|ws:${wsId}`)
    .row()
    .text('➕ Добавить по @username', `a:ws_editor_add_username|ws:${wsId}`)
    .row();

  if (editors.length) {
    for (const e of editors.slice(0, 20)) {
      const label = e.tg_username ? '@' + e.tg_username : ('id:' + e.tg_id);
      kb.text(`❌ ${String(label).slice(0, 28)}`, `a:ws_editor_rm_q|ws:${wsId}|u:${e.user_id}`).row();
    }
  }

  kb.text('⬅️ Назад', `a:folders_home|ws:${wsId}`);

  const lines = editors.map(e => `• ${e.tg_username ? '@' + escapeHtml(e.tg_username) : 'id:' + escapeHtml(String(e.tg_id))}`);

  const text = `👥 <b>Editors</b>\n\n` +
    `Редакторы могут управлять папками (добавлять/удалять @каналы).\n` +
    `По умолчанию папки редактирует только owner.\n\n` +
    (lines.length ? lines.join('\n') : 'Пока нет редакторов.');

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}


function bxTypeLabel(t) {
  switch (t) {
    case 'ad': return '📣 Реклама/упоминание';
    case 'review': return '🎥 Обзор/распаковка';
    case 'giveaway': return '🎁 Розыгрыш';
    default: return '✍️ Другое';
  }
}

function bxCompLabel(p) {
  switch (p) {
    case 'barter': return '🤝 Бартер';
    case 'cert': return '🎟 Сертификат';
    case 'rub': return '💸 ₽';
    default: return '🔁 Смешано';
  }
}

const BX_CATS = [null, 'cosmetics', 'fashion', 'unboxing', 'other'];
const BX_TYPES = [null, 'ad', 'review', 'giveaway', 'other'];
const BX_COMPS = [null, 'barter', 'cert', 'rub', 'mixed'];

function bxAnyLabel(v, kind) {
  if (!v) return 'Все';
  if (kind == 'cat') return bxCategoryLabel(v);
  if (kind == 'type') return bxTypeLabel(v);
  return bxCompLabel(v);
}

async function getBxFilter(tgId, wsId) {
  const key = k(['bx_filter', tgId, wsId]);
  const v = await redis.get(key);
  const base = v || {};

  // Canonical shape
  const f = {
    category: base.category ?? null,
    offerType: base.offerType ?? null,
    compensationType: base.compensationType ?? null
  };

  // Back-compat: older UI stored short keys (cat/type/comp)
  const norm = (x) => {
    if (x == null) return null;
    const s = String(x);
    if (!s || s === 'all' || s === 'undefined' || s === 'null') return null;
    return s;
  };
  if (f.category == null && base.cat != null) f.category = norm(base.cat);
  if (f.offerType == null && base.type != null) f.offerType = norm(base.type);
  if (f.compensationType == null && base.comp != null) f.compensationType = norm(base.comp);

  // If we had to normalize anything, persist back in canonical shape
  const needsPersist = !base.category && !base.offerType && !base.compensationType && (base.cat || base.type || base.comp);
  if (needsPersist) {
    try {
      await redis.set(key, f, { ex: 30 * 24 * 3600 });
    } catch {}
  }

  return f;
}

async function setBxFilter(tgId, wsId, patch) {
  const key = k(['bx_filter', tgId, wsId]);
  const cur = await getBxFilter(tgId, wsId);
  const next = { ...cur, ...patch };
  await redis.set(key, next, { ex: 30 * 24 * 3600 });
  return next;
}

function bxFilterSummary(f) {
  const parts = [
    `Кат: ${bxAnyLabel(f.category, 'cat')}`,
    `Формат: ${bxAnyLabel(f.offerType, 'type')}`,
    `Оплата: ${bxAnyLabel(f.compensationType, 'comp')}`,
  ];
  return parts.join(' · ');
}

async function renderBxOpen(ctx, ownerUserId, wsId) {
  const wsNum = Number(wsId || 0);
  if (wsNum === 0) {
    const credits = await db.getBrandCredits(ownerUserId);
    const retry = CFG.INTRO_RETRY_ENABLED ? await db.countAvailableBrandRetryCredits(ownerUserId) : 0;
    const planRow = await db.getBrandPlan(ownerUserId);
    const active = await db.isBrandPlanActive(ownerUserId);
    const planName = active ? String(planRow?.brand_plan || 'basic').toLowerCase() : null;
    const plan = { active, name: planName, until: planRow?.brand_plan_until };

    const untilTxt = (active && planRow?.brand_plan_until) ? `
До: <b>${escapeHtml(fmtTs(planRow.brand_plan_until))}</b>` : '';

    await ctx.editMessageText(
      `🏷 <b>Для брендов</b>

Здесь бренд может работать с UGC/офферами без подключения канала.

🎫 Brand Pass: <b>${credits}</b>
🎟 Retry credits: <b>${retry}</b>
⭐️ Brand Plan: <b>${active ? (planName === 'max' ? 'Max' : 'Basic') : 'OFF'}</b>${untilTxt}

Выбери действие:`,
      { parse_mode: 'HTML', reply_markup: bxBrandMenuKb(0, credits, plan, retry) }
    );
    return;
  }

  const ws = await db.getWorkspace(ownerUserId, wsNum);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  if (!ws.network_enabled) {
    await ctx.editMessageText(
      `🎬 <b>UGC / Офферы</b>

Это лента UGC/Collab офферов: контент, интеграции, бартер/бюджет.

Чтобы видеть ленту и публиковать офферы, включи “🌐 Сеть”.`,
      { parse_mode: 'HTML', reply_markup: bxNeedNetworkKb(wsNum) }
    );
    return;
  }

  await ctx.editMessageText(
    `🎬 <b>UGC / Офферы</b>

Канал: <b>${escapeHtml(ws.channel_username ? '@' + ws.channel_username : ws.title)}</b>

• Лента — офферы от участников сети
• Разместить — твой UGC/оффер попадет в ленту
• Мои офферы — пауза/удаление`,
    { parse_mode: 'HTML', reply_markup: bxMenuKb(wsNum, ws.network_enabled) }
  );
}

async function renderBxFeed(ctx, ownerUserId, wsId, page = 0) {
  const wsNum = Number(wsId || 0);
  if (wsNum !== 0) {
    const ws = await db.getWorkspace(ownerUserId, wsNum);
    if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
    if (!ws.network_enabled) return renderBxOpen(ctx, ownerUserId, wsNum);
  }

  const filter = await getBxFilter(ctx.from.id, wsNum);

  const limit = CFG.BARTER_FEED_PAGE_SIZE;
  const offset = page * limit;
  const total = await db.countNetworkBarterOffers({
    category: filter.category,
    offerType: filter.offerType,
    compensationType: filter.compensationType,
  });
  let rows;
  if (CFG.VERIFICATION_ENABLED) {
    rows = await safeUserVerifications(
      () => db.listNetworkBarterOffersWithVerified({
        category: filter.category,
        offerType: filter.offerType,
        compensationType: filter.compensationType,
        limit,
        offset,
      }),
      () => db.listNetworkBarterOffers({
        category: filter.category,
        offerType: filter.offerType,
        compensationType: filter.compensationType,
        limit,
        offset,
      })
    );
  } else {
    rows = await db.listNetworkBarterOffers({
      category: filter.category,
      offerType: filter.offerType,
      compensationType: filter.compensationType,
      limit,
      offset,
    });
  }

  const featured = await db.listActiveFeatured(CFG.FEATURED_MAX_SLOTS);

  const header = `🛍 <b>Лента офферов</b>
<tg-spoiler>${escapeHtml(bxFilterSummary(filter))}</tg-spoiler>`;

  const featLines = featured.map((f) => {
    const title = (f.title || 'Featured').toString();
    const body = (f.body || '').toString();
    const contact = (f.contact || '').toString();
    const blurb = body ? body.replace(/\s+/g, ' ').slice(0, 90) : '';
    const c = contact ? `
Контакт: <b>${escapeHtml(contact.slice(0, 64))}</b>` : '';
    return `🔥 <b>${escapeHtml(title.slice(0, 64))}</b>${blurb ? `
${escapeHtml(blurb)}${body.length > 90 ? '…' : ''}` : ''}${c}`;
  });

  const offerLines = rows.map((o) => {
    const ch = o.channel_username ? `@${o.channel_username}` : (o.ws_title || 'канал');
    return `#${o.id} · ${escapeHtml(bxCategoryLabel(o.category))}
<b>${escapeHtml(o.title)}</b>
${escapeHtml(bxTypeLabel(o.offer_type))} · ${escapeHtml(bxCompLabel(o.compensation_type))}
Канал: ${escapeHtml(ch)}${o.creator_verified ? ' ✅' : ''}`;
  });

  const text = `${header}

${featLines.length ? `🔥 <b>Featured</b>

${featLines.join('\n\n')}

` : ''}${offerLines.length ? offerLines.join('\n\n') : 'Пока нет офферов по этим фильтрам.'}`;

  const kb = new InlineKeyboard();

  for (const f of featured) {
    kb.text(`🔥 #F${f.id}`, `a:feat_view|ws:${wsNum}|id:${f.id}|p:${page}`).row();
  }
  for (const o of rows) {
    kb.text(`🔎 #${o.id}`, `a:bx_pub|ws:${wsNum}|o:${o.id}|p:${page}`).row();
  }

  const hasPrev = page > 0;
  const hasNext = offset + rows.length < total;
  const nav = bxFeedNavKb(wsNum, page, hasPrev, hasNext);
  for (const row of nav.inline_keyboard) kb.inline_keyboard.push(row);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderBxMy(ctx, ownerUserId, wsId, page = 0) {
  const ws = await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  if (!ws.network_enabled) return renderBxOpen(ctx, ownerUserId, wsId);

  const limit = 8;
  const offset = page * limit;
  const rows = await db.listBarterOffersForOwnerWorkspace(ownerUserId, wsId, limit, offset);

  const kb = new InlineKeyboard();
  kb.text('📁 Архив', `a:bx_my_arch|ws:${wsId}|p:0`).row();
  for (const o of rows) {
    const st = String(o.status || 'ACTIVE').toUpperCase();
    const stEmoji = st === 'ACTIVE' ? '✅' : (st === 'PAUSED' ? '⏸' : '⛔');
    kb
      .text(`${stEmoji} #${o.id} · ${o.title}`, `a:bx_view|ws:${wsId}|o:${o.id}|back:my`)
      .text('🗑', `a:bx_archive|ws:${wsId}|o:${o.id}|p:${page}`)
      .row();
  }
  kb.text('⬅️ Назад', `a:bx_open|ws:${wsId}`);

  await ctx.editMessageText(
    `📦 <b>Мои офферы</b>

Нажми оффер, чтобы открыть. Кнопка 🗑 — архивирует и сразу убирает из списка.`,
    { parse_mode: 'HTML', reply_markup: kb }
  );
}

async function renderBxMyArchive(ctx, ownerUserId, wsId, page = 0) {
  const ws = await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  if (!ws.network_enabled) return renderBxOpen(ctx, ownerUserId, wsId);

  const limit = 8;
  const offset = page * limit;
  const rows = await db.listArchivedBarterOffersForOwnerWorkspace(ownerUserId, wsId, limit, offset);

  const kb = new InlineKeyboard();

  if (!rows.length) {
    kb.text('⬅️ Назад', `a:bx_my|ws:${wsId}|p:0`).row().text('🏠 Меню', 'a:menu');
    await ctx.editMessageText(
      `📁 <b>Архив офферов</b>

Пока пусто. Нажми 🗑 в «Мои офферы», чтобы архивировать оффер (он останется в истории).`,
      { parse_mode: 'HTML', reply_markup: kb }
    );
    return;
  }

  for (const o of rows) {
    kb
      .text(`⛔ #${o.id} · ${o.title}`, `a:bx_view|ws:${wsId}|o:${o.id}|back:arch`)
      .text('↩️', `a:bx_restore|ws:${wsId}|o:${o.id}|p:${page}`)
      .row();
  }

  const hasPrev = page > 0;
  const hasNext = rows.length === limit;
  if (hasPrev || hasNext) {
    const nav = new InlineKeyboard();
    if (hasPrev) nav.text('⬅️', `a:bx_my_arch|ws:${wsId}|p:${page - 1}`);
    if (hasNext) nav.text('➡️', `a:bx_my_arch|ws:${wsId}|p:${page + 1}`);
    kb.inline_keyboard.push(nav.inline_keyboard[0]);
  }

  kb.text('⬅️ Назад', `a:bx_my|ws:${wsId}|p:0`).row().text('🏠 Меню', 'a:menu');

  await ctx.editMessageText(
    `📁 <b>Архив офферов</b>

Открой оффер, чтобы посмотреть. ↩️ — вернуть в активные.`,
    { parse_mode: 'HTML', reply_markup: kb }
  );
}


function bxMediaLabel(mt) {
  const t = String(mt || '').toLowerCase();
  if (t === 'photo') return '🖼 Фото';
  if (t === 'video') return '🎥 Видео';
  if (t === 'animation') return '🎞 GIF';
  return '—';
}

function bxMediaKb(wsId, offerId, back = 'my', hasMedia = false) {
  const kb = new InlineKeyboard()
    .text('🖼 Фото', `a:bx_media_photo|ws:${wsId}|o:${offerId}|back:${back}`)
    .text('🎞 GIF', `a:bx_media_gif|ws:${wsId}|o:${offerId}|back:${back}`)
    .row()
    .text('🎥 Видео', `a:bx_media_video|ws:${wsId}|o:${offerId}|back:${back}`)
    .text('👁 Превью', `a:bx_media_preview|ws:${wsId}|o:${offerId}|back:${back}`)
    .row();

  if (hasMedia) {
    kb.text('🗑 Убрать', `a:bx_media_clear|ws:${wsId}|o:${offerId}|back:${back}`)
      .text('✅ Готово', `a:bx_view|ws:${wsId}|o:${offerId}|back:${back}`);
  } else {
    kb.text('✅ Готово', `a:bx_view|ws:${wsId}|o:${offerId}|back:${back}`);
  }

  kb.row().text('⬅️ Назад', `a:bx_view|ws:${wsId}|o:${offerId}|back:${back}`);
  return kb;
}

async function renderBxMediaStep(ctx, ownerUserId, wsId, offerId, back = 'my', opts = {}) {
  const { edit = true } = opts;
  const o = await db.getBarterOfferForOwner(ownerUserId, offerId);
  if (!o) {
    if (ctx.callbackQuery) await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
    return;
  }

  const hasMedia = !!(o.media_file_id && String(o.media_type || '').trim());
  const text =
`📎 <b>Медиа оффера #${o.id}</b>

Текущее: <b>${escapeHtml(bxMediaLabel(o.media_type))}</b>

ℹ️ Медиа появится в официальном канале только при <b>PAID-размещении</b>.
(Внутри “Мои офферы” медиа не показываем — только в официальной публикации.)

Выбери тип и пришли файл одним сообщением.`;

  const kb = bxMediaKb(wsId, offerId, back, hasMedia);
  const send = (edit && ctx.callbackQuery) ? ctx.editMessageText.bind(ctx) : ctx.reply.bind(ctx);
  await send(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function sendBxPreview(ctx, ownerUserId, wsId, offerId, back = 'my') {
  const o = await db.getBarterOfferForOwner(ownerUserId, offerId);
  if (!o) return ctx.reply('Оффер не найден или нет доступа.');

  const { text } = await buildOfficialOfferPost(o, { forCaption: true });
  const note = `\n\n<i>Это превью (пересылать не нужно).</i>\n<i>Кнопки появятся при публикации.</i>\n<i>Медиа попадёт в официальный канал только при PAID-размещении.</i>`;
  const caption = `${text}${note}`;

  try {
    if (o.media_file_id && String(o.media_type) === 'photo') {
      await ctx.replyWithPhoto(o.media_file_id, { caption, parse_mode: 'HTML' });
    } else if (o.media_file_id && String(o.media_type) === 'animation') {
      await ctx.replyWithAnimation(o.media_file_id, { caption, parse_mode: 'HTML' });
    } else if (o.media_file_id && String(o.media_type) === 'video') {
      await ctx.replyWithVideo(o.media_file_id, { caption, parse_mode: 'HTML' });
    } else {
      await ctx.reply(`${text}${note}`, { parse_mode: 'HTML', disable_web_page_preview: true });
    }
  } catch (_) {
    await ctx.reply('Не удалось отправить превью. Попробуй ещё раз или убери медиа.');
  }

  // Return user to offer view
  await renderBxView(ctx, ownerUserId, wsId, offerId, back);
}

async function renderBxView(ctx, ownerUserId, wsId, offerId, back = 'feed') {
  const o = await db.getBarterOfferForOwner(ownerUserId, offerId);
  if (!o) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const st = String(o.status || 'ACTIVE').toUpperCase();
  const contact = (o.contact || '').trim();

  let partnerBlock = ''
  let partnerBtnLabel = '📁 Папка партнёров'
  if (o.partner_folder_id) {
    try {
      const folder = await db.getChannelFolder(Number(o.partner_folder_id));
      if (folder && Number(folder.workspace_id) === Number(wsId)) {
        const items = await db.listChannelFolderItems(folder.id);
        const shown = items.slice(0, 10).map(i => i.channel_username);
        const more = items.length > shown.length ? `
… и ещё ${items.length - shown.length}` : '';
        const safeTitle = escapeHtml(String(folder.title || '').slice(0, 40));
        partnerBlock = `

Партнёры (папка “${safeTitle}”, ${items.length}):
${shown.map(x => escapeHtml(x)).join('\n')}${more}`;
        partnerBtnLabel = `📁 Папка: ${String(folder.title || '').slice(0, 18)} (${items.length})`;
      }
    } catch (_) {}
  }

  const text =
`🤝 <b>Оффер #${o.id}</b>

Статус: <b>${escapeHtml(st)}</b>
Категория: <b>${escapeHtml(bxCategoryLabel(o.category))}</b>
Формат: <b>${escapeHtml(bxTypeLabel(o.offer_type))}</b>
Оплата: <b>${escapeHtml(bxCompLabel(o.compensation_type))}</b>
Медиа: <b>${escapeHtml(bxMediaLabel(o.media_type))}</b>

<b>${escapeHtml(o.title)}</b>

${escapeHtml(o.description)}${partnerBlock}

${contact ? `Контакт: <b>${escapeHtml(contact)}</b>` : ''}`;

  const kb = new InlineKeyboard();
  if (st === 'ACTIVE') {
    kb.text('⬆️ Поднять', `a:bx_bump|ws:${wsId}|o:${o.id}`).row();

    kb.text(partnerBtnLabel, `a:bx_partner_folder_pick|ws:${wsId}|o:${o.id}`).row();
    kb.text('📎 Медиа', `a:bx_media_step|ws:${wsId}|o:${o.id}|back:${back}`).text('👁 Превью', `a:bx_media_preview|ws:${wsId}|o:${o.id}|back:${back}`).row();

    const wsInfo = await db.getWorkspace(ownerUserId, wsId);
    const isPro = await db.isWorkspacePro(wsId);
    if (isPro) {
      const pinnedId = wsInfo.pro_pinned_offer_id ? Number(wsInfo.pro_pinned_offer_id) : null;
      if (pinnedId === Number(o.id)) {
        kb.text('📌 Снять пин', `a:bx_pin_clear|ws:${wsId}|o:${o.id}`).row();
      } else {
        kb.text('📌 Закрепить в ленте', `a:bx_pin_set|ws:${wsId}|o:${o.id}`).row();
      }
    }
    kb.text('⏸ Пауза', `a:bx_pause|ws:${wsId}|o:${o.id}`).row();
  }
  if (st === 'PAUSED') kb.text('✅ Возобновить', `a:bx_resume|ws:${wsId}|o:${o.id}`).row();

  if (st === 'CLOSED') {
    kb.text('↩️ Восстановить', `a:bx_restore|ws:${wsId}|o:${o.id}|p:0`).row();
  } else {
    kb.text('🗑 Архивировать', `a:bx_del_q|ws:${wsId}|o:${o.id}`).row();
  }

  const backCb = back === 'my'
    ? `a:bx_my|ws:${wsId}|p:0`
    : (back === 'arch' ? `a:bx_my_arch|ws:${wsId}|p:0` : `a:bx_feed|ws:${wsId}|p:0`);
  kb.text('⬅️ Назад', backCb);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}


async function renderBxFilters(ctx, ownerUserId, wsId, page = 0) {
  const wsNum = Number(wsId || 0);
  if (wsNum !== 0) {
    const ws = await db.getWorkspace(ownerUserId, wsNum);
    if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
    if (!ws.network_enabled) return renderBxOpen(ctx, ownerUserId, wsNum);
  }

  const f = await getBxFilter(ctx.from.id, wsNum);
  const text = `🎛 <b>Фильтры ленты</b>

${escapeHtml(bxFilterSummary(f))}

Выбери, что показывать в ленте.`;
  await ctx.editMessageText(text, {
    parse_mode: 'HTML',
    reply_markup: bxFiltersKb(wsNum, f, page)
  });
}

async function renderBxFilterPick(ctx, ownerUserId, wsId, key, page = 0) {
  const wsNum = Number(wsId || 0);
  if (wsNum !== 0) {
    const ws = await db.getWorkspace(ownerUserId, wsNum);
    if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
    if (!ws.network_enabled) return renderBxOpen(ctx, ownerUserId, wsNum);
  }

  const title = key === 'cat' ? 'Категория' : (key === 'type' ? 'Формат' : 'Оплата');
  await ctx.editMessageText(`🎛 <b>${title}</b>

Выбери значение:`, {
    parse_mode: 'HTML',
    reply_markup: bxPickKb(wsNum, key, page)
  });
}

async function renderBxPublicView(ctx, userId, wsId, offerId, page = 0) {
  const o = CFG.VERIFICATION_ENABLED
    ? await safeUserVerifications(() => db.getBarterOfferPublicWithVerified(offerId), () => db.getBarterOfferPublic(offerId))
    : await db.getBarterOfferPublic(offerId);

  const fail = async (msg) => {
    if (ctx.callbackQuery) return ctx.answerCallbackQuery({ text: msg, show_alert: true });
    return ctx.reply(msg);
  };

  if (!o) return fail('Оффер не найден.');
  if (String(o.status || '').toUpperCase() !== 'ACTIVE') return fail('Оффер закрыт.');
  if (!o.network_enabled) return fail('Оффер вне сети.');

  const ch = o.channel_username ? `@${o.channel_username}` : (o.ws_title || 'канал');
  const contact = (o.contact || '').trim();

  let partnerBlock = '';
  if (o.partner_folder_id) {
    try {
      const folder = await db.getChannelFolder(Number(o.partner_folder_id));
      if (folder && Number(folder.workspace_id) === Number(wsId)) {
        const items = await db.listChannelFolderItems(folder.id);
        const shown = items.slice(0, 10).map((i) => i.channel_username);
        const more = items.length > shown.length ? `\n… и ещё ${items.length - shown.length}` : '';
        const safeTitle = escapeHtml(String(folder.title || '').slice(0, 40));
        partnerBlock = `\n\nПартнёры (папка “${safeTitle}”, ${items.length}):\n${shown.map((x) => escapeHtml(x)).join('\n')}${more}`;
      }
    } catch (_) {}
  }

  const text =
    `🤝 <b>Оффер #${o.id}</b>\n\n` +
    `Категория: <b>${escapeHtml(bxCategoryLabel(o.category))}</b>\n` +
    `Формат: <b>${escapeHtml(bxTypeLabel(o.offer_type))}</b>\n` +
    `Оплата: <b>${escapeHtml(bxCompLabel(o.compensation_type))}</b>\n\n` +
    `<b>${escapeHtml(o.title)}</b>\n\n` +
    `${escapeHtml(o.description)}${partnerBlock}\n\n` +
    `Канал: <b>${escapeHtml(ch)}${o.creator_verified ? ' ✅' : ''}</b>\n` +
    `${contact ? `Контакт: <b>${escapeHtml(contact)}</b>\n` : ''}` +
    `\nЕсли бот не может проверить каналы — попроси админа добавить бота в канал-спонсор.`;

  const kb = new InlineKeyboard().text('💬 Написать', `a:bx_msg|ws:${wsId}|o:${offerId}|p:${page}`);

  const isOwner = Number(o.owner_user_id) === Number(userId);
  let canOfficial = false;
  if (CFG.OFFICIAL_PUBLISH_ENABLED) {
    try {
      canOfficial = isOwner || (await isModerator({ id: userId }, ctx.from?.id));
    } catch {
      canOfficial = isOwner;
    }
  }

  if (canOfficial) {
    kb.row().text('📣 Офиц.канал', `a:off_manage|ws:${wsId}|o:${offerId}|p:${page}`);
  }

  kb.row().text('🚩 Жалоба', `a:bx_report_offer|ws:${wsId}|o:${offerId}|p:${page}`);
  // Back: for non-owners this wsId feed is inaccessible; send them to Brand Mode feed
  const backCb = isOwner ? `a:bx_feed|ws:${wsId}|p:${page}` : `a:bx_feed|ws:0|p:0`;
  kb.row().text('⬅️ Назад', backCb);

  const send = ctx.callbackQuery ? ctx.editMessageText.bind(ctx) : ctx.reply.bind(ctx);
  await send(text, { parse_mode: 'HTML', reply_markup: kb });
}

// -----------------------------
// Official channel publishing (barter offers)
// -----------------------------

function offerDeepLink(offerId) {
  const u = String(CFG.BOT_USERNAME || '').trim();
  if (!u) return '';
  return `https://t.me/${u}?start=bxo_${offerId}`;
}

function truncateText(s, maxLen = 800) {
  const txt = String(s || '').trim();
  if (txt.length <= maxLen) return txt;
  return txt.slice(0, maxLen - 1) + '…';
}

async function safeOfficialPosts(primaryFn, fallbackFn) {
  try {
    return await primaryFn();
  } catch (e) {
    if (isMissingRelationError(e, 'official_posts')) {
      return await fallbackFn();
    }
    throw e;
  }
}

async function buildOfficialOfferPost(offerRow, opts = {}) {
  const forCaption = Boolean(opts.forCaption);

  const offerId = Number(offerRow.id);
  const ch = offerRow.channel_username ? `@${offerRow.channel_username}` : (offerRow.ws_title || 'канал');
  const contact = (offerRow.contact || '').trim();
  const link = offerDeepLink(offerId);

  const title = escapeHtml(String(offerRow.title || ''));
  const desc = escapeHtml(truncateText(offerRow.description || '', forCaption ? 520 : 900));
  const cat = escapeHtml(bxCategoryLabel(offerRow.category));
  const fmt = escapeHtml(bxTypeLabel(offerRow.offer_type));
  const comp = escapeHtml(bxCompLabel(offerRow.compensation_type));

  const text =
    `🤝 <b>Коллабка</b> · оффер #${offerId}

` +
    `Категория: <b>${cat}</b>
` +
    `Формат: <b>${fmt}</b>
` +
    `Оплата: <b>${comp}</b>

` +
    `<b>${title}</b>

` +
    `${desc}

` +
    `Канал: <b>${escapeHtml(ch)}${offerRow.creator_verified ? ' ✅' : ''}</b>
` +
    `${contact ? `Контакт: <b>${escapeHtml(contact)}</b>
` : ''}` +
    `${link ? `
Открыть в боте: ${escapeHtml(link)}` : ''}`;

  const kb = new InlineKeyboard();
  if (link) kb.url('🚀 Открыть оффер', link);
  return { text, kb };
}

async function publishOfferToOfficialChannel(api, offerId, opts = {}) {
  if (!CFG.OFFICIAL_PUBLISH_ENABLED) throw new Error('OFFICIAL_PUBLISH_ENABLED=false');

  const channelId = Number(CFG.OFFICIAL_CHANNEL_ID || 0);
  if (!channelId) throw new Error('OFFICIAL_CHANNEL_ID is missing');

  // Normalize placement type.
  const placementRaw = String(opts.placementType || 'MANUAL').toUpperCase();
  const keepExpiry = !!opts.keepExpiry;

  // Existing DB record (if any).
  const existing = await safeOfficialPosts(() => db.getOfficialPostByOfferId(offerId), async () => null);
  let placementType = placementRaw;
  if (placementType === 'UPDATE') {
    placementType = existing?.placement_type ? String(existing.placement_type).toUpperCase() : 'MANUAL';
  }
  if (!['MANUAL', 'PAID'].includes(placementType)) placementType = 'MANUAL';

  const offer = CFG.VERIFICATION_ENABLED
    ? await safeUserVerifications(() => db.getBarterOfferPublicWithVerified(offerId), () => db.getBarterOfferPublic(offerId))
    : await db.getBarterOfferPublic(offerId);

  if (!offer) throw new Error('Offer not found');
  if (String(offer.status || '').toUpperCase() !== 'ACTIVE') throw new Error('Offer is not active');
  if (!offer.network_enabled) throw new Error('Offer is not in network');

  // Slot params
  const defaultDays = Math.max(1, Number(CFG.OFFICIAL_MANUAL_DEFAULT_DAYS || 3));
  const days = Math.max(
    1,
    Number(opts.days || existing?.slot_days || defaultDays)
  );

  // Expiry: keep existing if asked, otherwise (re)compute.
  let slotExpiresAt = null;
  if (keepExpiry && existing?.slot_expires_at) {
    try { slotExpiresAt = new Date(existing.slot_expires_at).toISOString(); } catch { slotExpiresAt = null; }
  }
  if (!slotExpiresAt) slotExpiresAt = new Date(Date.now() + days * 24 * 60 * 60 * 1000).toISOString();

  const paymentId = opts.paymentId ? Number(opts.paymentId) : (existing?.payment_id ? Number(existing.payment_id) : null);
  const publishedByUserId = opts.publishedByUserId ? Number(opts.publishedByUserId) : null;

  // Build message
  const hasMedia = !!(offer.media_file_id && String(offer.media_type || '').trim());
  const built = await buildOfficialOfferPost(offer, { forCaption: hasMedia });
  const text = built.text;
  const replyMarkup = built.kb;

  const isActive = String(existing?.status || '').toUpperCase() === 'ACTIVE' && existing?.message_id;
  let messageId = isActive ? Number(existing.message_id) : null;

  async function tryEditExisting() {
    if (!messageId) return false;
    // Try both (text vs media).
    try {
      await api.editMessageText(channelId, messageId, text, { parse_mode: 'HTML', reply_markup: replyMarkup });
      return true;
    } catch {}
    try {
      await api.editMessageCaption(channelId, messageId, { caption: text, parse_mode: 'HTML', reply_markup: replyMarkup });
      return true;
    } catch {}
    return false;
  }

  const edited = await tryEditExisting();
  if (!edited) {
    // Send new message
    let sent;
    const mt = String(offer.media_type || '').toLowerCase();
    const fid = String(offer.media_file_id || '').trim();

    if (hasMedia && fid) {
      if (mt === 'photo') {
        sent = await api.sendPhoto(channelId, fid, { caption: text, parse_mode: 'HTML', reply_markup: replyMarkup });
      } else if (mt === 'video') {
        sent = await api.sendVideo(channelId, fid, { caption: text, parse_mode: 'HTML', reply_markup: replyMarkup });
      } else if (mt === 'animation' || mt === 'gif') {
        sent = await api.sendAnimation(channelId, fid, { caption: text, parse_mode: 'HTML', reply_markup: replyMarkup });
      } else {
        sent = await api.sendMessage(channelId, text, { parse_mode: 'HTML', reply_markup: replyMarkup });
      }
    } else {
      sent = await api.sendMessage(channelId, text, { parse_mode: 'HTML', reply_markup: replyMarkup });
    }

    const newId = sent?.message_id ? Number(sent.message_id) : null;
    if (!newId) throw new Error('Failed to publish: missing message_id');

    // Remove old message (best-effort) if it existed.
    if (messageId && newId !== messageId) {
      try { await api.deleteMessage(channelId, messageId); } catch {}
    }
    messageId = newId;
  }

  // Persist ACTIVE post record
  await safeOfficialPosts(
    () => db.setOfficialPostActive(offerId, {
      channelChatId: channelId,
      messageId,
      placementType,
      paymentId,
      slotDays: days,
      slotExpiresAt,
      publishedByUserId
    }),
    async () => null
  );

  // If this was a paid placement, mark payment as "applied" (best-effort).
  if (placementType === 'PAID' && paymentId) {
    try {
      await db.setPaymentApplied(paymentId, { offerId, meta: { official: true } });
    } catch {
      // ignore
    }
  }

  return { ok: true, messageId, placementType, days, slotExpiresAt };
}



async function removeOfficialOfferPost(api, offerId, reason = 'REMOVED') {
  const existing = await safeOfficialPosts(() => db.getOfficialPostByOfferId(offerId), async () => null);
  if (!existing) return { removed: false };
  const channelId = Number(existing.channel_chat_id || 0);
  const msgId = Number(existing.message_id || 0);
  if (channelId && msgId) {
    try {
      const text = reason === 'EXPIRED'
        ? '⌛️ Размещение истекло.'
        : '📴 Размещение снято.';
      try {
        await api.editMessageText(channelId, msgId, text, { parse_mode: 'HTML' });
      } catch (_) {
        try { await api.editMessageCaption(channelId, msgId, { caption: text, parse_mode: 'HTML' }); } catch (_) {}
      }
    } catch (_) {}
  }

  await safeOfficialPosts(
    () => db.setOfficialPostStatus(offerId, reason, { lastError: null }),
    async () => null,
  );

  return { removed: true };
}

async function renderOfficialManageView(ctx, userId, wsId, offerId, page = 0) {
  if (!CFG.OFFICIAL_PUBLISH_ENABLED) {
    if (ctx.callbackQuery) await ctx.answerCallbackQuery({ text: 'Официальный канал выключен.', show_alert: true });
    return;
  }

  const offer = CFG.VERIFICATION_ENABLED
    ? await safeUserVerifications(() => db.getBarterOfferPublicWithVerified(offerId), () => db.getBarterOfferPublic(offerId))
    : await db.getBarterOfferPublic(offerId);

  if (!offer) {
    if (ctx.callbackQuery) await ctx.answerCallbackQuery({ text: 'Оффер не найден.', show_alert: true });
    return;
  }

  const isOwner = Number(offer.owner_user_id) === Number(userId);
  const isMod = await isModerator({ id: userId }, ctx.from?.id);
  if (!isOwner && !isMod) {
    if (ctx.callbackQuery) await ctx.answerCallbackQuery({ text: 'Нет доступа.', show_alert: true });
    return;
  }

  const post = await safeOfficialPosts(() => db.getOfficialPostByOfferId(offerId), async () => null);
  const st = String(post?.status || 'NONE').toUpperCase();

  const statusLabel = {
    NONE: '—',
    PENDING: '⏳ Pending',
    ACTIVE: '✅ Active',
    REMOVED: '📴 Removed',
    EXPIRED: '⌛️ Expired',
    ERROR: '⚠️ Error',
  }[st] || st;

  const expiresLine = post?.slot_expires_at ? `
Слот до: <b>${escapeHtml(new Date(post.slot_expires_at).toLocaleString('ru-RU'))}</b>` : '';
  const mode = String(CFG.OFFICIAL_PUBLISH_MODE || 'manual').toLowerCase();

  const text = `📣 <b>Официальный канал</b>

Оффер: <b>#${offerId}</b>
Статус: <b>${escapeHtml(statusLabel)}</b>${expiresLine}

Режим: <b>${escapeHtml(mode)}</b>
Канал: <b>${escapeHtml(String(CFG.OFFICIAL_CHANNEL_USERNAME || CFG.OFFICIAL_CHANNEL_ID || ''))}</b>`;

  const kb = new InlineKeyboard();

  const canRequest = isOwner && (mode === 'manual' || mode === 'mixed');
  if (canRequest) {
    if (st === 'PENDING') {
      kb.text('⏳ В очереди (отменить)', `a:off_req_cancel|ws:${wsId}|o:${offerId}|p:${page}`).row();
    } else if (st !== 'ACTIVE') {
      kb.text('📝 В очередь публикаций', `a:off_req_home|ws:${wsId}|o:${offerId}|p:${page}`).row();
    }
  }

  if (isOwner && (mode === 'paid' || mode === 'mixed')) {
    kb.text('💳 Купить размещение', `a:off_buy_home|ws:${wsId}|o:${offerId}|p:${page}`).row();
  }
  const canPublishManual = isMod && (mode === 'manual' || mode === 'mixed');
  // Commit F: in paid mode allow publish only if there is a paid PENDING record
  const canPublishPaid = isMod && (mode === 'paid' || mode === 'mixed') && st === 'PENDING' && post?.payment_id;
  if (canPublishManual || canPublishPaid) {
    kb.text('✅ Опубликовать сейчас', `a:off_pub|ws:${wsId}|o:${offerId}|p:${page}`).row();
  }

  if (isMod && st === 'ACTIVE') {
    kb.text('♻️ Обновить пост', `a:off_upd|ws:${wsId}|o:${offerId}|p:${page}`).row();
  }

  if (isMod && (st === 'ACTIVE' || st === 'PENDING')) {
    kb.text('🗑 Снять', `a:off_rm|ws:${wsId}|o:${offerId}|p:${page}`).row();
  }

  kb.text('⬅️ Назад к офферу', `a:bx_pub|ws:${wsId}|o:${offerId}|p:${page}`);

  const send = ctx.callbackQuery ? ctx.editMessageText.bind(ctx) : ctx.reply.bind(ctx);
  await send(text, { parse_mode: 'HTML', reply_markup: kb });
}


async function renderOfficialRequestHome(ctx, userId, wsId, offerId, page = 0) {
  if (!CFG.OFFICIAL_PUBLISH_ENABLED) {
    if (ctx.callbackQuery) await ctx.answerCallbackQuery({ text: 'Официальный канал выключен.', show_alert: true });
    return;
  }
  const mode = String(CFG.OFFICIAL_PUBLISH_MODE || 'manual').toLowerCase();
  if (!(mode === 'manual' || mode === 'mixed')) {
    if (ctx.callbackQuery) await ctx.answerCallbackQuery({ text: 'Очередь доступна только в manual/mixed.', show_alert: true });
    return;
  }

  const offer = await db.getBarterOfferPublic(offerId);
  if (!offer) {
    if (ctx.callbackQuery) await ctx.answerCallbackQuery({ text: 'Оффер не найден.', show_alert: true });
    return;
  }

  const isOwner = Number(offer.owner_user_id) === Number(userId);
  const isMod = await isModerator({ id: userId }, ctx.from?.id);

  // Allow owner (or moderator) to create a PENDING draft.
  if (!isOwner && !isMod) {
    if (ctx.callbackQuery) await ctx.answerCallbackQuery({ text: 'Нет доступа.', show_alert: true });
    return;
  }

  const defaultDays = Math.max(1, Number(CFG.OFFICIAL_MANUAL_DEFAULT_DAYS || 3));

  const text = `📝 <b>Заявка в официальный канал</b>

Оффер: <b>#${offerId}</b>

Выбери длительность слота (можно потом продлить/перепостить):

• 1 день — быстрый тест
• 7 дней — нормальный слот
• 30 дней — “топ‑слот”`;

  const kb = new InlineKeyboard()
    .text(`🕒 1 день`, `a:off_req|ws:${wsId}|o:${offerId}|days:1|p:${page}`)
    .text(`📅 7 дней`, `a:off_req|ws:${wsId}|o:${offerId}|days:7|p:${page}`)
    .row()
    .text(`🏆 30 дней`, `a:off_req|ws:${wsId}|o:${offerId}|days:30|p:${page}`)
    .row()
    .text(`⚙️ По умолчанию (${defaultDays}д)`, `a:off_req|ws:${wsId}|o:${offerId}|days:${defaultDays}|p:${page}`)
    .row()
    .text('⬅️ Назад', `a:off_manage|ws:${wsId}|o:${offerId}|p:${page}`)
    .text('🏠 Меню', 'a:menu');

  if (ctx.callbackQuery) await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
  else await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb });
}



async function renderOfficialBuyHome(ctx, userId, wsId, offerId, page = 0) {
  const mode = String(CFG.OFFICIAL_PUBLISH_MODE || 'manual').toLowerCase();
  if (!(mode === 'paid' || mode === 'mixed')) {
    if (ctx.callbackQuery) await ctx.answerCallbackQuery({ text: 'Покупка размещения выключена.', show_alert: true });
    return;
  }

  const offer = await db.getBarterOfferPublic(offerId);
  if (!offer) {
    if (ctx.callbackQuery) await ctx.answerCallbackQuery({ text: 'Оффер не найден.', show_alert: true });
    return;
  }
  const isOwner = Number(offer.owner_user_id) === Number(userId);
  if (!isOwner) {
    if (ctx.callbackQuery) await ctx.answerCallbackQuery({ text: 'Только владелец канала может купить слот.', show_alert: true });
    return;
  }

  const text = `💳 <b>Размещение в официальном канале</b>

Оффер #${offerId}

Выбери срок слота:`;

  const kb = new InlineKeyboard();
  for (const d of OFFICIAL_DURATIONS) {
    kb.text(`⭐ ${d.label} · ${d.price} XTR`, `a:off_buy|ws:${wsId}|o:${offerId}|dur:${d.id}|p:${page}`).row();
  }
  kb.text('⬅️ Назад', `a:off_manage|ws:${wsId}|o:${offerId}|p:${page}`);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderOfficialQueue(ctx, userId, page = 0) {
  const isMod = await isModerator({ id: userId }, ctx.from?.id);
  if (!isMod) {
    if (ctx.callbackQuery) await ctx.answerCallbackQuery({ text: 'Нет доступа.', show_alert: true });
    return;
  }

  const limit = 8;
  const offset = page * limit;
  const rows = await safeOfficialPosts(() => db.listOfficialPending(limit, offset), async () => []);

  const text = `📣 <b>Офиц.канал: очередь</b>

Pending: <b>${rows.length}</b>${rows.length ? '' : '\n\nПока пусто.'}`;
  const kb = new InlineKeyboard();
  for (const r of rows) {
    const line = `#${r.offer_id} · ${escapeHtml(String(r.offer_title || '').slice(0, 35))}`;
    kb.text(line, `a:off_manage|ws:${r.workspace_id}|o:${r.offer_id}|p:0`).row();
  }
  const hasPrev = page > 0;
  const hasNext = rows.length >= limit;
  if (hasPrev) kb.text('⬅️', `a:off_queue|p:${page - 1}`);
  if (hasNext) kb.text('➡️', `a:off_queue|p:${page + 1}`);
  if (hasPrev || hasNext) kb.row();
  kb.text('⬅️ В админку', 'a:admin');

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}
async function renderBrandPaywall(ctx, userId, wsId, offerId, page = 0) {
  const cost = Math.max(1, Number(CFG.INTRO_COST_PER_INTRO || 1));
  const trialCredits = Math.max(0, Number(CFG.INTRO_TRIAL_CREDITS || 0));

  // Verification-aware daily limit
  let isVerified = false;
  if (CFG.VERIFICATION_ENABLED) {
    const v = await safeUserVerifications(() => db.getUserVerification(userId), async () => null);
    isVerified = String(v?.status || '').toUpperCase() === 'APPROVED';
  }
  const dailyLimit = Math.max(0, Number(isVerified ? CFG.INTRO_DAILY_LIMIT : CFG.INTRO_DAILY_LIMIT_UNVERIFIED));

  const meta = (await db.getBrandIntroMeta(userId)) || { brand_credits: 0, brand_trial_granted: false };
  const credits = Number(meta.brand_credits || 0);
  let usedToday = 0;
  try {
    usedToday = await db.getIntroDailyUsage(userId);
  } catch {
    usedToday = 0;
  }

  const retry = CFG.INTRO_RETRY_ENABLED ? await db.countAvailableBrandRetryCredits(userId) : 0;

  const trialLine = !meta.brand_trial_granted && trialCredits > 0
    ? `
🎁 Стартовый бонус: <b>${trialCredits}</b> кредит(ов) (1 раз, при первом интро).
`
    : '';

  const limitLine = dailyLimit > 0
    ? `
📆 Лимит интро в день: <b>${dailyLimit}</b> (сегодня использовано: <b>${usedToday}</b>).
`
    : '';

  const verifiedLimit = Math.max(0, Number(CFG.INTRO_DAILY_LIMIT || 0));
  const unverifiedLimit = Math.max(0, Number(CFG.INTRO_DAILY_LIMIT_UNVERIFIED || 0));
  const verifyHintLine = (CFG.VERIFICATION_ENABLED && !isVerified && verifiedLimit > unverifiedLimit)
    ? `

✅ Пройди <b>верификацию</b>, чтобы увеличить лимит до <b>${verifiedLimit}</b> интро/день.
`
    : '';

  const text = `🔒 <b>Brand Pass</b>

Чтобы <b>написать блогеру</b> и открыть новый диалог, нужен <b>${cost}</b> кредит(ов).
Переписка внутри открытого диалога — бесплатна.
👥 Команда бренда (менеджеры) открывается после покупки Brand Pass или Brand Plan.
${trialLine}${limitLine}${verifyHintLine}
Твой баланс: <b>${credits}</b> кредит(ов)
🎟 Retry credits: <b>${retry}</b>

Выбери пакет:`;

  const kb = new InlineKeyboard();
  if (CFG.VERIFICATION_ENABLED && !isVerified && verifiedLimit > unverifiedLimit) {
    kb.text('✅ Увеличить лимит (верификация)', 'a:verify_home').row();
  }
  for (const p of BRAND_PACKS) {
    const contacts = Math.max(1, Math.floor(Number(p.credits || 0) / Math.max(1, cost)));
    kb.text(`⭐ ${p.title} · ${contacts} контактов`, `a:brand_buy|ws:${wsId}|o:${offerId}|pack:${p.id}|p:${page}`).row();
  }
  kb.text('⭐️ Brand Plan', `a:brand_plan|ws:${wsId}`).text('🎯 Smart Matching', `a:match_home|ws:${wsId}`).row();
  kb.text('⬅️ Назад', `a:bx_pub|ws:${wsId}|o:${offerId}|p:${page}`);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderBxInbox(ctx, userId, wsId, page = 0, opts = {}) {

  const limit = CFG.BARTER_INBOX_PAGE_SIZE;
  const offset = page * limit;
  const rows = CFG.VERIFICATION_ENABLED
    ? await safeUserVerifications(() => db.listBarterThreadsForUserWithVerified(userId, limit, offset), () => db.listBarterThreadsForUser(userId, limit, offset))
    : await db.listBarterThreadsForUser(userId, limit, offset);

  let header = `📨 <b>Inbox</b>`;

  // Brand Manager: show current brand + quick switch прямо в Inbox
  if (opts?.bm?.enabled) {
    header += `\n\n<b>Бренд:</b> <b>${escapeHtml(opts.bm.brandLabel || '—')}</b>`;
  }

  header += `\n\nДиалоги по офферам (бренд ↔ блогер).`;

  const kb = new InlineKeyboard();

  if (opts?.bm?.enabled && (opts.bm.brands || []).length > 1) {
    kb.text('🔁 Сменить бренд', `a:bm_pick_brand|ret:bx_inbox|ws:${wsId}|p:${page}`).row();
  }

  for (const t of rows) {
    const other = t.other_username ? '@' + t.other_username : ('user #' + t.other_user_id);
    const v = t.other_verified ? ' ✅' : '';
    let stageEmoji = '';
    if (Number(t.buyer_user_id) === Number(userId) && t.buyer_stage) {
      const st = CRM_STAGES.find((s) => s.id === String(t.buyer_stage));
      stageEmoji = st ? String(st.title).trim().split(' ')[0] : '';
    }

    const prefix = stageEmoji ? `#${t.id} ${stageEmoji}` : `#${t.id}`;

    const st = computeThreadReplyStatus(t, userId, {
      retryEnabled: CFG.INTRO_RETRY_ENABLED,
      afterHours: CFG.INTRO_RETRY_AFTER_HOURS
    });
    const stLine = st.retry ? `${st.base} · ${st.retry}` : st.base;

    const line = `${prefix} · ${stLine} · ${escapeHtml(t.offer_title || 'оффер')} · ${escapeHtml(other)}${v}`;
    kb.text(line.slice(0, 60), `a:bx_thread|ws:${wsId}|t:${t.id}|p:${page}`).row();
  }

  const hasPrev = page > 0;
  const hasNext = rows.length >= limit; // heuristic
  const nav = bxInboxNavKb(wsId, page, hasPrev, hasNext);
  for (const row of nav.inline_keyboard) kb.inline_keyboard.push(row);

  await ctx.editMessageText(header + (rows.length ? '' : '\n\nПока нет диалогов.'), { parse_mode: 'HTML', reply_markup: kb });
}

async function buildBxThreadView(userId, threadId) {
  const thread = CFG.VERIFICATION_ENABLED
    ? await safeUserVerifications(() => db.getBarterThreadForUserWithVerified(threadId, userId), () => db.getBarterThreadForUser(threadId, userId))
    : await db.getBarterThreadForUser(threadId, userId);
  if (!thread) return null;

  // Proofs are optional (feature may be deployed later)
  let proofsCount = 0;
  try {
    proofsCount = await db.countBarterThreadProofs(threadId);
  } catch (e) {
    if (!isMissingRelationError(e, 'barter_thread_proofs')) throw e;
    proofsCount = 0;
  }
  const msgs = await db.listBarterMessages(threadId, 12);
  msgs.reverse();

  const isBuyer = Number(thread.buyer_user_id) === Number(userId);
  const otherUserId = isBuyer ? thread.seller_user_id : thread.buyer_user_id;
  const otherUsername = isBuyer ? thread.seller_username : thread.buyer_username;
  const otherVerified = isBuyer ? Boolean(thread.seller_verified) : Boolean(thread.buyer_verified);
  const other = otherUsername ? '@' + otherUsername : ('user #' + otherUserId);
  const otherMark = otherVerified ? ' ✅' : '';
  const status = String(thread.status || 'OPEN').toUpperCase();
  const stageTitle = thread.buyer_stage
    ? (CRM_STAGES.find((s) => s.id === String(thread.buyer_stage))?.title || String(thread.buyer_stage))
    : null;

const replySt = computeThreadReplyStatus(thread, userId, {
  retryEnabled: CFG.INTRO_RETRY_ENABLED,
  afterHours: CFG.INTRO_RETRY_AFTER_HOURS
});
const replyLine = `Reply: <b>${escapeHtml(replySt.base)}</b>`;
const retryLine = replySt.retry ? `Retry: <b>${escapeHtml(replySt.retry)}</b>` : null;

const chargeLine = isBuyer ? formatBxChargeLine(thread) : '';
const chargeHtml = chargeLine ? `${escapeHtml(chargeLine)}` : null;

  const headLines = [
    `💬 <b>Диалог #${thread.id}</b>`,
    `Оффер: <b>${escapeHtml(thread.offer_title || '—')}</b>`,
    `С кем: <b>${escapeHtml(other)}${otherMark}</b>`,
    `Статус: <b>${escapeHtml(status)}</b>`,
    stageTitle ? `CRM: <b>${escapeHtml(stageTitle)}</b>` : null,
    replyLine,
    retryLine,
    chargeHtml
  ].filter(Boolean);

  const head = headLines.join('\n');

  const body = msgs.length ? msgs.map(m => {
    const who = Number(m.sender_user_id) === Number(userId) ? 'Вы' : (m.tg_username ? '@' + m.tg_username : 'Собеседник');
    const ts = m.created_at ? fmtTs(m.created_at) : '';
    return `<b>${escapeHtml(who)}</b> <tg-spoiler>${escapeHtml(ts)}</tg-spoiler>
${escapeHtml(m.body)}`;
  }).join('\n\n') : 'Сообщений пока нет.';

  const text = `${head}

${body}`;
  return { thread, text, proofsCount };
}

async function renderBxThread(ctx, userId, wsId, threadId, opts = {}) {
  const built = await buildBxThreadView(userId, threadId);
  if (!built) return ctx.answerCallbackQuery({ text: 'Диалог не найден.' });
  const { thread, text, proofsCount } = built;

  let canStage = false;
  const curStage = thread.buyer_stage ? String(thread.buyer_stage) : null;
  if (Number(thread.buyer_user_id) === Number(userId)) {
    canStage = await db.isBrandPlanActive(userId);
  }


  const replySt = computeThreadReplyStatus(thread, userId, {
    retryEnabled: CFG.INTRO_RETRY_ENABLED,
    afterHours: CFG.INTRO_RETRY_AFTER_HOURS
  });
  const showRetryInfo = replySt.isBuyer && CFG.INTRO_RETRY_ENABLED && thread.buyer_first_msg_at && !thread.seller_first_reply_at;

  const kb = bxThreadKb(wsId, threadId, {
    ...opts,
    offerId: thread.offer_id,
    canStage,
    stage: curStage,
    proofsCount,
    showRetryInfo,
    retryText: replySt.retry || ''
  });
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

function bxProofsKb(wsId, threadId, opts = {}) {
  const back = opts.back || 'inbox';
  const page = Number(opts.page || 0);
  const offerId = opts.offerId ? Number(opts.offerId) : null;

  const cbTail = `${offerId ? `|o:${offerId}` : ''}|b:${back}|p:${page}`;
  return new InlineKeyboard()
    .text('➕ Ссылка', `a:bx_proof_link|ws:${wsId}|t:${threadId}${cbTail}`)
    .text('📎 Скрин', `a:bx_proof_photo|ws:${wsId}|t:${threadId}${cbTail}`)
    .row()
    .text('⬅️ Назад', `a:bx_thread|ws:${wsId}|t:${threadId}|p:${page}${offerId ? `|o:${offerId}` : ''}|b:${back}`);
}

async function renderBxProofs(ctx, userId, wsId, threadId, opts = {}) {
  const built = await buildBxThreadView(userId, threadId);
  if (!built) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  const offerId = built.thread.offer_id ? Number(built.thread.offer_id) : null;

  let proofs = [];
  try {
    proofs = await db.listBarterThreadProofs(threadId, 12);
  } catch (e) {
    if (!isMissingRelationError(e, 'barter_thread_proofs')) throw e;
    proofs = [];
  }

  const lines = proofs.map((p) => {
    const ts = p.created_at ? fmtTs(p.created_at) : '';
    if (String(p.kind) === 'LINK') {
      const url = String(p.url || '').trim();
      const shown = url.length > 120 ? (url.slice(0, 117) + '…') : url;
      return `🔗 <b>${escapeHtml(shown)}</b> <tg-spoiler>${escapeHtml(ts)}</tg-spoiler>`;
    }
    return `🖼 <b>Скрин</b> <tg-spoiler>${escapeHtml(ts)}</tg-spoiler>`;
  });

  const text =
`🧾 <b>Proofs</b>

Сюда можно добавить подтверждение, что пост опубликован:
• ссылка на пост (t.me/...)
• скрин (фото)

${lines.length ? lines.join('\n') : 'Пока пусто.'}`;

  await ctx.editMessageText(text, {
    parse_mode: 'HTML',
    reply_markup: bxProofsKb(wsId, threadId, { ...opts, offerId })
  });
  }

// -----------------------------
// Brand Mode tools: Brand Pass topup / Brand Plan / Matching / Featured
// -----------------------------

function brandPlanStatusText(planRow, active) {
  if (!active) return 'OFF';
  const name = String(planRow?.brand_plan || 'basic').toLowerCase();
  const until = planRow?.brand_plan_until ? fmtTs(planRow.brand_plan_until) : null;
  const label = name === 'max' ? 'Max' : 'Basic';
  return until ? `${label} (до ${until})` : label;
}

async function renderBrandPassTopup(ctx, userId, wsId) {
  const credits = await db.getBrandCredits(userId);
  const retry = CFG.INTRO_RETRY_ENABLED ? await db.countAvailableBrandRetryCredits(userId) : 0;
  const kb = new InlineKeyboard();
  for (const p of BRAND_PACKS) {
    kb.text(`💳 ${p.title} · ${p.credits} контакт(ов) · ${p.stars}⭐️`, `a:brand_buy|ws:${wsId}|pack:${p.id}`).row();
  }
  kb.text('⬅️ Назад', `a:bx_open|ws:${wsId}`);

  await ctx.editMessageText(
    `🎫 <b>Brand Pass</b>

Баланс контактов: <b>${credits}</b>
🎟 Retry credits: <b>${retry}</b>

Retry начисляется, если блогер не отвечает за 24ч (действует 7 дней).

👥 Команда бренда (менеджеры) открывается после покупки Brand Pass или Brand Plan.

Пополняй, чтобы открывать новые диалоги с микро-каналами.`,
    { parse_mode: 'HTML', reply_markup: kb }
  );
}

// Back-compat alias (some UI buttons still call renderBrandPass)
async function renderBrandPass(ctx, userId, wsId) {
  return renderBrandPassTopup(ctx, userId, wsId);
}

async function renderBrandPlan(ctx, userId, wsId) {
  const planRow = await db.getBrandPlan(userId);
  const active = await db.isBrandPlanActive(userId);
  const status = brandPlanStatusText(planRow, active);

  const kb = new InlineKeyboard();
  for (const pl of BRAND_PLANS) {
    kb.text(`⭐️ ${pl.id === 'max' ? 'Max' : 'Basic'} · ${pl.stars}⭐️/30д`, `a:brand_plan_buy|ws:${wsId}|plan:${pl.id}`).row();
  }
  kb.text('⬅️ Назад', `a:bx_open|ws:${wsId}`);

  await ctx.editMessageText(
    `⭐️ <b>Brand Plan</b>

Статус: <b>${escapeHtml(status)}</b>

Brand Plan даёт инструменты внутри Inbox (CRM-стадии) и быстрые действия.
Также открывает «Команда бренда» (добавление менеджеров).
Кредиты Brand Pass покупаются отдельно.`,
    { parse_mode: 'HTML', reply_markup: kb }
  );
}

async function renderMatchingHome(ctx, wsId) {
  const kb = new InlineKeyboard();
  for (const t of MATCH_TIERS) {
    kb.text(`🎯 ${t.title} · ${t.count} каналов · ${t.stars}⭐️`, `a:match_buy|ws:${wsId}|tier:${t.id}`).row();
  }
  kb.text('⬅️ Назад', `a:bx_open|ws:${wsId}`);

  await ctx.editMessageText(
    `🎯 <b>Smart Matching</b>

Платишь Stars за экономию времени: бот подберёт релевантные микро-каналы под твой бриф.

После оплаты отправь бриф текстом (ниша, гео, аудитория, формат).`,
    { parse_mode: 'HTML', reply_markup: kb }
  );
}

async function renderFeaturedHome(ctx, userId, wsId) {
  const kb = new InlineKeyboard();
  for (const d of FEATURED_DURATIONS) {
    kb.text(`🔥 ${d.title} · ${d.stars}⭐️`, `a:feat_buy|ws:${wsId}|dur:${d.id}`).row();
  }
  kb.text('⬅️ Назад', `a:bx_open|ws:${wsId}`);

  await ctx.editMessageText(
    `🔥 <b>Featured</b>

Подними внимание: твой блок появится сверху в ленте у всех (бренд + блогеры).

После оплаты отправь контент: 1 строка — заголовок, далее описание, последняя строка — контакт (@username / ссылка).`,
    { parse_mode: 'HTML', reply_markup: kb }
  );
}

async function renderFeaturedView(ctx, userId, wsId, id, page = 0) {
  const f = await db.getFeaturedPlacement(id);
  if (!f || String(f.status) !== 'ACTIVE') return ctx.answerCallbackQuery({ text: 'Featured не найден.' });

  const ends = f.ends_at ? fmtTs(f.ends_at) : '—';
  const title = f.title || 'Featured';
  const body = f.body || '';
  const contact = f.contact || '';

  const kb = new InlineKeyboard();
  if (Number(f.user_id) === Number(userId)) {
    kb.text('⛔ Остановить', `a:feat_stop|ws:${wsId}|id:${id}|p:${page}`).row();
  }
  kb.text('⬅️ Назад', `a:bx_feed|ws:${wsId}|p:${page}`);

  await ctx.editMessageText(
    `🔥 <b>${escapeHtml(String(title))}</b>

${escapeHtml(String(body))}

${contact ? `Контакт: <b>${escapeHtml(String(contact))}</b>
` : ''}До: <b>${escapeHtml(String(ends))}</b>`,
    { parse_mode: 'HTML', reply_markup: kb }
  );
}

// Giveaway status labels (RU + emoji)
function gwStatusLabel(status) {
  const st = String(status || '').toUpperCase();
  switch (st) {
    case 'ACTIVE':
      return '🟢 Идёт';
    case 'ENDED':
      return '🏁 Завершён';
    case 'DRAFT':
      return '📝 Черновик';
    case 'PAUSED':
      return '⏸ Пауза';
    case 'WINNERS_DRAWN':
      return '🎲 Победители выбраны';
    case 'RESULTS_PUBLISHED':
      return '🏆 Итоги опубликованы';
    case 'CANCELLED':
      return '⛔ Отменён';
    case 'PUBLISHED':
      return '📣 Опубликован';
    default:
      return st ? `ℹ️ ${st}` : '—';
  }
}

async function renderGwList(ctx, ownerUserId, wsId = null) {
  const items = await db.listGiveaways(ownerUserId, 25);
  const wsNum = (wsId === null || wsId === undefined) ? null : Number(wsId);
  // workspace_id can come from PG as a string (BIGINT), so compare by Number to avoid empty lists
  const filtered = wsNum ? items.filter(x => Number(x.workspace_id) === wsNum) : items;

  const activeWs = wsNum || (ctx?.from?.id ? await getActiveWorkspace(ctx.from.id) : null);
  const createCb = activeWs ? `a:gw_new|ws:${activeWs}` : 'a:gw_new_pick';

  const kb = new InlineKeyboard();
  kb.text('➕ Новый розыгрыш', createCb);
  if (!wsId) kb.text('📣 Выбрать канал', 'a:gw_new_pick');
  kb.row();

  if (!filtered.length) {
    kb.text('⬅️ Назад', wsId ? `a:ws_open|ws:${wsId}` : 'a:menu');
    await ctx.editMessageText(`🎁 Розыгрышей пока нет.

Жми «➕ Новый розыгрыш», чтобы создать первый.`, { reply_markup: kb });
    return;
  }

  for (const g of filtered) {
    const st = gwStatusLabel(g.status);
    const wsLabel = !wsId ? ` · ${String(g.workspace_title || '').slice(0, 18)}` : '';
    kb.text(`#${g.id} · ${st}${wsLabel}`, `a:gw_open|i:${g.id}`)
      .text('🗑', `a:gw_del_q|i:${g.id}|ws:${g.workspace_id}`)
      .row();
  }

  kb.text('⬅️ Назад', wsId ? `a:ws_open|ws:${wsId}` : 'a:menu');
  await ctx.editMessageText(
    `🎁 <b>${wsId ? 'Розыгрыши канала' : 'Розыгрыши'}</b>

Выбери розыгрыш (или создай новый):`,
    { parse_mode: 'HTML', reply_markup: kb }
  );
}

async function renderGwOpen(ctx, ownerUserId, gwId) {
  const g = await db.getGiveawayForOwner(gwId, ownerUserId);
  if (!g) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  const sponsors = await db.listGiveawaySponsors(gwId);
  const sponsorLines = sponsors.map(s => `• ${escapeHtml(s.sponsor_text)}`).join('\n') || '—';

  const checked = await getCurGwChecked(g.id);
  const notes = await getCurGwNotes(g.id, 3);

  const checkedLine = checked
    ? `✅ Проверено: <b>${escapeHtml(curatorLabelFromMeta(checked))}</b> · ${escapeHtml(fmtTs(checked.at))}`
    : '✅ Проверено: —';

  const notesBlock = curatorNotesBlock(notes);

  const text = `🎁 <b>Конкурс #${g.id}</b>

Статус: <b>${escapeHtml(gwStatusLabel(g.status))}</b>
Приз: <b>${escapeHtml(g.prize_value_text || '—')}</b>
Мест: <b>${g.winners_count}</b>
Дедлайн: <b>${g.ends_at ? escapeHtml(fmtTs(g.ends_at)) : '—'}</b>

Спонсоры:\n${sponsorLines}

👤 <b>Куратор</b>
${checkedLine}
${notesBlock}

Если ведёшь конкурс не один — пригласи помощника (👥 Кураторы канала → 👤 Пригласить куратора).`;
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: gwOpenKb(g, { isAdmin: isSuperAdminTg(ctx.from?.id) }) });
}

async function renderGwStats(ctx, ownerUserId, gwId) {
  const st = await db.getGiveawayStats(gwId, ownerUserId);
  if (!st) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  const total = Number(st.entries_total || 0);
  const elig = Number(st.eligible_count || 0);
  const notElig = Number(st.not_eligible_count || 0);
  const eligPct = total > 0 ? Math.round((elig / total) * 1000) / 10 : 0;
  const text =
`📊 <b>Статистика конкурса #${gwId}</b>

👥 Entries total: <b>${total}</b>
✅ Eligible: <b>${elig}</b>  (<b>${eligPct}%</b>)
⚠️ Not eligible: <b>${notElig}</b>

🕒 Last join: <b>${fmtTs(st.last_joined_at)}</b>
🔎 Last check: <b>${fmtTs(st.last_checked_at)}</b>

🔍 Transparency log: 🧾`;

  const kb = new InlineKeyboard()
    .text('✅ Готовность конкурса', `a:gw_preflight|i:${gwId}`)
    .row()
    .text('ℹ️ Почему не прошёл', `a:gw_why|i:${gwId}`)
    .row()
    .text('🧾 Transparency log', `a:gw_log|i:${gwId}`)
    .row()
    .text('📤 Экспорт всех', `a:gw_export|i:${gwId}|t:all`)
    .row()
    .text('📤 Экспорт eligible', `a:gw_export|i:${gwId}|t:eligible`)
    .row()
    .text('🏆 Экспорт winners', `a:gw_export|i:${gwId}|t:winners`)
    .row();

  if (isSuperAdminTg(ctx.from?.id)) kb.text('🧩 Проверка доступа', `a:gw_access|i:${gwId}`).row();

  kb
    .text('📣 Напомнить проверить', `a:gw_remind_q|i:${gwId}`)
    .row()
    .text('⬅️ Назад', `a:gw_open|i:${gwId}`);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderGwLog(ctx, ownerUserIdOrNull, gwId) {
  // both owner & participants can open log: show last audit rows
  const rows = await db.listGiveawayAudit(gwId, 30);
  const lines = rows.map(r => `• <b>${escapeHtml(r.action)}</b> — ${fmtTs(r.created_at)}`);
  const text = `🧾 <b>Лог конкурса #${gwId}</b>

${lines.length ? lines.join('\n') : 'Пока пусто.'}`;
  const back = ownerUserIdOrNull ? `a:gw_open|i:${gwId}` : `a:gw_open_public|i:${gwId}`;
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: new InlineKeyboard().text('⬅️ Назад', back) });
}

async function renderGwOpenPublic(ctx, gwId, userId) {
  const g = await db.getGiveawayInfoForUser(gwId);
  if (!g) return ctx.answerCallbackQuery({ text: 'Конкурс не найден.' });
  const entry = await db.getEntryStatus(gwId, userId);

  const sponsorRows = await db.listGiveawaySponsors(gwId);
  const sponsors = (sponsorRows || []).map(r => r.sponsor_text).filter(Boolean);

  const text = renderParticipantScreen(g, entry, { hint: true, sponsors });
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: participantKb(gwId, entry, { pub: true }) });
}

// ----------------------
// Curator cabinet (safe permissions)
// ----------------------

function wsLabelNice(w) {
  const title = String(w?.title || '').trim();
  const unameRaw = String(w?.channel_username || '').trim();
  const uname = unameRaw ? (unameRaw.startsWith('@') ? unameRaw : '@' + unameRaw) : '';
  if (title && uname) return `${title} ${uname}`.trim();
  if (title) return title;
  if (uname) return uname;
  return `Канал #${w?.id}`;
}

function curatorHomeKb(items, modeEnabled = false) {
  const kb = new InlineKeyboard();
  const label = modeEnabled ? '🧹 Режим куратора: ✅ ВКЛ' : '🧹 Режим куратора: ❌ ВЫКЛ';
  kb.text(label, `a:cur_mode_set|v:${modeEnabled ? 0 : 1}|ret:cur`).row();
  for (const w of items) {
    const on = !!w.curator_enabled;
    const label = `${on ? '✅' : '❌'} ${wsLabelNice(w)}`;
    kb.text(label, `a:cur_ws|ws:${w.id}`).row();
  }
  kb.text('⬅️ Назад', 'a:menu').row();
  return kb;
}

async function renderCuratorHome(ctx, userId) {
  const items = await db.listCuratorWorkspaces(userId);
  const modeEnabled = await getCuratorMode(ctx.from.id);
  const text = `👤 <b>Куратор</b>

Тут ты смотришь конкурсы чужих каналов, где тебя назначили куратором.

<b>Как пользоваться:</b>
• Жми на канал ниже → увидишь конкурсы.
• ✅ — доступ включён, можно работать.
• ❌ — владелец выключил куратора (попроси включить или выйди из канала).

<b>Что тебе доступно:</b> 📊 Статистика • 🧾 Лог • 📣 Напомнить проверить • ✅ Проверено • 📝 Заметки

🧹 <b>Режим куратора</b> — прячет лишнее меню (оставляет только кураторское).

${items.length ? 'Выбери канал:' : 'Пока тебя не назначили куратором ни в одном канале.'}`;
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: curatorHomeKb(items, modeEnabled) });
}

// Same as renderCuratorHome, but for /start (new message instead of edit)
async function replyCuratorHome(ctx, userId) {
  const items = await db.listCuratorWorkspaces(userId);
  const modeEnabled = await getCuratorMode(ctx.from.id);
  const text = `👤 <b>Куратор</b>

Тут ты смотришь конкурсы чужих каналов, где тебя назначили куратором.

<b>Как пользоваться:</b>
• Жми на канал ниже → увидишь конкурсы.
• ✅ — доступ включён, можно работать.
• ❌ — владелец выключил куратора (попроси включить или выйди из канала).

<b>Что тебе доступно:</b> 📊 Статистика • 🧾 Лог • 📣 Напомнить проверить • ✅ Проверено • 📝 Заметки

🧹 <b>Режим куратора</b> — прячет лишнее меню (оставляет только кураторское).

${items.length ? 'Выбери канал:' : 'Пока тебя не назначили куратором ни в одном канале.'}`;

  await ctx.reply(text, { parse_mode: 'HTML', reply_markup: curatorHomeKb(items, modeEnabled) });
}

function curatorWsKb(wsId, giveaways) {
  const kb = new InlineKeyboard();
  for (const g of giveaways) {
    kb.text(`🎁 #${g.id} · ${gwStatusLabel(g.status)}`, `a:cur_gw_open|ws:${wsId}|i:${g.id}`).row();
  }
  kb.text('❌ Выйти из канала', `a:cur_leave_q|ws:${wsId}`).row();
  kb.text('⬅️ Назад', 'a:cur_home').row();
  return kb;
}

async function renderCuratorWorkspace(ctx, userId, wsId) {
  const wsIdNum = Number(wsId);
  const ws = await db.getWorkspaceAny(wsIdNum);

  const wsTitle = ws ? wsLabelNice(ws) : `Канал #${wsIdNum}`;

  // If owner disabled curator mode — show info + allow leaving
  if (ws && !ws.curator_enabled) {
    const kb = new InlineKeyboard()
      .text('❌ Выйти из канала', `a:cur_leave_q|ws:${wsIdNum}`)
      .row()
      .text('⬅️ Назад', 'a:cur_home');
    const text = `👤 <b>Куратор</b> • ${escapeHtml(wsTitle)}

Режим куратора в этом канале выключен владельцем.

Если хочешь — выйди из канала (удалишь свою роль куратора).`;
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
    return;
  }

  const giveaways = await db.listGiveawaysForCurator(wsIdNum, userId, 30);

  
  const text = `👤 <b>Куратор</b> • ${escapeHtml(wsTitle)}

${giveaways.length ? 'Конкурсы:' : 'Пока нет конкурсов.'}

Если тебя назначили по ошибке или помощь больше не нужна — нажми “❌ Выйти из канала”.`;
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: curatorWsKb(wsIdNum, giveaways) });
}

function curatorGwKb(wsId, gwId) {
  return new InlineKeyboard()
    .text('📊 Статистика', `a:cur_gw_stats|ws:${wsId}|i:${gwId}`)
    .text('🧾 Лог', `a:cur_gw_log|ws:${wsId}|i:${gwId}`)
    .row()
    .text('✅ Проверено', `a:cur_gw_check_q|ws:${wsId}|i:${gwId}`)
    .text('📝 Заметки', `a:cur_gw_note_q|ws:${wsId}|i:${gwId}`)
    .row()
    .text('📣 Напомнить проверить', `a:cur_gw_remind_q|ws:${wsId}|i:${gwId}`)
    .row()
    .text('📩 Владельцу', `a:cur_gw_owner_q|ws:${wsId}|i:${gwId}`)
    .row()
    .text('⬅️ Назад', `a:cur_ws|ws:${wsId}`);
}

async function renderCuratorGiveawayOpen(ctx, userId, wsId, gwId) {
  const g = await db.getGiveawayForCurator(Number(gwId), userId);
  if (!g || Number(g.workspace_id) !== Number(wsId)) {
    return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  }

  const checked = await getCurGwChecked(g.id);
  const notes = await getCurGwNotes(g.id, 3);

  const checkedLine = checked
    ? `✅ Проверено: <b>${escapeHtml(curatorLabelFromMeta(checked))}</b> · ${escapeHtml(fmtTs(checked.at))}`
    : '✅ Проверено: —';

  const notesBlock = curatorNotesBlock(notes);

  const text = `🎁 <b>Конкурс #${g.id}</b>

Статус: <b>${escapeHtml(gwStatusLabel(g.status))}</b>
Приз: <b>${escapeHtml(g.prize_value_text || '—')}</b>
Мест: <b>${g.winners_count}</b>
Дедлайн: <b>${g.ends_at ? escapeHtml(fmtTs(g.ends_at)) : '—'}</b>

${checkedLine}
${notesBlock}

Режим: <b>Куратор</b> (безопасные права)`;
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: curatorGwKb(Number(wsId), Number(gwId)) });
}

async function renderCuratorGiveawayStats(ctx, userId, wsId, gwId) {
  const st = await db.getGiveawayStatsForCurator(Number(gwId), userId);
  if (!st) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const text = `📊 <b>Статистика конкурса #${gwId}</b>

Всего заявок: <b>${st.entries_total ?? 0}</b>
Прошли проверку: <b>${st.eligible_count ?? 0}</b>
Не прошли: <b>${st.not_eligible_count ?? 0}</b>
Последняя заявка: <b>${st.last_joined_at ? escapeHtml(fmtTs(st.last_joined_at)) : '—'}</b>
Последняя проверка: <b>${st.last_checked_at ? escapeHtml(fmtTs(st.last_checked_at)) : '—'}</b>`;

  const kb = new InlineKeyboard()
    .text('🧾 Лог', `a:cur_gw_log|ws:${wsId}|i:${gwId}`)
    .row()
    .text('⬅️ Назад', `a:cur_gw_open|ws:${wsId}|i:${gwId}`);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderCuratorGiveawayLog(ctx, userId, wsId, gwId) {
  const g = await db.getGiveawayForCurator(Number(gwId), userId);
  if (!g) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  const rows = await db.listGiveawayAudit(Number(gwId), 30);
  const lines = rows.map(r => `• <b>${escapeHtml(r.action)}</b> — ${fmtTs(r.created_at)}`);
  const text = `🧾 <b>Лог конкурса #${gwId}</b>

${lines.length ? lines.join('\n') : 'Пока пусто.'}`;
  const kb = new InlineKeyboard().text('⬅️ Назад', `a:cur_gw_open|ws:${wsId}|i:${gwId}`);
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderCuratorGiveawayRemindQ(ctx, userId, wsId, gwId) {
  const g = await db.getGiveawayForCurator(Number(gwId), userId);
  if (!g) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const text = `📣 <b>Напомнить проверить</b>

Бот отправит сообщение в канал конкурса, чтобы участники открыли бота и нажали <b>«Проверить»</b>.

Отправить сейчас?`;
  const kb = new InlineKeyboard()
    .text('✅ Отправить', `a:cur_gw_remind_send|ws:${wsId}|i:${gwId}`)
    .text('⬅️ Отмена', `a:cur_gw_open|ws:${wsId}|i:${gwId}`);
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderCuratorGiveawayRemindSend(ctx, userId, wsId, gwId) {
  const g = await db.getGiveawayForCurator(Number(gwId), userId);
  if (!g) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const chatId = g.published_chat_id ?? g.published_chat ?? g.channel_id ?? null;
  if (!chatId) {
    await ctx.answerCallbackQuery({ text: 'Не найден канал конкурса.' });
    return renderCuratorGiveawayOpen(ctx, userId, wsId, gwId);
  }

  // rate-limit: 1 remind per 10 minutes per giveaway
  const rlKey = k(['rl', 'gw_remind', String(gwId)]);
  const rl = await rateLimit(rlKey, { limit: 1, windowSec: 10 * 60 });
  if (!rl.allowed) {
    await ctx.answerCallbackQuery({ text: `⏳ Слишком часто. Подожди ${fmtWait(rl.resetSec || 60)}.` });
    return;
  }

  // Use URL button (works reliably inside channel posts and always opens the bot).
  const link = `https://t.me/${CFG.BOT_USERNAME}?start=gw_${g.id}`;
  const msg = `🔔 <b>Проверка участия</b>

Открой бота и нажми <b>«Проверить»</b>, чтобы подтвердить подписки.

🤖 Бот: ${escapeHtml(link)}`;
  const kb = { inline_keyboard: [[{ text: '🤖 Открыть бота', url: link }]] };

  try {
    await ctx.api.sendMessage(chatId, msg, { parse_mode: 'HTML', disable_web_page_preview: true, reply_markup: kb });
    await db.auditGiveaway(g.id, g.workspace_id, userId, 'gw.reminder_posted', { actor_role: 'curator' });
    await ctx.answerCallbackQuery({ text: '✅ Отправлено' });
  } catch (e) {
    await ctx.answerCallbackQuery({ text: 'Не удалось отправить в канал.' });
  }

  
await renderCuratorGiveawayOpen(ctx, userId, wsId, gwId);
}

async function renderCuratorGiveawayOwnerNotifyQ(ctx, userId, wsId, gwId) {
  const g = await db.getGiveawayForCurator(Number(gwId), userId);
  if (!g) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const checked = await getCurGwChecked(g.id);
  const notes = await getCurGwNotes(g.id, 3);
  const checkedLine = checked
    ? `✅ Проверено: <b>${escapeHtml(curatorLabelFromMeta(checked))}</b> · ${fmtTs(checked.at)}`
    : '✅ Проверено: —';

  const chTitle = g.ws_title || 'канал';
  const chUser = g.ws_username ? `@${g.ws_username}` : '';

  const msg = `📩 <b>Сообщение владельцу</b>

Отправлю владельцу короткий апдейт по конкурсу <b>#${g.id}</b>.

🏷 Канал: <b>${escapeHtml(chTitle)}</b>${chUser ? ` (${escapeHtml(chUser)})` : ''}
${checkedLine}
${curatorNotesBlock(notes)}

⏱ Лимит: 1 раз / 10 минут на конкурс.`;

  const kb = new InlineKeyboard()
    .text('📩 Отправить', `a:cur_gw_owner_send|ws:${wsId}|i:${gwId}`)
    .row()
    .text('❌ Отмена', `a:cur_gw_open|ws:${wsId}|i:${gwId}`);

  await ctx.editMessageText(msg, { parse_mode: 'HTML', disable_web_page_preview: true, reply_markup: kb });
}

async function renderCuratorGiveawayOwnerNotifySend(ctx, userId, wsId, gwId) {
  const g = await db.getGiveawayForCurator(Number(gwId), userId);
  if (!g) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  // rate-limit: 1 notify per 10 minutes per giveaway (protect owner from spam)
  const rlKey = k(['rl', 'cur_owner_notify', String(g.id), String(userId)]);
  const rl = await rateLimit(rlKey, { limit: 1, windowSec: 10 * 60 });
  if (!rl.allowed) {
    await ctx.answerCallbackQuery({ text: `⏳ Слишком часто. Подожди ${fmtWait(rl.resetSec || 60)}.` });
    return;
  }

  const ownerTgId = await db.getUserTgIdByUserId(g.owner_user_id);
  if (!ownerTgId) {
    await ctx.answerCallbackQuery({ text: 'Не найден владелец.' });
    return;
  }

  const checked = await getCurGwChecked(g.id);
  const notes = await getCurGwNotes(g.id, 3);
  const checkedLine = checked
    ? `✅ Проверено: <b>${escapeHtml(curatorLabelFromMeta(checked))}</b> · ${fmtTs(checked.at)}`
    : '✅ Проверено: —';

  const actor = ctx.from?.username ? `@${ctx.from.username}` : (ctx.from?.first_name ? ctx.from.first_name : 'куратор');
  const chTitle = g.ws_title || 'канал';
  const chUser = g.ws_username ? `@${g.ws_username}` : '';

  const link = `https://t.me/${CFG.BOT_USERNAME}?start=gwo_${g.id}`;
  const out = `📩 <b>Апдейт от куратора</b>

От: <b>${escapeHtml(actor)}</b>
Конкурс: <b>#${g.id}</b>
Канал: <b>${escapeHtml(chTitle)}</b>${chUser ? ` (${escapeHtml(chUser)})` : ''}
${checkedLine}
${curatorNotesBlock(notes)}

Открыть конкурс: ${escapeHtml(link)}`;

  const kb = new InlineKeyboard()
    .text('🎁 Открыть конкурс', `a:gw_open|i:${g.id}`)
    .row()
    .url('🤖 Открыть бота', link);

  try {
    await ctx.api.sendMessage(ownerTgId, out, { parse_mode: 'HTML', disable_web_page_preview: true, reply_markup: kb });
    await db.auditGiveaway(g.id, g.workspace_id, userId, 'curator.owner_notified', { actor_role: 'curator' });
    await ctx.answerCallbackQuery({ text: '📩 Отправлено' });
  } catch (e) {
    await ctx.answerCallbackQuery({ text: 'Не удалось отправить владельцу.' });
  }

  await renderCuratorGiveawayOpen(ctx, userId, wsId, gwId);
}


function formatChatRef(chat) {
  const s = String(chat);
  // For -100... channel ids we keep as-is; for @username we keep as-is.
  return s;
}

async function checkBotAccessCached(api, chat, botId, { forceRecheck = false } = {}) {
  const key = k(['acc2', chat]);
  if (forceRecheck) {
    try { await redis.del(key); } catch {}
  }

  const cached = await redis.get(key);
  if (cached) {
    try { return typeof cached === 'string' ? JSON.parse(cached) : cached; } catch {}
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

function accessLine(chat, a) {
  const ref = formatChatRef(chat);
  if (a.state === 'admin') return `✅ ${ref} — bot: <b>admin</b>`;
  if (a.state === 'member') return `🟦 ${ref} — bot: <b>member</b>`;
  return `❌ ${ref} — bot: <b>no access</b>`;
}

export async function renderGwPreflight(ctx, ownerUserId, gwId, { forceRecheck = false } = {}) {
  const g = await db.getGiveawayForOwner(gwId, ownerUserId);
  if (!g) {
    await ctx.editMessageText('Нет доступа.');
    return;
  }

  const botId = await ensureBotId(ctx);
  const botUsername = CFG.BOT_USERNAME || 'YourBotUsername';

  // Main chat where giveaway is/will be published
  const mainChat = g.published_chat_id ?? g.published_chat ?? g.channel_id ?? null;

  const sponsorsRaw = await db.listGiveawaySponsors(gwId);
  const sponsorChats = sponsorsRaw.map(s => sponsorToChatId(s.sponsor_text)).filter(Boolean);

  const chats = [...new Set([mainChat, ...sponsorChats].filter(Boolean).map((x) => String(x)))];

  let mainAcc = null;
  if (mainChat) mainAcc = await checkBotAccessCached(ctx.api, String(mainChat), botId, { forceRecheck });

  const results = [];
  for (const chat of sponsorChats.map(String)) {
    const a = await checkBotAccessCached(ctx.api, chat, botId, { forceRecheck });
    results.push({ chat, a });
  }

  const adminCount = results.filter(r => r.a.state === 'admin').length + (mainAcc?.state === 'admin' ? 1 : 0);
  const memberCount = results.filter(r => r.a.state === 'member').length + (mainAcc?.state === 'member' ? 1 : 0);
  const noCount = results.filter(r => r.a.state === 'no').length + (mainAcc?.state === 'no' ? 1 : 0);

  let verdict = '✅ <b>Готово к запуску</b>';
  let hint = `Можно публиковать — бот сможет проверять подписки.`;

  if (!mainChat) {
    verdict = '⚠️ <b>Не выбран канал конкурса</b>';
    hint = 'Сначала опубликуй конкурс в канал (или перепроверь, что бот подключён к workspace).';
  } else if (noCount > 0) {
    verdict = '❌ <b>Не готово: нет доступа</b>';
    hint = `Добавь бота @${escapeHtml(botUsername)} админом в каналы, где стоит ❌.`;
  } else if (memberCount > 0) {
    verdict = '⚠️ <b>Почти готово</b>';
    hint = `Лучше выдать боту @${escapeHtml(botUsername)} права <b>админа</b> в каналах (сейчас часть каналов — member).`;
  }

  const lines = [];
  lines.push(`<b>Канал конкурса</b>:`);
  lines.push(mainChat ? accessLine(String(mainChat), mainAcc) : '—');

  lines.push('');
  lines.push(`<b>Спонсоры</b>: ${sponsorChats.length ? '' : '—'}`);
  if (sponsorChats.length) {
    for (const r of results) lines.push(accessLine(r.chat, r.a));
  }

  const text =
`🧪 <b>Готовность конкурса #${gwId}</b>

${verdict}
${hint}

${lines.join('\n')}

<i>Зачем это:</i> чтобы бот мог подтвердить подписки участников, ему нужен доступ к каналам.`;

  const kb = new InlineKeyboard()
    .text('🔄 Перепроверить', `a:gw_preflight|i:${gwId}|r:1`)
    .row()
    .text('⬅️ Назад', `a:gw_stats|i:${gwId}`);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });

  try {
    await db.auditGiveaway(gwId, g.workspace_id, ownerUserId, 'gw.preflight_checked', {
      mainChat: mainChat ? String(mainChat) : null,
      sponsors: sponsorChats.map(String),
      adminCount, memberCount, noCount
    });
  } catch {}
}

export async function renderGwWhyMenu(ctx, ownerUserId, gwId) {
  const g = await db.getGiveawayForOwner(gwId, ownerUserId);
  if (!g) {
    await ctx.editMessageText('Нет доступа.');
    return;
  }

  const kb = new InlineKeyboard()
    .text('🔎 Ввести ID', `a:gw_why_enter|i:${gwId}`)
    .row()
    .text('📨 Переслать сообщение', `a:gw_why_forward|i:${gwId}`)
    .row()
    .text('⬅️ Назад', `a:gw_stats|i:${gwId}`);

  await ctx.editMessageText(
    `ℹ️ <b>Почему участник не прошёл</b>\n\nВыбери режим:\n• <b>Ввести ID</b> — быстро и надёжно.\n• <b>Переслать сообщение</b> — сработает только если у участника выключена “Forward privacy”.`,
    { parse_mode: 'HTML', reply_markup: kb }
  );
}

async function clearEligibilityCacheForGw(gwId, userTgId) {
  let mainChat = null;
  try {
    const g = await db.getGiveawayInfoForUser(gwId);
    mainChat = g?.published_chat_id ?? g?.published_chat ?? g?.channel_id ?? null;
  } catch {}
  const sponsors = await db.listGiveawaySponsors(gwId);
  const sponsorChats = sponsors.map(s => sponsorToChatId(s.sponsor_text)).filter(Boolean);

  const chats = [...new Set([mainChat, ...sponsorChats].filter(Boolean).map((x) => String(x)))];
  for (const chat of chats) {
    try { await redis.del(k(['cm', chat, userTgId])); } catch {}
  }
}

function buildWhyText({ gwId, targetUserId, check }) {
  const who = `<a href="tg://user?id=${Number(targetUserId)}">id:${Number(targetUserId)}</a>`;
  const ok = check.isEligible ? '✅ <b>Eligible</b>' : (check.unknown ? '❔ <b>Не могу проверить полностью</b>' : '⚠️ <b>Not eligible</b>');

  const lines = (check.results || []).map(r => {
    const ref = formatChatRef(r.chat);
    if (r.status === 'ok') return `✅ ${ref} — подписка OK`;
    if (r.status === 'no') return `❌ ${ref} — <b>нет подписки</b>`;
    return `❔ ${ref} — <b>не могу проверить</b> (нет доступа/приватный канал)`;
  });

  let help = 'Если участник подписался только что — пусть нажмёт “Проверить” заново.';
  if (check.unknown) help = 'Есть ❔: обычно это значит, что бот не админ в одном из каналов или канал приватный.';
  if (!check.isEligible && !check.unknown) help = 'Есть ❌: участник не подписан на один из каналов.';

  const text =
`ℹ️ <b>Почему не прошёл</b> · конкурс #${gwId}

Участник: ${who}
Результат: ${ok}

${lines.length ? lines.join('\n') : 'Нет каналов для проверки.'}

<i>${help}</i>`;
  return text;
}

export async function renderGwWhyResult(ctx, ownerUserId, gwId, targetUserId, { forceRecheck = false } = {}) {
  const g = await db.getGiveawayForOwner(gwId, ownerUserId);
  if (!g) {
    await ctx.editMessageText('Нет доступа.');
    return;
  }

  if (forceRecheck) await clearEligibilityCacheForGw(gwId, targetUserId);

  const check = await doEligibilityCheck(ctx, gwId, targetUserId);
  const text = buildWhyText({ gwId, targetUserId, check });

  const kb = new InlineKeyboard()
    .text('🔄 Проверить ещё раз', `a:gw_why_recheck|i:${gwId}|tu:${Number(targetUserId)}`)
    .row()
    .text('🔎 Проверить другого', `a:gw_why_enter|i:${gwId}`)
    .row()
    .text('⬅️ Назад', `a:gw_stats|i:${gwId}`);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

export async function sendGwWhyResult(ctx, ownerUserId, gwId, targetUserId, { forceRecheck = false } = {}) {
  const g = await db.getGiveawayForOwner(gwId, ownerUserId);
  if (!g) {
    await ctx.reply('Нет доступа.');
    return;
  }

  if (forceRecheck) await clearEligibilityCacheForGw(gwId, targetUserId);

  const check = await doEligibilityCheck(ctx, gwId, targetUserId);
  const text = buildWhyText({ gwId, targetUserId, check });

  const kb = new InlineKeyboard()
    .text('🔄 Проверить ещё раз', `a:gw_why_recheck|i:${gwId}|tu:${Number(targetUserId)}`)
    .row()
    .text('🔎 Проверить другого', `a:gw_why_enter|i:${gwId}`)
    .row()
    .text('⬅️ Назад', `a:gw_stats|i:${gwId}`);

  await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
}


async function ensureBotId(ctx) {
  if (CFG.BOT_ID) return CFG.BOT_ID;
  const me = await ctx.api.getMe();
  return me.id;
}

async function getChatMemberStateCached(ctx, chat, userTgId) {
  const cacheKey = k(['cm', chat, userTgId]);
  try {
    const cached = await redis.get(cacheKey);
    if (cached) return String(cached);
  } catch {}

  let state = 'unknown';
  try {
    const cm = await ctx.api.getChatMember(chat, userTgId);
    const st = String(cm?.status || '');
    const ok = (st === 'member' || st === 'administrator' || st === 'creator' || st === 'restricted');
    state = ok ? 'ok' : 'no';
  } catch {
    state = 'unknown';
  }

  // IMPORTANT UX: do NOT cache negative too long, otherwise user subscribes but still sees ❌ for minutes.
  const ex = state === 'ok' ? 10 * 60 : 10;
  try {
    await redis.set(cacheKey, state, { ex });
  } catch {}
  return state;
}

async function doEligibilityCheck(ctx, gwId, userTgId) {
  // One check at a time per user+giveaway (avoid double-taps and Telegram retry storms)
  const resKey = k(['gw_check', gwId, userTgId]);
  const lockKey = k(['lock', 'gw_check', gwId, userTgId]);

  try {
    const cached = await redis.get(resKey);
    if (cached) return cached;
  } catch {}

  let lock = null;
  try {
    lock = await redis.set(lockKey, '1', { nx: true, ex: 15 });
  } catch {
    lock = null;
  }

  if (!lock) {
    // If a check is already running, return cached result if we have it; otherwise return a small "busy" payload.
    try {
      const cached = await redis.get(resKey);
      if (cached) return cached;
    } catch {}
    return { isEligible: false, unknown: true, results: [], firstBlocker: null, firstBlockerHandle: null, sponsors: [], busy: true };
  }

  try {
    // Always check the main giveaway channel (where the post is published), plus optional sponsor channels.
    let mainChat = null;
    try {
      const g = await db.getGiveawayInfoForUser(gwId);
      mainChat = g?.published_chat_id ?? g?.published_chat ?? g?.channel_id ?? null;
    } catch {}

    const sponsorRows = await db.listGiveawaySponsors(gwId);
    const sponsors = normalizeSponsorsList((sponsorRows || []).map((s) => s?.sponsor_text ?? s?.sponsorText ?? s));

    const sponsorChats = [];
    const chatToHandle = new Map();
    for (const s of sponsors) {
      const chat = sponsorToChatId(s);
      if (!chat) continue;
      sponsorChats.push(chat);
      // Prefer @handle for UI when possible
      chatToHandle.set(String(chat), (String(chat).startsWith('@') ? String(chat) : (String(s).startsWith('@') ? String(s) : null)));
    }

    const chats = [...new Set([mainChat, ...sponsorChats].filter(Boolean).map((x) => String(x)))];

    const results = [];
    let unknown = false;
    let firstBlocker = null;

    const checkChat = async (chat) => {
      const state = await getChatMemberStateCached(ctx, chat, userTgId);
      const handle = chatToHandle.get(String(chat)) || (String(chat).startsWith('@') ? String(chat) : null);
      return { chat, state, handle };
    };

    // Parallelize for small lists (feels snappier), and keep fail-fast for larger ones.
    if (chats.length <= 9) {  // ≤8 sponsors (+ main)
      const arr = await Promise.all(chats.map(checkChat));
      for (const r of arr) results.push({ chat: r.chat, state: r.state, handle: r.handle });
      const bad = arr.find((r) => r.state !== 'ok');
      if (bad) {
        firstBlocker = { chat: bad.chat, state: bad.state };
        if (bad.state === 'unknown') unknown = true;
      }
    } else {
      for (const chat of chats) {
        const r = await checkChat(chat);
        results.push({ chat: r.chat, state: r.state, handle: r.handle });
        if (r.state !== 'ok') {
          if (r.state === 'unknown') unknown = true;
          firstBlocker = { chat: r.chat, state: r.state };
          break;
        }
      }
    }

    const isEligible = results.length === chats.length && results.every((r) => r.state === 'ok') && !unknown;
    const firstBlockerHandle = firstBlocker ? (chatToHandle.get(String(firstBlocker.chat)) || (String(firstBlocker.chat).startsWith('@') ? String(firstBlocker.chat) : null)) : null;

    const payload = { isEligible, unknown, results, firstBlocker, firstBlockerHandle, sponsors };
    try {
      // Cache OK for longer; cache NO/UNKNOWN briefly (so subscribing updates quickly).
      const ttl = isEligible ? 60 : 10;
      await redis.set(resKey, payload, { ex: ttl });
    } catch {}

    return payload;
  } finally {
    try { await redis.del(lockKey); } catch {}
  }
}

async function renderSetupInstructions(ctx) {
  const text =
`🚀 <b>Подключение канала</b>

1) Добавь бота админом в свой канал.
2) Перешли сюда любой пост из канала (forward).

Бот создаст workspace и ты сможешь запускать конкурсы.`;
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: new InlineKeyboard().text('⬅️ В меню', 'a:menu') });
}

export function getBot() {
  if (BOT) return BOT;
  assertEnv();
  const bot = new Bot(CFG.BOT_TOKEN);

  // Never log ctx/api/token. Log only safe identifiers.
  bot.catch((err) => {
    const ctx = err?.ctx;
    console.error('[BOT] error', {
      update_id: ctx?.update?.update_id ?? null,
      chat_id: ctx?.chat?.id ?? null,
      from_id: ctx?.from?.id ?? null,
      message: String(err?.error?.message || err?.message || err?.error || err),
      name: err?.error?.name || err?.name || 'Error',
    });
  });

  // --- TEXT INPUT router (expectText) ---

  // Setup channel expects a forwarded post (any message type). We handle it on `message`
  // so that photo/video-only forwards also work.
  bot.on('message', async (ctx, next) => {
    // Some update variants may not have ctx.from (e.g., anonymous/channel-sent messages).
    // In that case we must not touch Redis expectText state.
    if (!ctx.from) return next();
    const exp = await getExpectText(ctx.from.id);
    if (!exp || String(exp.type) !== 'setup_forward') return next();

    // If user sends a command while we ожидали форвард — не блокируем команду.
    const txt = String(ctx.message?.text || '');
    const isCommand = txt.startsWith('/') &&
      Array.isArray(ctx.message?.entities) &&
      ctx.message.entities.some((e) => e.type === 'bot_command' && e.offset === 0);
    if (isCommand) {
      await clearExpectText(ctx.from.id);
      return next();
    }

    await clearExpectText(ctx.from.id);

    const f = ctx.message.forward_from_chat || ctx.message.sender_chat;
    if (!f || !f.id) {
      await ctx.reply('Не вижу пересланный пост из канала. Перешли сюда пост именно из канала 🙏');
      await setExpectText(ctx.from.id, exp);
      return;
    }

    const u = await db.upsertUser(ctx.from.id, ctx.from.username ?? null);

    const title = f.title || 'Channel';
    const channelUsername = f.username || null;

    const ws = await db.createWorkspace({ ownerUserId: u.id, title, channelId: f.id, channelUsername });
    await db.ensureWorkspaceSettings(ws.id);

    db.trackEvent('ws_created', { userId: u.id, wsId: ws.id, meta: { channelId: f.id, channelUsername } });
    await db.auditWorkspace(ws.id, u.id, 'ws.created', { title, channelId: f.id, channelUsername });

    await setActiveWorkspace(ctx.from.id, ws.id);

    await ctx.reply(`✅ Канал подключен: <b>${escapeHtml(channelUsername ? '@' + channelUsername : title)}</b>`, {
      parse_mode: 'HTML',
      reply_markup: wsMenuKb(ws.id),
    });
  });



  // --- Why-not-eligible helper: expects a forwarded message from a participant (optional) ---
  bot.on('message', async (ctx, next) => {
    const exp = await getExpectText(ctx.from.id);
    if (!exp || String(exp.type) !== 'gw_why_forward') return next();

    const txt = String(ctx.message?.text || '');
    const isCommand = txt.startsWith('/') &&
      Array.isArray(ctx.message?.entities) &&
      ctx.message.entities.some((e) => e.type === 'bot_command' && e.offset === 0);
    if (isCommand) {
      await clearExpectText(ctx.from.id);
      return next();
    }

    const targetId = ctx.message?.forward_from?.id;
    if (!targetId) {
      await ctx.reply('Не вижу user_id в форварде (возможно у участника включена Forward privacy). Используй кнопку “Ввести ID” и пришли user_id цифрами.');
      await setExpectText(ctx.from.id, exp);
      return;
    }

    await clearExpectText(ctx.from.id);

    const u = await db.upsertUser(ctx.from.id, ctx.from.username ?? null);
    await sendGwWhyResult(ctx, u.id, Number(exp.gwId), Number(targetId), { forceRecheck: true });
  });



  
  // Support: accept text OR media (photo/screenshot) as one message.
  // Text is handled in message:text router; media is handled here.
  bot.on('message', async (ctx, next) => {
    if (!ctx.from) return next();

    const exp = await getExpectText(ctx.from.id);
    if (!exp || String(exp.type) !== 'support_any') return next();

    // Let text messages be handled by message:text router below
    if (ctx.message?.text) return next();

    // If user sends a command-like caption while we ожидали support — don't block commands
    const cap = String(ctx.message?.caption || '');
    const capIsCommand = cap.startsWith('/') &&
      Array.isArray(ctx.message?.caption_entities) &&
      ctx.message.caption_entities.some((e) => e.type === 'bot_command' && e.offset === 0);
    if (capIsCommand) {
      await clearExpectText(ctx.from.id);
      return next();
    }

    const hasPhoto = Array.isArray(ctx.message?.photo) && ctx.message.photo.length > 0;
    const hasDoc = !!ctx.message?.document;
    const hasVideo = !!ctx.message?.video;

    // We accept photo/document/video as "support media"
    if (!hasPhoto && !hasDoc && !hasVideo) {
      const backCb = expectBackCb(exp);
      await ctx.reply('Можно отправить текст или фото/скрин (лучше с подписью). Стикеры/голос не подойдут 🙏', {
        reply_markup: navKb(backCb),
      });
      // Keep ожидание ввода активным
      try { await setExpectText(ctx.from.id, exp); } catch {}
      return;
    }

    await clearExpectText(ctx.from.id);

    const u = await db.upsertUser(ctx.from.id, ctx.from.username ?? null);

    // Rate-limit: allow up to 2 support messages per 5 minutes per user (text+media)
    const rlKey = k(['rl', 'support_any', String(u.id)]);
    const rl = await rateLimit(rlKey, { limit: 2, windowSec: 5 * 60 });
    if (!rl.allowed) {
      const backCb = expectBackCb(exp);
      await ctx.reply(`⏳ Слишком часто. Подожди ${fmtWait(rl.resetSec || 60)}.`, { reply_markup: navKb(backCb) });
      return;
    }

    const admins = Array.isArray(CFG.SUPER_ADMIN_TG_IDS) ? CFG.SUPER_ADMIN_TG_IDS : [];
    if (!admins.length) {
      const backCb = expectBackCb(exp);
      await ctx.reply('⚠️ Поддержка не настроена. Напиши владельцу бота.', { reply_markup: navKb(backCb) });
      return;
    }

    const mode = await resolveUiMode(ctx.from.id);
    const modeHuman = uiModeHuman(mode);
    const uname = ctx.from?.username ? `@${ctx.from.username}` : '—';
    const fullName = [ctx.from?.first_name, ctx.from?.last_name].filter(Boolean).join(' ').trim() || '—';

    // best-effort detect manager state
    let bmEnabled = false;
    let bmBrand = '';
    try {
      const bm = await resolveBmBrandContext(ctx, u, { requirePickWhenMissingActive: false });
      bmEnabled = !!bm.enabled;
      bmBrand = bm.brandLabel ? String(bm.brandLabel) : '';
    } catch {}

    const kind = hasPhoto ? 'photo' : (hasDoc ? 'document' : 'video');
    const caption = String(ctx.message?.caption || '').trim();
    const safeCap = caption.length > 800 ? (caption.slice(0, 800) + '…') : caption;

    const header = `💬 <b>Support</b> (media)

` +
      `От: <b>${escapeHtml(fullName)}</b> (${escapeHtml(uname)})
` +
      `TG ID: <code>${ctx.from.id}</code>
` +
      `User ID: <code>${u.id}</code>
` +
      `Mode: <b>${escapeHtml(modeHuman)}</b>${bmEnabled ? ' · <b>Brand Manager</b>' : ''}${bmBrand ? `
Brand: <b>${escapeHtml(bmBrand)}</b>` : ''}
` +
      `Type: <code>${escapeHtml(kind)}</code>
` +
      `Time: <code>${new Date().toISOString()}</code>
` +
      (safeCap ? `
<b>Caption:</b>
${escapeHtml(safeCap)}
` : '');

    let sent = 0;
    for (const a of admins) {
      const adminId = Number(a || 0);
      if (!adminId || adminId == ctx.from.id) continue;
      try {
        // Send header first
        await ctx.api.sendMessage(adminId, header, { parse_mode: 'HTML', disable_web_page_preview: true });

        // Copy original media message (preserves attachment)
        try {
          await ctx.api.copyMessage(adminId, ctx.chat.id, ctx.message.message_id);
        } catch {
          // Fallback: forwardMessage if copyMessage fails
          try { await ctx.api.forwardMessage(adminId, ctx.chat.id, ctx.message.message_id); } catch {}
        }

        sent += 1;
      } catch {}
    }

    const backCb = expectBackCb(exp);
    if (sent > 0) {
      await ctx.reply('✅ Отправлено в поддержку. Если нужно — нажми «💬 Поддержка» и отправь уточнение (текстом).', {
        reply_markup: navKb(backCb),
      });
    } else {
      await ctx.reply('⚠️ Не удалось отправить в поддержку. Попробуй позже или напиши владельцу.', {
        reply_markup: navKb(backCb),
      });
    }
    return;
  });

// Generic non-text guard for expectText steps
  // If we are waiting for a text input and user sends sticker/photo/voice/etc,
  // respond with a helpful hint + navigation buttons (Back/Menu), instead of a dead-end text.
  bot.on('message', async (ctx, next) => {
    if (!ctx.from) return next();

    const exp = await getExpectText(ctx.from.id);
    if (!exp) return next();
    // Text messages are handled by message:text router below
    if (ctx.message?.text) return next();
    const t = String(exp.type || '');
    // Some expectText steps actually expect media/forwarded messages — do not intercept those.
    if (t === 'setup_forward' || t === 'gw_why_forward') return next();
    if (t.endsWith('_photo') || t.endsWith('_gif') || t.endsWith('_video')) return next();

    const backCb = expectBackCb(exp);
    await ctx.reply('Я жду текст одним сообщением. Пожалуйста, напиши текст (не голос/стикер/фото).', {
      reply_markup: navKb(backCb),
    });
    // Keep ожидание ввода активным (обновим TTL на всякий случай)
    try { await setExpectText(ctx.from.id, exp); } catch {}
  });
  bot.on('message:text', async (ctx, next) => {
    if (!ctx.from) return next();
    const text = String(ctx.message?.text || '');
    const isCommand = text.startsWith('/') &&
      Array.isArray(ctx.message?.entities) &&
      ctx.message.entities.some((e) => e.type === 'bot_command' && e.offset === 0);

    const exp = await getExpectText(ctx.from.id);
if (!exp) {
  if (isCommand) return next(); // allow commands like /start to reach bot.command()
  const flags = await getRoleFlags(null, ctx.from.id);
  await renderMainMenu(ctx, flags, { edit: false });
  return;
}

    // If user sends a command while мы ждали ввод — не блокируем команду.
    if (isCommand) {
      await clearExpectText(ctx.from.id);
      return next();
    }

    const u = await db.upsertUser(ctx.from.id, ctx.from.username ?? null);
    const tgId = Number(ctx.from.id);
    await clearExpectText(ctx.from.id);

// Default navigation keyboard for any "text input" step.
// Prevents "what next?" dead-ends when we ask user to type something.
const backCb = expectBackCb(exp);
const _reply = ctx.reply.bind(ctx);
ctx.reply = (text, extra) => {
  const opts = extra ? { ...extra } : {};
  if (!opts.reply_markup) opts.reply_markup = navKb(backCb);
  return _reply(text, opts);
};



// Support message (send to SUPER_ADMIN_TG_IDS)
    if (exp.type === 'support_any') {
      const txt = String(ctx.message?.text || '').trim();
      if (!txt) {
        await ctx.reply('Напиши текст одним сообщением.');
        try { await setExpectText(ctx.from.id, exp); } catch {}
        return;
      }

      // Rate-limit: 1 support message per 5 minutes per user
      const rlKey = k(['rl', 'support_any', String(u.id)]);
      const rl = await rateLimit(rlKey, { limit: 1, windowSec: 5 * 60 });
      if (!rl.allowed) {
        await ctx.reply(`⏳ Слишком часто. Подожди ${fmtWait(rl.resetSec || 60)}.`);
        return;
      }

      const admins = Array.isArray(CFG.SUPER_ADMIN_TG_IDS) ? CFG.SUPER_ADMIN_TG_IDS : [];
      if (!admins.length) {
        await ctx.reply('⚠️ Поддержка не настроена. Напиши владельцу бота.');
        return;
      }

      const mode = await resolveUiMode(ctx.from.id);
      const modeHuman = uiModeHuman(mode);
      const uname = ctx.from?.username ? `@${ctx.from.username}` : '—';
      const fullName = [ctx.from?.first_name, ctx.from?.last_name].filter(Boolean).join(' ').trim() || '—';

      // best-effort detect manager state
      let bmEnabled = false;
      let bmBrand = '';
      try {
        const bm = await resolveBmBrandContext(ctx, u, { requirePickWhenMissingActive: false });
        bmEnabled = !!bm.enabled;
        bmBrand = bm.brandLabel ? String(bm.brandLabel) : '';
      } catch {}

      const safe = txt.length > 3500 ? (txt.slice(0, 3500) + '…') : txt;

      const header = `💬 <b>Support</b>

` +
        `От: <b>${escapeHtml(fullName)}</b> (${escapeHtml(uname)})
` +
        `TG ID: <code>${ctx.from.id}</code>
` +
        `User ID: <code>${u.id}</code>
` +
        `Mode: <b>${escapeHtml(modeHuman)}</b>${bmEnabled ? ' · <b>Brand Manager</b>' : ''}${bmBrand ? `
Brand: <b>${escapeHtml(bmBrand)}</b>` : ''}
` +
        `Time: <code>${new Date().toISOString()}</code>

` +
        `<b>Сообщение:</b>
${escapeHtml(safe)}`;

      let sent = 0;
      for (const a of admins) {
        const adminId = Number(a || 0);
        if (!adminId || adminId == ctx.from.id) continue;
        try {
          await ctx.api.sendMessage(adminId, header, { parse_mode: 'HTML', disable_web_page_preview: true });
          sent += 1;
        } catch {}
      }

      if (sent > 0) {
        await ctx.reply('✅ Отправлено в поддержку. Если нужно — нажми «💬 Поддержка» → «✍️ Написать в поддержку» и отправь уточнение.');
      } else {
        await ctx.reply('⚠️ Не удалось отправить в поддержку. Попробуй позже или напиши владельцу.');
      }
      return;
    }

// Add curator by username
    if (exp.type === 'curator_username') {
      const txt = String(ctx.message.text || '').trim();
      const m = txt.match(/^@?([a-zA-Z0-9_]{5,})$/);
      if (!m) {
        await ctx.reply('Введи @username (пример: @zarinka)');
        return;
      }
      const username = m[1];
      const curator = await db.findUserByUsername(username);
      if (!curator) {
        await ctx.reply(`⚠️ Эта функция требует, чтобы пользователь уже запускал бота.
Попроси его открыть бота и нажать /start, потом повтори добавление.`);
        return;
      }
      await db.addCurator(exp.wsId, curator.id, u.id);
      const ws = await db.getWorkspaceAny(Number(exp.wsId));
      const wsTitle = ws ? wsLabelNice(ws) : `Канал #${exp.wsId}`;
      await ctx.reply(`✅ Куратор @${username} добавлен.

Включи 👤 Куратор: ВКЛ, если хочешь чтобы он мог помогать с конкурсами (статы/лог/напоминания).`);

      // best-effort notify curator in DM
      try {
        const kb = new InlineKeyboard()
          .text('👤 Открыть кабинет куратора', 'a:cur_home')
          .row()
          .text('🧹 Включить режим куратора', `a:cur_mode_set|v:1|ret:cur`)
          .row()
          .text('🏠 Главное меню', 'a:menu');

        await ctx.api.sendMessage(
          Number(curator.tg_id),
          `✅ Тебя назначили <b>куратором</b> для: <b>${escapeHtml(wsTitle)}</b>.

Открой кабинет куратора — там будут каналы и конкурсы, где нужна твоя помощь.`,
          { parse_mode: 'HTML', reply_markup: kb }
        );
      } catch {}
      return;
    }

    // Add brand manager by username (Brand Team)
    if (exp.type === 'bm_username') {
      const txt = String(ctx.message.text || '').trim();
      const m = txt.match(/^@?([a-zA-Z0-9_]{5,})$/);
      if (!m) {
        await ctx.reply('Введи @username (пример: @manager)');
        return;
      }
      const username = m[1];

      // Brand Team access guard (owner-only + unlock)
      const bm = await resolveBmBrandContext(ctx, u, { requirePickWhenMissingActive: false });
      if (bm.dbMissing) {
        await ctx.reply('⚠️ Не найдена таблица brand_managers. Примените миграцию 026_brand_managers.sql в Neon.');
        return;
      }
      if (bm.enabled && bm.brandUserId !== u.id) {
        await ctx.reply('⛔️ Недостаточно прав. Добавлять менеджеров может только владелец бренда.');
        return;
      }

      const st = await getBrandTeamGateState(u.id);
      if (!st.ok) {
        const miss = st.missingBasic && st.missingBasic.length ? ` (не хватает: ${st.missingBasic.join(', ')})` : '';
        const profileLine = `• Профиль: ${st.basicDone || 0}/4${miss}`;
        const payLine = `• Покупка: ${st.paidOk ? '✅' : '❌'} (Brand Pass / Brand Plan)`;

        await ctx.reply(
          `👥 <b>Команда бренда</b>

` +
          `Раздел доступен после заполнения профиля бренда и покупки Brand Pass/Plan.

` +
          `${escapeHtml(profileLine)}
${escapeHtml(payLine)}

` +
          `Открой профиль, заполни базовые поля и оформи Brand Pass или Brand Plan — после этого сможешь добавлять менеджеров.`,
          { parse_mode: 'HTML', reply_markup: brandTeamLockedKb() }
        );
        return;
      }

      const manager = await db.findUserByUsername(username);
      if (!manager) {
        await ctx.reply(`⚠️ Эта функция требует, чтобы пользователь уже запускал бота.
Попроси его открыть бота и нажать /start, потом повтори добавление.`);
        return;
      }
      const brandUserId = u.id; // brand owner is the current user
      if (manager.id === brandUserId) {
        await ctx.reply('Это твой аккаунт. Нельзя добавить самого себя менеджером.');
        return;
      }
      await db.addBrandManager(brandUserId, manager.id, u.id);
      await ctx.reply(`✅ Менеджер @${username} добавлен в команду бренда.`);
      // best-effort notify manager
      try {
        const kb = new InlineKeyboard()
          .text('🧑‍💼 Открыть кабинет менеджера', 'a:bm_home')
          .row()
          .text('🏠 Главное меню', 'a:menu');
        await ctx.api.sendMessage(
          Number(manager.tg_id),
          `✅ Тебя добавили в <b>команду бренда</b>.

Нажми <b>«🧑‍💼 Открыть кабинет менеджера»</b> — там будут Inbox и поиск креаторов.`,
          { parse_mode: 'HTML', reply_markup: kb }
        );
      } catch {}
      return;
    }

    // Curator note (safe): store last note for the giveaway
    if (exp.type === 'curator_note') {
      const wsId = Number(exp.wsId || 0);
      const gwId = Number(exp.gwId || 0);
      if (!wsId || !gwId) {
        await ctx.reply('⚠️ Не могу сохранить заметку: нет данных конкурса.');
        return;
      }

      let noteText = String(ctx.message.text || '').trim();
      if (!noteText || noteText.length < 2) {
        await ctx.reply('Пришли заметку одним сообщением (минимум 2 символа).');
        await setExpectText(ctx.from.id, exp);
        return;
      }
      if (noteText.length > 400) noteText = noteText.slice(0, 400);

      const g = await db.getGiveawayForCurator(gwId, u.id);
      if (!g || Number(g.workspace_id) !== wsId) {
        await ctx.reply('Нет доступа.');
        return;
      }

      const meta = {
        text: noteText,
        by_tg_id: Number(ctx.from.id),
        by_username: ctx.from.username ?? null,
        by_name: [ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(' ').trim(),
        at: Date.now()
      };

      await setCurGwNote(gwId, meta);
      try {
        await db.auditGiveaway(gwId, Number(g.workspace_id), u.id, 'curator.note', {
          by_tg_id: meta.by_tg_id,
          by_username: meta.by_username,
          by_name: meta.by_name,
          text: noteText,
          len: noteText.length
        });
      } catch {}

      const kb = new InlineKeyboard()
        .text('⬅️ Назад к конкурсу', `a:cur_gw_open|ws:${wsId}|i:${gwId}`)
        .row()
        .text('👤 Куратор', 'a:cur_home');

      await ctx.reply('✅ Заметка сохранена.', { reply_markup: kb });
      return;
    }




    // Giveaway: why not eligible (owner tool)
    if (exp.type === 'gw_why_userid') {
      const gwId = Number(exp.gwId);
      const m = String(ctx.message.text || '').match(/(\d{5,})/);
      if (!m) {
        await ctx.reply('Пришли user_id цифрами (пример: 611377976).');
        await setExpectText(ctx.from.id, exp);
        return;
      }
      const targetId = Number(m[1]);
      await sendGwWhyResult(ctx, u.id, gwId, targetId, { forceRecheck: true });
      return;
    }



    // Workspace folders (owner/editor)
    if (exp.type === 'folder_create_title') {
      const wsId = Number(exp.wsId);
      const titleRaw = String(ctx.message.text || '').trim();
      const title = titleRaw.slice(0, 40);
      if (!title || title.length < 2) {
        await ctx.reply('Название папки: минимум 2 символа.');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      const access = await getFolderAccess(u.id, wsId);
      if (!access || !access.canEdit) {
        await ctx.reply('Нет доступа.');
        return;
      }

      try {
        const folder = await db.createChannelFolder(wsId, u.id, title);
        await db.auditWorkspace(wsId, u.id, 'folders.created', { folderId: folder.id });

        const kb = new InlineKeyboard()
          .text('📁 Открыть папку', `a:folder_open|ws:${wsId}|f:${folder.id}`)
          .row()
          .text('📁 Все папки', `a:folders_home|ws:${wsId}`);

        await ctx.reply(`✅ Папка создана: <b>${escapeHtml(title)}</b>`, { parse_mode: 'HTML', reply_markup: kb });
        return;
      } catch (e) {
        const msg = String(e?.message || e || '');
        if (msg.includes('uniq_channel_folders_workspace_title')) {
          await ctx.reply('Такая папка уже есть. Дай другое название.');
          await setExpectText(ctx.from.id, exp);
          return;
        }
        await ctx.reply('Не получилось создать папку. Попробуй ещё раз.');
        await setExpectText(ctx.from.id, exp);
        return;
      }
    }

    if (exp.type === 'folder_add_items') {
      const wsId = Number(exp.wsId);
      const folderId = Number(exp.folderId);

      const access = await getFolderAccess(u.id, wsId);
      if (!access || !access.canEdit) {
        await ctx.reply('Нет доступа.');
        return;
      }

      const folder = await db.getChannelFolder(folderId);
      if (!folder || Number(folder.workspace_id) !== Number(wsId)) {
        await ctx.reply('Папка не найдена.');
        return;
      }

      const isPro = await db.isWorkspacePro(wsId);
      const max = isPro ? Number(CFG.WORKSPACE_FOLDER_MAX_ITEMS_PRO) : Number(CFG.WORKSPACE_FOLDER_MAX_ITEMS_FREE);
      const current = Number(folder.items_count || 0);
      const left = Math.max(0, max - current);
      if (left <= 0) {
        await ctx.reply(`Лимит этой папки: <b>${max}</b>. Удалите часть каналов или включите ⭐️ PRO.`, { parse_mode: 'HTML' });
        return;
      }

      let items = parseSponsorsFromText(ctx.message.text).map(x => String(x).toLowerCase());
      if (!items.length) {
        await ctx.reply('Пришли список @каналов или ссылок t.me (через пробел/перенос строки).');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      let truncated = false;

      if (items.length > left) {
        items = items.slice(0, left);
        truncated = true;
      }

      const res = await db.addChannelFolderItems(folderId, items);
      await db.auditWorkspace(wsId, u.id, 'folders.items_added', { folderId, added: res.added });

      const kb = new InlineKeyboard()
        .text('📁 Открыть папку', `a:folder_open|ws:${wsId}|f:${folderId}`)
        .row()
        .text('📁 Все папки', `a:folders_home|ws:${wsId}`);

      const tail = truncated ? `

⚠️ Влезло только <b>${left}</b> (лимит папки: <b>${max}</b>).` : '';
      await ctx.reply(`✅ Добавлено: <b>${res.added}</b>${tail}`, { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

    if (exp.type === 'folder_remove_items') {
      const wsId = Number(exp.wsId);
      const folderId = Number(exp.folderId);

      const access = await getFolderAccess(u.id, wsId);
      if (!access || !access.canEdit) {
        await ctx.reply('Нет доступа.');
        return;
      }

      const folder = await db.getChannelFolder(folderId);
      if (!folder || Number(folder.workspace_id) !== Number(wsId)) {
        await ctx.reply('Папка не найдена.');
        return;
      }

      const items = parseSponsorsFromText(ctx.message.text).map(x => String(x).toLowerCase());
      if (!items.length) {
        await ctx.reply('Пришли список @каналов, которые удалить.');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      const res = await db.removeChannelFolderItems(folderId, items);
      await db.auditWorkspace(wsId, u.id, 'folders.items_removed', { folderId, removed: res.removed });

      const kb = new InlineKeyboard()
        .text('📁 Открыть папку', `a:folder_open|ws:${wsId}|f:${folderId}`)
        .row()
        .text('📁 Все папки', `a:folders_home|ws:${wsId}`);

      await ctx.reply(`✅ Удалено: <b>${res.removed}</b>`, { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

    if (exp.type === 'folder_rename_title') {
      const wsId = Number(exp.wsId);
      const folderId = Number(exp.folderId);

      const access = await getFolderAccess(u.id, wsId);
      if (!access || !access.canEdit) {
        await ctx.reply('Нет доступа.');
        return;
      }

      const titleRaw = String(ctx.message.text || '').trim();
      const title = titleRaw.slice(0, 40);
      if (!title || title.length < 2) {
        await ctx.reply('Название папки: минимум 2 символа.');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      try {
        const folder = await db.getChannelFolder(folderId);
        if (!folder || Number(folder.workspace_id) !== Number(wsId)) {
          await ctx.reply('Папка не найдена.');
          return;
        }

        await db.renameChannelFolder(folderId, title);
        await db.auditWorkspace(wsId, u.id, 'folders.renamed', { folderId });

        const kb = new InlineKeyboard()
          .text('📁 Открыть папку', `a:folder_open|ws:${wsId}|f:${folderId}`)
          .row()
          .text('📁 Все папки', `a:folders_home|ws:${wsId}`);

        await ctx.reply(`✅ Переименовано: <b>${escapeHtml(title)}</b>`, { parse_mode: 'HTML', reply_markup: kb });
        return;
      } catch (e) {
        const msg = String(e?.message || e || '');
        if (msg.includes('uniq_channel_folders_workspace_title')) {
          await ctx.reply('Такая папка уже есть. Дай другое название.');
          await setExpectText(ctx.from.id, exp);
          return;
        }
        await ctx.reply('Не получилось переименовать. Попробуй ещё раз.');
        await setExpectText(ctx.from.id, exp);
        return;
      }
    }

    if (exp.type === 'ws_editor_username') {
      const wsId = Number(exp.wsId);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) {
        await ctx.reply('Нет доступа.');
        return;
      }

      const uname = String(ctx.message.text || '').trim();
      const m = uname.match(/^@?([a-zA-Z0-9_]{5,})$/);
      if (!m) {
        await ctx.reply('Формат: @username');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      const target = await db.findUserByUsername(m[1]);
      if (!target) {
        await ctx.reply('Пользователь не найден в базе. Попроси его открыть бота и нажать /start, затем повтори.');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      await db.addWorkspaceEditor(wsId, target.id, u.id);
      await db.auditWorkspace(wsId, u.id, 'ws.editor_added', { userId: target.id });

      const kb = new InlineKeyboard()
        .text('👥 Editors', `a:ws_editors|ws:${wsId}`)
        .row()
        .text('📁 Папки', `a:folders_home|ws:${wsId}`);

      await ctx.reply(`✅ Добавил редактора: <b>@${escapeHtml(target.tg_username || m[1])}</b>`, { parse_mode: 'HTML', reply_markup: kb });
      return;
    }



    
    // Brand lead from public profile (vitrina) — 2-step (contact -> request)
    if (exp.type === 'wsp_lead_step1') {
      const wsId = Number(exp.wsId || 0);
      const ws = await db.getWorkspaceAny(wsId);
      if (!ws) {
        await ctx.reply('Профиль не найден.');
        return;
      }

      const contact = String(ctx.message.text || '').trim();
      if (!contact || contact.length < 2) {
        await ctx.reply('Шаг 1/2: пришли контакт бренда (IG / @username / ссылка / сайт).\nПример: https://instagram.com/brand или @brand');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      await setExpectText(ctx.from.id, { type: 'wsp_lead_step2', wsId, contact: contact.slice(0, 200) });
      await renderWsLeadCompose(ctx, wsId, 2, { contact: contact.slice(0, 200) });
      return;
    }

    if (exp.type === 'wsp_lead_step2') {
      const wsId = Number(exp.wsId || 0);
      const ws = await db.getWorkspaceAny(wsId);
      if (!ws) {
        await ctx.reply('Профиль не найден.');
        return;
      }

      // Anti-spam: 1 lead per N minutes per (workspace + brand)
      const leadLim = Number(CFG.BRAND_LEAD_RATE_LIMIT || 0);
      const leadWin = Number(CFG.BRAND_LEAD_RATE_WINDOW_SEC || 0);
      if (Number.isFinite(leadLim) && leadLim > 0 && Number.isFinite(leadWin) && leadWin > 0) {
        const rl = await rateLimit(k(['rl', 'brandLead', wsId, tgId]), { limit: leadLim, windowSec: leadWin });
        if (!rl.allowed) {
          const mins = Math.max(1, Math.ceil(leadWin / 60));
          const waitMins = Math.max(1, Math.ceil((rl.resetSec || leadWin) / 60));
          const kb = new InlineKeyboard()
            .text('⬅️ Назад к витрине', `a:wsp_open|ws:${wsId}`)
            .text('📋 Меню', 'a:menu');
          await ctx.reply(
            `⏳ Слишком часто. Можно отправлять <b>${leadLim}</b> заявку каждые <b>${mins}</b> мин в одну витрину.\nПопробуй снова через <b>${waitMins}</b> мин.`,
            { parse_mode: 'HTML', reply_markup: kb }
          );
          return;
        }
      }

      const details = String(ctx.message.text || '').trim();
      if (!details || details.length < 3) {
        await ctx.reply('Шаг 2/2: опиши запрос чуть подробнее (UGC/интеграция, сроки, условия).');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      const brandName = String(exp.brandName || '').trim() || String(exp.contact || '').trim() || ([ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(' ') || null);

      const lead = await db.createBrandLead({
        workspaceId: wsId,
        ownerUserId: Number(ws.owner_user_id),
        brandUserId: Number(u.id),
        brandTgId: tgId,
        brandUsername: ctx.from.username || null,
        brandName,
        message: details,
        meta: { contact: String(exp.contact || '').trim() || null, brand_profile: (exp.brandName || exp.brandLink) ? { brand_name: exp.brandName || null, brand_link: exp.brandLink || null, contact: String(exp.contact || '').trim() || null } : null, from: { tg_id: tgId, username: ctx.from.username || null } }
      });

      const owner = await db.getUserById(Number(ws.owner_user_id));
      const targets = new Set();
      if (owner?.tg_id) targets.add(Number(owner.tg_id));
      for (const id of (CFG.SUPER_ADMIN_TG_IDS || [])) targets.add(Number(id));
      targets.delete(Number(tgId));

      const channel = ws.channel_username ? '@' + ws.channel_username : ws.title;
      const link = wsBrandLink(wsId);

      const ig = ws.profile_ig ? String(ws.profile_ig).replace(/^@/, '') : null;
      const igUrl = ig ? `https://instagram.com/${ig}` : null;

      const who = ctx.from.username ? '@' + ctx.from.username : (brandName || 'brand');

      const contactLine = exp.contact ? `Контакт бренда: <b>${escapeHtml(String(exp.contact).slice(0, 200))}</b>\n` : '';

      const notif =
        `🆕 <b>Новая заявка от бренда</b>\n\n` +
        `Кому: <b>${escapeHtml(String(ws.profile_title || channel))}</b>\n` +
        `Канал: <b>${escapeHtml(channel)}</b>\n` +
        (link ? `Витрина: <a href="${escapeHtml(link)}">${escapeHtml(link)}</a>\n` : '') +
        (igUrl ? `IG: <a href="${escapeHtml(String(igUrl))}">${escapeHtml(shortUrl(String(igUrl)))}</a>\n` : '') +
        contactLine +
        `От: <b>${escapeHtml(String(who))}</b> (<code>${tgId}</code>)\n\n` +
        `<b>Запрос:</b>\n${escapeHtml(details)}`;

      const kb = new InlineKeyboard()
        .text('🔎 Открыть', `a:lead_view|id:${lead.id}|ws:${wsId}|s:new|p:0`)
        .text('⚡ Шаблоны', `a:lead_tpls|id:${lead.id}|ws:${wsId}|s:new|p:0`)
        .row()
        .text('✍️ Ответить', `a:lead_reply|id:${lead.id}|ws:${wsId}|s:new|p:0`)
        .row()
        .text('👤 Профиль', `a:ws_profile|ws:${wsId}`);

      let sent = 0;
      let failed = 0;

      for (const toId of targets) {
        try {
          await ctx.api.sendMessage(toId, notif, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
          sent++;
        } catch (e) {
          failed++;
          try { console.error('[LEAD_NOTIFY] failed', { toId, wsId, leadId: lead.id, err: String(e?.message || e) }); } catch {}
        }
      }

      const backKb = new InlineKeyboard()
        .text('⬅️ Назад к витрине', `a:wsp_open|ws:${wsId}`)
        .text('📋 Меню', 'a:menu');

      if (sent > 0) {
        await ctx.reply('✅ Заявка отправлена. Уведомление владельцу доставлено.', { reply_markup: backKb });
      } else if (targets.size === 0) {
        await ctx.reply('✅ Заявка отправлена. (Тест) Ты отправил заявку с аккаунта владельца — уведомление не требуется.', { reply_markup: backKb });
      } else {
        await ctx.reply(
          '⚠️ Заявка отправлена, но уведомление владельцу НЕ доставлено.\n\n' +
          'Проверь: владелец открыл бота /start (чтобы бот мог писать ему) и не блокировал бота.\n' +
          'Если нужно — SUPER_ADMIN тоже получит уведомление (если указан в ENV).',
          { reply_markup: backKb }
        );
      }
      return;
    }

// Reply to brand lead (owner / SUPER_ADMIN)
    if (exp.type === 'lead_reply') {
      const leadId = Number(exp.leadId || 0);
      const lead = await db.getBrandLeadById(leadId);
      if (!lead) {
        await ctx.reply('Заявка не найдена.');
        return;
      }

    if (exp.type === 'brand_apply') {
      const brandUserId = Number(exp.brandUserId || 0);
      const backPage = Math.max(0, Number(exp.backPage || 0));
      const msg = String(ctx.message.text || '').trim();

      if (!brandUserId) {
        await clearExpectText(ctx.from.id);
        return ctx.reply('⚠️ Не найден бренд для заявки. Открой бренд в каталоге и нажми “Оставить заявку” ещё раз.');
      }

      if (msg.length < 10) {
        return ctx.reply('⚠️ Сделай сообщение чуть подробнее (минимум 10 символов).');
      }
      if (msg.length > 2000) {
        return ctx.reply('⚠️ Слишком длинно. Укороти до 2000 символов.');
      }

      const rlPairKey = k(['rl', 'creator_brand_apply', String(ctx.from.id), String(brandUserId)]);
      const rlDayKey = k(['rl', 'creator_brand_apply_day', String(ctx.from.id)]);

      const rl1 = await rateLimit(rlPairKey, {
        limit: CFG.CREATOR_BRAND_APPLY_RATE_LIMIT,
        windowSec: CFG.CREATOR_BRAND_APPLY_RATE_WINDOW_SEC
      });
      if (!rl1.allowed) {
        return ctx.reply(`⏳ Слишком часто. Повтори через ~${Math.max(1, Math.ceil(rl1.retryAfterSec / 60))} мин.`);
      }

      const rl2 = await rateLimit(rlDayKey, {
        limit: CFG.CREATOR_BRAND_APPLY_DAILY_LIMIT,
        windowSec: CFG.CREATOR_BRAND_APPLY_DAILY_WINDOW_SEC
      });
      if (!rl2.allowed) {
        return ctx.reply('⏳ Лимит заявок на сегодня исчерпан. Попробуй позже.');
      }

      const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
      const brandName = String(prof?.brand_name || '').trim() || 'Бренд';

      // Try store in DB (for brand inbox). If DB isn't migrated yet, still deliver to brand as fallback.
      let app = null;
      let stored = true;
      try {
        app = await db.createBrandApplication({
          brandUserId,
          creatorUserId: u.id,
          creatorTgId: ctx.from.id,
          creatorUsername: ctx.from.username || null,
          message: msg,
          meta: {
            from_first_name: ctx.from.first_name || null,
            from_last_name: ctx.from.last_name || null
          }
        });
      } catch (e) {
        if (isMissingRelationError(e, 'brand_applications')) {
          stored = false;
        } else {
          throw e;
        }
      }

      // Optional: include creator showcase (workspace)
      let creatorShowcase = null;
      try {
        const wss = await db.listWorkspacesByOwner(u.id, { limit: 1, offset: 0 });
        if (wss?.length) creatorShowcase = wss[0];
      } catch {}

      const creatorDisplay = escapeHtml([ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(' ').trim() || (ctx.from.username ? '@' + String(ctx.from.username).replace(/^@/, '') : String(ctx.from.id)));
      const creatorLink = `<a href="tg://user?id=${ctx.from.id}">${creatorDisplay}</a>`;
      const creatorUname = ctx.from.username ? '@' + String(ctx.from.username).replace(/^@/, '') : `id:${ctx.from.id}`;
      const showLine = creatorShowcase
        ? `\n🪟 Витрина креатора: <a href="${wsBrandLink(creatorShowcase.id)}">открыть</a>`
        : '';

      const inboxLine = stored && app
        ? `\n📥 Inbox: #${app.id}`
        : `\n⚠️ Inbox: выключен (нужна миграция 027_brand_applications.sql)`;

      const notifyText =
        `📝 <b>Новая заявка от креатора</b>\n\n` +
        `Бренд: <b>${escapeHtml(brandName)}</b>\n` +
        `От: ${creatorLink} · <b>${escapeHtml(creatorUname)}</b>` +
        showLine +
        inboxLine +
        `\n\n<b>Сообщение:</b>\n${escapeHtml(msg)}`;


      const notifyKb = new InlineKeyboard();
      if (stored && app) {
        notifyKb
          .text('✅ Принять', `a:brand_app_accept|id:${app.id}|s:new|p:0`)
          .row()
          .text('📨 Открыть в Inbox', `a:brand_app_view|id:${app.id}|s:new|p:0`);
      }

      // Recipients: owner + managers
      const recipients = new Set();
      const ownerTgId = await db.getUserTgIdByUserId(brandUserId);
      if (ownerTgId) recipients.add(Number(ownerTgId));
      let managers = [];
      try { managers = await db.listBrandManagers(brandUserId); } catch { managers = []; }
      for (const m of managers || []) {
        const t = Number(m.manager_tg_id || 0);
        if (t) recipients.add(t);
      }

      for (const tgId of recipients) {
        try {
          await bot.api.sendMessage(tgId, notifyText, {
            parse_mode: 'HTML',
            reply_markup: notifyKb.inline_keyboard?.length ? notifyKb : undefined,
            disable_web_page_preview: true
          });
        } catch {}
      }

      await clearExpectText(ctx.from.id);

      const doneKb = new InlineKeyboard()
        .text('🔎 Открыть бренд', `a:brand_dir_open|u:${brandUserId}|p:${backPage}`)
        .row()
        .text('⬅️ Назад к списку', `a:brands_home|p:${backPage}`)
        .text('🏠 Меню', 'a:menu');

      const doneText = stored
        ? '✅ Заявка отправлена. Бренд увидит её в Inbox.'
        : '✅ Заявка отправлена бренду. (Inbox временно недоступен — нужен апдейт бота.)';

      return ctx.reply(doneText, { reply_markup: doneKb });
    }

    if (exp.type === 'brand_app_reply') {
      const appId = Number(exp.appId || 0);
      const brandUserId = Number(exp.brandUserId || 0);
      const creatorTgId = Number(exp.creatorTgId || 0);
      const reply = String(ctx.message.text || '').trim();

      if (!appId || !brandUserId || !creatorTgId) {
        await clearExpectText(ctx.from.id);
        return ctx.reply('⚠️ Не удалось отправить ответ: отсутствуют данные заявки.');
      }

      if (reply.length < 2) return ctx.reply('⚠️ Ответ слишком короткий.');
      if (reply.length > 2000) return ctx.reply('⚠️ Слишком длинно. Укороти до 2000 символов.');

      const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
      const brandName = String(prof?.brand_name || '').trim() || 'Бренд';

      const cUrl = prof?.contact ? brandContactUrl(prof.contact) : null;
      const linkLine = prof?.link ? `
🔗 Сайт/ссылка: ${escapeHtml(String(prof.link))}` : '';
      const contactLine = cUrl ? `
✍️ Контакт: ${escapeHtml(String(prof.contact))}` : '';

      const outText =
        `📩 <b>Ответ бренда</b>

` +
        `Бренд: <b>${escapeHtml(brandName)}</b>` +
        linkLine +
        contactLine +
        `

<b>Сообщение:</b>
${escapeHtml(reply)}`;

      const outKb = new InlineKeyboard()
        .text('💬 Написать бренду', `a:brand_app_chat|id:${appId}`)
        .row()
        .text('🪟 Открыть бренд', `a:brand_dir_open|u:${brandUserId}|p:0`);

      try {
        await bot.api.sendMessage(creatorTgId, outText, {
          parse_mode: 'HTML',
          reply_markup: outKb,
          disable_web_page_preview: true
        });
      } catch {
        // ignore send errors (user may not have started bot)
      }

      // Persist reply + append to thread + move to "in progress" if still new
      const app = await safeBrandApplications(() => db.getBrandApplicationById(appId), async () => null);
      await safeBrandApplications(() => db.markBrandApplicationReplied(appId, reply, u.id), async () => null);
      await safeBrandApplications(() => db.appendBrandApplicationThreadMessage(appId, {
        from: 'brand',
        text: reply,
        at: new Date().toISOString(),
        by_user_id: Number(u.id),
        by_tg_id: Number(ctx.from?.id || 0),
        by_username: ctx.from?.username || null
      }), async () => null);
      if (app && String(app.status) === 'new') {
        await safeBrandApplications(() => db.updateBrandApplicationStatus(appId, 'in_progress'), async () => null);
      }

      await clearExpectText(ctx.from.id);

      const backCb = String(exp.backCb || `a:brand_app_view|id:${appId}|s:new|p:0`);
      const kb = new InlineKeyboard()
        .text('⬅️ Назад', backCb)
        .text('🏠 Меню', 'a:menu');

      return ctx.reply('✅ Ответ отправлен креатору.', { reply_markup: kb });
    }


if (exp.type === 'brand_deals_search') {
  const brandUserId = Number(exp.brandUserId || 0);
  const stage = String(exp.stage || 'negotiation');
  const page = Math.max(0, Number(exp.page || 0));
  const qRaw = String(ctx.message.text || '').trim();

  const backCb = String(exp.backCb || `a:brand_deals|ws:0|st:${stage}|p:${page}`);

  const kb = new InlineKeyboard().text('⬅️ Назад', backCb).text('🏠 Меню', 'a:menu');

  if (!brandUserId) {
    await clearExpectText(ctx.from.id);
    return ctx.reply('⚠️ Не удалось применить поиск: нет brandUserId.', { reply_markup: kb });
  }

  if (!qRaw) {
    return ctx.reply(
      '⚠️ Введи <code>@username</code> или <code>TG id</code> (цифры).\nПример: <code>@zarinka</code> или <code>123456789</code>\n\nЧтобы сбросить: <code>сброс</code>',
      { parse_mode: 'HTML', reply_markup: kb }
    );
  }

  if (/^(сброс|clear|off|нет)$/i.test(qRaw)) {
    await clearBrandDealsSearch(ctx.from.id, brandUserId);
    await clearExpectText(ctx.from.id);
    return ctx.reply('✅ Поиск сброшен.', { reply_markup: kb });
  }

  let q = qRaw.replace(/\s+/g, ' ').trim().slice(0, 80);

  // Smart hints/validation
  if (q.startsWith('@')) {
    const uname = q.replace(/^@+/, '').trim();
    if (uname.length < 2) {
      return ctx.reply(
        '⚠️ После <code>@</code> нужно минимум 2 символа.\nПример: <code>@zarinka</code>',
        { parse_mode: 'HTML', reply_markup: kb }
      );
    }
    q = '@' + uname;
  } else if (/^\d+$/.test(q)) {
    if (q.length < 6) {
      return ctx.reply(
        '⚠️ Похоже на <code>TG id</code>, но слишком коротко.\nПример: <code>123456789</code>\n\nИли введи <code>@username</code>.',
        { parse_mode: 'HTML', reply_markup: kb }
      );
    }
  }

  await setBrandDealsSearch(ctx.from.id, brandUserId, q);
  await clearExpectText(ctx.from.id);

  return ctx.reply(
    `✅ Поиск установлен: <code>${escapeHtml(q)}</code>\n\n💡 Сброс: <code>сброс</code>`,
    { parse_mode: 'HTML', reply_markup: kb }
  );
}

    if (exp.type === 'brand_app_chat_send') {
      const appId = Number(exp.appId || 0);
      const msg = String(ctx.message.text || '').trim();

      if (!appId) {
        await clearExpectText(ctx.from.id);
        return ctx.reply('⚠️ Не удалось отправить сообщение: нет id заявки.');
      }

      if (msg.length < 2) return ctx.reply('⚠️ Сообщение слишком короткое.');
      if (msg.length > 2000) return ctx.reply('⚠️ Слишком длинно. Укороти до 2000 символов.');

      const app = await safeBrandApplications(() => db.getBrandApplicationById(appId), async () => null);
      if (!app) { await clearExpectText(ctx.from.id); return ctx.reply('⚠️ Заявка не найдена.'); }
      if (Number(app.creator_user_id) !== Number(u.id)) { await clearExpectText(ctx.from.id); return ctx.reply('Нет доступа.'); }

      const brandUserId = Number(app.brand_user_id);
      const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
      const brandName = String(prof?.brand_name || '').trim() || 'Бренд';
      const who = ctx.from?.username ? '@' + String(ctx.from.username).replace(/^@/, '') : `id:${ctx.from?.id}`;

      await safeBrandApplications(() => db.appendBrandApplicationThreadMessage(appId, {
        from: 'creator',
        text: msg,
        at: new Date().toISOString(),
        by_user_id: Number(u.id),
        by_tg_id: Number(ctx.from?.id || 0),
        by_username: ctx.from?.username || null
      }), async () => null);

      if (normLeadStatus(app.status) === 'new') {
        await safeBrandApplications(() => db.updateBrandApplicationStatus(appId, 'in_progress'), async () => null);
      }

      // Notify brand owner + managers
      const managers = await safeBrandManagers(() => db.listBrandManagers(brandUserId), async () => []);
      const targets = new Set();
      const brandOwner = await db.getUserById(brandUserId);
      if (brandOwner?.tg_id) targets.add(Number(brandOwner.tg_id));
      for (const m of managers || []) {
        if (m?.manager_tg_id) targets.add(Number(m.manager_tg_id));
      }

      const preview = msg.replace(/\s+/g, ' ').slice(0, 280);
      const notif =
        `💬 <b>Новое сообщение по заявке #${appId}</b>\n\n` +
        `Бренд: <b>${escapeHtml(brandName)}</b>\n` +
        `От: <b>${escapeHtml(String(who))}</b>\n\n` +
        `${escapeHtml(preview)}${msg.length > preview.length ? '…' : ''}`;

      const kb = new InlineKeyboard().text('📨 Открыть в Inbox', `a:brand_app_view|id:${appId}|s:in_progress|p:0`);
      for (const tgId of targets) {
        try {
          await bot.api.sendMessage(tgId, notif, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
        } catch {}
      }

      await clearExpectText(ctx.from.id);
      return ctx.reply('✅ Сообщение доставлено бренду.', {
        reply_markup: new InlineKeyboard()
          .text('📨 Открыть заявку', `a:brand_app_view|id:${appId}|s:in_progress|p:0`)
          .text('🏠 Меню', 'a:menu')
      });
    }


      const ws = await db.getWorkspaceAny(Number(lead.workspace_id));
      if (!ws) {
        await ctx.reply('Канал не найден.');
        return;
      }

      const isOwner = Number(ws.owner_user_id) === Number(u.id);
      const isAdmin = isSuperAdminTg(tgId);
      if (!isOwner && !isAdmin) {
        await ctx.reply('Нет доступа.');
        return;
      }

      const replyText = String(ctx.message.text || '').trim();
      if (!replyText || replyText.length < 1) {
        await ctx.reply('Напиши ответ текстом.');
        return;
      }

      await db.markBrandLeadReplied(leadId, replyText, Number(u.id));
      if (String(lead.status) === 'new') await db.updateBrandLeadStatus(leadId, 'in_progress');

      const channel = ws.channel_username ? '@' + ws.channel_username : ws.title;
      const link = wsBrandLink(Number(ws.id));

      const card = formatWsContactCard(ws, wsId);

      const out =
        `💬 <b>Ответ по заявке #${leadId}</b>\n\n` +
        `Канал: <b>${escapeHtml(String(ws.profile_title || channel))}</b>\n` +
        (link ? `Витрина: <a href="${escapeHtml(link)}">${escapeHtml(shortUrl(link))}</a>\n\n` : `\n`) +
        `${escapeHtml(replyText)}\n\n` +
        `<b>Контакты:</b>\n${card}`;
;

      try {
        await ctx.api.sendMessage(Number(lead.brand_tg_id), out, { parse_mode: 'HTML', disable_web_page_preview: true });
      } catch {}

      const kb = new InlineKeyboard()
        .text('🔎 Открыть заявку', `a:lead_view|id:${leadId}|ws:${Number(ws.id)}|s:${String(exp.backStatus || 'new')}|p:${Number(exp.backPage || 0)}`)
        .text('📨 Заявки', `a:ws_leads|ws:${Number(ws.id)}|s:${String(exp.backStatus || 'new')}|p:${Number(exp.backPage || 0)}`);

      await ctx.reply('✅ Ответ отправлен бренду.', { reply_markup: kb });
      return;
    }

    // Workspace profile edit
    if (exp.type === 'ws_profile_edit') {
      const wsId = Number(exp.wsId);
      const field = String(exp.field || '');
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) { await ctx.reply('Нет доступа к этому каналу.'); return; }

      const raw = String(ctx.message.text || '').trim();
      const rawLc = raw.toLowerCase();
      const wantClear = ['-', '—', 'нет', 'no', 'clear'].includes(rawLc);

      const patch = {};

      // title / niche / contact / geo
      if (field === 'title') {
        const v = wantClear ? null : raw.slice(0, 120);
        if (!wantClear && (!v || v.length < 2)) { await ctx.reply('Слишком коротко. Введи ещё раз.'); await setExpectText(ctx.from.id, exp); return; }
        patch.profile_title = v;
      }
      if (field === 'niche') {
        const v = wantClear ? null : raw.slice(0, 120);
        if (!wantClear && (!v || v.length < 2)) { await ctx.reply('Слишком коротко. Введи ещё раз.'); await setExpectText(ctx.from.id, exp); return; }
        patch.profile_niche = v;
      }
      if (field === 'contact') {
        const v = wantClear ? null : raw.slice(0, 160);
        if (!wantClear && (!v || v.length < 2)) { await ctx.reply('Слишком коротко. Введи ещё раз.'); await setExpectText(ctx.from.id, exp); return; }
        patch.profile_contact = v;
      }
      if (field === 'geo') {
        const v = wantClear ? null : raw.slice(0, 120);
        if (!wantClear && (!v || v.length < 2)) { await ctx.reply('Слишком коротко. Введи ещё раз.'); await setExpectText(ctx.from.id, exp); return; }
        patch.profile_geo = v;
      }

      // Instagram
      if (field === 'ig') {
        if (wantClear) {
          patch.profile_ig = null;
        } else {
          const handle = normalizeIgHandle(raw);
          if (!handle) {
            {
            const kb = new InlineKeyboard()
              .text('⬅️ Назад', `a:ws_profile|ws:${wsId}`)
              .text('📋 Меню', 'a:menu');
            await ctx.reply('⚠️ Пришли @handle или ссылку на профиль вида instagram.com/handle.\n\nЧтобы очистить поле — отправь “-”.', { reply_markup: kb });
          }
            await setExpectText(ctx.from.id, exp);
            return;
          }
          patch.profile_ig = handle;
        }
      }

      // About
      if (field === 'about') {
        const v = wantClear ? null : raw.slice(0, 400);
        if (!wantClear && (!v || v.length < 5)) { await ctx.reply('Слишком коротко (нужно 5+ символов).'); await setExpectText(ctx.from.id, exp); return; }
        patch.profile_about = v;
      }

      // Portfolio URLs (1–3)
      if (field === 'portfolio') {
        if (wantClear) {
          patch.profile_portfolio_urls = [];
        } else {
          const urls = parseUrlsFromText(raw, 3);
          if (!urls.length) {
            {
            const kb = new InlineKeyboard()
              .text('⬅️ Назад', `a:ws_profile|ws:${wsId}`)
              .text('📋 Меню', 'a:menu');
            await ctx.reply('⚠️ Пришли 1–3 ссылки (https://...). Можно в одном сообщении или по строкам.\n\nЧтобы очистить поле — отправь “-”.', { reply_markup: kb });
          }
            await setExpectText(ctx.from.id, exp);
            return;
          }
          patch.profile_portfolio_urls = urls;
        }
      }

      if (!Object.keys(patch).length) { await ctx.reply('Поле не найдено.'); return; }
      await db.setWorkspaceSetting(wsId, patch);
      await db.auditWorkspace(wsId, u.id, 'ws.profile_updated', { field });

      await ctx.reply('✅ Сохранено.', { reply_markup: wsMenuKb(wsId) });
      return;
    }

    // Moderation report (offer/thread)
    if (exp.type === 'bx_report') {
      const offerId = exp.offerId ? Number(exp.offerId) : null;
      const threadId = exp.threadId ? Number(exp.threadId) : null;
      const reason = String(ctx.message.text || '').trim().slice(0, 500);
      if (!reason || reason.length < 5) { await ctx.reply('Опиши причину (5+ символов).'); await setExpectText(ctx.from.id, exp); return; }
      let wsId = null;
      if (offerId) {
        const o = CFG.VERIFICATION_ENABLED
    ? await safeUserVerifications(() => db.getBarterOfferPublicWithVerified(offerId), () => db.getBarterOfferPublic(offerId))
    : await db.getBarterOfferPublic(offerId);
        wsId = o ? o.workspace_id : null;
      }
      if (threadId) {
        const t = await db.getBarterThreadForUser(threadId, u.id);
        if (t) wsId = wsId || t.workspace_id;
      }
      const r = await db.createBarterReport({ workspaceId: wsId, reporterUserId: u.id, offerId, threadId, reason });
      await ctx.reply(`✅ Жалоба отправлена (id: ${r.id}). Модератор посмотрит.`);
      return;
    }
    // Admin: add moderator by @username
    if (exp.type === 'admin_add_mod_username') {
      const txt = String(ctx.message.text || '').trim();
      const mm = txt.match(/^@?([a-zA-Z0-9_]{5,})$/);
      if (!mm) {
        await ctx.reply('Введи @username (пример: @user)');
        await setExpectText(ctx.from.id, exp);
        return;
      }
      const username = mm[1];

      // Telegram Bot API cannot reliably resolve a *user* by @username via getChat().
      // Correct flow: the person should have started the bot at least once so we have them in DB.
      const u2 = await db.findUserByUsername(username);
      if (!u2) {
        await ctx.reply(
          `⚠️ Не нашёл пользователя @${username} в базе.

` +
          `Пусть он откроет бота и нажмёт /start (это добавит его в базу), ` +
          `и потом повтори добавление модератора.`
        );
        return;
      }

      await db.addNetworkModerator(u2.id, u.id);
      await ctx.reply(`✅ Модератор добавлен: @${u2.tg_username || username}`);
      return;
    }

    // Smart Matching brief (after payment)
    if (exp.type === 'match_brief') {
      const brief = String(ctx.message.text || '').trim().slice(0, 1000);
      if (!brief || brief.length < 10) {
        await ctx.reply('Слишком коротко. Пришли бриф одним сообщением (10+ символов).');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      const reqId = Number(exp.requestId);
      const wsId = Number(exp.wsId || 0);
      const count = Number(exp.count || 10);

      const req = await db.getMatchingRequest(reqId, u.id);
      if (!req) {
        await ctx.reply('Запрос matching не найден (возможно, устарел). Открой 🎯 Smart Matching и попробуй ещё раз.');
        return;
      }

      await db.setMatchingBrief(reqId, u.id, brief);
      const rows = await db.searchNetworkBarterOffersByBrief(brief, count);
      const offerIds = rows.map((r) => Number(r.id));
      await db.completeMatchingRequest(reqId, u.id, offerIds);

      if (!rows.length) {
        const kb = new InlineKeyboard()
          .text('🎯 Matching', `a:match_home|ws:${wsId}`)
          .text('🛍 Лента', `a:bx_feed|ws:${wsId}|p:0`)
          .row()
          .text('⬅️ Назад', `a:bx_open|ws:${wsId}`);
        await ctx.reply(
          '😶 Не нашёл релевантных офферов по брифу. Попробуй упростить: ниша + гео + формат (например: "косметика, Москва, обзор").',
          { reply_markup: kb }
        );
        return;
      }

      const showN = Math.min(rows.length, 15);
      const lines = rows.slice(0, showN).map((o) => {
        const ch = o.channel_username ? `@${o.channel_username}` : (o.ws_title || 'канал');
        return `#${o.id} · ${bxCategoryLabel(o.category)}\n<b>${escapeHtml(String(o.title || '').slice(0, 70))}</b>\n${escapeHtml(bxTypeLabel(o.offer_type))} · ${escapeHtml(bxCompLabel(o.compensation_type))}\nКанал: ${escapeHtml(String(ch).slice(0, 60))}`;
      });

      const kb = new InlineKeyboard();
      const btnN = Math.min(showN, 12);
      for (const o of rows.slice(0, btnN)) {
        kb.text(`🔎 #${o.id}`, `a:bx_pub|ws:${wsId}|o:${o.id}|p:0`).row();
      }
      kb.text('🛍 Лента', `a:bx_feed|ws:${wsId}|p:0`)
        .text('🎯 Matching', `a:match_home|ws:${wsId}`)
        .row()
        .text('⬅️ Назад', `a:bx_open|ws:${wsId}`);

      await ctx.reply(
        `🎯 <b>Smart Matching</b>\n\nБриф: <tg-spoiler>${escapeHtml(brief)}</tg-spoiler>\n\nНайдено: <b>${rows.length}</b>\nПоказаны: <b>${showN}</b>\n\n${lines.join('\n\n')}`,
        { parse_mode: 'HTML', reply_markup: kb }
      );
      return;
    }

    // Featured content (after payment)
    if (exp.type === 'feat_content') {
      const raw = String(ctx.message.text || '').trim();
      const lines = raw.split(/\n+/).map(s => s.trim()).filter(Boolean);
      if (lines.length < 2) {
        await ctx.reply('Формат: 1-я строка — заголовок, последняя — контакт (@username / ссылка).');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      const title = String(lines[0]).slice(0, 80);
      const contact = String(lines[lines.length - 1]).slice(0, 160);
      const body = String(lines.slice(1, -1).join('\n')).slice(0, 800);

      const contactOk = /(@[a-zA-Z0-9_]{5,}|t\.me\/|https?:\/\/)/i.test(contact);
      if (!contactOk) {
        await ctx.reply('Не вижу контакта. Последняя строка должна быть @username или ссылкой.');
        await setExpectText(ctx.from.id, exp);
        return;
      }
      if (!title || title.length < 3) {
        await ctx.reply('Слишком короткий заголовок.');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      const wsId = Number(exp.wsId || 0);
      const featuredId = Number(exp.featuredId);
      const f = await db.activateFeaturedPlacementWithContent(featuredId, u.id, title, body, contact);
      if (!f) {
        await ctx.reply('Не смог активировать Featured (возможно, доступ истёк). Открой 🔥 Featured и попробуй снова.');
        return;
      }

      const ends = f.ends_at ? fmtTs(f.ends_at) : '—';
      const kb = new InlineKeyboard()
        .text('🔥 Посмотреть', `a:feat_view|ws:${wsId}|id:${f.id}|p:0`)
        .row()
        .text('🛍 Лента', `a:bx_feed|ws:${wsId}|p:0`)
        .text('⬅️ Назад', `a:bx_open|ws:${wsId}`);

      await ctx.reply(`✅ Featured активирован до <b>${escapeHtml(String(ends))}</b>.`, { parse_mode: 'HTML', reply_markup: kb });
      return;
    }
    // Barter offer create (one-message input)
    if (exp.type === 'bx_offer_text') {
      const draft = (await getDraft(ctx.from.id)) || {};
      const wsId = Number(exp.wsId || draft.wsId);
      const lines = String(ctx.message.text || '').trim().split(/\n+/);
      const baseTitle = (lines[0] || '').trim().slice(0, 80);
      const kind = String(draft.kind || '');
      const kindPrefix = kind === 'integration' ? 'Интеграция: ' : (kind === 'ugc' ? 'UGC: ' : '');
      const title = (kindPrefix + baseTitle).slice(0, 80);
      const description = (lines.slice(1).join('\n') || '').trim().slice(0, 2000);

      if (!wsId || !draft.category || !draft.offer_type || !draft.compensation_type) {
        await ctx.reply('Черновик оффера потерян. Начни заново: 🎬 UGC / Офферы → ➕ Разместить оффер');
        return;
      }
      if (!title || title.length < 3) {
        await ctx.reply('Первой строкой напиши короткий заголовок (3+ символа).');
        await setExpectText(ctx.from.id, exp);
        return;
      }
      if (!description || description.length < 10) {
        await ctx.reply('Добавь детали (со 2-й строки): условия/гео/что хочешь получить.');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      // Contact: prefer @username; if отсутствует — просим указать в тексте.
      const contactFromProfile = ctx.from.username ? '@' + ctx.from.username : null;
      const contactInText = String(ctx.message.text || '').match(/@([a-zA-Z0-9_]{5,})/);
      const contact = contactFromProfile || (contactInText ? '@' + contactInText[1] : null);
      if (!contact) {
        await ctx.reply('Не вижу контакта. Либо включи @username в Telegram, либо добавь его в текст (например: Контакт: @myname) и отправь ещё раз.');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      // owner gate
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) {
        await ctx.reply('Нет доступа к этому каналу.');
        return;
      }
      if (!ws.network_enabled) {
        await ctx.reply('Сначала включи “🌐 Сеть” в настройках канала, чтобы оффер попал в ленту.');
        return;
      }

      const offer = await db.createBarterOffer({
        workspaceId: wsId,
        creatorUserId: u.id,
        category: draft.category,
        offerType: draft.offer_type,
        compensationType: draft.compensation_type,
        title,
        description,
        contact,
      });
      await db.auditBarterOffer(offer.id, wsId, u.id, 'bx.offer_created', { category: draft.category, offerType: draft.offer_type, compensationType: draft.compensation_type, kind: draft.kind || null });
      db.trackEvent('bx_offer_published', { userId: u.id, wsId, meta: { offerId: offer.id, category: draft.category, offerType: draft.offer_type, compensationType: draft.compensation_type } });
      await clearDraft(ctx.from.id);

      const kb = new InlineKeyboard()
        .text('📁 Прикрепить папку каналов', `a:bx_partner_folder_pick|ws:${wsId}|o:${offer.id}`)
        .row()
        .text('⏭ Пропустить', `a:bx_view|ws:${wsId}|o:${offer.id}|back:my`)
        .row()
        .text('🏠 Меню бартер-биржи', `a:bx_open|ws:${wsId}`);

      await ctx.reply(
        `✅ Оффер опубликован в ленте сети.

#${offer.id} · ${bxCategoryLabel(offer.category)}
<b>${escapeHtml(offer.title)}</b>
${escapeHtml(bxTypeLabel(offer.offer_type))} · ${escapeHtml(bxCompLabel(offer.compensation_type))}
Контакт: <b>${escapeHtml(contact)}</b>

📁 Хочешь добавить папку совместных каналов (партнёры/спонсоры)?`,
        { parse_mode: 'HTML', reply_markup: kb }
      );
      return;
    }

    // Barter thread reply
    if (exp.type === 'bx_thread_msg') {
      const threadId = Number(exp.threadId);
      const wsId = Number(exp.wsId);

      const bm = wsId === 0 ? await resolveBmBrandContext(ctx, u) : { enabled: false };
      const effectiveUserId = (wsId === 0 && bm.enabled) ? bm.brandUserId : u.id;

      // Brand-lock: фиксируем, от какого бренда отвечаем (на момент нажатия "Ответить")
      const asUserId = Number(exp.asUserId || effectiveUserId);

      const body = String(ctx.message.text || '').trim().slice(0, 800);
      if (!threadId || !body) {
        await ctx.reply('Пустое сообщение.', { reply_markup: navKb('a:menu') });
        return;
      }

      if (CFG.RATE_LIMIT_ENABLED) {
        try {
          const rl = await rateLimit(
            k(['rl', 'bxmsg', asUserId, threadId]),
            { limit: CFG.BX_MSG_RATE_LIMIT, windowSec: CFG.BX_MSG_RATE_WINDOW_SEC }
          );
          if (!rl.allowed) {
            await ctx.reply(`⏳ Слишком часто. Подожди ${fmtWait(rl.resetSec)} и отправь ещё раз.`, { reply_markup: navKb('a:menu') });
            // we cleared expectation at the start of message router; restore it for retry
            await setExpectText(ctx.from.id, exp);
            return;
          }
        } catch {}
      }

      const built = await buildBxThreadView(asUserId, threadId);
      if (!built) {
        await ctx.reply('Диалог не найден.', { reply_markup: navKb('a:menu') });
        return;
      }
      const { thread } = built;
      if (String(thread.status || '').toUpperCase() !== 'OPEN') {
        await ctx.reply('Диалог закрыт.', { reply_markup: navKb('a:menu') });
        return;
      }

      await db.addBarterMessage(threadId, asUserId, body);

      const auditMeta = { threadId };
      if (Number(ctx.from.id) !== Number(asUserId)) auditMeta.actorTgId = Number(ctx.from.id);
      await db.auditBarterOffer(thread.offer_id, thread.workspace_id, asUserId, 'bx.thread_message', auditMeta);
      db.trackEvent('thread_message_sent', {
        userId: asUserId,
        wsId: Number(thread.workspace_id) || null,
        meta: { threadId, offerId: Number(thread.offer_id), ...(auditMeta.actorTgId ? { actorTgId: auditMeta.actorTgId } : {}) }
      });

      // notify other side (best-effort)
      const otherUserId = Number(thread.buyer_user_id) == Number(asUserId) ? Number(thread.seller_user_id) : Number(thread.buyer_user_id);
      try {
        const otherInfo = await db.getUserTgIdByUserId(otherUserId);
        const otherTgId = otherInfo?.tg_id ? Number(otherInfo.tg_id) : null;
        if (otherTgId) {
          const link = `https://t.me/${CFG.BOT_USERNAME}?start=bxth_${threadId}`;
          await ctx.api.sendMessage(otherTgId, `📨 Новое сообщение по офферу #${thread.offer_id}

Открыть: ${link}`);
        }
      } catch {}

      // show updated thread in reply
      const again = await buildBxThreadView(asUserId, threadId);
      const kb = new InlineKeyboard()
        .text('💬 Открыть диалог', `a:bx_thread|ws:${wsId}|t:${threadId}|p:0`)
        .row()
        .text('📨 Inbox', `a:bx_inbox|ws:${wsId}|p:0`);
      await ctx.reply(again ? again.text : '✅ Отправлено.', { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

// Proofs: link
    if (exp.type === 'bx_proof_link') {
      const wsId = Number(exp.wsId);
      const threadId = Number(exp.threadId);
      const back = exp.back ? String(exp.back) : 'inbox';
      const offerId = exp.offerId ? Number(exp.offerId) : null;
      const page = Number(exp.page || 0);
      const asUserId = Number(exp.asUserId || u.id);


      const raw = String(ctx.message.text || '').trim();
      // allow bare t.me, https links, or @channel/... patterns
      const ok = raw.length >= 8 && raw.length <= 500 && (/^https?:\/\//i.test(raw) || /t\.me\//i.test(raw) || /^@?[a-zA-Z0-9_]{5,}/.test(raw));
      if (!ok) {
        await ctx.reply('Нужна ссылка на пост (пример: https://t.me/...)');
        await setExpectText(ctx.from.id, { type: 'bx_proof_link', wsId, threadId, back, offerId, page, asUserId });
        return;
      }

      try {
        await db.addBarterThreadProofLink(threadId, asUserId, raw);
      } catch (e) {
        if (String(e?.message || '') === 'NO_THREAD_ACCESS') {
          await ctx.reply('Нет доступа к этому диалогу.');
          return;
        }
        throw e;
      }

      const kb = new InlineKeyboard()
        .text('🧾 Proofs', `a:bx_proofs|ws:${wsId}|t:${threadId}|p:${page}${offerId ? `|o:${offerId}` : ''}|b:${back}`)
        .row()
        .text('💬 Диалог', `a:bx_thread|ws:${wsId}|t:${threadId}|p:${page}${offerId ? `|o:${offerId}` : ''}|b:${back}`);
      await ctx.reply('✅ Proof добавлен.', { reply_markup: kb });
      return;
    }

    
    // Brand profile edit (Brand Mode)
    if (exp.type === 'brand_prof_field') {
      const field = String(exp.field || '');
      const raw = String(ctx.message.text || '').trim();

      if (!field) {
        await clearExpectText(ctx.from.id);
        await ctx.reply('Ошибка: неизвестное поле профиля.');
        return;
      }

      if (!raw) {
        await ctx.reply('Пустое значение. Пришли текст.');
        return;
      }

      let value = raw;

      // Allow clearing a field with a simple token
      if (/^(—|-|none|null|clear|удалить)$/i.test(value)) value = null;

      // Basic validation
      if (value !== null) {
        const maxLen = field === 'requirements' ? 600 : 220;
        if (value.length > maxLen) value = value.slice(0, maxLen).trim();

        if (field === 'brand_name' && value.length < 2) {
          await ctx.reply('Слишком короткое название. Пришли 2+ символа.');
          return;
        }
        if ((field === 'brand_link' || field === 'contact') && value.length < 3) {
          await ctx.reply('Слишком коротко. Пришли нормальный контакт/ссылку.');
          return;
        }
      }

      const patch = { [field]: value };
      const saved = await safeBrandProfiles(
        () => db.upsertBrandProfile(u.id, patch),
        async () => ({ __missing_relation: true })
      );

      if (saved && saved.__missing_relation) {
        await clearExpectText(ctx.from.id);
        await ctx.reply('⚠️ В базе нет таблицы brand_profiles. Применяй миграцию migrations/024_brand_profiles.sql в Neon и повтори.');
        return;
      }

      await clearExpectText(ctx.from.id);
      await ctx.reply('✅ Профиль обновлён.');

      const wsId = Number(exp.wsId || 0);
      const ret = String(exp.ret || 'brand');
      const backOfferId = exp.backOfferId ? Number(exp.backOfferId) : null;
      const backPage = Number(exp.backPage || 0);

      // Keep UX consistent: if user edits an "extended" field, stay on the extended screen.
      const EXT_FIELDS = new Set(['niche', 'geo', 'collab_types', 'budget', 'goals', 'requirements']);
      if (EXT_FIELDS.has(field)) {
        await renderBrandProfileMore(ctx, u.id, { wsId, ret, backOfferId, backPage, edit: false });
      } else {
        await renderBrandProfileHome(ctx, u.id, { wsId, ret, backOfferId, backPage, edit: false });
      }
      return;
    }

// Verification request submit
    if (exp.type === 'verify_submit') {
      if (!CFG.VERIFICATION_ENABLED) {
        await ctx.reply('Верификация сейчас отключена.');
        return;
      }
      const kind = String(exp.kind || 'creator');
      const submittedText = String(ctx.message.text || '').trim();
      if (submittedText.length < 20) {
        await ctx.reply('Слишком коротко. Напиши чуть подробнее (минимум 20 символов).');
        await setExpectText(ctx.from.id, { type: 'verify_submit', kind });
        return;
      }
      const trimmed = submittedText.length > 1800 ? submittedText.slice(0, 1800) : submittedText;

      await safeUserVerifications(() => db.upsertVerificationRequest(u.id, { kind, submittedText: trimmed }), async () => null);

      // notify moderators (super admins + network moderators)
      const modIds = new Set((CFG.SUPER_ADMIN_TG_IDS || []).map((n) => Number(n)).filter(Boolean));
      try {
        const mods = await db.listNetworkModerators();
        for (const m of mods) if (m?.tg_id) modIds.add(Number(m.tg_id));
      } catch {}

      const who = ctx.from.username ? '@' + ctx.from.username : ('tg:' + String(ctx.from.id));
      const msg = `✅ <b>Новая заявка на верификацию</b>

Пользователь: <b>${escapeHtml(who)}</b>
Тип: <b>${escapeHtml(kind)}</b>

${escapeHtml(trimmed)}`;
      const kb = new InlineKeyboard()
        .text('👀 View', `a:mod_verif_view|uid:${u.id}|p:0`)
        .row()
        .text('✅ Approve', `a:mod_verif_approve|uid:${u.id}|p:0`)
        .text('❌ Reject', `a:mod_verif_reject|uid:${u.id}|p:0`);

      for (const tgId of modIds) {
        try { await ctx.api.sendMessage(tgId, msg, { parse_mode: 'HTML', reply_markup: kb }); } catch {}
      }

      await ctx.reply('✅ Заявка отправлена. Обычно проверка занимает время — ты получишь ответ в этом чате.');
      return;
    }

    // Moderator: reject reason
    if (exp.type === 'mod_verif_reject_reason') {
      if (!CFG.VERIFICATION_ENABLED) return;
      const reason = String(ctx.message.text || '').trim();
      if (reason.length < 3) {
        await ctx.reply('Причина слишком короткая. Напиши 1–2 предложения.');
        await setExpectText(ctx.from.id, exp);
        return;
      }
      const targetUserId = Number(exp.targetUserId);
      await safeUserVerifications(() => db.setVerificationStatus(targetUserId, 'REJECTED', u.id, reason), async () => null);

      try {
        const target = await db.getUserById(targetUserId);
        if (target?.tg_id) {
          await ctx.api.sendMessage(Number(target.tg_id), `❌ Верификация отклонена.

Причина:
${reason}

Ты можешь подать заявку повторно: /start`, {});
        }
      } catch {}

      await ctx.reply('✅ Отправил пользователю причину отказа.');
      // optionally return to view
      try {
        await renderModVerifView(ctx, targetUserId, Number(exp.page || 0));
      } catch {}
      return;
    }

    // Giveaway drafts
    if (exp.type === 'gw_prize_text') {
      const draft = (await getDraft(ctx.from.id)) || {};
      const prize = String(ctx.message.text || '').trim();
      if (!prize || prize.length < 3) {
        await ctx.reply('Слишком коротко. Опиши приз (минимум 3 символа).');
        await setExpectText(ctx.from.id, exp);
        return;
      }
      draft.prize_value_text = prize.slice(0, 200);
      await setDraft(ctx.from.id, draft);
      await ctx.reply('Ок. Сколько призовых мест?', { reply_markup: gwNewStepWinnersKb(exp.wsId) });
      return;
    }

    if (exp.type === 'gw_winners_custom') {
      const n = Number(String(ctx.message.text || '').trim());
      if (!Number.isFinite(n) || n < 1 || n > 50) {
        await ctx.reply('Введи число от 1 до 50');
        return;
      }
      const draft = (await getDraft(ctx.from.id)) || {};
      draft.winners_count = Math.floor(n);
      await setDraft(ctx.from.id, draft);
      const isPro = await db.isWorkspacePro(exp.wsId);
      const max = isPro ? CFG.GIVEAWAY_SPONSORS_MAX_PRO : CFG.GIVEAWAY_SPONSORS_MAX_FREE;
      await ctx.reply(`Ок. Спонсоры (необязательно, до ${max}).

` +
`Если это соло-розыгрыш — нажми «✅ Без спонсоров (соло)».
` +
`Если есть партнёры — нажми «✍️ Ввести списком» и пришли список @каналов или ссылками t.me (через пробел/перенос строки).

` +
`Можно и через папку: нажми «📁 Из папки».`,
{ reply_markup: gwSponsorsOptionalKb(exp.wsId) });
      await setExpectText(ctx.from.id, { type: 'gw_sponsors_text', wsId: exp.wsId });
      return;
    }

    if (exp.type === 'gw_sponsors_text') {
      const sponsors = parseSponsorsFromText(ctx.message.text);
      if (!sponsors.length) {
        await ctx.reply(
          'Спонсоры не распознаны. Пришли список @каналов / t.me-ссылок\nили нажми «✅ Без спонсоров (соло)».',
          { reply_markup: gwSponsorsOptionalKb(exp.wsId) }
        );
        await setExpectText(ctx.from.id, exp);
        return;
      }
      const isPro = await db.isWorkspacePro(exp.wsId);
      const max = isPro ? CFG.GIVEAWAY_SPONSORS_MAX_PRO : CFG.GIVEAWAY_SPONSORS_MAX_FREE;
      if (sponsors.length > max) {
        await ctx.reply(`Максимум ${max} спонсоров. Укороти список.`);
        await setExpectText(ctx.from.id, exp);
        return;
      }
      const draft = (await getDraft(ctx.from.id)) || {};
      draft.sponsors = sponsors;
      await setDraft(ctx.from.id, draft);

      const list = sponsors.map(x => `• ${escapeHtml(String(x))}`).join('\n');
      await ctx.reply(
        `✅ Спонсоры: <b>${sponsors.length}</b>
${list}

Эти каналы появятся в конкурсе как обязательные подписки.
Дальше жми «➡️ Дальше» и выбери дедлайн.

⚠️ Чтобы «Проверить» работало, добавь бота админом в каналы-спонсоры.`,
        { parse_mode: 'HTML', reply_markup: gwSponsorsReviewKb(exp.wsId) }
      );
      return;
    }

    if (exp.type === 'gw_deadline_custom') {
      const dt = parseMoscowDateTime(ctx.message.text);
      if (!dt) {
        await ctx.reply('Формат: DD.MM HH:MM (МСК). Пример: 20.01 18:00');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      const now = Date.now();
      const delta = dt.getTime() - now;
      if (delta < 5 * 60 * 1000) {
        await ctx.reply('Дедлайн должен быть минимум через 5 минут.');
        await setExpectText(ctx.from.id, exp);
        return;
      }
      if (delta > 30 * 24 * 60 * 60 * 1000) {
        await ctx.reply('Слишком далеко. Максимум 30 дней вперёд.');
        await setExpectText(ctx.from.id, exp);
        return;
      }

      const draft = (await getDraft(ctx.from.id)) || {};
      draft.ends_at = dt.toISOString();
      await setDraft(ctx.from.id, draft);
      await renderGwMediaStep(ctx, exp.wsId, { edit: false });
      return;
    }
  });

  // Proofs: screenshot (photo) + Giveaway media (photo)
  bot.on('message:photo', async (ctx, next) => {
    const exp = await getExpectText(ctx.from.id);
    if (!exp) return next();

    // Barter: screenshot proof
    if (String(exp.type) === 'bx_proof_photo') {
      const u = await db.upsertUser(ctx.from.id, ctx.from.username ?? null);
      await clearExpectText(ctx.from.id);

      const wsId = Number(exp.wsId);
      const threadId = Number(exp.threadId);
      const back = exp.back ? String(exp.back) : 'inbox';
      const offerId = exp.offerId ? Number(exp.offerId) : null;
      const page = Number(exp.page || 0);
      const asUserId = Number(exp.asUserId || u.id);


      const photos = ctx.message.photo || [];
      const last = photos.length ? photos[photos.length - 1] : null;
      const fileId = last?.file_id;
      if (!fileId) {
        await ctx.reply('Не вижу фото. Пришли скрин как картинку (не файл).');
        await setExpectText(ctx.from.id, { type: 'bx_proof_photo', wsId, threadId, back, offerId, page, asUserId });
        return;
      }

      try {
        await db.addBarterThreadProofScreenshot(threadId, asUserId, fileId);
      } catch (e) {
        if (String(e?.message || '') === 'NO_THREAD_ACCESS') {
          await ctx.reply('Нет доступа к этому диалогу.');
          return;
        }
        throw e;
      }

      const kb = new InlineKeyboard()
        .text('🧾 Proofs', `a:bx_proofs|ws:${wsId}|t:${threadId}|p:${page}${offerId ? `|o:${offerId}` : ''}|b:${back}`)
        .row()
        .text('💬 Диалог', `a:bx_thread|ws:${wsId}|t:${threadId}|p:${page}${offerId ? `|o:${offerId}` : ''}|b:${back}`);
      await ctx.reply('✅ Скрин добавлен.', { reply_markup: kb });
      return;
    }

    // Giveaway: attach photo to draft
    if (String(exp.type) === 'gw_media_photo') {
      const wsId = Number(exp.wsId);
      const photos = ctx.message.photo || [];
      const last = photos.length ? photos[photos.length - 1] : null;
      const fileId = last?.file_id;
      if (!fileId) {
        await ctx.reply('Не вижу фото. Пришли картинку как фото (не файл).');
        return;
      }

      const draft = (await getDraft(ctx.from.id)) || { wsId };
      draft.media_type = 'photo';
      draft.media_file_id = fileId;
      await setDraft(ctx.from.id, draft);
      await clearExpectText(ctx.from.id);

      await ctx.reply('✅ Картинка прикреплена. Продолжаем:', {
        reply_markup: gwMediaKb(wsId, true)
      });
      return;
    }


    // Barter offer: attach photo to offer (media in official channel for PAID)
    if (String(exp.type) === 'bx_media_photo') {
      const wsId = Number(exp.wsId);
      const offerId = Number(exp.offerId);
      const back = exp.back ? String(exp.back) : 'my';

      const photos = ctx.message.photo || [];
      const last = photos.length ? photos[photos.length - 1] : null;
      const fileId = last?.file_id;
      if (!fileId) {
        await ctx.reply('Не вижу фото. Пришли картинку как фото (не файл).');
        return;
      }

      const o = await db.getBarterOfferForOwner(ctx.from.id, offerId);
      if (!o) {
        await clearExpectText(ctx.from.id);
        await ctx.reply('Оффер не найден или нет доступа.');
        return;
      }

      await db.updateBarterOffer(offerId, { media_type: 'photo', media_file_id: fileId });
      await clearExpectText(ctx.from.id);

      await ctx.reply('✅ Картинка прикреплена. Продолжаем:', {
        reply_markup: bxMediaKb(wsId, offerId, back, true)
      });
      return;
    }

    return next();
  });

  // Giveaway media (GIF/animation)
  bot.on('message:animation', async (ctx, next) => {
    const exp = await getExpectText(ctx.from.id);
    if (!exp) return next();

    const fileId = ctx.message.animation?.file_id;
    if (!fileId) {
      await ctx.reply('Не вижу GIF/анимацию. Пришли GIF одним сообщением.');
      return;
    }

    // Giveaway: GIF
    if (String(exp.type) === 'gw_media_gif') {
      const wsId = Number(exp.wsId);
      const draft = (await getDraft(ctx.from.id)) || { wsId };
      draft.media_type = 'animation';
      draft.media_file_id = fileId;
      await setDraft(ctx.from.id, draft);
      await clearExpectText(ctx.from.id);

      await ctx.reply('✅ GIF прикреплён. Продолжаем:', { reply_markup: gwMediaKb(wsId, true) });
      return;
    }

    // Barter offer: GIF
    if (String(exp.type) === 'bx_media_gif') {
      const wsId = Number(exp.wsId);
      const offerId = Number(exp.offerId);
      const back = exp.back ? String(exp.back) : 'my';

      const o = await db.getBarterOfferForOwner(ctx.from.id, offerId);
      if (!o) {
        await clearExpectText(ctx.from.id);
        await ctx.reply('Оффер не найден или нет доступа.');
        return;
      }

      await db.updateBarterOffer(offerId, { media_type: 'animation', media_file_id: fileId });
      await clearExpectText(ctx.from.id);

      await ctx.reply('✅ GIF прикреплён. Продолжаем:', { reply_markup: bxMediaKb(wsId, offerId, back, true) });
      return;
    }

    return next();
  });

  bot.on('message:video', async (ctx, next) => {
    const exp = await getExpectText(ctx.from.id);
    if (!exp) return next();

    // Giveaway: attach video to draft
    if (String(exp.type) === 'gw_media_video') {
      const wsId = Number(exp.wsId);
      const fileId = ctx.message.video?.file_id;
      if (!fileId) {
        await ctx.reply('Не вижу видео. Пришли видео одним сообщением.');
        return;
      }

      const draft = (await getDraft(ctx.from.id)) || { wsId };
      draft.media_type = 'video';
      draft.media_file_id = fileId;
      await setDraft(ctx.from.id, draft);
      await clearExpectText(ctx.from.id);

      await ctx.reply('✅ Видео прикреплено. Продолжаем:', { reply_markup: gwMediaKb(wsId, true) });
      return;
    }

    // Barter offer: attach video to offer (media in official channel for PAID)
    if (String(exp.type) === 'bx_media_video') {
      const wsId = Number(exp.wsId);
      const offerId = Number(exp.offerId);
      const back = exp.back ? String(exp.back) : 'my';

      const fileId = ctx.message.video?.file_id;
      if (!fileId) {
        await ctx.reply('Не вижу видео. Пришли видео одним сообщением.');
        return;
      }

      const o = await db.getBarterOfferForOwner(ctx.from.id, offerId);
      if (!o) {
        await clearExpectText(ctx.from.id);
        await ctx.reply('Оффер не найден или нет доступа.');
        return;
      }

      await db.updateBarterOffer(offerId, { media_type: 'video', media_file_id: fileId });
      await clearExpectText(ctx.from.id);

      await ctx.reply('✅ Видео прикреплено. Продолжаем:', {
        reply_markup: bxMediaKb(wsId, offerId, back, true)
      });
      return;
    }

    return next();
  });


  bot.on('message:document', async (ctx, next) => {
    const exp = await getExpectText(ctx.from.id);
    if (!exp) return next();

    const doc = ctx.message.document;
    const mime = doc?.mime_type || '';

    // Giveaway: GIF as document
    if (String(exp.type) === 'gw_media_gif') {
      const wsId = Number(exp.wsId);

      if (!doc?.file_id || (mime && mime !== 'image/gif')) {
        await ctx.reply('Похоже, это не GIF. Пришли GIF как “анимацию” (или файл .gif).');
        return;
      }

      const draft = (await getDraft(ctx.from.id)) || { wsId };
      draft.media_type = 'animation';
      draft.media_file_id = doc.file_id;
      await setDraft(ctx.from.id, draft);
      await clearExpectText(ctx.from.id);

      await ctx.reply('✅ GIF прикреплён. Продолжаем:', { reply_markup: gwMediaKb(wsId, true) });
      return;
    }

    // Giveaway: video can come as document
    if (String(exp.type) === 'gw_media_video') {
      const wsId = Number(exp.wsId);
      if (!doc?.file_id || (mime && !String(mime).startsWith('video/'))) {
        await ctx.reply('Похоже, это не видео. Пришли mp4 как “видео” или как файл.');
        return;
      }

      const draft = (await getDraft(ctx.from.id)) || { wsId };
      draft.media_type = 'video';
      draft.media_file_id = doc.file_id;
      await setDraft(ctx.from.id, draft);
      await clearExpectText(ctx.from.id);

      await ctx.reply('✅ Видео прикреплено. Продолжаем:', { reply_markup: gwMediaKb(wsId, true) });
      return;
    }

    // Barter offer: GIF as document
    if (String(exp.type) === 'bx_media_gif') {
      const wsId = Number(exp.wsId);
      const offerId = Number(exp.offerId);
      const back = exp.back ? String(exp.back) : 'my';

      if (!doc?.file_id || (mime && mime !== 'image/gif')) {
        await ctx.reply('Похоже, это не GIF. Пришли GIF как “анимацию” (или файл .gif).');
        return;
      }

      const o = await db.getBarterOfferForOwner(ctx.from.id, offerId);
      if (!o) {
        await clearExpectText(ctx.from.id);
        await ctx.reply('Оффер не найден или нет доступа.');
        return;
      }

      await db.updateBarterOffer(offerId, { media_type: 'animation', media_file_id: doc.file_id });
      await clearExpectText(ctx.from.id);

      await ctx.reply('✅ GIF прикреплён. Продолжаем:', { reply_markup: bxMediaKb(wsId, offerId, back, true) });
      return;
    }

    // Barter offer: video can come as document
    if (String(exp.type) === 'bx_media_video') {
      const wsId = Number(exp.wsId);
      const offerId = Number(exp.offerId);
      const back = exp.back ? String(exp.back) : 'my';

      if (!doc?.file_id || (mime && !String(mime).startsWith('video/'))) {
        await ctx.reply('Похоже, это не видео. Пришли mp4 как “видео” или как файл.');
        return;
      }

      const o = await db.getBarterOfferForOwner(ctx.from.id, offerId);
      if (!o) {
        await clearExpectText(ctx.from.id);
        await ctx.reply('Оффер не найден или нет доступа.');
        return;
      }

      await db.updateBarterOffer(offerId, { media_type: 'video', media_file_id: doc.file_id });
      await clearExpectText(ctx.from.id);

      await ctx.reply('✅ Видео прикреплено. Продолжаем:', { reply_markup: bxMediaKb(wsId, offerId, back, true) });
      return;
    }

    return next();
  });

  // --- Commands ---
  bot.command('start', async (ctx) => {
    let preMsg = null;
    try {
      const payload = parseStartPayload(ctx.message?.text || '');

      // Early feedback for giveaway deep-links (Jobs-style)
      if (payload?.type === 'gw') preMsg = await ctx.reply('⏳ Открываю конкурс…');
      else if (payload?.type === 'gwj') preMsg = await ctx.reply('⏳ Записываю участие…');
      else if (payload?.type === 'gwc') preMsg = await ctx.reply('⏳ Открываю конкурс…');

      const u = await db.upsertUser(ctx.from.id, ctx.from.username ?? null);
      db.trackEvent('start', { userId: u.id, meta: { payloadType: payload?.type || null, hasPayload: !!payload } });
    if (payload?.type === 'gwj') {
      const loading = preMsg || await ctx.reply('⏳ Записываю участие…');
      const g = await db.getGiveawayInfoForUser(payload.id);
      if (!g) return ctx.api.editMessageText(ctx.chat.id, loading.message_id, 'Конкурс не найден.');
      await db.upsertGiveawayEntry(payload.id, u.id);
      await db.auditGiveaway(payload.id, g.workspace_id, u.id, 'gw.joined', { from: 'start_link' });
      const sponsors = await db.listGiveawaySponsors(payload.id);
      const entry = await db.getEntryStatus(payload.id, u.id);
      const text = renderParticipantScreen(g, entry, { hint: true, sponsors });
      try {
        return await ctx.api.editMessageText(ctx.chat.id, loading.message_id, text, { parse_mode: 'HTML', reply_markup: participantKb(payload.id, entry, { pub: true }) });
      } catch {
        return ctx.reply(text, { parse_mode: 'HTML', reply_markup: participantKb(payload.id, entry, { pub: true }) });
      }
    }
    if (payload?.type === 'gwc') {
      // Variant A: open the giveaway screen (no auto-check, no auto-join)
      const loading = preMsg || await ctx.reply('⏳ Открываю конкурс…');
      const g = await db.getGiveawayInfoForUser(payload.id);
      if (!g) return ctx.api.editMessageText(ctx.chat.id, loading.message_id, 'Конкурс не найден.');
      const sponsors = await db.listGiveawaySponsors(payload.id);
      const entry = await db.getEntryStatus(payload.id, u.id);
      const text = renderParticipantScreen(g, entry, { hint: true, sponsors });
      try {
        return await ctx.api.editMessageText(ctx.chat.id, loading.message_id, text, { parse_mode: 'HTML', reply_markup: participantKb(payload.id, entry, { pub: true }) });
      } catch {
        return ctx.reply(text, { parse_mode: 'HTML', reply_markup: participantKb(payload.id, entry, { pub: true }) });
      }
    }
    if (payload?.type === 'gw') {
      const loading = preMsg || await ctx.reply('⏳ Открываю конкурс…');
      const g = await db.getGiveawayInfoForUser(payload.id);
      if (!g) return ctx.api.editMessageText(ctx.chat.id, loading.message_id, 'Конкурс не найден.');
      const sponsors = await db.listGiveawaySponsors(payload.id);
      const entry = await db.getEntryStatus(payload.id, u.id);
      const text = renderParticipantScreen(g, entry, { hint: true, sponsors });
      try {
        return await ctx.api.editMessageText(ctx.chat.id, loading.message_id, text, { parse_mode: 'HTML', reply_markup: participantKb(payload.id, entry, { pub: true }) });
      } catch {
        return ctx.reply(text, { parse_mode: 'HTML', reply_markup: participantKb(payload.id, entry, { pub: true }) });
      }
    }
    if (payload?.type === 'gwo') {
      const g = await db.getGiveawayForOwner(payload.id, u.id);
      if (!g) return ctx.reply('Нет доступа к этому конкурсу.');
      const sponsors = await db.listGiveawaySponsors(payload.id);
      const sponsorLines = sponsors.map(s => `• ${escapeHtml(s.sponsor_text)}`).join('\n') || '—';
      const text = `🎁 <b>Конкурс #${g.id}</b>\n\nСтатус: <b>${escapeHtml(gwStatusLabel(g.status))}</b>\nПриз: <b>${escapeHtml(g.prize_value_text || '—')}</b>\nМест: <b>${g.winners_count}</b>\nДедлайн: <b>${g.ends_at ? escapeHtml(fmtTs(g.ends_at)) : '—'}</b>\n\nСпонсоры:\n${sponsorLines}`;
      return ctx.reply(text, { parse_mode: 'HTML', reply_markup: gwOpenKb(g, { isAdmin: isSuperAdminTg(ctx.from?.id) }) });
    }
    if (payload?.type === 'cur') {
      // curator invite flow
      const key = k(['cur_invite', payload.wsId, payload.token]);
      // single-use: consume value atomically when possible
      const val = await consumeOnce(key);
      if (!val) return ctx.reply('Ссылка устарела, недействительна или уже была использована.');
      const ownerUserId = Number(val.ownerUserId || val.owner_user_id || val.owner || 0);
      await db.addCurator(payload.wsId, u.id, ownerUserId || u.id);

      const ws = await db.getWorkspaceAny(Number(payload.wsId));
      const wsTitle = ws ? wsLabelNice(ws) : `Канал #${payload.wsId}`;
      const already = await getCuratorMode(ctx.from.id);
      const kb = new InlineKeyboard()
        .text('👤 Открыть кабинет куратора', 'a:cur_home')
        .row()
        .text(already ? '🧹 Режим куратора: ✅ ВКЛ' : '🧹 Включить режим куратора', `a:cur_mode_set|v:1|ret:cur`)
        .row()
        .text('🏠 Главное меню', 'a:menu');

      await ctx.reply(
        `✅ Ты назначен куратором для: <b>${escapeHtml(wsTitle)}</b>.

Что дальше:
1) Нажми <b>«👤 Открыть кабинет куратора»</b>
2) Выбери канал в списке:
   ✅ — доступ включен, можно смотреть конкурсы
   ❌ — владелец выключил кураторов (попроси включить в настройках канала)

💡 Для простоты включи <b>«🧹 Режим куратора»</b> — он прячет лишнее меню.`,
        { parse_mode: 'HTML', reply_markup: kb }
      );
      return;
    }



    if (payload?.type === 'bminv') {
      // brand manager invite flow
      const key = k(['bm_invite', payload.token]);
      const val = await consumeOnce(key);
      if (!val) return ctx.reply('Ссылка устарела, недействительна или уже была использована.');

      const brandUserId = Number(val.brandUserId || val.brand_user_id || val.brand || 0);
      const addedByUserId = Number(val.addedByUserId || val.added_by_user_id || brandUserId || 0);

      if (!brandUserId) return ctx.reply('Ссылка недействительна.');

      await db.addBrandManager(brandUserId, u.id, addedByUserId || u.id);

      let brandLabel = null;
      const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
      if (prof && !prof.__missing_relation && prof.brand_name) brandLabel = prof.brand_name;

      const owner = await db.getUserTgIdByUserId(brandUserId);
      if (!brandLabel) brandLabel = owner?.tg_username ? `@${owner.tg_username}` : `Бренд #${brandUserId}`;

      // включаем режим Brand Manager + ставим текущий бренд + переключаем UI в Brand
      await setBrandManagerMode(ctx.from.id, true);
      await setBmActiveBrand(ctx.from.id, brandUserId);
      await setUiMode(ctx.from.id, UI_MODES.BRAND);

      const kb = new InlineKeyboard()
        .text('🧑‍💼 Кабинет менеджера', 'a:bm_home')
        .row()
        .text('📨 Inbox', 'a:bx_inbox|ws:0|p:0')
        .text('🔎 Поиск креаторов', 'a:pm_home|ws:0')
        .row()
        .text('🏠 Меню', 'a:menu');

      await ctx.reply(
        `✅ Ты добавлен в <b>команду бренда</b>: <b>${escapeHtml(brandLabel)}</b>

<b>Ты сейчас в режиме:</b> <b>Brand Manager</b>

Доступ:
• 📩 Inbox
• 🔎 Поиск креаторов

Ограничения:
• нельзя менять профиль бренда
• нельзя управлять оплатами/подпиской
• нельзя управлять командой`,
        { parse_mode: 'HTML', reply_markup: kb }
      );
      return;
    }

    if (payload?.type === 'fed') {
      // workspace folder editor invite flow
      const key = k(['ws_editor_invite', payload.wsId, payload.token]);
      const val = await redis.get(key);
      if (!val) return ctx.reply('Ссылка устарела или недействительна.');

      const ownerUserId = Number(val.ownerUserId || val.owner_user_id || val.owner || 0);
      await db.addWorkspaceEditor(payload.wsId, u.id, ownerUserId || u.id);
      await redis.del(key);

      const kb = new InlineKeyboard()
        .text('📁 Открыть папки Workspace', `a:folders_home|ws:${payload.wsId}`)
        .row()
        .text('🏠 Главное меню', 'a:menu');

      await ctx.reply(
        `✅ Готово! Ты добавлен как editor папок этого Workspace.

Можешь создавать папки, добавлять/удалять каналы и использовать их в конкурсах/офферах.`,
        { reply_markup: kb }
      );
      return;
    }

    
    if (payload?.type === 'wsp') {
      const wsId = Number(payload.wsId || 0);
      if (!wsId) return ctx.reply('Профиль не найден.');
      await renderWsPublicProfile(ctx, wsId);
      return;
    }

if (payload?.type === 'bxo') {
      const offer = await db.getBarterOfferPublic(payload.id);
      if (!offer) return ctx.reply('Оффер не найден.');
      const wsId = Number(offer.workspace_id);
      return renderBxPublicView(ctx, u.id, wsId, payload.id, 0);
    }

    if (payload?.type === 'bxth') {
      const built = await buildBxThreadView(u.id, payload.id);
      if (!built) return ctx.reply('Диалог не найден.');
      const { thread, text } = built;
      const wsId = Number(thread.workspace_id);
      const kb = bxThreadKb(wsId, thread.id, { back: 'inbox', page: 0, offerId: thread.offer_id });
      return ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb });
    }

    const flags = await getRoleFlags(u, ctx.from.id);
    const curMode = !!flags.isCurator && (await getCuratorMode(ctx.from.id));
    // If curator mode is enabled — go straight to curator cabinet (more direct than showing the mode menu).
    if (curMode) {
      await replyCuratorHome(ctx, u.id);
      return;
    }
    if (CFG.ONBOARDING_V2_ENABLED) {
  const existingMode = await redis.get(k(['ui_mode', ctx.from.id]));
  if (!existingMode) {
    await ctx.reply('Привет! 👋\n\nЭто <b>UGC/Collab CRM</b> в Telegram.\nIG → лиды. TG → сделки.\n\nВыбери роль — и я покажу быстрый старт:', { parse_mode: 'HTML', reply_markup: onboardingKb(flags) });
    return;
  }
}

await renderMainMenu(ctx, flags, { edit: false });
await maybeSendBanner(ctx, 'menu', CFG.MENU_BANNER_FILE_ID);

    } catch (e) {
      console.error('[START] error', {
        chat_id: ctx?.chat?.id ?? null,
        from_id: ctx?.from?.id ?? null,
        message: String(e?.message || e?.error?.message || e || ''),
        name: String(e?.name || e?.error?.name || 'Error'),
      });
      try {
        const msg = '⚠️ Сейчас есть техническая ошибка. Попробуй ещё раз через минуту.';
        if (preMsg?.message_id) {
          await ctx.api.editMessageText(ctx.chat.id, preMsg.message_id, msg);
        } else {
          await ctx.reply(msg);
        }
      } catch {}
    }


  });


  bot.command('help', async (ctx) => {
    try {
      await clearExpectText(ctx.from.id);
    } catch {}

    const text = `❓ Collabka — UGC/Collab CRM в Telegram

Для Creator’ов
• 🚀 Подключи канал (бот админ) и перешли любой пост
• 🪟 Заполни витрину: IG, портфолио, ниши/форматы, гео, контакт
• 🔗 Поставь ссылку витрины в Instagram (bio / stories)
• 📨 Запросы брендов → Inbox, статусы, история

Для брендов
• 🔎 Поиск креаторов → фильтры → кампании (сохранённые поиски)
• 📩 Запрос можно отправить прямо из списка или из витрины
• Всё дальше в TG: интро, дедлайны, материалы

UGC vs Интеграция
🎬 UGC — контент без аудитории (важно качество/вкус)
📣 Интеграция — публикация у креатора (важны охваты)

Розыгрыши
• 🎟 «Участвовать» → 🔄 «Проверить»
• Если подписался только что — подожди 10 сек и проверь снова
• ❔ в проверке = бот не админ в одном из каналов или канал приватный

Команды
/start — главное меню
/help — помощь и быстрый старт
/paysupport — помощь по оплате и Stars`;

    const kb = new InlineKeyboard()
      .text('🧭 Быстрый старт', 'a:guide')
      .text('📋 Меню', 'a:menu')
      .row()
      .text('🏷 Для брендов', 'a:bx_open|ws:0')
      .text('🎬 UGC / Офферы', 'a:bx_home')
      .row()
      .text('🎁 Розыгрыши', 'a:gw_list');

    await ctx.reply(text, { reply_markup: kb });
  });

  bot.command('whoami', async (ctx) => {
    const me = await ctx.api.getMe();
    await ctx.reply(`BOT_ID=${me.id}\nBOT_USERNAME=@${me.username}`);
  });

  bot.command('paysupport', async (ctx) => {
    // Telegram expects bots that accept payments to provide a support contact via /paysupport.
    const contactRaw = (CFG.PAY_SUPPORT_TEXT && String(CFG.PAY_SUPPORT_TEXT).trim())
      ? String(CFG.PAY_SUPPORT_TEXT).trim()
      : '@collabka_support';

    const contactHtml = String(contactRaw)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');

    const msg = [
      '💬 <b>Поддержка по оплате / Stars</b>',
      `Если что-то пошло не так — напиши в поддержку: <b>${contactHtml}</b>`,
      '',
      '<b>Что указать:</b>',
      '1) Что покупал (PRO / Brand Pass / Plan / Featured / Matching)',
      '2) Примерное время оплаты',
      '3) Скрин чека (если есть)',
      '4) Твой @username и что случилось'
    ].join('\n');

    await ctx.reply(msg, { parse_mode: 'HTML', disable_web_page_preview: true });
  });



  // --- Payments (Telegram Stars) ---
  bot.on('pre_checkout_query', async (ctx) => {
    try {
      await ctx.answerPreCheckoutQuery(true);
    } catch (_) {
      // ignore
    }
  });

bot.on('message:successful_payment', async (ctx) => {
  const sp = ctx.message.successful_payment;
  const invoicePayload = sp?.invoice_payload || '';
  if (!invoicePayload) return;

  // ensure user exists
  const u = await db.upsertUser(ctx.from.id, ctx.from.username ?? null);

  const kind =
    invoicePayload.startsWith('pro_') ? 'pro' :
    invoicePayload.startsWith('brand_') ? 'brand_pass' :
    invoicePayload.startsWith('bplan_') ? 'brand_plan' :
    invoicePayload.startsWith('match_') ? 'matching' :
    invoicePayload.startsWith('feat_') ? 'featured' :
    invoicePayload.startsWith('offpub_') ? 'official_publish' :
    'unknown';

  db.trackEvent('payment_success', { userId: u.id, meta: { kind, payload: invoicePayload, amount: sp.total_amount, currency: sp.currency || 'XTR' } });

  // 1) Old ledger: protects from Telegram retries/duplicates
  const starsLedger = await db.recordStarsPayment({
    userId: u.id,
    kind,
    invoicePayload,
    currency: sp.currency,
    totalAmount: sp.total_amount,
    telegramPaymentChargeId: sp.telegram_payment_charge_id,
    providerPaymentChargeId: sp.provider_payment_charge_id,
    raw: sp
  });
  if (starsLedger && starsLedger.inserted === false) {
    await ctx.reply('✅ Платеж уже обработан.');
    return;
  }

  // 2) New payments ledger (admin apply + statuses)
  const pay = await db.insertPayment({
    userId: u.id,
    kind,
    invoicePayload,
    currency: sp.currency,
    totalAmount: sp.total_amount,
    telegramPaymentChargeId: sp.telegram_payment_charge_id,
    providerPaymentChargeId: sp.provider_payment_charge_id,
    raw: sp,
    status: 'RECEIVED'
  });
  if (pay && pay.inserted === false) {
    await ctx.reply('✅ Платеж уже обработан.');
    return;
  }
  const paymentId = pay?.id || null;

  const markStatus = async (status, note) => {
    if (!paymentId) return null;
    try {
      return await db.setPaymentStatus(paymentId, status, note);
    } catch {
      return null;
    }
  };
  const markApplied = async (note) => {
    if (!paymentId) return null;
    try {
      return await db.markPaymentApplied(paymentId, u.id, note);
    } catch {
      return null;
    }
  };

  // We keep Smart Matching / Featured in UI, but post-payment they are always ORPHANED
  // (so the team can decide later; avoids accidental auto-fulfillment).
  if (invoicePayload.startsWith('match_') || invoicePayload.startsWith('feat_') || invoicePayload.startsWith('offpub_')) {
    await markStatus('ORPHANED', 'postpay_orphaned');
    db.trackEvent('payment_orphaned', { userId: u.id, meta: { kind, payload: invoicePayload, reason: 'postpay_orphaned' } });
    if (invoicePayload.startsWith('offpub_')) {
      try {
        const parts = String(invoicePayload).split('_');
        const offerId = Number(parts[2]);
        const days = Number(parts[3] || CFG.OFFICIAL_MANUAL_DEFAULT_DAYS);
        const channelChatId = Number(CFG.OFFICIAL_CHANNEL_ID || 0);
        if (offerId && channelChatId) {
          await db.upsertOfficialPostDraft({
            offerId,
            channelChatId,
            placementType: 'PAID',
            paymentId,
            slotDays: days
          });
        }
      } catch (_) { /* ignore */ }
      await ctx.reply('✅ Оплата получена! Оффер поставлен в очередь на публикацию в официальном канале. Модератор опубликует его вручную.');
    } else {
      await ctx.reply('✅ Платеж получен. Сейчас эта услуга обрабатывается вручную — я свяжусь с тобой в ближайшее время.');
    }

    // Notify super admins so the service request is not lost.
    try {
      const admins = Array.isArray(CFG.SUPER_ADMIN_TG_IDS) ? CFG.SUPER_ADMIN_TG_IDS : [];
      if (admins.length) {
        const userTag = ctx.from?.username ? `@${ctx.from.username}` : `tg:${ctx.from?.id}`;
        const amount = sp.total_amount;
        const currency = sp.currency || 'XTR';
        const msg = [
          '🧾 ORPHANED service payment',
          `Kind: ${kind}`,
          `Payload: ${invoicePayload}`,
          `From: ${userTag} (userId=${u.id})`,
          `Amount: ${amount} ${currency}`,
          `TG charge: ${sp.telegram_payment_charge_id || '-'}`,
          `PaymentId: ${paymentId || '-'}`,
          '',
          'Next: open Admin → Payments → filter ORPHANED and process it.'
        ].join('\n');
        for (const a of admins) {
          await ctx.api.sendMessage(a, msg);
        }
      }
    } catch (_) { /* ignore */ }

    return;
  }

  const { autoApply } = await getPaymentsRuntimeFlags();
  if (!autoApply) {
    await markStatus('ORPHANED', 'auto_apply_paused');
    db.trackEvent('payment_orphaned', { userId: u.id, meta: { kind, payload: invoicePayload, reason: 'auto_apply_paused' } });
    await ctx.reply('✅ Платеж получен. Автовыдача сейчас на паузе — я применю вручную.');
    return;
  }

  // PRO activation
  if (invoicePayload.startsWith('pro_')) {
    try {
      const parts = invoicePayload.split('_');
      const wsId = Number(parts[1]);

      // New format: pro_<wsId>_<userId>_<token>
      // Old format (backwards compatible): pro_<wsId>_<token>
      let payUserId = 0;
      let token = '';
      if (parts.length >= 4 && /^\d+$/.test(String(parts[2] || ''))) {
        payUserId = Number(parts[2]);
        token = parts.slice(3).join('_');
      } else {
        token = parts.slice(2).join('_');
      }

      const data = await redis.get(k(['pay_pro', token]));
      const tgOk = !data?.tgId || Number(data.tgId) === Number(ctx.from.id);
      const userOk = !payUserId || Number(data?.ownerUserId) === payUserId;

      if (!data || Number(data.wsId) != wsId || !tgOk || !userOk) {
        await markStatus('ORPHANED', 'missing_session');
        await ctx.reply('✅ Платеж получен. Но сессия оплаты не найдена (возможно, истекла). Напиши /start и открой ⭐️ PRO, я помогу вручную.');
        return;
      }

      await db.activateWorkspacePro(wsId, CFG.PRO_DURATION_DAYS);
      await db.auditWorkspace(wsId, data.ownerUserId, 'pro.activated', {
        currency: sp.currency,
        total_amount: sp.total_amount,
        telegram_payment_charge_id: sp.telegram_payment_charge_id
      });
      await redis.del(k(['pay_pro', token]));
      await markApplied('auto_apply_pro');
      await ctx.reply('⭐️ PRO активирован! Открой настройки канала → ⭐️ PRO, чтобы управлять пином и лимитами.');
      return;
    } catch (e) {
      await markStatus('ERROR', `auto_apply_error: ${String(e?.message || e).slice(0, 120)}`);
      await ctx.reply('✅ Платеж получен. Возникла ошибка авто-выдачи — я применю вручную.');
      return;
    }
  }

  // Brand Pass credits
  if (invoicePayload.startsWith('brand_')) {
    try {
      const parts = invoicePayload.split('_');
      const payUserId = Number(parts[1]);
      const token = parts.slice(3).join('_');

      const data = await redis.get(k(['pay_brand', token]));
      if (!data || Number(data.userId) !== payUserId || Number(data.tgId) !== Number(ctx.from.id)) {
        await markStatus('ORPHANED', 'missing_session');
        await ctx.reply('✅ Платеж получен. Но сессия оплаты не найдена (возможно, истекла). Напиши /start — я помогу.');
        return;
      }

      const creditsToAdd = Number(data.credits || 0);
      const newBalance = await db.addBrandCredits(payUserId, creditsToAdd);
      await redis.del(k(['pay_brand', token]));

      const kb = new InlineKeyboard();
      if (data.offerId) {
        kb.text('↩️ Вернуться к офферу', `a:bx_pub|ws:${data.wsId}|o:${data.offerId}|p:${Number(data.page || 0)}`)
          .row();
      }
      kb.text('🎫 Brand Pass', `a:brand_pass|ws:${data.wsId}`)
        .text('📨 Inbox', `a:bx_inbox|ws:${data.wsId}|p:0`);

      await markApplied('auto_apply_brand_pass');
      await ctx.reply(
        `✅ Brand Pass активирован!\n\nНачислено: +${creditsToAdd}\nБаланс: ${newBalance}\n\nТеперь можешь писать блогерам — нажми “💬 Написать”.`,
        { reply_markup: kb }
      );
      return;
    } catch (e) {
      await markStatus('ERROR', `auto_apply_error: ${String(e?.message || e).slice(0, 120)}`);
      await ctx.reply('✅ Платеж получен. Возникла ошибка авто-выдачи — я применю вручную.');
      return;
    }
  }

  // Brand Plan tools subscription
  if (invoicePayload.startsWith('bplan_')) {
    try {
      const parts = invoicePayload.split('_');
      const payUserId = Number(parts[1]);
      const plan = String(parts[2] || 'basic').toLowerCase();
      const token = parts.slice(3).join('_');

      const data = await redis.get(k(['pay_bplan', token]));
      if (!data || Number(data.userId) !== payUserId || Number(data.tgId) !== Number(ctx.from.id)) {
        await markStatus('ORPHANED', 'missing_session');
        await ctx.reply('✅ Платеж получен. Но сессия оплаты не найдена (возможно, истекла). Напиши /start — я помогу.');
        return;
      }

      await db.activateBrandPlan(payUserId, plan, CFG.BRAND_PLAN_DURATION_DAYS);
      await redis.del(k(['pay_bplan', token]));

      const wsId = Number(data.wsId || 0);
      const kb = new InlineKeyboard()
        .text('⭐️ Brand Plan', `a:brand_plan|ws:${wsId}`)
        .text('📨 Inbox', `a:bx_inbox|ws:${wsId}|p:0`)
        .row()
        .text('⬅️ Назад', `a:bx_open|ws:${wsId}`);

      await markApplied('auto_apply_brand_plan');
      await ctx.reply('✅ Brand Plan активирован! CRM-стадии в Inbox доступны (для бренда).', { reply_markup: kb });
      return;
    } catch (e) {
      await markStatus('ERROR', `auto_apply_error: ${String(e?.message || e).slice(0, 120)}`);
      await ctx.reply('✅ Платеж получен. Возникла ошибка авто-выдачи — я применю вручную.');
      return;
    }
  }

  await markStatus('ORPHANED', 'unknown_payload');
  await ctx.reply('✅ Платеж получен. Я проверю и применю вручную.');
});
// --- Callback router ---
  bot.on('callback_query:data', async (ctx) => {
      // Make callback UX resilient: ack immediately, and never crash on edit/ack edge-cases
    const _acq = ctx.answerCallbackQuery.bind(ctx);
    ctx.answerCallbackQuery = (opts) => _acq(opts).catch(() => {});
    const _editText = ctx.editMessageText.bind(ctx);
    ctx.editMessageText = (text, extra) =>
      _editText(text, extra).catch(async (e) => {
        const msg = String(e?.description || e?.message || e);
        if (msg.includes('message is not modified')) return;
        // Invoices and some system messages can't be edited — fallback to a new message
        return ctx.reply(text, extra).catch(() => {});
      });
    if (typeof ctx.editMessageReplyMarkup === 'function') {
      const _editMarkup = ctx.editMessageReplyMarkup.bind(ctx);
      ctx.editMessageReplyMarkup = (markup) => _editMarkup(markup).catch(() => {});
    }

    // Stop Telegram "loading" spinner ASAP
    await ctx.answerCallbackQuery();

  const p = parseCb(ctx.callbackQuery.data);
    const u = await db.upsertUser(ctx.from.id, ctx.from.username ?? null);
    // Cancel any pending text input step when user clicks an inline button
    try { await clearExpectText(ctx.from.id); } catch {}
if (p.a === 'a:ui_mode_set') {
  await ctx.answerCallbackQuery();
  const mode = normalizeUiMode(p.m);
  await setUiMode(ctx.from.id, mode);

  const flags = await getRoleFlags(u, ctx.from.id);
  const curMode = !!flags.isCurator && (await getCuratorMode(ctx.from.id));
  if (curMode) {
    await ctx.editMessageText(
      `🧹 <b>Режим куратора</b> включен.\n\nДля простоты я скрываю лишнее меню.\n\nТы сейчас в режиме: <b>Curator</b>`,
      { parse_mode: 'HTML', reply_markup: curatorModeMenuKb(flags) }
    );
    return;
  }

  await renderMainMenu(ctx, flags, { edit: true });
  return;
}

if (p.a === 'a:guide') {
  const flags = await getRoleFlags(u, ctx.from.id);
  const mode = await resolveUiMode(ctx.from.id);

  let text = `🧭 <b>Быстрый старт</b>

`;
  const kb = new InlineKeyboard();

  if (mode === UI_MODES.BRAND) {
    const bm = await resolveBmBrandContext(ctx, u, { requirePickWhenMissingActive: false });

    text += `🏷 <b>Для бренда</b>
` +
      `1) Заполни <b>Профиль бренда</b> (Название, Ниша, Контакт, Ссылка).
` +
      `2) Ищи креаторов в <b>Ленте</b> или <b>Поиске</b>.
` +
      `3) Все заявки и ответы — в <b>Inbox</b>.
` +
      `4) <b>Команда бренда</b> открывается после покупки <b>Brand Pass</b> или <b>Brand Plan</b>.

`;

    if (bm.enabled) {
      text += `🧑‍💼 <b>Если ты менеджер бренда</b>
` +
        `• Открой кабинет менеджера и работай в Inbox
` +
        `• Для оплат/профиля — попроси владельца бренда

`;
    }

    kb.text('🛍 Лента', 'a:bx_feed|ws:0|p:0')
      .text('🔎 Поиск', 'a:pm_home|ws:0')
      .row()
      .text('📨 Inbox', 'a:bx_inbox|ws:0|p:0');

    if (!bm.enabled) {
      kb.text('🏷 Профиль', 'a:brand_profile|ws:0|ret:brand')
        .row()
        .text('🎫 Brand Pass', 'a:brand_pass|ws:0')
        .text('⭐️ Подписка', 'a:brand_plan|ws:0');
    } else {
      // Manager shortcuts
      if ((bm.brands || []).length > 1) kb.row().text('🔁 Сменить бренд', 'a:bm_pick_brand|ret:menu');
      kb.row().text('🧑‍💼 Кабинет менеджера', 'a:bm_home');
    }
  } else {
    text += `✨ <b>Для Creator / канала</b>
` +
      `1) Нажми <b>«🚀 Подключить канал»</b> — добавь бота админом и перешли любой пост.
` +
      `2) Открой <b>«📣 Мои каналы»</b> → заполни витрину (контакты/ниши/форматы).
` +
      `3) Делись витриной — бренды будут писать тебе в <b>Inbox</b>.

` +
      `4) Хочешь найти активные бренды — открой <b>«🏷 Бренды»</b>.

` +
      `Если ты бренд — переключись в режим <b>«🏷 Я бренд»</b>.

`;

    kb.text('🚀 Подключить канал', 'a:setup')
      .text('📣 Мои каналы', 'a:ws_list')
      .row()
      .text('🎬 UGC / Офферы', 'a:bx_home')
      .text('🏷 Бренды', 'a:brands_home|p:0')
      .row()
      .text('🎁 Розыгрыши', 'a:gw_list')
      .row()
      .text('🏷 Я бренд', 'a:ui_mode_set|m:brand|ret:menu');
  }

  kb.row().text('💬 Поддержка', 'a:support').text('🏠 Меню', 'a:menu');

  await ctx.editMessageText(text, { parse_mode: 'HTML', disable_web_page_preview: true, reply_markup: kb });
  await maybeSendBanner(ctx, 'guide', CFG.GUIDE_BANNER_FILE_ID);
  return;
}

if (p.a === 'a:support') {
  const text = `💬 <b>Поддержка</b>

` +
    `Если что-то не работает или есть вопрос — напиши одним сообщением.
` +
    `Я отправлю это в поддержку и вернусь с ответом здесь.

` +
    `Что помогает быстрее решить:
` +
    `• в каком режиме ты был (Creator / Brand / Manager)
` +
    `• что нажимал (кнопки)
` +
    `• текст ошибки из логов/скрин (опиши)

` +
    `⚠️ Спам/реклама — бан.`;

  const kb = new InlineKeyboard()
    .text('✍️ Написать в поддержку', 'a:support_write')
    .row()
    .text('🧭 Быстрый старт', 'a:guide')
    .text('🏠 Меню', 'a:menu');

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
  return;
}

if (p.a === 'a:support_write') {
  await ctx.answerCallbackQuery();
  await setExpectText(ctx.from.id, { type: 'support_any', backCb: 'a:support' });

  const text = `✍️ <b>Пришли одним сообщением</b> текст или фото/скрин (можно с подписью).

Пример: «В режиме Brand нажимаю X → ошибка Y».

Я отправлю это в поддержку.`;

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: navKb('a:support') });
  return;
}


// Brand Directory (Creator)
if (p.a === 'a:brands_home') {
  await ctx.answerCallbackQuery();
  const page = Math.max(0, Number(p.p || 0));
  await renderBrandsDirectory(ctx, u.id, { page, edit: true });
  return;
}

if (p.a === 'a:brands_filters') {
  await ctx.answerCallbackQuery();
  const page = Math.max(0, Number(p.p || 0));
  await renderBrandDirFilters(ctx, u.id, { page });
  return;
}

if (p.a === 'a:bd_fpick') {
  await ctx.answerCallbackQuery();
  const page = Math.max(0, Number(p.p || 0));
  const key = String(p.k || 'cat');
  await renderBrandDirFilterPick(ctx, u.id, { page, key });
  return;
}

if (p.a === 'a:bd_fset') {
  await ctx.answerCallbackQuery();
  const page = Math.max(0, Number(p.p || 0));
  const key = String(p.k || 'cat');
  const v = String(p.v || 'all');

  const norm = (x) => {
    if (!x || x === 'all' || x === 'undefined' || x === 'null') return null;
    return String(x);
  };

  const patch = {};
  if (key === 'cat') patch.category = norm(v);
  if (key === 'type') patch.offerType = norm(v);
  if (key === 'comp') patch.compensationType = norm(v);

  await setBrandDirFilter(ctx.from.id, patch);
  await renderBrandDirFilters(ctx, u.id, { page });
  return;
}

if (p.a === 'a:bd_freset') {
  await ctx.answerCallbackQuery();
  const page = Math.max(0, Number(p.p || 0));
  await setBrandDirFilter(ctx.from.id, { category: null, offerType: null, compensationType: null });
  await renderBrandsDirectory(ctx, u.id, { page, edit: true });
  return;
}

if (p.a === 'a:brand_dir_open') {
  await ctx.answerCallbackQuery();
  const brandUserId = Number(p.u || 0);
  const backPage = Math.max(0, Number(p.p || 0));
  await renderBrandDirectoryCard(ctx, u.id, { brandUserId, backPage, edit: true });
  return;
}



if (p.a === 'a:brand_apply') {
  await ctx.answerCallbackQuery();
  const brandUserId = Number(p.u || 0);
  const backPage = Math.max(0, Number(p.p || 0));

  const prof = await safeBrandProfiles(() => db.getBrandProfile(brandUserId), async () => null);
  const brandName = String(prof?.brand_name || '').trim() || 'Бренд';

  await setExpectText(ctx.from.id, {
    type: 'brand_apply',
    brandUserId,
    backPage,
    backCb: `a:brand_dir_open|u:${brandUserId}|p:${backPage}`
  });

  const kb = new InlineKeyboard()
    .text('⬅️ Назад', `a:brand_dir_open|u:${brandUserId}|p:${backPage}`)
    .text('🏠 Меню', 'a:menu');

  const text = `📝 <b>Заявка бренду</b>

Бренд: <b>${escapeHtml(brandName)}</b>

Напиши одним сообщением:
• кто ты / канал
• аудитория / охваты
• что предлагаешь (формат)
• условия (бартер/сертификат/оплата)
• контакт

Я отправлю это бренду и добавлю в их Inbox.`;

  try {
    await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  } catch {
    await ctx.reply(text, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true });
  }
  return;
}





    // MENU
        // BRAND MANAGER MODE (Brand Team)

    if (p.a === 'a:bm_home') {
      await ctx.answerCallbackQuery();

      // Enter manager cabinet (turn ON manager-mode + set Brand UI)
      await setBrandManagerMode(ctx.from.id, true);
      await setUiMode(ctx.from.id, UI_MODES.BRAND);

      const bm = await resolveBmBrandContext(ctx, u, { requirePickWhenMissingActive: true });

      if (bm.dbMissing) {
        const text = `⚠️ <b>Нужна миграция 026_brand_managers</b>

В Neon должна быть таблица <code>brand_managers</code>.`;
        await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: navKb('a:menu') });
        return;
      }

      if (bm.revoked) {
        await disableBrandManagerState(ctx.from.id);
        const text = `⛔ <b>Доступ менеджера отозван</b>

Если это ошибка — попроси владельца бренда добавить тебя в «👥 Команда бренда».`;
        await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: navKb('a:menu') });
        return;
      }

      if (bm.enabled && bm.needsPick) {
        // Multiple brands and no active brand yet — go to picker
        await renderBmPickBrand(ctx, u, { ret: 'bx_inbox', wsId: 0, page: 0, edit: true });
        return;
      }

      // One brand (or already chosen) — go straight to Inbox
      await renderBxInbox(ctx, bm.brandUserId, 0, 0, { bm });
      return;
    }

    if (p.a === 'a:bm_help') {
      await ctx.answerCallbackQuery();
      const text = `🧑‍💼 <b>Brand Manager</b>

Это роль для команды бренда.

✅ Можно:
• 📩 Inbox (переписка по заявкам/сделкам)
• 🔎 Поиск креаторов (подбор)

⛔️ Нельзя:
• менять профиль бренда
• управлять оплатами / подпиской
• управлять командой бренда

Если у тебя несколько брендов — используй «🔁 Сменить бренд» в меню.`;

      await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: navKb('a:menu') });
      return;
    }

    if (p.a === 'a:bm_mode_set') {
      await ctx.answerCallbackQuery();
      const v = Number(p.v || 0);
      await setBrandManagerMode(ctx.from.id, v === 1);
      const ret = String(p.ret || 'menu');
      const flags = await getRoleFlags(u, ctx.from.id);
      if (ret === 'menu') {
        await renderMainMenu(ctx, flags, { edit: true, user: u });
      } else {
        await ctx.editMessageText('Готово.', { parse_mode: 'HTML', reply_markup: navKb('a:menu') });
      }
      return;
    }

    if (p.a === 'a:bm_pick_brand') {
      await ctx.answerCallbackQuery();

      const ret = String(p.ret || 'menu');
      const wsId = Number(p.ws || 0);
      const page = Number(p.p || 0);

      await renderBmPickBrand(ctx, u, { ret, wsId, page, edit: true });
      return;
    }

    if (p.a === 'a:bm_set_brand') {
      await ctx.answerCallbackQuery();
      const brandUserId = Number(p.bu || 0);
      if (!brandUserId) return;

      const ret = String(p.ret || 'menu');
      const wsId = Number(p.ws || 0);
      const page = Number(p.p || 0);

      // Validate that manager is still assigned to this brand
      let brands = [];
      try {
        brands = await db.listBrandsForManager(u.id);
      } catch (e) {
        if (isMissingRelationError(e, 'brand_managers')) {
          const text = `⚠️ <b>Нужна миграция 026_brand_managers</b>

В Neon должна быть таблица <code>brand_managers</code>.`;
          await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: navKb('a:menu') });
          return;
        }
        brands = [];
      }

      if (!brands.length) {
        await disableBrandManagerState(ctx.from.id);
        const text = `⛔ <b>Доступ менеджера отозван</b>

Попроси владельца бренда добавить тебя в «👥 Команда бренда».`;
        await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: navKb('a:menu') });
        return;
      }

      const ok = brands.some((b) => Number(b.user_id) === brandUserId);
      if (!ok) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа к этому бренду.' });
        await renderBmPickBrand(ctx, u, { ret, wsId, page, edit: true });
        return;
      }

      await setBrandManagerMode(ctx.from.id, true);
      await setUiMode(ctx.from.id, UI_MODES.BRAND);
      await setBmActiveBrand(ctx.from.id, brandUserId);

      // Route after pick
      if (ret === 'bx_inbox') {
        const bm = await resolveBmBrandContext(ctx, u);
        await renderBxInbox(ctx, brandUserId, wsId, page, { bm });
        return;
      }
      if (ret === 'bx_feed') {
        await renderBxFeed(ctx, brandUserId, wsId, page);
        return;
      }
      if (ret === 'bx_open') {
        await renderBxOpen(ctx, brandUserId, wsId);
        return;
      }
      if (ret === 'pm_home') {
        await renderProfileMatchingHome(ctx, brandUserId, wsId);
        return;
      }
      if (ret === 'brand_apps') {
        await renderBrandAppsList(ctx, u.id, brandUserId, 'new', 0);
        return;
      }
	      if (ret === 'brand_deals') {
	        await renderBrandDealsList(ctx, u.id, brandUserId, 'negotiation', 0);
	        return;
	      }

      const flags = await getRoleFlags(u, ctx.from.id);
      await renderMainMenu(ctx, flags, { edit: true, user: u });
      return;
    }

if (p.a === 'a:menu') {
      await ctx.answerCallbackQuery();
      const flags = await getRoleFlags(u, ctx.from.id);
      const curMode = !!flags.isCurator && (await getCuratorMode(ctx.from.id));
      if (curMode) {
        await ctx.editMessageText(`👤 <b>Режим куратора</b>

Здесь показаны только действия куратора, чтобы не путаться.
Чтобы вернуть полное меню — нажми “🔓 Обычный режим”.`, {
          parse_mode: 'HTML',
          reply_markup: curatorModeMenuKb(flags)
        });
        return;
      }
      await renderMainMenu(ctx, flags, { edit: true });
      await maybeSendBanner(ctx, 'menu', CFG.MENU_BANNER_FILE_ID);
      return;
    }


    // CURATOR (safe cabinet)
    if (p.a === 'a:cur_home') {
      await ctx.answerCallbackQuery();
      await clearExpectText(ctx.from.id);
      const flags = await getRoleFlags(u, ctx.from.id);
      if (!flags.isCurator && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      await renderCuratorHome(ctx, u.id);
      return;
    }

    if (p.a === 'a:cur_ws_off') {
      // Backward-compat: old buttons for disabled workspaces
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      if (!wsId) return;
      await renderCuratorWorkspace(ctx, u.id, wsId);
      return;
    }


    if (p.a === 'a:cur_ws') {
      await ctx.answerCallbackQuery();
      await clearExpectText(ctx.from.id);
      const flags = await getRoleFlags(u, ctx.from.id);
      if (!flags.isCurator && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      const wsId = Number(p.ws || 0);
      if (!wsId) return;
      await renderCuratorWorkspace(ctx, u.id, wsId);
      return;
    }

    if (p.a === 'a:cur_leave_q') {
      await ctx.answerCallbackQuery();
      const flags = await getRoleFlags(u, ctx.from.id);
      if (!flags.isCurator && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      const wsId = Number(p.ws || 0);
      if (!wsId) return;

      // ensure user is actually curator for this workspace
      const items = await db.listCuratorWorkspaces(u.id);
      const ok = items.some(w => Number(w.id) === wsId);
      if (!ok && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }

      const ws = await db.getWorkspaceAny(wsId);
      const wsTitle = ws ? wsLabelNice(ws) : `Канал #${wsId}`;
      const kb = new InlineKeyboard()
        .text('✅ Выйти', `a:cur_leave_do|ws:${wsId}`)
        .text('❌ Отмена', `a:cur_ws|ws:${wsId}`);
      await ctx.editMessageText(`❌ <b>Выйти из канала</b>

Ты больше не будешь куратором: <b>${escapeHtml(wsTitle)}</b>

Продолжить?`, {
        parse_mode: 'HTML',
        reply_markup: kb
      });
      return;
    }

    if (p.a === 'a:cur_leave_do') {
      await ctx.answerCallbackQuery();
      const flags = await getRoleFlags(u, ctx.from.id);
      if (!flags.isCurator && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      const wsId = Number(p.ws || 0);
      if (!wsId) return;

      const items = await db.listCuratorWorkspaces(u.id);
      const ok = items.some(w => Number(w.id) === wsId);
      if (!ok && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }

      await db.removeCurator(wsId, u.id);
      await db.auditWorkspace(wsId, u.id, 'ws.curator_left', { curatorUserId: u.id });
      await ctx.answerCallbackQuery({ text: 'Готово' });

      await renderCuratorHome(ctx, u.id);
      return;
    }

    if (p.a === 'a:cur_gw_open') {
      await ctx.answerCallbackQuery();
      await clearExpectText(ctx.from.id);
      const flags = await getRoleFlags(u, ctx.from.id);
      if (!flags.isCurator && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      await renderCuratorGiveawayOpen(ctx, u.id, Number(p.ws || 0), Number(p.i || 0));
      return;
    }

    if (p.a === 'a:cur_gw_stats') {
      await ctx.answerCallbackQuery();
      const flags = await getRoleFlags(u, ctx.from.id);
      if (!flags.isCurator && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      await renderCuratorGiveawayStats(ctx, u.id, Number(p.ws || 0), Number(p.i || 0));
      return;
    }

    if (p.a === 'a:cur_gw_log') {
      await ctx.answerCallbackQuery();
      const flags = await getRoleFlags(u, ctx.from.id);
      if (!flags.isCurator && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      await renderCuratorGiveawayLog(ctx, u.id, Number(p.ws || 0), Number(p.i || 0));
      return;
    }

    if (p.a === 'a:cur_gw_remind_q') {
      await ctx.answerCallbackQuery();
      const flags = await getRoleFlags(u, ctx.from.id);
      if (!flags.isCurator && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      await renderCuratorGiveawayRemindQ(ctx, u.id, Number(p.ws || 0), Number(p.i || 0));
      return;
    }

    if (p.a === 'a:cur_gw_remind_send') {
      await ctx.answerCallbackQuery();
      const flags = await getRoleFlags(u, ctx.from.id);
      if (!flags.isCurator && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      await renderCuratorGiveawayRemindSend(ctx, u.id, Number(p.ws || 0), Number(p.i || 0));
      return;
    }

    if (p.a === 'a:cur_gw_owner_q') {
      await ctx.answerCallbackQuery();
      const flags = await getRoleFlags(u, ctx.from.id);
      if (!flags.isCurator && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      await renderCuratorGiveawayOwnerNotifyQ(ctx, u.id, Number(p.ws || 0), Number(p.i || 0));
      return;
    }

    if (p.a === 'a:cur_gw_owner_send') {
      await ctx.answerCallbackQuery();
      const flags = await getRoleFlags(u, ctx.from.id);
      if (!flags.isCurator && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      await renderCuratorGiveawayOwnerNotifySend(ctx, u.id, Number(p.ws || 0), Number(p.i || 0));
      return;
    }


    // CURATOR: safe "checked" mark + note (teamwork helpers)
    if (p.a === 'a:cur_gw_check_q') {
      await ctx.answerCallbackQuery();
      const flags = await getRoleFlags(u, ctx.from.id);
      if (!flags.isCurator && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      const wsId = Number(p.ws || 0);
      const gwId = Number(p.i || 0);
      if (!wsId || !gwId) return;

      const kb = new InlineKeyboard()
        .text('✅ Подтвердить', `a:cur_gw_check_do|ws:${wsId}|i:${gwId}`)
        .text('❌ Отмена', `a:cur_gw_open|ws:${wsId}|i:${gwId}`);

      await ctx.editMessageText(`✅ <b>Отметить как проверено?</b>

Это внутренняя отметка для владельца и других кураторов.
Ничего не меняет в конкурсе — только фиксирует “я проверил”.

Продолжить?`, { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

    if (p.a === 'a:cur_gw_check_do') {
      await ctx.answerCallbackQuery();
      const flags = await getRoleFlags(u, ctx.from.id);
      if (!flags.isCurator && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      const wsId = Number(p.ws || 0);
      const gwId = Number(p.i || 0);
      if (!wsId || !gwId) return;

      const g = await db.getGiveawayForCurator(gwId, u.id);
      if (!g || Number(g.workspace_id) !== wsId) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }

      const meta = {
        by_tg_id: Number(ctx.from.id),
        by_username: ctx.from.username ?? null,
        by_name: [ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(' ').trim(),
        at: Date.now()
      };
      await setCurGwChecked(gwId, meta);
      try { await db.auditGiveaway(gwId, Number(g.workspace_id), u.id, 'curator.checked', { by_tg_id: meta.by_tg_id, by_username: meta.by_username }); } catch {}
      await ctx.answerCallbackQuery({ text: '✅ Отмечено' });

      await renderCuratorGiveawayOpen(ctx, u.id, wsId, gwId);
      return;
    }

    if (p.a === 'a:cur_gw_note_q') {
      await ctx.answerCallbackQuery();
      const flags = await getRoleFlags(u, ctx.from.id);
      if (!flags.isCurator && !flags.isAdmin) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      const wsId = Number(p.ws || 0);
      const gwId = Number(p.i || 0);
      if (!wsId || !gwId) return;

      const g = await db.getGiveawayForCurator(gwId, u.id);
      if (!g || Number(g.workspace_id) !== wsId) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }

      await setExpectText(ctx.from.id, { type: 'curator_note', wsId, gwId });

      const kb = new InlineKeyboard()
        .text('❌ Отмена', `a:cur_note_cancel|ws:${wsId}|i:${gwId}`)
        .row()
        .text('⬅️ Назад', `a:cur_gw_open|ws:${wsId}|i:${gwId}`);

      await ctx.editMessageText(`📝 <b>Заметки к конкурсу #${gwId}</b>

Это внутренние пометки для владельца и кураторов — участникам не показывается.
Примеры: «согласовали приз», «ждём фото», «уточнить условия», «риск/сомнительно».

Пришли заметку одним сообщением (до 400 символов).
Она будет видна владельцу и другим кураторам.

Чтобы отменить — нажми “❌ Отмена”.`, { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

    if (p.a === 'a:cur_note_cancel') {
      await ctx.answerCallbackQuery();
      await clearExpectText(ctx.from.id);
      const wsId = Number(p.ws || 0);
      const gwId = Number(p.i || 0);
      if (!wsId || !gwId) return;
      await renderCuratorGiveawayOpen(ctx, u.id, wsId, gwId);
      return;
    }

if (p.a === 'a:wsp_preview') {
      const wsId = Number(p.ws || 0);
      if (!wsId) return ctx.answerCallbackQuery({ text: 'Workspace не найден.' });

      try { await ctx.answerCallbackQuery({ text: 'Открываю витрину…' }); } catch {}

      await renderWsPublicProfile(ctx, wsId, { backCb: `a:ws_profile|ws:${wsId}` });
      return;
    }

    // Public profile (vitrina)
    if (p.a === 'a:wsp_open') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      if (!wsId) return;
      await renderWsPublicProfile(ctx, wsId);
      return;
    }

    if (p.a === 'a:wsp_lead_new') {
      const wsId = Number(p.ws || 0);
      if (!wsId) return;

      // Gate by Brand Profile (basic 3 fields) and skip Step 1 when complete
      if (CFG.BRAND_PROFILE_REQUIRED) {
        const prof = await safeBrandProfiles(() => db.getBrandProfile(u.id), async () => null);
        if (!isBrandBasicComplete(prof)) {
          await ctx.answerCallbackQuery({ text: 'Заполни профиль бренда (3 поля), чтобы оставить заявку.', show_alert: true });
          await renderBrandProfileHome(ctx, u.id, { wsId, ret: 'lead', edit: true });
          return;
        }

        const contact = String(prof.contact || '').trim().slice(0, 200);
        await ctx.answerCallbackQuery();
        await setExpectText(ctx.from.id, {
          type: 'wsp_lead_step2',
          wsId,
          contact,
          brandName: String(prof.brand_name || '').trim() || null,
          brandLink: String(prof.brand_link || '').trim() || null,
        });
        await renderWsLeadCompose(ctx, wsId, 2, { contact });
        return;
      }

      await ctx.answerCallbackQuery();
      await setExpectText(ctx.from.id, { type: 'wsp_lead_step1', wsId });
      await renderWsLeadCompose(ctx, wsId, 1);
      return;
    }

    // Leads inbox (owner + SUPER_ADMIN)
    
	if (p.a === 'a:brand_apps') {
	  await ctx.answerCallbackQuery();
	  const status = String(p.s || 'new');
	  const page = Math.max(0, Number(p.p || 0));

	  const bmRes = await bmResolveAssert(ctx, u, 0, 'brand_apps', page);
	  if (!bmRes) return;

	  await renderBrandAppsList(ctx, u.id, bmRes.userId, status, page);
	  return;
	}

	if (p.a === 'a:brand_deals') {
	  await ctx.answerCallbackQuery();
	  const stage = String(p.st || 'negotiation');
	  const page = Math.max(0, Number(p.p || 0));

	  const bmRes = await bmResolveAssert(ctx, u, 0, 'brand_deals', page);
	  if (!bmRes) return;

	  await renderBrandDealsList(ctx, u.id, bmRes.userId, stage, page);
	  return;
	}

	if (p.a === 'a:brand_deals_search') {
  await ctx.answerCallbackQuery();
  const stage = String(p.st || 'negotiation');
  const page = Math.max(0, Number(p.p || 0));
  const bmRes = await bmResolveAssert(ctx, u, 0, 'brand_deals', page);
  if (!bmRes) return;
  const backCb = `a:brand_deals|ws:0|st:${stage}|p:${page}`;
  await setExpectText(ctx.from.id, { type: 'brand_deals_search', brandUserId: bmRes.userId, stage, page, backCb });
  const kb = new InlineKeyboard().text('⬅️ Назад', backCb);
  const t = '🔎 <b>Поиск по сделкам</b>\n\nВарианты:\n• <code>@username</code> — пример: <code>@zarinka</code>\n• <code>TG id</code> (цифры) — пример: <code>123456789</code>\n\nПодсказки:\n• если начинаешь с <code>@</code>, добавь минимум 2 символа после @\n• если вводишь цифры — обычно 6–12 цифр\n\nЧтобы сбросить: <code>сброс</code>';
  try { await ctx.editMessageText(t, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true }); }
  catch { await ctx.reply(t, { parse_mode: 'HTML', reply_markup: kb, disable_web_page_preview: true }); }
  return;
}

if (p.a === 'a:brand_deals_search_clear') {
  await ctx.answerCallbackQuery();
  const stage = String(p.st || 'negotiation');
  const page = Math.max(0, Number(p.p || 0));
  const bmRes = await bmResolveAssert(ctx, u, 0, 'brand_deals', page);
  if (!bmRes) return;
  await clearBrandDealsSearch(ctx.from.id, bmRes.userId);
  await renderBrandDealsList(ctx, u.id, bmRes.userId, stage, page);
  return;
}



if (p.a === 'a:brand_deals_mine_toggle') {
  await ctx.answerCallbackQuery();
  const stage = String(p.st || 'negotiation');
  const page = Math.max(0, Number(p.p || 0));

  const bmRes = await bmResolveAssert(ctx, u, 0, 'brand_deals', page);
  if (!bmRes) return;

  const access = await assertBrandAppsAccess(ctx, u.id, bmRes.userId);
  if (!access.ok) return;

  if (!access.isManager) {
    try { await ctx.answerCallbackQuery({ text: 'Доступно только менеджеру.' }); } catch {}
    return;
  }

  const cur = await getBrandDealsMineOnly(ctx.from.id, bmRes.userId);
  if (cur) await clearBrandDealsMineOnly(ctx.from.id, bmRes.userId);
  else await setBrandDealsMineOnly(ctx.from.id, bmRes.userId, true);

  await renderBrandDealsList(ctx, u.id, bmRes.userId, stage, page);
  return;
}

if (p.a === 'a:brand_deal_view') {
	  await ctx.answerCallbackQuery();
	  const appId = Number(p.id || 0);
	  const back = { stage: String(p.st || 'negotiation'), page: Math.max(0, Number(p.p || 0)) };
	  await renderBrandDealView(ctx, u.id, appId, back);
	  return;
	}

	if (p.a === 'a:brand_deal_set') {
	  await ctx.answerCallbackQuery();
	  const appId = Number(p.id || 0);
	  const stage = normDealStage(String(p.st || 'negotiation'));
	  const back = { stage: String(p.b || 'negotiation'), page: Math.max(0, Number(p.p || 0)) };
	  if (!appId) return;

	  await safeBrandApplications(() => db.setBrandApplicationDealStage(appId, stage, u.id), async () => null);
	  await renderBrandDealView(ctx, u.id, appId, back);
	  return;
	}
if (p.a === 'a:brand_deal_reply') {
  await ctx.answerCallbackQuery();
  const appId = Number(p.id || 0);
  const back = { stage: String(p.b || p.st || 'negotiation'), page: Math.max(0, Number(p.p || 0)) };
  if (!appId) return;
  await startBrandDealReply(ctx, u.id, appId, back);
  return;
}

if (p.a === 'a:brand_deal_tpls') {
  await ctx.answerCallbackQuery();
  const appId = Number(p.id || 0);
  const back = { stage: String(p.b || p.st || 'negotiation'), page: Math.max(0, Number(p.p || 0)) };
  if (!appId) return;
  await renderBrandDealTemplates(ctx, u.id, appId, back);
  return;
}

if (p.a === 'a:brand_deal_tpl') {
  await ctx.answerCallbackQuery();
  const appId = Number(p.id || 0);
  const key = String(p.k || 'discuss');
  const back = { stage: String(p.b || 'negotiation'), page: Math.max(0, Number(p.p || 0)) };
  if (!appId) return;
  await sendBrandDealTemplateReply(ctx, u.id, appId, key, back);
  return;
}


if (p.a === 'a:brand_app_view') {
  await ctx.answerCallbackQuery();
  const appId = Number(p.id || 0);
  const back = { status: String(p.s || 'new'), page: Math.max(0, Number(p.p || 0)) };
  await renderBrandAppView(ctx, u.id, appId, back);
  return;
}

if (p.a === 'a:brand_app_set') {
  await ctx.answerCallbackQuery();
  const appId = Number(p.id || 0);
  const st = normLeadStatus(String(p.st || 'new'));
  const back = { status: String(p.s || 'new'), page: Math.max(0, Number(p.p || 0)) };

  // Update in DB if available
  await safeBrandApplications(() => db.updateBrandApplicationStatus(appId, st), async () => null);

  await renderBrandAppView(ctx, u.id, appId, back);
  return;
}

if (p.a === 'a:brand_app_reply') {
  await ctx.answerCallbackQuery();
  const appId = Number(p.id || 0);
  const back = { status: String(p.s || 'new'), page: Math.max(0, Number(p.p || 0)) };
  await startBrandAppReply(ctx, u.id, appId, back);
  return;
}

if (p.a === 'a:brand_app_tpls') {
  await ctx.answerCallbackQuery();
  const appId = Number(p.id || 0);
  if (!appId) return;
  const back = { status: String(p.s || 'new'), page: Math.max(0, Number(p.p || 0)) };
  await renderBrandAppTemplates(ctx, u.id, appId, back);
  return;
}

if (p.a === 'a:brand_app_tpl') {
  await ctx.answerCallbackQuery();
  const appId = Number(p.id || 0);
  if (!appId) return;
  const key = String(p.k || 'discuss');
  const back = { status: String(p.s || 'new'), page: Math.max(0, Number(p.p || 0)) };
  await sendBrandAppTemplateReply(ctx, u.id, appId, key, back);
  return;
}

if (p.a === 'a:brand_app_accept') {
  await ctx.answerCallbackQuery();
  const appId = Number(p.id || 0);
  if (!appId) return;
  const back = { status: String(p.s || 'new'), page: Math.max(0, Number(p.p || 0)) };
  await acceptBrandApplication(ctx, u.id, appId, back);
  return;
}

if (p.a === 'a:brand_app_chat') {
  await ctx.answerCallbackQuery();
  const appId = Number(p.id || 0);
  if (!appId) return;
  await startBrandAppChatForCreator(ctx, u.id, appId);
  return;
}

if (p.a === 'a:ws_leads') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      if (!wsId) return;
      await renderWsLeadsList(ctx, u.id, wsId, String(p.s || 'new'), Number(p.p || 0));
      return;
    }

    if (p.a === 'a:lead_view') {
      await ctx.answerCallbackQuery();
      const leadId = Number(p.id || 0);
      if (!leadId) return;
      await renderLeadView(ctx, u.id, leadId, { wsId: Number(p.ws || 0) || null, status: String(p.s || 'new'), page: Number(p.p || 0) });
      return;
    }

    
    if (p.a === 'a:lead_tpls') {
      await ctx.answerCallbackQuery();
      const leadId = Number(p.id || 0);
      if (!leadId) return;
      await renderLeadTemplates(ctx, u.id, leadId, { wsId: Number(p.ws || 0) || null, status: String(p.s || 'new'), page: Number(p.p || 0) });
      return;
    }

    if (p.a === 'a:lead_tpl') {
      await ctx.answerCallbackQuery();
      const leadId = Number(p.id || 0);
      if (!leadId) return;
      const key = String(p.k || 'thanks');
      await sendLeadTemplateReply(ctx, u.id, leadId, key, { wsId: Number(p.ws || 0) || null, status: String(p.s || 'new'), page: Number(p.p || 0) });
      return;
    }

if (p.a === 'a:lead_set') {
      await ctx.answerCallbackQuery();
      const leadId = Number(p.id || 0);
      if (!leadId) return;
      const st = normLeadStatus(p.st);
      await db.updateBrandLeadStatus(leadId, st);
      await renderLeadView(ctx, u.id, leadId, { wsId: Number(p.ws || 0) || null, status: String(p.s || st), page: Number(p.p || 0) });
      return;
    }

    if (p.a === 'a:lead_reply') {
      await ctx.answerCallbackQuery();
      const leadId = Number(p.id || 0);
      if (!leadId) return;

      const lead = await db.getBrandLeadById(leadId);
      if (!lead) return ctx.editMessageText('Заявка не найдена.');

      const ws = await db.getWorkspaceAny(Number(lead.workspace_id));
      if (!ws) return ctx.editMessageText('Канал не найден.');

      const isOwner = Number(ws.owner_user_id) === Number(u.id);
      const isAdmin = isSuperAdminTg(ctx.from.id);
      if (!isOwner && !isAdmin) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      await setExpectText(ctx.from.id, { type: 'lead_reply', leadId, wsId: Number(ws.id), backStatus: String(p.s || 'new'), backPage: Number(p.p || 0) });

      const kb = new InlineKeyboard()
        .text('⬅️ Назад', `a:lead_view|id:${leadId}|ws:${Number(ws.id)}|s:${String(p.s || 'new')}|p:${Number(p.p || 0)}`);

      await ctx.editMessageText(
        `✍️ <b>Ответ на заявку #${leadId}</b>

Напиши ответ одним сообщением.`,
        { parse_mode: 'HTML', reply_markup: kb }
      );
      return;
    }

// ONBOARDING V2 (feature-flag)
    if (p.a === 'a:onb_creator') {
      await ctx.answerCallbackQuery();
      await setUiMode(ctx.from.id, UI_MODES.CREATOR);
      const text =
        '✨ <b>Creator / Канал</b>\n\n' +
        'Это UGC/Collab CRM: IG → лиды, TG → сделки.\n\n' +
        '1) 🚀 Подключи канал (workspace)\n' +
        '2) 🪟 Заполни витрину (IG, портфолио, форматы)\n' +
        '3) 🔗 Поставь ссылку витрины в Instagram\n' +
        '4) 📨 Принимай запросы брендов и веди статусы\n\n' +
        'Давай начнём:';
      const kb = new InlineKeyboard()
        .text('🚀 Подключить канал', 'a:setup')
        .row()
        .text('📣 Мои каналы', 'a:ws_list')
        .row()
        .text('🎬 UGC / Офферы', 'a:bx_home')
        .row()
        .text('⬅️ Меню', 'a:menu');
      await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

    if (p.a === 'a:onb_brand') {
      await ctx.answerCallbackQuery();
      await setUiMode(ctx.from.id, UI_MODES.BRAND);
      const text =
        '🏷 <b>Brand / Бренд</b>\n\n' +
        'Нашли креатора в Instagram → открываете витрину → закрываете сделку в Telegram.\n\n' +
        '• 🛍 Смотри ленту UGC/офферов\n' +
        '• 📨 Пиши в Inbox через <b>Brand Pass</b> (анти-спам)\n' +
        '• 🧾 Держи историю и статусы\n\n' +
        'Открыть режим бренда:';
      const kb = new InlineKeyboard()
        .text('🏷 Для брендов', 'a:bx_open|ws:0')
        .row()
        .text('🎫 Brand Pass', 'a:brand_pass|ws:0')
        .row()
        .text('⬅️ Меню', 'a:menu');
      await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

    // VERIFICATION (feature-flag)
    if (p.a === 'a:verify_home') {
      await ctx.answerCallbackQuery();
      if (!CFG.VERIFICATION_ENABLED) {
        await ctx.editMessageText('✅ Верификация сейчас отключена.', { reply_markup: mainMenuKb(await getRoleFlags(u, ctx.from.id)) });
        return;
      }
      await renderVerifyHome(ctx, u);
      return;
    }
    if (p.a === 'a:verify_info') {
      await ctx.answerCallbackQuery();
      await renderVerifyInfo(ctx);
      return;
    }
    if (p.a === 'a:verify_kind') {
      await ctx.answerCallbackQuery();
      if (!CFG.VERIFICATION_ENABLED) return ctx.answerCallbackQuery({ text: 'Верификация отключена.' });
      const kind = String(p.k || 'creator');


      if (kind === 'brand' && CFG.BRAND_VERIFY_REQUIRES_EXTENDED) {
        const prof = await safeBrandProfiles(() => db.getBrandProfile(u.id), async () => null);
        if (!isBrandExtendedComplete(prof)) {
          await ctx.editMessageText(
            `🏷 <b>Верификация Brand</b>

Чтобы подать заявку как бренд, заполни расширенный профиль:
• ниша
• гео
• форматы сотрудничества

<i>Зачем:</i> модерации нужны факты, а креаторам — понятность.`,
            {
              parse_mode: 'HTML',
              reply_markup: new InlineKeyboard()
                .text('🏷 Профиль бренда', 'a:brand_profile|ws:0|ret:verify')
                .row()
                .text('⬅️ Назад', 'a:verify_home')
            }
          );
          return;
        }
      }

      await setExpectText(ctx.from.id, { type: 'verify_submit', kind });
      await ctx.editMessageText(
        `✅ <b>Заявка на верификацию</b>

Отправь одним сообщением:
1) ссылку на твой канал/профиль
2) 2–3 цифры/факта (охваты/подписчики/ниша)
3) контакты для связи
4) коротко: что предлагаешь / что ищешь

<i>Важно:</i> только текст (1 сообщение).`,
        { parse_mode: 'HTML', reply_markup: new InlineKeyboard().text('⬅️ Назад', 'a:verify_home') }
      );
      return;
    }

    // SETUP
    if (p.a === 'a:setup') {
      await ctx.answerCallbackQuery();
      db.trackEvent('setup_open', { userId: u.id });
      await renderSetupInstructions(ctx);
      await setExpectText(ctx.from.id, { type: 'setup_forward' });
      return;
    }

    // WORKSPACES
    if (p.a === 'a:ws_list') {
      await ctx.answerCallbackQuery();
      await renderWsList(ctx, u.id);
      return;
    }
    if (p.a === 'a:ws_open') {
      await ctx.answerCallbackQuery();
      await renderWsOpen(ctx, u.id, Number(p.ws));
      return;
    }
    if (p.a === 'a:ws_settings') {
      await ctx.answerCallbackQuery();
      await renderWsSettings(ctx, u.id, Number(p.ws));
      return;
    }
    if (p.a === 'a:ws_history') {
      await ctx.answerCallbackQuery();
      await renderWsHistory(ctx, u.id, Number(p.ws));
      return;
    }

    if (p.a === 'a:ws_profile') {
      await ctx.answerCallbackQuery();
      await renderWsProfile(ctx, u.id, Number(p.ws));
      return;
    }

    
    if (p.a === 'a:ws_share') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      if (!wsId) return;
      await renderWsShareMenu(ctx, u.id, wsId);
      return;
    }

    if (p.a === 'a:ws_share_send') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      if (!wsId) return;
      const v = String(p.v || 'short') === 'long' ? 'long' : 'short';
      await sendWsShareTextMessage(ctx, u.id, wsId, v);
      return;
    }


if (p.a === 'a:ws_ig_templates') {
  await ctx.answerCallbackQuery();
  const wsId = Number(p.ws || 0);
  if (!wsId) return;
  await renderWsIgTemplatesMenu(ctx, u.id, wsId);
  return;
}

if (p.a === 'a:ws_ig_templates_send') {
  await ctx.answerCallbackQuery();
  const wsId = Number(p.ws || 0);
  if (!wsId) return;
  const t = String(p.t || 'story');
  await sendWsIgTemplateMessage(ctx, u.id, wsId, t);
  return;
}


if (p.a === 'a:ws_ig_dm') {
  await ctx.answerCallbackQuery();
  const wsId = Number(p.ws || 0);
  if (!wsId) return;
  const tone = String(p.tone || 'soft');
  const i = Number(p.i || 0);
  await renderWsIgDmTemplate(ctx, u.id, wsId, tone, i);
  return;
}


if (p.a === 'a:ws_prof_mode') {
      await ctx.answerCallbackQuery();
      await renderWsProfileMode(ctx, u.id, Number(p.ws));
      return;
    }
    if (p.a === 'a:ws_prof_mode_set') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const mode = String(p.m || 'both');
      const allowed = ['channel', 'ugc', 'both'];
      if (!allowed.includes(mode)) return ctx.answerCallbackQuery({ text: 'Неверный режим.' });
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.setWorkspaceSetting(wsId, { profile_mode: mode });
      await db.auditWorkspace(wsId, u.id, 'ws.profile_mode_updated', { mode });
      await renderWsProfileMode(ctx, u.id, wsId);
      return;
    }

    if (p.a === 'a:ws_prof_verticals') {
      await ctx.answerCallbackQuery();
      await renderWsProfileVerticals(ctx, u.id, Number(p.ws));
      return;
    }
    if (p.a === 'a:ws_prof_vert_t') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const key = String(p.v || '');
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      const cur = Array.isArray(ws.profile_verticals) ? ws.profile_verticals.map(String) : [];
      const has = cur.includes(key);
      let next = cur.filter(x => x !== key);
      if (!has) {
        if (cur.length >= 3) {
          await ctx.answerCallbackQuery({ text: 'Максимум 3 ниши.', show_alert: true });
          return renderWsProfileVerticals(ctx, u.id, wsId);
        }
        next = [...cur, key];
      }
      await db.setWorkspaceSetting(wsId, { profile_verticals: next });
      await db.auditWorkspace(wsId, u.id, 'ws.profile_verticals_updated', { count: next.length });
      await renderWsProfileVerticals(ctx, u.id, wsId);
      return;
    }
    if (p.a === 'a:ws_prof_vert_clear') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.setWorkspaceSetting(wsId, { profile_verticals: [] });
      await db.auditWorkspace(wsId, u.id, 'ws.profile_verticals_cleared', {});
      await renderWsProfileVerticals(ctx, u.id, wsId);
      return;
    }

    if (p.a === 'a:ws_prof_formats') {
      await ctx.answerCallbackQuery();
      await renderWsProfileFormats(ctx, u.id, Number(p.ws));
      return;
    }
    if (p.a === 'a:ws_prof_fmt_t') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const key = String(p.f || '');
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      const cur = Array.isArray(ws.profile_formats) ? ws.profile_formats.map(String) : [];
      const has = cur.includes(key);
      let next = cur.filter(x => x !== key);
      if (!has) {
        if (cur.length >= 5) {
          await ctx.answerCallbackQuery({ text: 'Максимум 5 форматов.', show_alert: true });
          return renderWsProfileFormats(ctx, u.id, wsId);
        }
        next = [...cur, key];
      }
      await db.setWorkspaceSetting(wsId, { profile_formats: next });
      await db.auditWorkspace(wsId, u.id, 'ws.profile_formats_updated', { count: next.length });
      await renderWsProfileFormats(ctx, u.id, wsId);
      return;
    }
    if (p.a === 'a:ws_prof_fmt_clear') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.setWorkspaceSetting(wsId, { profile_formats: [] });
      await db.auditWorkspace(wsId, u.id, 'ws.profile_formats_cleared', {});
      await renderWsProfileFormats(ctx, u.id, wsId);
      return;
    }

    if (p.a === 'a:ws_prof_edit') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const field = String(p.f || 'title');
      const prompts = {
        title: '✍️ Введи название витрины (как тебя видит бренд).',
        niche: '✍️ Введи нишу (устар.) — лучше выбрать “🏷 Ниши”.',
        ig: '✍️ Пришли Instagram: @handle или ссылку на профиль (instagram.com/handle).\n\nЧтобы очистить поле — отправь “-”.',
        about: '✍️ Короткое описание (1–2 предложения).\n\nПример: “Тестирую косметику и делаю распаковки. Люблю честные обзоры.”',
        portfolio: '✍️ Пришли 1–3 ссылки на портфолио (каждая с новой строки или в одном сообщении).\n\nЧтобы очистить поле — отправь “-”.',
        contact: '✍️ Введи контакт (например: @username / ссылка / почта).',
        geo: '✍️ Введи город/гео.'
      };
      await ctx.editMessageText(prompts[field] || prompts.title, {
        reply_markup: new InlineKeyboard().text('⬅️ Отмена', `a:ws_profile|ws:${wsId}`)
      });
      await setExpectText(ctx.from.id, { type: 'ws_profile_edit', wsId, field });
      return;
    }

    if (p.a === 'a:ws_pro') {
      await ctx.answerCallbackQuery();
      await renderWsPro(ctx, u.id, Number(p.ws));
      return;
    }
    if (p.a === 'a:ws_pro_buy') {
      const { accept } = await getPaymentsRuntimeFlags();
      if (!accept) {
        return ctx.answerCallbackQuery({ text: '💤 Платежи на паузе. Попробуй позже.', show_alert: true });
      }
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const token = randomToken(10);
      await redis.set(k(['pay_pro', token]), { wsId, ownerUserId: u.id, tgId: ctx.from.id }, { ex: 15 * 60 });
      const payload = `pro_${wsId}_${u.id}_${token}`;
      await sendStarsInvoice(ctx, {
        title: 'MicroGiveaways PRO',
        description: 'PRO на 30 дней: чаще bump, больше офферов, пин в ленте, расширенная аналитика.',
        payload,
        amount: CFG.PRO_STARS_PRICE,
        backCb: `a:ws_pro|ws:${wsId}`,
      });
      return;
    }

    // Brand Pass (Stars) - buy credits to open new threads as a brand
    if (p.a === 'a:brand_buy') {
      const { accept } = await getPaymentsRuntimeFlags();
      if (!accept) {
        return ctx.answerCallbackQuery({ text: '💤 Платежи на паузе. Попробуй позже.', show_alert: true });
      }
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      const offerId = (p.o !== undefined && p.o !== null && p.o !== '') ? Number(p.o) : null;
      const packId = String(p.pack || 'S');
      const page = Number(p.p || 0);
      const pack = getBrandPack(packId);
      if (!pack) return ctx.answerCallbackQuery({ text: 'Пакет не найден.' });

      const token = randomToken(10);
      await redis.set(
        k(['pay_brand', token]),
        { tgId: ctx.from.id, userId: u.id, packId: pack.id, credits: pack.credits, wsId, offerId, page },
        { ex: 15 * 60 }
      );

      const payload = `brand_${u.id}_${pack.id}_${token}`;
      const back = offerId ? `a:bx_pub|ws:${wsId}|o:${offerId}|p:${page}` : `a:brand_pass|ws:${wsId}`;
      await sendStarsInvoice(ctx, {
        title: `Brand Pass · ${pack.credits} контактов`,
        description: 'Кредиты нужны только для открытия НОВОГО диалога. Переписка внутри диалога — бесплатна.',
        payload,
        amount: pack.stars,
        backCb: back,
      });
      return;
    }

    // Brand Mode tools


    // Brand Team (Brand Managers)

    if (p.a === 'a:brand_team') {
      await ctx.answerCallbackQuery();

      const gate = await ensureBrandTeamUnlocked(ctx, u);
      if (!gate) return;


      const managers = await db.listBrandManagers(u.id);
      const count = managers.length;

      const text = `👥 <b>Команда бренда</b>

Добавь менеджеров — они смогут быстрее отвечать на заявки и закрывать сделки.
У менеджера нет доступа к оплатам, профилю бренда и управлению командой.

Сейчас менеджеров: <b>${count}</b>`;

      await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: brandTeamKb() });
      return;
    }

    if (p.a === 'a:bm_invite') {
      await ctx.answerCallbackQuery();
      const gate = await ensureBrandTeamUnlocked(ctx, u);
      if (!gate) return;
      const token = randomToken(10);
      await redis.set(
        k(['bm_invite', token]),
        { brandUserId: u.id, addedByUserId: u.id },
        { ex: 24 * 3600 }
      );

      const link = `https://t.me/${CFG.BOT_USERNAME}?start=bminv_${token}`;
      const text = `🔗 <b>Приглашение менеджера</b>

Ссылка одноразовая, действует <b>24 часа</b>.
Отправь её человеку, которого хочешь добавить в команду:

${link}`;

      await ctx.editMessageText(text, {
        parse_mode: 'HTML',
        disable_web_page_preview: true,
        reply_markup: navKb('a:brand_team|ws:0'),
      });
      return;
    }

    if (p.a === 'a:bm_add_username') {
      await ctx.answerCallbackQuery();
      const gate = await ensureBrandTeamUnlocked(ctx, u);
      if (!gate) return;
      await setExpectText(ctx.from.id, { type: 'bm_username' });
      await ctx.editMessageText('Введи @username менеджера одним сообщением (пример: @manager).', {
        reply_markup: navKb('a:brand_team|ws:0'),
      });
      return;
    }

    if (p.a === 'a:bm_list') {
      await ctx.answerCallbackQuery();
      const gate = await ensureBrandTeamUnlocked(ctx, u);
      if (!gate) return;
      const managers = await db.listBrandManagers(u.id);
      if (!managers.length) {
        await ctx.editMessageText('Пока менеджеров нет. Добавь менеджера через приглашение или по @username.', {
          reply_markup: navKb('a:brand_team|ws:0'),
        });
        return;
      }

      const lines = managers.map((m) => {
        const label = m.tg_username ? `@${m.tg_username}` : `id:${m.tg_id}`;
        return `• ${escapeHtml(label)}`;
      }).join('\n');

      await ctx.editMessageText(`👥 <b>Менеджеры бренда</b>\n\n${lines}\n\nНажми на кнопку, чтобы удалить менеджера.`, {
        parse_mode: 'HTML',
        reply_markup: brandManagersListKb(managers),
      });
      return;
    }

    if (p.a === 'a:bm_rm_q') {
      await ctx.answerCallbackQuery();
      const gate = await ensureBrandTeamUnlocked(ctx, u);
      if (!gate) return;
      const managerUserId = Number(p.u || 0);
      if (!managerUserId) return;

      const info = await db.getUserTgIdByUserId(managerUserId);
      const label = info?.tg_username ? `@${info.tg_username}` : (info?.tg_id ? `id:${info.tg_id}` : `user #${managerUserId}`);

      await ctx.editMessageText(`Удалить менеджера <b>${escapeHtml(label)}</b> из команды бренда?`, {
        parse_mode: 'HTML',
        reply_markup: brandManagerRemoveConfirmKb(managerUserId),
      });
      return;
    }

    if (p.a === 'a:bm_rm_ok') {
      await ctx.answerCallbackQuery();
      const gate = await ensureBrandTeamUnlocked(ctx, u);
      if (!gate) return;
      const managerUserId = Number(p.u || 0);
      if (!managerUserId) return;
      await db.removeBrandManager(u.id, managerUserId);

      // Best-effort notification to removed manager
      let notifyOk = false;
      try {
        const mi = await db.getUserTgIdByUserId(managerUserId);
        const managerTgId = Number(mi?.tg_id || 0);
        if (managerTgId) {
          // clean up manager state if this brand was active
          try {
            const active = await getBmActiveBrand(managerTgId);
            if (active === u.id) await clearBmActiveBrand(managerTgId);
          } catch { }

          // if no more brands left -> disable manager mode
          try {
            const still = await db.listBrandsForManager(managerUserId);
            if (!still || !still.length) await disableBrandManagerState(managerTgId);
          } catch { }

          const prof = await safeBrandProfiles(() => db.getBrandProfile(u.id), async () => null);
          const brandLabel = prof?.brand_name ? String(prof.brand_name).trim()
            : (prof?.tg_username ? `@${String(prof.tg_username).trim()}` : `Бренд #${u.id}`);

          const msg = `⛔️ <b>Доступ отозван</b>\n\nТебя удалили из команды бренда <b>${escapeHtml(brandLabel)}</b>.\n\nЕсли у тебя есть другие бренды — открой кабинет менеджера и выбери бренд.`;
          const kb = new InlineKeyboard().text('🧑‍💼 Кабинет менеджера', 'a:bm_home');
          await ctx.api.sendMessage(managerTgId, msg, { parse_mode: 'HTML', reply_markup: kb });
          notifyOk = true;
        }
      } catch { }

      // refresh list
      const managers = await db.listBrandManagers(u.id);
      if (!managers.length) {
        await ctx.editMessageText('✅ Менеджер удалён. Сейчас менеджеров нет.', {
          reply_markup: navKb('a:brand_team|ws:0'),
        });
        return;
      }
      const lines = managers.map((m) => {
        const label = m.tg_username ? `@${m.tg_username}` : `id:${m.tg_id}`;
        return `• ${escapeHtml(label)}`;
      }).join('\n');

      const note = notifyOk ? '\n\n📩 Менеджеру отправлено уведомление.' : '';
      await ctx.editMessageText(`✅ Менеджер удалён.${note}\n\n👥 <b>Менеджеры бренда</b>\n\n${lines}`, {
        parse_mode: 'HTML',
        reply_markup: brandManagersListKb(managers),
      });
      return;
    }

    if (p.a === 'a:brand_profile') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      const ret = String(p.ret || 'brand'); // brand | offer | lead | verify
      const bo = p.bo ? Number(p.bo) : null;
      const bp = p.bp ? Number(p.bp) : 0;

      const bm = wsId === 0 ? await resolveBmBrandContext(ctx, u) : { enabled: false };
      if (wsId === 0 && bm.enabled && bm.brandUserId !== u.id) {
        await ctx.editMessageText(
          '⛔️ Недостаточно прав. Этот раздел доступен только владельцу бренда.',
          { parse_mode: 'HTML', reply_markup: navKb('a:menu') }
        );
        return;
      }

      await renderBrandProfileHome(ctx, u.id, { wsId, ret, backOfferId: bo, backPage: bp, edit: true });
      return;
    }

    if (p.a === 'a:brand_continue') {
      const wsId = Number(p.ws || 0);
      if (String(p.ret || '') !== 'lead' || !wsId) {
        await ctx.answerCallbackQuery();
        await renderBrandProfileHome(ctx, u.id, { wsId, ret: String(p.ret || 'brand'), backOfferId: p.bo ? Number(p.bo) : null, backPage: p.bp ? Number(p.bp) : 0, edit: true });
        return;
      }

      const prof = await safeBrandProfiles(() => db.getBrandProfile(u.id), async () => null);
      if (!isBrandBasicComplete(prof)) {
        await ctx.answerCallbackQuery({ text: 'Заполни 4 поля профиля (Название, Ниша, Контакт, Ссылка).', show_alert: true });
        await renderBrandProfileHome(ctx, u.id, { wsId, ret: 'lead', edit: true });
        return;
      }

      const contact = String(prof.contact || '').trim().slice(0, 200);
      await ctx.answerCallbackQuery();
      await setExpectText(ctx.from.id, { type: 'wsp_lead_step2', wsId, contact, brandName: String(prof.brand_name || '').trim() || null, brandLink: String(prof.brand_link || '').trim() || null });
      await renderWsLeadCompose(ctx, wsId, 2, { contact });
      return;
    }


    if (p.a === 'a:brand_prof_more') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      const ret = String(p.ret || 'brand');
      const bo = p.bo ? Number(p.bo) : null;
      const bp = p.bp ? Number(p.bp) : 0;
      await renderBrandProfileMore(ctx, u.id, { wsId, ret, backOfferId: bo, backPage: bp, edit: true });
      return;
    }

    if (p.a === 'a:brand_prof_set') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      const ret = String(p.ret || 'brand');
      const bo = p.bo ? Number(p.bo) : null;
      const bp = p.bp ? Number(p.bp) : 0;
      const field = String(p.f || '');
      const map = {
        bn: 'brand_name',
        bl: 'brand_link',
        ct: 'contact',
        ni: 'niche',
        ge: 'geo',
        ty: 'collab_types',
        bu: 'budget',
        go: 'goals',
        rq: 'requirements'
      };
      const realField = map[field] || null;
      if (!realField) return;

      // Structured multi-select for collaboration types (no free-text input)
      if (realField === 'collab_types') {
        await renderBrandCollabTypesPicker(ctx, u.id, { wsId, ret, backOfferId: bo, backPage: bp, edit: true });
        return;
      }

      await setExpectText(ctx.from.id, { type: 'brand_prof_field', field: realField, wsId, ret, backOfferId: bo, backPage: bp });
      await ctx.editMessageText(brandFieldPrompt(realField), {
        parse_mode: 'HTML',
        reply_markup: brandFieldPromptKb({ wsId, ret, backOfferId: bo, backPage: bp })
      });
      return;
    }

    // Brand profile: structured collab types multi-select
    if (p.a === 'a:brand_ty_t') {
      const wsId = Number(p.ws || 0);
      const ret = String(p.ret || 'brand');
      const bo = p.bo ? Number(p.bo) : null;
      const bp = p.bp ? Number(p.bp) : 0;
      const key = String(p.k || '');
      if (!BRAND_COLLAB_KEYS.has(key)) {
        await ctx.answerCallbackQuery();
        return;
      }

      await ctx.answerCallbackQuery();

      const prof = await safeBrandProfiles(
        () => db.getBrandProfile(u.id),
        async () => ({ __missing_relation: true })
      );
      if (prof && prof.__missing_relation) {
        await ctx.editMessageText('⚠️ В базе нет таблицы brand_profiles. Применяй миграцию migrations/024_brand_profiles.sql в Neon и повтори.', {
          reply_markup: navKb('a:menu')
        });
        return;
      }

      const current = parseBrandCollabTypes(String(prof?.collab_types || '').trim());
      const set = new Set(current);
      if (set.has(key)) set.delete(key); else set.add(key);
      const next = Array.from(set);
      const csv = brandCollabTypesToCsv(next);

      const saved = await safeBrandProfiles(
        () => db.upsertBrandProfile(u.id, { collab_types: csv }),
        async () => ({ __missing_relation: true })
      );
      if (saved && saved.__missing_relation) {
        await ctx.editMessageText('⚠️ В базе нет таблицы brand_profiles. Применяй миграцию migrations/024_brand_profiles.sql в Neon и повтори.', {
          reply_markup: navKb('a:menu')
        });
        return;
      }

      await renderBrandCollabTypesPicker(ctx, u.id, { wsId, ret, backOfferId: bo, backPage: bp, edit: true });
      return;
    }

    if (p.a === 'a:brand_ty_clear') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      const ret = String(p.ret || 'brand');
      const bo = p.bo ? Number(p.bo) : null;
      const bp = p.bp ? Number(p.bp) : 0;

      const saved = await safeBrandProfiles(
        () => db.upsertBrandProfile(u.id, { collab_types: null }),
        async () => ({ __missing_relation: true })
      );
      if (saved && saved.__missing_relation) {
        await ctx.editMessageText('⚠️ В базе нет таблицы brand_profiles. Применяй миграцию migrations/024_brand_profiles.sql в Neon и повтори.', {
          reply_markup: navKb('a:menu')
        });
        return;
      }

      await renderBrandCollabTypesPicker(ctx, u.id, { wsId, ret, backOfferId: bo, backPage: bp, edit: true });
      return;
    }

    if (p.a === 'a:brand_ty_done') {
      await ctx.answerCallbackQuery({ text: '✅ Сохранено' });
      const wsId = Number(p.ws || 0);
      const ret = String(p.ret || 'brand');
      const bo = p.bo ? Number(p.bo) : null;
      const bp = p.bp ? Number(p.bp) : 0;
      await renderBrandProfileMore(ctx, u.id, { wsId, ret, backOfferId: bo, backPage: bp, edit: true });
      return;
    }


    if (p.a === 'a:brand_prof_reset') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      const ret = String(p.ret || 'brand');
      const bo = p.bo ? Number(p.bo) : null;
      const bp = p.bp ? Number(p.bp) : 0;
      const suf = brandCbSuffix({ wsId, ret, backOfferId: bo, backPage: bp });

      const kb = new InlineKeyboard()
        .text('✅ Да, сбросить', `a:brand_prof_reset_ok${suf}`)
        .row()
        .text('⬅️ Отмена', `a:brand_profile${suf}`);

      const txt = `🧹 <b>Сбросить профиль бренда?</b>

Это удалит базовые и расширенные поля профиля. Действие необратимо.`;
      await ctx.editMessageText(txt, { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

    if (p.a === 'a:brand_prof_reset_ok') {
      const wsId = Number(p.ws || 0);
      const ret = String(p.ret || 'brand');
      const bo = p.bo ? Number(p.bo) : null;
      const bp = p.bp ? Number(p.bp) : 0;

      const res = await safeBrandProfiles(
        () => db.deleteBrandProfile(u.id),
        async () => ({ __missing_relation: true })
      );

      if (res && res.__missing_relation) {
        await ctx.answerCallbackQuery({ text: '⚠️ Не найдена таблица brand_profiles. Нужна миграция 024_brand_profiles.sql.', show_alert: true });
        await renderBrandProfileHome(ctx, u.id, { wsId, ret, backOfferId: bo, backPage: bp, edit: true });
        return;
      }

      await ctx.answerCallbackQuery({ text: '✅ Профиль сброшен.' });
      await renderBrandProfileHome(ctx, u.id, { wsId, ret, backOfferId: bo, backPage: bp, edit: true });
      return;
    }


    if (p.a === 'a:brand_pass') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);

      const bm = wsId === 0 ? await resolveBmBrandContext(ctx, u) : { enabled: false };
      if (wsId === 0 && bm.enabled && bm.brandUserId !== u.id) {
        await ctx.editMessageText(
          '⛔️ Недостаточно прав. Этот раздел доступен только владельцу бренда.',
          { parse_mode: 'HTML', reply_markup: navKb('a:menu') }
        );
        return;
      }

      await renderBrandPass(ctx, u.id, wsId);
      return;
    }

    if (p.a === 'a:brand_plan') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);

      const bm = wsId === 0 ? await resolveBmBrandContext(ctx, u) : { enabled: false };
      if (wsId === 0 && bm.enabled && bm.brandUserId !== u.id) {
        await ctx.editMessageText(
          '⛔️ Недостаточно прав. Этот раздел доступен только владельцу бренда.',
          { parse_mode: 'HTML', reply_markup: navKb('a:menu') }
        );
        return;
      }

      await renderBrandPlan(ctx, u.id, wsId);
      return;
    }

    if (p.a === 'a:brand_plan_buy') {
      const { accept } = await getPaymentsRuntimeFlags();
      if (!accept) {
        return ctx.answerCallbackQuery({ text: '💤 Платежи на паузе. Попробуй позже.', show_alert: true });
      }
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      const plan = String(p.plan || 'basic').toLowerCase();
      if (plan !== 'basic' && plan !== 'max') {
        return ctx.answerCallbackQuery({ text: 'План не найден.' });
      }
      const stars = plan === 'max' ? Number(CFG.BRAND_PLAN_MAX_PRICE) : Number(CFG.BRAND_PLAN_BASIC_PRICE);
      const token = randomToken(10);
      await redis.set(
        k(['pay_bplan', token]),
        { tgId: ctx.from.id, userId: u.id, wsId, plan, stars },
        { ex: 15 * 60 }
      );
      const payload = `bplan_${u.id}_${plan}_${token}`;
      const label = plan === 'max' ? 'Max' : 'Basic';
      await sendStarsInvoice(ctx, {
        title: `Brand Plan · ${label} · ${CFG.BRAND_PLAN_DURATION_DAYS} дней`,
        description: 'Подписка на инструменты бренда: CRM стадии, расширенная воронка, удобный менеджмент диалогов.',
        payload,
        amount: stars,
        backCb: `a:brand_plan|ws:${wsId}`,
      });
      return;
    }

    
    // Profile Matching (pm_*)
    if (p.a === 'a:pm_home') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);

      const bm = wsId === 0 ? await resolveBmBrandContext(ctx, u) : { enabled: false };
      const effectiveUserId = (wsId === 0 && bm.enabled) ? bm.brandUserId : u.id;

      await renderProfileMatchingHome(ctx, effectiveUserId, wsId);
      return;
    }

    if (p.a === 'a:pm_reset') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      await pmResetState(ctx.from.id, wsId);
      await renderProfileMatchingHome(ctx, u.id, wsId);
      return;
    }

    if (p.a === 'a:pm_pick') {
      await ctx.answerCallbackQuery();
      await renderProfileMatchingPick(ctx, u.id, Number(p.ws || 0), String(p.t || 'v'));
      return;
    }

    if (p.a === 'a:pm_tog') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      const type = String(p.t || 'v');
      const key = String(p.k || '');

      const st = await pmGetState(ctx.from.id, wsId);
      const sel = type === 'v' ? st.v : st.f;
      const max = type === 'v' ? PM_LIMITS.verticals : PM_LIMITS.formats;

      const has = sel.includes(key);
      let next = has ? sel.filter(x => x !== key) : [...sel, key];

      if (!has && next.length > max) {
        await ctx.answerCallbackQuery({ text: `Лимит: максимум ${max}`, show_alert: true });
        await renderProfileMatchingPick(ctx, u.id, wsId, type);
        return;
      }

      next = Array.from(new Set(next));
      if (type === 'v') st.v = next;
      else st.f = next;

      await pmSetState(ctx.from.id, wsId, st);
      await renderProfileMatchingPick(ctx, u.id, wsId, type);
      return;
    }

    if (p.a === 'a:pm_run') {
      await ctx.answerCallbackQuery();
      await renderProfileMatchingResults(ctx, u.id, Number(p.ws || 0), Number(p.p || 0));
      return;
    }

    if (p.a === 'a:pm_view') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      const target = Number(p.id || 0);
      const page = Number(p.p || 0);
      if (!target) return;
      await renderWsPublicProfile(ctx, target, { backCb: `a:pm_run|ws:${wsId}|p:${page}` });
      return;
    }


if (p.a === 'a:match_home') {
      await ctx.answerCallbackQuery();
      await renderMatchingHome(ctx, Number(p.ws || 0));
      return;
    }

    if (p.a === 'a:match_buy') {
      const { accept } = await getPaymentsRuntimeFlags();
      if (!accept) {
        return ctx.answerCallbackQuery({ text: '💤 Платежи на паузе. Попробуй позже.', show_alert: true });
      }
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      const tierId = String(p.tier || 'S').toUpperCase();
      const tier = MATCH_TIERS.find(t => t.id === tierId);
      if (!tier) return ctx.answerCallbackQuery({ text: 'Тариф не найден.' });

      const token = randomToken(10);
      await redis.set(
        k(['pay_match', token]),
        { tgId: ctx.from.id, userId: u.id, wsId, tierId: tier.id, stars: tier.stars, count: tier.count },
        { ex: 15 * 60 }
      );
      const payload = `match_${u.id}_${tier.id}_${token}`;
      await sendStarsInvoice(ctx, {
        title: `Smart Matching · ${tier.title}`,
        description: 'Подбор подходящих микро-каналов под твой бриф. После оплаты отправь бриф одним сообщением.',
        payload,
        amount: tier.stars,
        backCb: `a:match_home|ws:${wsId}`,
      });
      return;
    }

    if (p.a === 'a:feat_home') {
      await ctx.answerCallbackQuery();
      await renderFeaturedHome(ctx, u.id, Number(p.ws || 0));
      return;
    }

    if (p.a === 'a:feat_buy') {
      const { accept } = await getPaymentsRuntimeFlags();
      if (!accept) {
        return ctx.answerCallbackQuery({ text: '💤 Платежи на паузе. Попробуй позже.', show_alert: true });
      }
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      const durId = String(p.dur || '1d');
      const d = FEATURED_DURATIONS.find(x => x.id === durId);
      if (!d) return ctx.answerCallbackQuery({ text: 'Тариф не найден.' });

      const token = randomToken(10);
      await redis.set(
        k(['pay_feat', token]),
        { tgId: ctx.from.id, userId: u.id, wsId, days: d.days, durId: d.id, stars: d.stars },
        { ex: 15 * 60 }
      );
      const payload = `feat_${u.id}_${d.days}_${token}`;
      await sendStarsInvoice(ctx, {
        title: `Featured · ${d.title}`,
        description: 'Твой блок появится сверху в ленте у всех (бренд + блогеры). После оплаты отправь контент.',
        payload,
        amount: d.stars,
        backCb: `a:feat_home|ws:${wsId}`,
      });
      return;
    }

    if (p.a === 'a:feat_view') {
      await ctx.answerCallbackQuery();
      await renderFeaturedView(ctx, u.id, Number(p.ws || 0), Number(p.id), Number(p.p || 0));
      return;
    }

    if (p.a === 'a:feat_stop') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      const id = Number(p.id);
      const ok = await db.stopFeaturedPlacement(id, u.id);
      if (!ok) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery({ text: 'Остановлено.' });
      await renderBxFeed(ctx, u.id, wsId, Number(p.p || 0));
      return;
    }
    if (p.a === 'a:ws_pro_pin') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const isPro = await db.isWorkspacePro(wsId);
      if (!isPro) return ctx.answerCallbackQuery({ text: 'Доступно только в PRO.' });
      const offers = await db.listMyBarterOffers(wsId);
      const kb = new InlineKeyboard();
      for (const o of offers.filter(x => x.status !== 'DELETED')) {
        kb.text(`#${o.id} ${String(o.title || '').slice(0, 30)}`, `a:ws_pro_pin_set|ws:${wsId}|o:${o.id}`).row();
      }
      kb.text('❌ Снять пин', `a:ws_pro_pin_clear|ws:${wsId}`).row();
      kb.text('⬅️ Назад', `a:ws_pro|ws:${wsId}`);
      await ctx.editMessageText('📌 Выбери оффер для пина в ленте (PRO):', { reply_markup: kb });
      return;
    }
    if (p.a === 'a:ws_pro_pin_set') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const isPro = await db.isWorkspacePro(wsId);
      if (!isPro) return ctx.answerCallbackQuery({ text: 'Доступно только в PRO.' });
      await db.setWorkspacePinnedOffer(wsId, offerId);
      await db.auditWorkspace(wsId, u.id, 'ws.pro_pinned_offer', { offerId });
      await renderWsPro(ctx, u.id, wsId);
      return;
    }
    if (p.a === 'a:ws_pro_pin_clear') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.setWorkspacePinnedOffer(wsId, null);
      await db.auditWorkspace(wsId, u.id, 'ws.pro_pinned_offer', { offerId: null });
      await renderWsPro(ctx, u.id, wsId);
      return;
    }

    // Admin / Moderation
    if (p.a === 'a:admin_home') {
      await ctx.answerCallbackQuery();
      const isAdmin = isSuperAdminTg(ctx.from.id);
      if (!isAdmin) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      await renderAdminHome(ctx);
      return;
    }

    if (p.a === 'a:admin_metrics') {
      await ctx.answerCallbackQuery();
      const isAdmin = isSuperAdminTg(ctx.from.id);
      if (!isAdmin) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const days = Math.max(1, Math.min(90, Number(p.d) || 14));
      await renderAdminMetrics(ctx, days);
      return;
    }
    if (p.a === 'a:admin_mod_list') {
      await ctx.answerCallbackQuery();
      const isAdmin = isSuperAdminTg(ctx.from.id);
      if (!isAdmin) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await renderAdminModerators(ctx);
      return;
    }

    if (p.a === 'a:admin_mod_add') {
      const isAdmin = isSuperAdminTg(ctx.from.id);
      if (!isAdmin) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('➕ Введи @username модератора (он должен иметь username).', { reply_markup: new InlineKeyboard().text('⬅️ Отмена', 'a:admin_home') });
      await setExpectText(ctx.from.id, { type: 'admin_add_mod_username' });
      return;
    }
    if (p.a === 'a:admin_mod_rm') {
      const isAdmin = isSuperAdminTg(ctx.from.id);
      if (!isAdmin) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery({ text: 'Удалено.' });
      await db.removeNetworkModerator(Number(p.uid));
      await renderAdminModerators(ctx);
      return;
    }

    // Admin: Payments toggles / ledger
    if (p.a === 'a:admin_pay_accept_toggle') {
      const isAdmin = isSuperAdminTg(ctx.from.id);
      if (!isAdmin) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      const cur = await getSysBool(SYS_KEYS.pay_accept, CFG.PAYMENTS_ACCEPT_DEFAULT);
      await setSysBool(SYS_KEYS.pay_accept, !cur);
      await renderAdminHome(ctx);
      return;
    }
    if (p.a === 'a:admin_pay_auto_toggle') {
      const isAdmin = isSuperAdminTg(ctx.from.id);
      if (!isAdmin) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      const cur = await getSysBool(SYS_KEYS.pay_auto_apply, CFG.PAYMENTS_AUTO_APPLY_DEFAULT);
      await setSysBool(SYS_KEYS.pay_auto_apply, !cur);
      await renderAdminHome(ctx);
      return;
    }
    if (p.a === 'a:admin_payments') {
      const isAdmin = isSuperAdminTg(ctx.from.id);
      if (!isAdmin) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      await renderAdminPayments(ctx, String(p.st || 'ORPHANED'), Number(p.p || 0));
      return;
    }
    if (p.a === 'a:admin_pay_view') {
      const isAdmin = isSuperAdminTg(ctx.from.id);
      if (!isAdmin) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      await renderAdminPaymentView(ctx, Number(p.id), String(p.st || 'ORPHANED'), Number(p.p || 0));
      return;
    }
    if (p.a === 'a:admin_pay_apply') {
      const isAdmin = isSuperAdminTg(ctx.from.id);
      if (!isAdmin) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      await adminApplyPayment(ctx, u, Number(p.id), String(p.st || 'ORPHANED'), Number(p.p || 0));
      return;
    }

    if (p.a === 'a:mod_home') {
      await ctx.answerCallbackQuery();
      const isMod = await isModerator(u, ctx.from.id);
      if (!isMod) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await renderModHome(ctx);
      return;
    }
    if (p.a === 'a:mod_reports') {
      await ctx.answerCallbackQuery();
      const isMod = await isModerator(u, ctx.from.id);
      if (!isMod) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await renderModReports(ctx, Number(p.p || 0));
      return;
    }
    if (p.a === 'a:mod_report') {
      await ctx.answerCallbackQuery();
      const isMod = await isModerator(u, ctx.from.id);
      if (!isMod) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await renderModReportView(ctx, Number(p.r));
      return;
    }
    if (p.a === 'a:mod_r_freeze') {
      await ctx.answerCallbackQuery();
      const isMod = await isModerator(u, ctx.from.id);
      if (!isMod) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const rid = Number(p.r);
      const rep = await db.getBarterReport(rid);
      if (rep && rep.offer_id) {
        await db.moderatorFreezeBarterOffer(rep.offer_id);
        await db.auditBarterOffer(rep.offer_id, u.id, 'offer.frozen', { reportId: rid });
      }
      await renderModReportView(ctx, rid);
      return;
    }
    if (p.a === 'a:mod_r_close') {
      await ctx.answerCallbackQuery();
      const isMod = await isModerator(u, ctx.from.id);
      if (!isMod) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const rid = Number(p.r);
      const rep = await db.getBarterReport(rid);
      if (rep && rep.thread_id) {
        await db.moderatorCloseBarterThread(rep.thread_id);
        await db.auditBarterThread(rep.thread_id, u.id, 'thread.closed_by_mod', { reportId: rid });
      }
      await renderModReportView(ctx, rid);
      return;
    }
    if (p.a === 'a:mod_r_resolve') {
      await ctx.answerCallbackQuery();
      const isMod = await isModerator(u, ctx.from.id);
      if (!isMod) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const rid = Number(p.r);
      await db.resolveBarterReport(rid, u.id);
      await renderModReportView(ctx, rid);
      return;
    }

    if (p.a === 'a:mod_verifs') {
      await ctx.answerCallbackQuery();
      const isMod = await isModerator(u, ctx.from.id);
      if (!isMod) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      if (!CFG.VERIFICATION_ENABLED) return ctx.answerCallbackQuery({ text: 'Функция отключена.' });
      await renderModVerifs(ctx, Number(p.p || 0));
      return;
    }
    if (p.a === 'a:mod_verif_view') {
      await ctx.answerCallbackQuery();
      const isMod = await isModerator(u, ctx.from.id);
      if (!isMod) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      if (!CFG.VERIFICATION_ENABLED) return ctx.answerCallbackQuery({ text: 'Функция отключена.' });
      await renderModVerifView(ctx, Number(p.uid), Number(p.p || 0));
      return;
    }
    if (p.a === 'a:mod_verif_approve') {
      await ctx.answerCallbackQuery({ text: '✅ Approved' });
      const isMod = await isModerator(u, ctx.from.id);
      if (!isMod) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      if (!CFG.VERIFICATION_ENABLED) return ctx.answerCallbackQuery({ text: 'Функция отключена.' });
      const targetUserId = Number(p.uid);
      await safeUserVerifications(() => db.setVerificationStatus(targetUserId, 'APPROVED', u.id, null), async () => null);
      try {
        await ctx.api.sendMessage(Number((await db.getUserById(targetUserId))?.tg_id), '✅ Ты верифицирован(а)! Теперь рядом с твоими офферами будет значок ✅.', { parse_mode: 'HTML' });
      } catch {}
      await renderModVerifView(ctx, targetUserId, Number(p.p || 0));
      return;
    }
    if (p.a === 'a:mod_verif_reject') {
      await ctx.answerCallbackQuery();
      const isMod = await isModerator(u, ctx.from.id);
      if (!isMod) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      if (!CFG.VERIFICATION_ENABLED) return ctx.answerCallbackQuery({ text: 'Функция отключена.' });
      const targetUserId = Number(p.uid);
      await setExpectText(ctx.from.id, { type: 'mod_verif_reject_reason', targetUserId, page: Number(p.p || 0) });
      await ctx.editMessageText('❌ Напиши причиной отказа одним сообщением (текст), и я отправлю пользователю.', { reply_markup: new InlineKeyboard().text('⬅️ Отмена', `a:mod_verif_view|uid:${targetUserId}|p:${Number(p.p || 0)}`) });
      return;
    }


    // Barters
    if (p.a === 'a:bx_home') {
      await ctx.answerCallbackQuery();
      const ws = await ensureWorkspaceForOwner(ctx, u.id);
      if (!ws) return;
      await renderBxOpen(ctx, u.id, ws.id);
      return;
    }

    if (p.a === 'a:bx_open') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      if (wsId === 0) await setUiMode(ctx.from.id, UI_MODES.BRAND);
      if (wsId === 0) await maybeSendBanner(ctx, 'brand', CFG.BRAND_BANNER_FILE_ID);

      const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_open', 0);
      if (!bmRes) return;

      await renderBxOpen(ctx, bmRes.userId, wsId);
      return;
    }

    if (p.a === 'a:bx_enable_net') {
      const wsId = Number(p.ws);
      await renderNetConfirm(ctx, u.id, wsId, 'bx');
      return;
    }

    if (p.a === 'a:bx_feed') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const page = Number(p.p || 0);

      const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_feed', page);
      if (!bmRes) return;

      await renderBxFeed(ctx, bmRes.userId, wsId, page);
      return;
    }

    if (p.a === 'a:bx_filters') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const page = Number(p.p || 0);

      const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_feed', page);
      if (!bmRes) return;

      await renderBxFilters(ctx, bmRes.userId, wsId, page);
      return;
    }

    if (p.a === 'a:bx_fpick') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const page = Number(p.p || 0);
      const key = String(p.k || '');

      const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_feed', page);
      if (!bmRes) return;

      // Open picker (do NOT change the filter here)
      await renderBxFilterPick(ctx, bmRes.userId, wsId, key, page);
      return;
    }

    if (p.a === 'a:bx_fset') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const page = Number(p.p || 0);
      const keyRaw = String(p.k || '');
      const vRaw = p.v ? String(p.v) : null;

      // UI uses short keys (cat/type/comp). Storage uses canonical keys.
      const key = keyRaw === 'cat'
        ? 'category'
        : (keyRaw === 'type' ? 'offerType' : (keyRaw === 'comp' ? 'compensationType' : keyRaw));
      const v = vRaw === 'all' ? null : vRaw;

      const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_feed', page);
      if (!bmRes) return;

      await setBxFilter(ctx.from.id, wsId, { [key]: v });
      await renderBxFilters(ctx, bmRes.userId, wsId, page);
      return;
    }

    if (p.a === 'a:bx_freset') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const page = Number(p.p || 0);

      const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_feed', page);
      if (!bmRes) return;

      await setBxFilter(ctx.from.id, wsId, { category: null, offerType: null, compensationType: null });
      await renderBxFilters(ctx, bmRes.userId, wsId, page);
      return;
    }

    if (p.a === 'a:bx_pub') {
      await ctx.answerCallbackQuery();
      await renderBxPublicView(ctx, u.id, Number(p.ws), Number(p.o), Number(p.p || 0));
      return;
    }



    if (p.a === 'a:off_manage') {
      await ctx.answerCallbackQuery();
      await renderOfficialManageView(ctx, u.id, Number(p.ws), Number(p.o), Number(p.p || 0));
      return;
    }

    if (p.a === 'a:off_req_home') {
      await ctx.answerCallbackQuery();
      await renderOfficialRequestHome(ctx, u.id, Number(p.ws), Number(p.o), Number(p.p || 0));
      return;
    }

    if (p.a === 'a:off_req') {
      await ctx.answerCallbackQuery();
      if (!CFG.OFFICIAL_PUBLISH_ENABLED) {
        await ctx.answerCallbackQuery({ text: 'Фича отключена.', show_alert: true });
        return;
      }
      const mode = String(CFG.OFFICIAL_PUBLISH_MODE || 'manual').toLowerCase();
      if (!(mode === 'manual' || mode === 'mixed')) {
        await ctx.answerCallbackQuery({ text: 'Очередь доступна только в manual/mixed.', show_alert: true });
        return;
      }

      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const days = Math.max(1, Math.min(365, Number(p.days || 0) || Number(CFG.OFFICIAL_MANUAL_DEFAULT_DAYS || 3)));

      const offer = await db.getBarterOfferPublic(offerId);
      if (!offer) {
        await ctx.answerCallbackQuery({ text: 'Оффер не найден.', show_alert: true });
        return;
      }

      const isOwner = Number(offer.owner_user_id) === Number(u.id);
      const isMod = await isModerator(u, ctx.from.id);
      if (!isOwner && !isMod) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.', show_alert: true });
        return;
      }

      const channelId = Number(CFG.OFFICIAL_CHANNEL_ID || 0);
      if (!channelId) {
        await ctx.answerCallbackQuery({ text: 'OFFICIAL_CHANNEL_ID не задан.', show_alert: true });
        return;
      }

      const slotExpiresAt = new Date(Date.now() + days * 24 * 60 * 60 * 1000).toISOString();

      try {
        await safeOfficialPosts(
          () => db.upsertOfficialPostDraft({
            offerId,
            channelChatId: channelId,
            placementType: 'MANUAL',
            slotDays: days,
            slotExpiresAt
          }),
          async () => null
        );
      } catch (e) {
        await ctx.answerCallbackQuery({ text: `Ошибка: ${String(e?.message || e)}`.slice(0, 190), show_alert: true });
        return;
      }

      await ctx.answerCallbackQuery({ text: '✅ Заявка добавлена в очередь.', show_alert: false });
      await renderOfficialManageView(ctx, u.id, wsId, offerId, Number(p.p || 0));
      return;
    }

    if (p.a === 'a:off_req_cancel') {
      await ctx.answerCallbackQuery();
      if (!CFG.OFFICIAL_PUBLISH_ENABLED) {
        await ctx.answerCallbackQuery({ text: 'Фича отключена.', show_alert: true });
        return;
      }
      const wsId = Number(p.ws);
      const offerId = Number(p.o);

      const offer = await db.getBarterOfferPublic(offerId);
      if (!offer) {
        await ctx.answerCallbackQuery({ text: 'Оффер не найден.', show_alert: true });
        return;
      }

      const isOwner = Number(offer.owner_user_id) === Number(u.id);
      const isMod = await isModerator(u, ctx.from.id);
      if (!isOwner && !isMod) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.', show_alert: true });
        return;
      }

      try {
        await safeOfficialPosts(() => db.setOfficialPostStatus(offerId, 'REMOVED'), async () => null);
      } catch (e) {
        await ctx.answerCallbackQuery({ text: `Ошибка: ${String(e?.message || e)}`.slice(0, 190), show_alert: true });
        return;
      }

      await ctx.answerCallbackQuery({ text: '🗑 Заявка отменена.', show_alert: false });
      await renderOfficialManageView(ctx, u.id, wsId, offerId, Number(p.p || 0));
      return;
    }

    if (p.a === 'a:off_buy_home') {
      await ctx.answerCallbackQuery();
      await renderOfficialBuyHome(ctx, u.id, Number(p.ws), Number(p.o), Number(p.p || 0));
      return;
    }

    if (p.a === 'a:off_buy') {
      await ctx.answerCallbackQuery();
      if (!CFG.OFFICIAL_PUBLISH_ENABLED) {
        await ctx.answerCallbackQuery({ text: 'Фича отключена.', show_alert: true });
        return;
      }
      if (!['paid', 'mixed'].includes(CFG.OFFICIAL_PUBLISH_MODE)) {
        await ctx.answerCallbackQuery({ text: 'Покупка размещения выключена.', show_alert: true });
        return;
      }

      const pay = await getPaymentMode();
      if (!pay.accept) {
        await ctx.answerCallbackQuery({ text: 'Платежи временно отключены.', show_alert: true });
        return;
      }

      const offerId = Number(p.o);
      const wsId = Number(p.ws);
      const durId = String(p.dur || '').trim();
      const d = OFFICIAL_DURATIONS.find((x) => x.id === durId);
      if (!d) {
        await ctx.answerCallbackQuery({ text: 'Неверная длительность.', show_alert: true });
        return;
      }

      const offer = await db.getBarterOfferPublic(offerId);
      if (!offer) {
        await ctx.answerCallbackQuery({ text: 'Оффер не найден.', show_alert: true });
        return;
      }
      if (Number(offer.owner_user_id) !== Number(u.id)) {
        await ctx.answerCallbackQuery({ text: 'Покупать может только владелец Workspace.', show_alert: true });
        return;
      }

      const token = randomToken(16);
      await redis.setEx(
        k(['pay', 'offpub', token]),
        60 * 60,
        JSON.stringify({
          tgId: ctx.from.id,
          userId: u.id,
          offerId,
          days: d.days,
          stars: d.price,
          createdAt: Date.now()
        })
      );

      const title = 'Размещение в официальном канале';
      const description = `${d.label} • оффер #${offerId}`;
      const okInv = await sendStarsInvoice(ctx, {
        title,
        description,
        payload: `offpub_${u.id}_${offerId}_${d.days}_${token}`,
        amount: d.price,
        backCb: `a:off_manage|ws:${wsId}|o:${offerId}`,
      });
      if (!okInv) return;

      await ctx.editMessageText(
        `💳 Счёт выставлен на **${d.price}⭐️**.

Оплати Stars — и оффер попадёт в очередь на публикацию в офиц.канале.\n\nПосле оплаты модератор нажмёт Apply и поставит пост в канал.`,
        {
          parse_mode: 'Markdown',
          reply_markup: new InlineKeyboard()
            .text('⬅️ Назад', `a:off_buy_home|ws:${wsId}|o:${offerId}|p:${Number(p.p || 0)}`)
            .row()
            .text('🏠 Меню', 'a:menu')
        }
      );
      return;
    }

    if (p.a === 'a:off_pub') {
      await ctx.answerCallbackQuery();
      if (!CFG.OFFICIAL_PUBLISH_ENABLED) {
        await ctx.answerCallbackQuery({ text: 'Фича отключена.', show_alert: true });
        return;
      }

      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const can = await isModerator(u, ctx.from.id);
      if (!can) {
        await ctx.answerCallbackQuery({ text: 'Нет прав.', show_alert: true });
        return;
      }

      const mode = String(CFG.OFFICIAL_PUBLISH_MODE || 'manual').toLowerCase();
      let placementType = 'MANUAL';
      let days = Number(CFG.OFFICIAL_MANUAL_DEFAULT_DAYS || 3);
      let paymentId = null;

      // Commit F: in paid mode allow publish ONLY for paid PENDING record (with payment_id)
      const post = await safeOfficialPosts(() => db.getOfficialPostByOfferId(offerId), async () => null);
      const postStatus = String(post?.status || '').toUpperCase();
      const isPaidPending = postStatus === 'PENDING' && !!post?.payment_id;

      if (mode === 'paid') {
        if (!isPaidPending) {
          await ctx.answerCallbackQuery({ text: 'Нет оплаченной заявки в очереди (PENDING).', show_alert: true });
          await renderOfficialManageView(ctx, u.id, wsId, offerId, Number(p.p || 0));
          return;
        }
        placementType = 'PAID';
        days = Math.max(1, Number(post?.slot_days || days));
        paymentId = Number(post.payment_id);
      } else if (mode === 'manual' || mode === 'mixed') {
        // In mixed mode we prefer paid placement if it exists
        if (isPaidPending) {
          placementType = 'PAID';
          days = Math.max(1, Number(post?.slot_days || days));
          paymentId = Number(post.payment_id);
        }
      } else {
        await ctx.answerCallbackQuery({ text: 'Публикация отключена этим режимом.', show_alert: true });
        return;
      }

      try {
        await publishOfferToOfficialChannel(ctx.api, offerId, {
          placementType,
          days,
          paymentId,
          publishedByUserId: u.id,
          keepExpiry: false,
        });
      } catch (e) {
        try {
          await db.setOfficialPostStatus(offerId, 'ERROR', { lastError: String(e?.message || e) });
        } catch (_) {}
        await ctx.answerCallbackQuery({ text: `Ошибка: ${String(e?.message || e)}`.slice(0, 190), show_alert: true });
      }
      await renderOfficialManageView(ctx, u.id, wsId, offerId, Number(p.p || 0));
      return;
    }

    if (p.a === 'a:off_upd') {
      await ctx.answerCallbackQuery();
      if (!CFG.OFFICIAL_PUBLISH_ENABLED) {
        await ctx.answerCallbackQuery({ text: 'Фича отключена.', show_alert: true });
        return;
      }
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const can = await isModerator(u, ctx.from.id);
      if (!can) {
        await ctx.answerCallbackQuery({ text: 'Нет прав.', show_alert: true });
        return;
      }
      try {
        await publishOfferToOfficialChannel(ctx.api, offerId, {
          placementType: 'UPDATE',
          keepExpiry: true,
          publishedByUserId: u.id
        });
      } catch (e) {
        try { await db.setOfficialPostStatus(offerId, 'ERROR', { lastError: String(e?.message || e) }); } catch (_) {}
        await ctx.answerCallbackQuery({ text: `Ошибка: ${String(e?.message || e)}`.slice(0, 190), show_alert: true });
      }
      await renderOfficialManageView(ctx, u.id, wsId, offerId, Number(p.p || 0));
      return;
    }

    if (p.a === 'a:off_rm') {
      await ctx.answerCallbackQuery();
      if (!CFG.OFFICIAL_PUBLISH_ENABLED) {
        await ctx.answerCallbackQuery({ text: 'Фича отключена.', show_alert: true });
        return;
      }
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const can = await isModerator(u, ctx.from.id);
      if (!can) {
        await ctx.answerCallbackQuery({ text: 'Нет прав.', show_alert: true });
        return;
      }
      try {
        await removeOfficialOfferPost(ctx.api, offerId, 'REMOVED');
      } catch (e) {
        try { await db.setOfficialPostStatus(offerId, 'ERROR', { lastError: String(e?.message || e) }); } catch (_) {}
        await ctx.answerCallbackQuery({ text: `Ошибка: ${String(e?.message || e)}`.slice(0, 190), show_alert: true });
      }
      await renderOfficialManageView(ctx, u.id, wsId, offerId, Number(p.p || 0));
      return;
    }

    if (p.a === 'a:off_queue') {
      await ctx.answerCallbackQuery();
      if (!CFG.OFFICIAL_PUBLISH_ENABLED) {
        await ctx.editMessageText('Фича отключена.');
        return;
      }
      const can = await isModerator(u, ctx.from.id);
      if (!can) {
        await ctx.answerCallbackQuery({ text: 'Нет прав.', show_alert: true });
        return;
      }
      await renderOfficialQueue(ctx, u.id, Number(p.p || 0));
      return;
    }
    if (p.a === 'a:bx_report_offer') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      await ctx.editMessageText('🚩 Опиши проблему одним сообщением (почему жалоба).', {
        reply_markup: new InlineKeyboard().text('⬅️ Отмена', `a:bx_pub|ws:${wsId}|o:${offerId}|p:${Number(p.p || 0)}`)
      });
      await setExpectText(ctx.from.id, { type: 'bx_report', kind: 'offer', wsId, offerId, page: Number(p.p || 0) });
      return;
    }

    if (p.a === 'a:bx_report_thread') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const threadId = Number(p.t);
      await ctx.editMessageText('🚩 Опиши проблему одним сообщением (почему жалоба).', {
        reply_markup: new InlineKeyboard().text('⬅️ Отмена', `a:bx_thread|ws:${wsId}|t:${threadId}|p:${Number(p.p || 0)}`)
      });
      await setExpectText(ctx.from.id, { type: 'bx_report', kind: 'thread', wsId, threadId, page: Number(p.p || 0) });
      return;
    }
    if (p.a === 'a:bx_msg') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const page = Number(p.p || 0);

      // Brand Manager in Brand Mode (ws:0): act as selected brand (brandUserId)
      let actorUserId = u.id;
      let bm = { enabled: false };
      if (wsId === 0) {
        const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_feed', page);
        if (!bmRes) return;
        actorUserId = bmRes.userId;
        bm = bmRes.bm || { enabled: false };
      }

      // Brand profile gate (Brand Mode): require 4-step basic profile before messaging creators
      if (wsId === 0 && CFG.BRAND_PROFILE_REQUIRED) {
        const prof = await safeBrandProfiles(() => db.getBrandProfile(actorUserId), async () => null);
        if (!isBrandBasicComplete(prof)) {
          if (bm.enabled) {
            await ctx.answerCallbackQuery({
              text: '⚠️ Профиль бренда не заполнен. Попроси владельца бренда заполнить 4 базовых поля (Название, Ниша, Контакт, Ссылка).',
              show_alert: true
            });
            await renderBxPublicView(ctx, actorUserId, wsId, offerId, page);
            return;
          }

          await ctx.answerCallbackQuery({
            text: '⚠️ Заполни профиль бренда (4 шага), чтобы писать креаторам.',
            show_alert: true
          });
          await renderBrandProfileHome(ctx, actorUserId, { wsId, ret: 'offer', backOfferId: offerId, backPage: page, edit: true });
          return;
        }
      }

      if (CFG.RATE_LIMIT_ENABLED) {
        try {
          const rl = await rateLimit(
            k(['rl', 'intro', actorUserId]),
            { limit: CFG.INTRO_RATE_LIMIT, windowSec: CFG.INTRO_RATE_WINDOW_SEC }
          );
          if (!rl.allowed) {
            await ctx.answerCallbackQuery({
              text: `⏳ Слишком часто. Подожди ${fmtWait(rl.resetSec)} и попробуй снова.`,
              show_alert: true
            });
            return;
          }
        } catch {}
      }

      await ctx.answerCallbackQuery();
      db.trackEvent('intro_attempt', {
        userId: actorUserId,
        wsId: wsId || null,
        meta: {
          offerId,
          brandMode: wsId === 0,
          ...(bm.enabled ? { actingManagerTgId: Number(u.tg_id || 0) || Number(ctx.from?.id || 0) } : {})
        }
      });

      // Pricing / limits (configurable)
      const cost = Math.max(1, Number(CFG.INTRO_COST_PER_INTRO || 1));
      const trialCredits = Math.max(0, Number(CFG.INTRO_TRIAL_CREDITS || 0));

      let isVerified = false;
      if (CFG.VERIFICATION_ENABLED) {
        const v = await safeUserVerifications(() => db.getUserVerification(actorUserId), async () => null);
        isVerified = String(v?.status || '').toUpperCase() === 'APPROVED';
      }
      const dailyLimit = Math.max(0, Number(isVerified ? CFG.INTRO_DAILY_LIMIT : CFG.INTRO_DAILY_LIMIT_UNVERIFIED));

      const res = await db.getOrCreateBarterThreadWithCredits(
        offerId,
        actorUserId,
        {
          ...(wsId === 0 ? { forceBrand: true } : {}),
          cost,
          trialCredits,
          dailyLimit: dailyLimit > 0 ? dailyLimit : null,
          retryEnabled: CFG.INTRO_RETRY_ENABLED
        }
      );

      if (!res) {
        return ctx.answerCallbackQuery({ text: 'Не получилось открыть диалог. Возможно оффер закрыт.' });
      }

      if (res.limitReached) {
        const lim = Number(res.dailyLimit || dailyLimit || 0);
        const used = Number(res.dailyUsed || 0);
        db.trackEvent('intro_blocked_daily_limit', { userId: actorUserId, wsId: wsId || null, meta: { offerId, lim, used } });
        await ctx.answerCallbackQuery({ text: `Лимит интро на сегодня: ${lim} (использовано: ${used}). Попробуй завтра.`, show_alert: true });
        return;
      }

      if (res.needPaywall) {
        db.trackEvent('paywall_shown', { userId: actorUserId, wsId: wsId || null, meta: { offerId, cost, balance: Number(res.balance ?? 0), usedToday: Number(res.dailyUsed ?? 0), dailyLimit: Number(res.dailyLimit ?? dailyLimit ?? 0) } });
        await renderBrandPaywall(ctx, actorUserId, wsId, offerId, page);
        return;
      }

      if (!res.ok || !res.thread) {
        return ctx.answerCallbackQuery({ text: 'Не получилось открыть диалог. Возможно оффер закрыт.' });
      }

      db.trackEvent('thread_opened', { userId: actorUserId, wsId: wsId || null, meta: { offerId, threadId: res.thread.id, charged: !!res.charged, chargedAmount: Number(res.chargedAmount || cost || 1) } });

      if (res.charged) {
        const left = Number(res.balance ?? 0);
        const amt = Number(res.chargedAmount || cost || 1);
        const bonus = res.trialGranted ? '🎁 Бонус активирован. ' : '';
        await ctx.answerCallbackQuery({ text: `${bonus}✅ Диалог открыт. -${amt} кредит(ов). Осталось: ${left}`, show_alert: true });
      }
      else if (res.retryUsed) {
        await ctx.answerCallbackQuery({ text: `🎟 Диалог открыт. Использован Retry credit.`, show_alert: true });
      }

      await renderBxThread(ctx, actorUserId, wsId, res.thread.id, { back: 'offer', offerId, page });
      return;
    }

    if (p.a === 'a:bx_inbox') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      const page = Number(p.p || 0);

      const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_inbox', page);
      if (!bmRes) return;

      await renderBxInbox(ctx, bmRes.userId, wsId, page, { bm: bmRes.bm });
      return;
    }

    if (p.a === 'a:bx_thread') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const threadId = Number(p.t);
      const back = p.b ? String(p.b) : 'inbox';
      const offerId = p.o ? Number(p.o) : null;

      const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_inbox', Number(p.p || 0));
      if (!bmRes) return;

      await renderBxThread(ctx, bmRes.userId, wsId, threadId, { back, offerId });
      return;
    }


if (p.a === 'a:bx_retry_help') {
  const afterH = Number(CFG.INTRO_RETRY_AFTER_HOURS || 24);
  const expD = Number(CFG.INTRO_RETRY_EXPIRES_DAYS || 7);
  await ctx.answerCallbackQuery({
    show_alert: true,
    text: `Retry credit: если бренд написал, а ответа нет ${afterH}h → бот выдаёт 1 retry credit (действует ${expD}d). Следующий интро-диалог может открыться без списания Brand Pass.`
  });
  return;
}    if (p.a === 'a:bx_proofs') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const threadId = Number(p.t);
      const back = p.b ? String(p.b) : 'inbox';
      const offerId = p.o ? Number(p.o) : null;
      const page = Number(p.p || 0);

      const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_inbox', page);
      if (!bmRes) return;

      await renderBxProofs(ctx, bmRes.userId, wsId, threadId, { back, offerId, page });
      return;
    }

    if (p.a === 'a:bx_proof_link') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const threadId = Number(p.t);
      const back = p.b ? String(p.b) : 'inbox';
      const offerId = p.o ? Number(p.o) : null;
      const page = Number(p.p || 0);

      const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_inbox', page);
      if (!bmRes) return;

      await ctx.editMessageText('🔗 Пришли ссылку на пост (пример: https://t.me/... )', {
        reply_markup: new InlineKeyboard().text('⬅️ Отмена', `a:bx_proofs|ws:${wsId}|t:${threadId}|p:${page}${offerId ? `|o:${offerId}` : ''}|b:${back}`)
      });
      await setExpectText(ctx.from.id, { type: 'bx_proof_link', wsId, threadId, back, offerId, page, asUserId: bmRes.userId });
      return;
    }

    if (p.a === 'a:bx_proof_photo') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const threadId = Number(p.t);
      const back = p.b ? String(p.b) : 'inbox';
      const offerId = p.o ? Number(p.o) : null;
      const page = Number(p.p || 0);

      const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_inbox', page);
      if (!bmRes) return;

      await ctx.editMessageText('🖼️ Пришли скриншот (как фото)', {
        reply_markup: new InlineKeyboard().text('⬅️ Отмена', `a:bx_proofs|ws:${wsId}|t:${threadId}|p:${page}${offerId ? `|o:${offerId}` : ''}|b:${back}`)
      });
      await setExpectText(ctx.from.id, { type: 'bx_proof_photo', wsId, threadId, back, offerId, page, asUserId: bmRes.userId });
      return;
    }

    if (p.a === 'a:bx_stage') {
      const wsId = Number(p.ws);
      const threadId = Number(p.t);
      const stage = String(p.s || '');
      const back = p.b ? String(p.b) : 'inbox';
      const offerId = p.o ? Number(p.o) : null;

      const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_inbox', 0);
      if (!bmRes) return;

      const stageOk = ['new', 'in_progress', 'done'].includes(stage);
      const hasPlan = wsId === 0 ? await db.isBrandPlanActive(bmRes.userId) : true;
      if (!hasPlan) {
        await ctx.answerCallbackQuery({ text: '⛔ Нужен активный Brand Plan для стадий.' });
        return;
      }
      if (!stageOk) {
        await ctx.answerCallbackQuery({ text: 'Invalid stage' });
        return;
      }

      const updated = await db.setBarterThreadBuyerStage(threadId, bmRes.userId, stage);
      if (!updated) {
        await ctx.answerCallbackQuery({ text: 'Не удалось обновить стадию.' });
        return;
      }
      await ctx.answerCallbackQuery({ text: '✅ Обновлено' });
      await renderBxThread(ctx, bmRes.userId, wsId, threadId, { back, offerId });
      return;
    }

    if (p.a === 'a:bx_thread_reply') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws || 0);
      const threadId = Number(p.t);
      const back = p.b ? String(p.b) : 'inbox';
      const offerId = p.o ? Number(p.o) : null;

      const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_inbox', 0);
      if (!bmRes) return;

      await ctx.editMessageText('✍️ Напиши сообщение покупателю:', {
        reply_markup: new InlineKeyboard().text('⬅️ Отмена', `a:bx_thread|ws:${wsId}|t:${threadId}${offerId ? `|o:${offerId}` : ''}|b:${back}`)
      });
      await setExpectText(ctx.from.id, { type: 'bx_thread_msg', wsId, threadId, back, offerId, asUserId: bmRes.userId });
      return;
    }

    if (p.a === 'a:bx_thread_close_q') {
      await ctx.answerCallbackQuery();
      const kb = new InlineKeyboard()
        .text('✅ Закрыть', `a:bx_thread_close_do|ws:${Number(p.ws)}|t:${Number(p.t)}|p:${Number(p.p || 0)}`)
        .text('❌ Отмена', `a:bx_thread|ws:${Number(p.ws)}|t:${Number(p.t)}|p:${Number(p.p || 0)}`);
      await ctx.editMessageText('Закрыть диалог? После закрытия писать нельзя.', { reply_markup: kb });
      return;
    }

    if (p.a === 'a:bx_thread_close_do') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const threadId = Number(p.t);

      const bmRes = await bmResolveAssert(ctx, u, wsId, 'bx_inbox', 0);
      if (!bmRes) return;

      const ok = await db.closeBarterThread(threadId, bmRes.userId);
      if (!ok) {
        await ctx.answerCallbackQuery({ text: 'Не удалось закрыть тред.' });
        return;
      }
      await ctx.answerCallbackQuery({ text: '✅ Тред закрыт' });
      await renderBxInbox(ctx, bmRes.userId, wsId, 0, { bm: bmRes.bm });
      return;
    }



    if (p.a === 'a:bx_pin_set') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const isPro = await db.isWorkspacePro(wsId);
      if (!isPro) return ctx.answerCallbackQuery({ text: 'Доступно в PRO.' });
      await db.setWorkspacePinnedOffer(wsId, offerId);
      await db.auditWorkspace(wsId, u.id, 'ws.pro.pin_offer', { offerId });
      await ctx.answerCallbackQuery({ text: 'Закреплено.' });
      await renderBxView(ctx, u.id, wsId, offerId);
      return;
    }

    if (p.a === 'a:bx_pin_clear') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const isPro = await db.isWorkspacePro(wsId);
      if (!isPro) return ctx.answerCallbackQuery({ text: 'Доступно в PRO.' });
      await db.setWorkspacePinnedOffer(wsId, null);
      await db.auditWorkspace(wsId, u.id, 'ws.pro.unpin_offer', { offerId });
      await ctx.answerCallbackQuery({ text: 'Пин снят.' });
      await renderBxView(ctx, u.id, wsId, offerId);
      return;
    }
    if (p.a === 'a:bx_bump') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const o = await db.getBarterOfferForOwner(u.id, offerId);
      if (!o) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const isPro = await db.isWorkspacePro(wsId);
      const cooldownHours = isPro ? CFG.BARTER_BUMP_COOLDOWN_HOURS_PRO : CFG.BARTER_BUMP_COOLDOWN_HOURS_FREE;
      const cooldownMs = cooldownHours * 3600 * 1000;
      const last = o.bump_at ? new Date(o.bump_at).getTime() : 0;
      const now = Date.now();
      if (last && (now - last) < cooldownMs) {
        const left = cooldownMs - (now - last);
        const h = Math.floor(left / 3600000);
        const m = Math.floor((left % 3600000) / 60000);
        return ctx.answerCallbackQuery({ text: `Можно поднимать раз в ${cooldownHours}ч. Осталось ${h}ч ${m}м`, show_alert: true });
      }
      await db.bumpBarterOffer(offerId);
      await db.auditBarterOffer(offerId, wsId, u.id, 'bx.offer_bumped', { cooldownHours, isPro });
      await ctx.answerCallbackQuery({ text: '⬆️ Поднято!' });
      await renderBxView(ctx, u.id, wsId, offerId, 'my');
      return;
    }


    if (p.a === 'a:bx_my') {
      await ctx.answerCallbackQuery();
      await renderBxMy(ctx, u.id, Number(p.ws), Number(p.p || 0));
      return;
    }

    if (p.a === 'a:bx_my_arch') {
      await ctx.answerCallbackQuery();
      await renderBxMyArchive(ctx, u.id, Number(p.ws), Number(p.p || 0));
      return;
    }

    if (p.a === 'a:bx_new') {
      const wsId = Number(p.ws);
      db.trackEvent('bx_offer_new_open', { userId: u.id, wsId, meta: {} });
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      if (!ws.network_enabled) {
        await ctx.answerCallbackQuery();
        await renderBxOpen(ctx, u.id, wsId);
        return;
      }
      // PRO gating: active offers limit
      const isPro = await db.isWorkspacePro(wsId);
      const maxOffers = isPro ? CFG.BARTER_MAX_ACTIVE_OFFERS_PRO : CFG.BARTER_MAX_ACTIVE_OFFERS_FREE;
      const cntOffers = await db.countActiveBarterOffers(wsId);
      if (cntOffers >= maxOffers) {
        await ctx.editMessageText(`⚠️ Достигнут лимит активных офферов: <b>${cntOffers}/${maxOffers}</b>.

Хочешь больше — включи ⭐️ PRO.`, {
          parse_mode: 'HTML',
          reply_markup: new InlineKeyboard().text('⭐️ PRO', `a:ws_pro|ws:${wsId}`).row().text('⬅️ Назад', `a:bx_open|ws:${wsId}`)
        });
        return;
      }

      await ctx.answerCallbackQuery();
      await clearDraft(ctx.from.id);
      await ctx.editMessageText('➕ <b>Новый оффер</b>\n\nШаг 1/5: выбери тип:\n\n🎬 <b>UGC</b> — контент без аудитории (главное: вкус и качество)\n📣 <b>Интеграция</b> — публикация в TG/IG (нужна аудитория)', {
        parse_mode: 'HTML',
        reply_markup: bxKindKb(wsId)
      });
      await setDraft(ctx.from.id, { wsId });
      return;
    }

    
    if (p.a === 'a:bx_preset_home') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      await ctx.editMessageText(
        '🧩 <b>Шаблоны оффера</b>\n\nВыбери вариант — мы подготовим категорию/формат/оплату и сразу перейдём к тексту оффера.',
        { parse_mode: 'HTML', reply_markup: bxPresetKb(wsId) }
      );
      return;
    }

    if (p.a === 'a:bx_preset_apply') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      const presetId = String(p.id || '');
      const preset = BX_PRESETS.find((x) => x.id === presetId);
      if (!preset) return ctx.answerCallbackQuery({ text: 'Шаблон не найден.' });

      await ctx.answerCallbackQuery();
      // apply preset into draft and jump to step 4/4 (offer text)
      await setDraft(ctx.from.id, {
        wsId,
        category: preset.category,
        offer_type: preset.offer_type,
        compensation_type: preset.compensation_type,
        preset_id: presetId
      });

      const example = preset.example;
      await ctx.editMessageText(
        `Шаг 4/4: отправь одним сообщением\n\n1-я строка — <b>заголовок</b>\nсо 2-й строки — <b>детали</b> (условия/гео/что хочешь получить).\n\nПример:\n<code>${escapeHtml(example)}</code>`,
        {
          parse_mode: 'HTML',
          reply_markup: new InlineKeyboard()
            .text('⚙️ Изменить параметры', `a:bx_params|ws:${wsId}`)
            .row()
            .text('⬅️ Назад', `a:bx_new|ws:${wsId}`)
            .row()
            .text('⬅️ Отмена', `a:bx_open|ws:${wsId}`)
        }
      );
      await setExpectText(ctx.from.id, { type: 'bx_offer_text', wsId });
      return;
    }

    if (p.a === 'a:bx_params') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      await clearExpectText(ctx.from.id);
      await ctx.editMessageText('Шаг 1/4: выбери категорию:', {
        parse_mode: 'HTML',
        reply_markup: bxCategoryKb(wsId)
      });
      return;
    }


    if (p.a === 'a:bx_kind') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      const draft = (await getDraft(ctx.from.id)) || {};
      draft.wsId = wsId;
      draft.kind = String(p.k || 'ugc');
      await setDraft(ctx.from.id, draft);
      await ctx.editMessageText('Шаг 2/5: выбери категорию:', {
        parse_mode: 'HTML',
        reply_markup: bxCategoryKb(wsId)
      });
      return;
    }

if (p.a === 'a:bx_cat') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      const draft = (await getDraft(ctx.from.id)) || {};
      draft.wsId = wsId;
      draft.category = p.c;
      await setDraft(ctx.from.id, draft);
      await ctx.editMessageText('Шаг 3/5: выбери формат сотрудничества:', {
        parse_mode: 'HTML',
        reply_markup: bxTypeKb(wsId)
      });
      return;
    }

    if (p.a === 'a:bx_type') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      const draft = (await getDraft(ctx.from.id)) || {};
      draft.wsId = wsId;
      draft.offer_type = p.t;
      await setDraft(ctx.from.id, draft);
      await ctx.editMessageText('Шаг 4/5: выбери тип оплаты:', {
        parse_mode: 'HTML',
        reply_markup: bxCompKb(wsId)
      });
      return;
    }

    if (p.a === 'a:bx_comp') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      const draft = (await getDraft(ctx.from.id)) || {};
      draft.wsId = wsId;
      draft.compensation_type = p.p;
      await setDraft(ctx.from.id, draft);

      const kind = String(((await getDraft(ctx.from.id)) || {}).kind || 'ugc');
      const example =
        kind === 'integration'
          ? 'Заголовок: Возьму интеграцию в канале/IG\n\nФормат: пост/сторис/репост. Аудитория/охваты: ... Гео: ... Дедлайн: ... Бюджет/условия: ... Контакт: @myname'
          : 'Заголовок: Сниму UGC для бренда (без публикации)\n\nЧто сделаю: 1–3 вертикальных видео. Сроки: ... Референсы: ... Условия/бюджет: ... Контакт: @myname';
      await ctx.editMessageText(
        `Шаг 5/5: отправь одним сообщением\n\n1-я строка — <b>заголовок</b>\nсо 2-й строки — <b>детали</b> (что нужно / сроки / условия).\n\nПример:\n<code>${escapeHtml(example)}</code>`,
        {
          parse_mode: 'HTML',
          reply_markup: new InlineKeyboard().text('⬅️ Отмена', `a:bx_open|ws:${wsId}`)
        }
      );
      await setExpectText(ctx.from.id, { type: 'bx_offer_text', wsId });
      return;
    }

    if (p.a === 'a:bx_view') {
      await ctx.answerCallbackQuery();
      await renderBxView(ctx, u.id, Number(p.ws), Number(p.o), p.back || 'feed');
      return;
    }


    if (p.a === 'a:bx_media_step') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const back = p.back || 'my';
      await ctx.answerCallbackQuery();
      await clearExpectText(ctx.from.id);
      await renderBxMediaStep(ctx, u.id, wsId, offerId, back, { edit: true });
      return;
    }

    if (p.a === 'a:bx_media_clear') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const back = p.back || 'my';
      await ctx.answerCallbackQuery();
      await clearExpectText(ctx.from.id);

      const o = await db.getBarterOfferForOwner(u.id, offerId);
      if (!o) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      await db.updateBarterOffer(offerId, { media_type: null, media_file_id: null });
      await ctx.answerCallbackQuery({ text: 'Убрано' });
      await renderBxMediaStep(ctx, u.id, wsId, offerId, back, { edit: true });
      return;
    }

    if (p.a === 'a:bx_media_photo') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const back = p.back || 'my';
      await ctx.answerCallbackQuery();
      await setExpectText(ctx.from.id, { type: 'bx_media_photo', wsId, offerId, back });

      const kb = new InlineKeyboard().text('⬅️ Назад', `a:bx_media_step|ws:${wsId}|o:${offerId}|back:${back}`);
      await ctx.editMessageText('🖼 Пришли <b>картинку</b> одним сообщением.', { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

    if (p.a === 'a:bx_media_gif') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const back = p.back || 'my';
      await ctx.answerCallbackQuery();
      await setExpectText(ctx.from.id, { type: 'bx_media_gif', wsId, offerId, back });

      const kb = new InlineKeyboard().text('⬅️ Назад', `a:bx_media_step|ws:${wsId}|o:${offerId}|back:${back}`);
      await ctx.editMessageText('🎞 Пришли <b>GIF</b> (анимацию) одним сообщением.\n\n(Можно отправить как анимацию или как файл .gif)', { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

    if (p.a === 'a:bx_media_video') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const back = p.back || 'my';
      await ctx.answerCallbackQuery();
      await setExpectText(ctx.from.id, { type: 'bx_media_video', wsId, offerId, back });

      const kb = new InlineKeyboard().text('⬅️ Назад', `a:bx_media_step|ws:${wsId}|o:${offerId}|back:${back}`);
      await ctx.editMessageText('🎥 Пришли <b>видео</b> одним сообщением.\n\n(Поддержка: mp4. Можно отправить как видео или как файл.)', { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

    if (p.a === 'a:bx_media_preview') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const back = p.back || 'my';
      await ctx.answerCallbackQuery();
      await clearExpectText(ctx.from.id);
      await sendBxPreview(ctx, u.id, wsId, offerId, back);
      return;
    }

    if (p.a === 'a:bx_pause') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const o = await db.getBarterOfferForOwner(u.id, offerId);
      if (!o) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.updateBarterOfferStatus(offerId, 'PAUSED');
      await db.auditBarterOffer(offerId, wsId, u.id, 'bx.offer_paused', {});
      await ctx.answerCallbackQuery();
      await renderBxView(ctx, u.id, wsId, offerId, 'my');
      return;
    }

    if (p.a === 'a:bx_resume') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const o = await db.getBarterOfferForOwner(u.id, offerId);
      if (!o) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.updateBarterOfferStatus(offerId, 'ACTIVE');
      await db.auditBarterOffer(offerId, wsId, u.id, 'bx.offer_resumed', {});
      await ctx.answerCallbackQuery();
      await renderBxView(ctx, u.id, wsId, offerId, 'my');
      return;
    }

    // One-tap archive from list (soft delete). Hides immediately from "Мои офферы".
    if (p.a === 'a:bx_archive') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const page = Math.max(0, Number(p.p || 0));
      const o = await db.getBarterOfferForOwner(u.id, offerId);
      if (!o) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.updateBarterOfferStatus(offerId, 'CLOSED');
      await db.auditBarterOffer(offerId, wsId, u.id, 'bx.offer_archived', {});
      await ctx.answerCallbackQuery({ text: 'Архивировано.' });
      await renderBxMy(ctx, u.id, wsId, page);
      return;
    }

    if (p.a === 'a:bx_restore') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const page = Math.max(0, Number(p.p || 0));

      const restored = await db.restoreBarterOfferForOwner(offerId, u.id);
      if (!restored) {
        await ctx.answerCallbackQuery({ text: 'Не найдено / нет доступа.' });
        await renderBxMyArchive(ctx, u.id, wsId, page);
        return;
      }
      await db.auditBarterOffer(offerId, wsId, u.id, 'bx.offer_restored', {});
      await ctx.answerCallbackQuery({ text: 'Восстановлено.' });
      await renderBxMy(ctx, u.id, wsId, 0);
      return;
    }

    if (p.a === 'a:bx_del_q') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const o = await db.getBarterOfferForOwner(u.id, offerId);
      if (!o) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const kb = new InlineKeyboard()
        .text('✅ Архивировать', `a:bx_del_do|ws:${wsId}|o:${offerId}`)
        .text('❌ Отмена', `a:bx_view|ws:${wsId}|o:${offerId}|back:my`);
      await ctx.answerCallbackQuery();
      await ctx.editMessageText(`Архивировать оффер <b>#${offerId}</b>?

Он исчезнет из списка, но останется в базе для истории.`, { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

    if (p.a === 'a:bx_del_do') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const o = await db.getBarterOfferForOwner(u.id, offerId);
      if (!o) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.updateBarterOfferStatus(offerId, 'CLOSED');
      await db.auditBarterOffer(offerId, wsId, u.id, 'bx.offer_archived', {});
      await ctx.answerCallbackQuery({ text: 'Архивировано.' });
      await renderBxMy(ctx, u.id, wsId, 0);
      return;
    }

    if (p.a === 'a:net_q') {
      const wsId = Number(p.ws);
      const ret = String(p.ret || 'ws');
      await renderNetConfirm(ctx, u.id, wsId, ret);
      return;
    }

    if (p.a === 'a:net_set') {
      const wsId = Number(p.ws);
      const enabled = String(p.v) === '1';
      const ret = String(p.ret || 'ws') === 'bx' ? 'bx' : 'ws';
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      await db.setWorkspaceSetting(wsId, { network_enabled: enabled });
      await db.auditWorkspace(wsId, u.id, 'ws.network_toggled', { enabled, source: ret });
      await ctx.answerCallbackQuery({ text: enabled ? '✅ Сеть включена' : '❌ Сеть выключена' });
      if (ret === 'bx') {
        await renderBxOpen(ctx, u.id, wsId);
      } else {
        await renderWsSettings(ctx, u.id, wsId);
      }
      return;
    }

    // Backward compat: old toggle callback (messages already sent)
    if (p.a === 'a:ws_toggle_net') {
      const wsId = Number(p.ws);
      await renderNetConfirm(ctx, u.id, wsId, 'ws');
      return;
    }

    if (p.a === 'a:ws_toggle_cur') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.setWorkspaceSetting(wsId, { curator_enabled: !ws.curator_enabled });
      await db.auditWorkspace(wsId, u.id, 'ws.curator_toggled', { enabled: !ws.curator_enabled });
      await renderWsSettings(ctx, u.id, wsId);
      return;
    }

    // Curators
if (p.a === 'a:cur_manage') {
  const wsId = Number(p.ws);
  const ws = await db.getWorkspace(u.id, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const curators = await db.listCurators(wsId);
  const count = curators?.length || 0;

  await ctx.answerCallbackQuery();
  await ctx.editMessageText(
    `👥 <b>Кураторы</b>\n\nКураторы помогают проверять конкурсы и заявки.\n\nСейчас в списке: <b>${count}</b>`,
    { parse_mode: 'HTML', reply_markup: curManageKb(wsId) }
  );
  return;
}

    if (p.a === 'a:cur_invite') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      const token = randomToken(8);
      const key = k(['cur_invite', wsId, token]);
      await redis.set(key, { ownerUserId: u.id }, { ex: 10 * 60 });

      const link = `https://t.me/${CFG.BOT_USERNAME}?start=cur_${wsId}_${token}`;
      const text = `👤 <b>Приглашение куратора</b>\n\nСсылка (одноразовая • 10 минут):\n${escapeHtml(link)}\n\nНажми “Поделиться” и отправь приглашение нужному человеку.`;

      const shareText = `Приглашение куратора (одноразовая, 10 минут).\nОткрой ссылку: ${link}`;
      const shareUrl = `https://t.me/share/url?url=&text=${encodeURIComponent(shareText)}`;
      await ctx.answerCallbackQuery();
      await ctx.editMessageText(text, {
        parse_mode: 'HTML',
        disable_web_page_preview: true,
        reply_markup: new InlineKeyboard()
          .url('📤 Поделиться', shareUrl)
          .row()
          .text('⬅️ Назад', `a:cur_manage|ws:${wsId}`)
      });
      return;
    }

    if (p.a === 'a:cur_add_username') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('➕ Введи @username куратора (он должен уже запускать бота /start).', {
        reply_markup: new InlineKeyboard()
          .text('⬅️ Назад', `a:cur_manage|ws:${wsId}`)
          .text('🏠 Меню', 'a:menu')
      });
      await setExpectText(ctx.from.id, { type: 'curator_username', wsId });
      return;
    }

    if (p.a === 'a:cur_list') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const curators = await db.listCurators(wsId);
      const lines = curators.map(c => `• ${c.tg_username ? '@' + escapeHtml(c.tg_username) : 'id:' + c.tg_id}`);
      await ctx.answerCallbackQuery();
      await ctx.editMessageText(`👥 <b>Кураторы</b>

Нажми на 🗑 рядом с именем, чтобы удалить.

${lines.length ? lines.join('\n') : 'Пока нет.'}`, {
        parse_mode: 'HTML',
        reply_markup: curListKb(wsId, curators)
      });
      return;
    }

    if (p.a === 'a:cur_rm_q') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const curatorUserId = Number(p.u);
      const info = await db.getUserTgIdByUserId(curatorUserId);
      const label = info?.tg_username ? '@' + info.tg_username : 'id:' + (info?.tg_id || curatorUserId);
      const kb = new InlineKeyboard()
        .text('✅ Удалить', `a:cur_rm_do|ws:${wsId}|u:${curatorUserId}`)
        .text('❌ Отмена', `a:cur_list|ws:${wsId}`);
      await ctx.answerCallbackQuery();
      await ctx.editMessageText(`Удалить куратора <b>${escapeHtml(label)}</b>?`, { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

    if (p.a === 'a:cur_rm_do') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const curatorUserId = Number(p.u);
      await db.removeCurator(wsId, curatorUserId);
      await db.auditWorkspace(wsId, u.id, 'ws.curator_removed', { curatorUserId });

      // best-effort notify curator in DM
      try {
        const info = await db.getUserTgIdByUserId(curatorUserId);
        if (info?.tg_id) {
          const wsTitle = wsLabelNice(ws);
          const kb = new InlineKeyboard()
            .text('🏠 Главное меню', 'a:menu')
            .row()
            .text('💬 Support', 'a:support');
          await ctx.api.sendMessage(
            Number(info.tg_id),
            `❌ Твоя роль <b>куратора</b> для: <b>${escapeHtml(wsTitle)}</b> была удалена владельцем.`,
            { parse_mode: 'HTML', reply_markup: kb }
          );
        }
      } catch {}

      await ctx.answerCallbackQuery({ text: 'Удалено' });
      // refresh list
      const curators = await db.listCurators(wsId);
      const lines = curators.map(c => `• ${c.tg_username ? '@' + escapeHtml(c.tg_username) : 'id:' + c.tg_id}`);
      await ctx.editMessageText(`👥 <b>Кураторы</b>

Нажми на 🗑 рядом с именем, чтобы удалить.

${lines.length ? lines.join('\n') : 'Пока нет.'}`, {
        parse_mode: 'HTML',
        reply_markup: curListKb(wsId, curators)
      });
      return;
    }



    // FOLDERS (workspace shared @channel lists)
    if (p.a === 'a:folders_my') {
      await ctx.answerCallbackQuery();
      await renderFoldersMy(ctx, u.id);
      return;
    }

    if (p.a === 'a:folders_home') {
      await ctx.answerCallbackQuery();
      await renderFoldersHome(ctx, u.id, Number(p.ws));
      return;
    }

    if (p.a === 'a:folder_open') {
      await ctx.answerCallbackQuery();
      await renderFolderView(ctx, u.id, Number(p.ws), Number(p.f));
      return;
    }

    if (p.a === 'a:folder_new') {
      const wsId = Number(p.ws);
      const access = await getFolderAccess(u.id, wsId);
      if (!access || !access.canEdit) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('➕ <b>Новая папка</b>\n\nВведи название папки:', {
        parse_mode: 'HTML',
        reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:folders_home|ws:${wsId}`)
      });
      await setExpectText(ctx.from.id, { type: 'folder_create_title', wsId });
      return;
    }

    if (p.a === 'a:folder_add') {
      const wsId = Number(p.ws);
      const folderId = Number(p.f);
      const access = await getFolderAccess(u.id, wsId);
      if (!access || !access.canEdit) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const isPro = await db.isWorkspacePro(wsId);
      const max = isPro ? CFG.WORKSPACE_FOLDER_MAX_ITEMS_PRO : CFG.WORKSPACE_FOLDER_MAX_ITEMS_FREE;
      const folder = await db.getChannelFolder(folderId);
      const cnt = Number(folder?.items_count || 0);
      const left = Math.max(0, max - cnt);

      await ctx.answerCallbackQuery();
      await ctx.editMessageText(`➕ Добавь @каналы (или ссылки t.me) списком — каждый с новой строки.\n\nСвободно мест: <b>${left}</b> из <b>${max}</b>.`, {
        parse_mode: 'HTML',
        reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:folder_open|ws:${wsId}|f:${folderId}`)
      });
      await setExpectText(ctx.from.id, { type: 'folder_add_items', wsId, folderId });
      return;
    }

    if (p.a === 'a:folder_remove') {
      const wsId = Number(p.ws);
      const folderId = Number(p.f);
      const access = await getFolderAccess(u.id, wsId);
      if (!access || !access.canEdit) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('➖ Укажи @каналы (или ссылки t.me) списком — удалю их из папки:', {
        reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:folder_open|ws:${wsId}|f:${folderId}`)
      });
      await setExpectText(ctx.from.id, { type: 'folder_remove_items', wsId, folderId });
      return;
    }

    if (p.a === 'a:folder_rename') {
      const wsId = Number(p.ws);
      const folderId = Number(p.f);
      const access = await getFolderAccess(u.id, wsId);
      if (!access || !access.canEdit) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('✏️ Введи новое название папки:', {
        reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:folder_open|ws:${wsId}|f:${folderId}`)
      });
      await setExpectText(ctx.from.id, { type: 'folder_rename_title', wsId, folderId });
      return;
    }

    if (p.a === 'a:folder_clear_q') {
      const wsId = Number(p.ws);
      const folderId = Number(p.f);
      const access = await getFolderAccess(u.id, wsId);
      if (!access || !access.canEdit) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const kb = new InlineKeyboard()
        .text('✅ Очистить', `a:folder_clear_do|ws:${wsId}|f:${folderId}`)
        .text('❌ Отмена', `a:folder_open|ws:${wsId}|f:${folderId}`);
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('Очистить папку (удалить все каналы)?', { reply_markup: kb });
      return;
    }

    if (p.a === 'a:folder_clear_do') {
      const wsId = Number(p.ws);
      const folderId = Number(p.f);
      const access = await getFolderAccess(u.id, wsId);
      if (!access || !access.canEdit) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.clearChannelFolder(folderId);
      await db.auditWorkspace(wsId, u.id, 'folders.cleared', { folderId });
      await ctx.answerCallbackQuery({ text: 'Очищено.' });
      await renderFolderView(ctx, u.id, wsId, folderId);
      return;
    }

    if (p.a === 'a:folder_delete_q') {
      const wsId = Number(p.ws);
      const folderId = Number(p.f);
      const access = await getFolderAccess(u.id, wsId);
      if (!access || !access.isOwner) return ctx.answerCallbackQuery({ text: 'Только owner.' });
      const kb = new InlineKeyboard()
        .text('🗑 Удалить', `a:folder_delete_do|ws:${wsId}|f:${folderId}`)
        .text('❌ Отмена', `a:folder_open|ws:${wsId}|f:${folderId}`);
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('Удалить папку полностью?', { reply_markup: kb });
      return;
    }

    if (p.a === 'a:folder_delete_do') {
      const wsId = Number(p.ws);
      const folderId = Number(p.f);
      const access = await getFolderAccess(u.id, wsId);
      if (!access || !access.isOwner) return ctx.answerCallbackQuery({ text: 'Только owner.' });
      await db.deleteChannelFolder(folderId);
      await db.auditWorkspace(wsId, u.id, 'folders.deleted', { folderId });
      await ctx.answerCallbackQuery({ text: 'Удалено.' });
      await renderFoldersHome(ctx, u.id, wsId);
      return;
    }

    if (p.a === 'a:folder_export') {
      const wsId = Number(p.ws);
      const folderId = Number(p.f);
      const access = await getFolderAccess(u.id, wsId);
      if (!access) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const folder = await db.getChannelFolder(folderId);
      if (!folder || Number(folder.workspace_id) !== Number(wsId)) return ctx.answerCallbackQuery({ text: 'Папка не найдена.' });
      const items = await db.listChannelFolderItems(folderId);
      const lines = items.map(i => i.channel_username);
      const head = `📁 ${folder.title}\n`;
      const payload = head + (lines.length ? lines.join('\n') : '(пусто)');
      await ctx.answerCallbackQuery({ text: 'Отправил списком.' });

      // chunk to avoid Telegram limit
      const maxLen = 3500;
      let buf = '';
      for (const line of payload.split('\n')) {
        if ((buf + line + '\n').length > maxLen) {
          await ctx.reply(buf);
          buf = '';
        }
        buf += line + '\n';
      }
      if (buf.trim()) await ctx.reply(buf.trim());
      return;
    }

    // Workspace editors (folder-only)
    if (p.a === 'a:ws_editors') {
      await ctx.answerCallbackQuery();
      await renderWsEditors(ctx, u.id, Number(p.ws));
      return;
    }

    if (p.a === 'a:ws_editor_invite') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const token = randomToken(8);
      const key = k(['ws_editor_invite', wsId, token]);
      await redis.set(key, { ownerUserId: u.id }, { ex: Number(CFG.WORKSPACE_EDITOR_INVITE_TTL_MIN || 10) * 60 });

      const link = `https://t.me/${CFG.BOT_USERNAME}?start=fed_${wsId}_${token}`;
      await ctx.answerCallbackQuery();
      await ctx.editMessageText(`👥 <b>Invite editor</b>\n\nСсылка на ${CFG.WORKSPACE_EDITOR_INVITE_TTL_MIN || 10} минут:\n${escapeHtml(link)}\n\nРедактор сможет управлять папками этого Workspace.`, {
        parse_mode: 'HTML',
        disable_web_page_preview: true,
        reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:ws_editors|ws:${wsId}`)
      });
      return;
    }

    if (p.a === 'a:ws_editor_add_username') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('➕ Введи @username редактора (он должен уже запускать бота /start).', {
        reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:ws_editors|ws:${wsId}`)
      });
      await setExpectText(ctx.from.id, { type: 'ws_editor_username', wsId });
      return;
    }

    if (p.a === 'a:ws_editor_rm_q') {
      const wsId = Number(p.ws);
      const targetUserId = Number(p.u);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      const kb = new InlineKeyboard()
        .text('✅ Удалить', `a:ws_editor_rm_do|ws:${wsId}|u:${targetUserId}`)
        .text('❌ Отмена', `a:ws_editors|ws:${wsId}`);
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('Удалить редактора?', { reply_markup: kb });
      return;
    }

    if (p.a === 'a:ws_editor_rm_do') {
      const wsId = Number(p.ws);
      const targetUserId = Number(p.u);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.removeWorkspaceEditor(wsId, targetUserId);
      await db.auditWorkspace(wsId, u.id, 'ws.editor_removed', { userId: targetUserId });
      await ctx.answerCallbackQuery({ text: 'Удалено.' });
      await renderWsEditors(ctx, u.id, wsId);
      return;
    }

    // Barter: attach partner folder to offer
    if (p.a === 'a:bx_partner_folder_pick') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const o = await db.getBarterOfferForOwner(u.id, offerId);
      if (!o) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      const folders = await db.listChannelFolders(wsId);
      const kb = new InlineKeyboard();
      for (const f of folders.slice(0, 20)) {
        kb.text(`📁 ${String(f.title).slice(0, 32)} (${Number(f.items_count || 0)})`, `a:bx_partner_folder_set|ws:${wsId}|o:${offerId}|f:${f.id}`).row();
      }
      kb.text('⏭ Без папки', `a:bx_partner_folder_clear|ws:${wsId}|o:${offerId}`).row();
      kb.text('⬅️ Назад', `a:bx_view|ws:${wsId}|o:${offerId}|back:my`);

      await ctx.answerCallbackQuery();
      await ctx.editMessageText('📁 Выбери папку совместных каналов (она будет показываться в оффере):', { reply_markup: kb });
      return;
    }

    if (p.a === 'a:bx_partner_folder_set') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const folderId = Number(p.f);
      const o = await db.getBarterOfferForOwner(u.id, offerId);
      if (!o) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      const folder = await db.getChannelFolder(folderId);
      if (!folder || Number(folder.workspace_id) !== Number(wsId)) return ctx.answerCallbackQuery({ text: 'Папка не найдена.' });

      await db.updateBarterOffer(offerId, { partner_folder_id: folderId });
      await db.auditBarterOffer(offerId, wsId, u.id, 'bx.partner_folder_set', { folderId });
      await ctx.answerCallbackQuery({ text: 'Готово.' });
      await renderBxView(ctx, u.id, wsId, offerId, 'my');
      return;
    }

    if (p.a === 'a:bx_partner_folder_clear') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const o = await db.getBarterOfferForOwner(u.id, offerId);
      if (!o) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      await db.updateBarterOffer(offerId, { partner_folder_id: null });
      await db.auditBarterOffer(offerId, wsId, u.id, 'bx.partner_folder_cleared', {});
      await ctx.answerCallbackQuery({ text: 'Ок.' });
      await renderBxView(ctx, u.id, wsId, offerId, 'my');
      return;
    }

    
    // Giveaways: sponsors skip (solo mode)
    if (p.a === 'a:gw_sponsors_skip') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      const draft = (await getDraft(ctx.from.id)) || {};
      draft.wsId = wsId;
      draft.sponsors = [];
      await setDraft(ctx.from.id, draft);
      await clearExpectText(ctx.from.id);

      await ctx.answerCallbackQuery({ text: 'Соло: без спонсоров ✅' });
      await ctx.editMessageText('Ок. Выбери дедлайн:', { reply_markup: gwNewStepDeadlineKb(wsId) });
      return;
    }

    // Giveaways: sponsors enter list (explicit)
    if (p.a === 'a:gw_sponsors_enter') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      await clearExpectText(ctx.from.id);
      await setExpectText(ctx.from.id, { type: 'gw_sponsors_text', wsId });

      await ctx.answerCallbackQuery();
      const isPro = await db.isWorkspacePro(wsId);
      const max = isPro ? CFG.GIVEAWAY_SPONSORS_MAX_PRO : CFG.GIVEAWAY_SPONSORS_MAX_FREE;

      await ctx.editMessageText(
        `✍️ Пришли список спонсоров (до ${max}) — @каналы или ссылки t.me (через пробел/перенос строки).

` +
        `Если это соло — нажми «✅ Без спонсоров (соло)».`,
        { reply_markup: gwSponsorsOptionalKb(wsId) }
      );
      return;
    }


    // Giveaways: sponsors review (edit/clear/next)
    if (p.a === 'a:gw_sponsors_edit') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      await clearExpectText(ctx.from.id);
      await setExpectText(ctx.from.id, { type: 'gw_sponsors_text', wsId });

      const isPro = await db.isWorkspacePro(wsId);
      const max = isPro ? CFG.GIVEAWAY_SPONSORS_MAX_PRO : CFG.GIVEAWAY_SPONSORS_MAX_FREE;

      await ctx.answerCallbackQuery();
      await ctx.editMessageText(
        `✍️ Пришли список спонсоров (до ${max}) — @каналы или ссылки t.me (через пробел/перенос строки).\n\nЕсли это соло — нажми «✅ Без спонсоров (соло)».`,
        { reply_markup: gwSponsorsOptionalKb(wsId) }
      );
      return;
    }

    if (p.a === 'a:gw_sponsors_clear') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      const draft = (await getDraft(ctx.from.id)) || {};
      draft.wsId = wsId;
      draft.sponsors = [];
      await setDraft(ctx.from.id, draft);
      await clearExpectText(ctx.from.id);

      await ctx.answerCallbackQuery({ text: 'Соло: без спонсоров ✅' });
      await ctx.editMessageText('Ок. Выбери дедлайн:', { reply_markup: gwNewStepDeadlineKb(wsId) });
      return;
    }

    if (p.a === 'a:gw_sponsors_next') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      const draft = (await getDraft(ctx.from.id)) || {};
      draft.wsId = wsId;
      if (!Array.isArray(draft.sponsors)) draft.sponsors = [];
      await setDraft(ctx.from.id, draft);
      await clearExpectText(ctx.from.id);

      await ctx.answerCallbackQuery();
      await ctx.editMessageText('Ок. Выбери дедлайн:', { reply_markup: gwNewStepDeadlineKb(wsId) });
      return;
    }






    // Giveaways: sponsors help (Jobs-style micro guide)
    if (p.a === 'a:gw_sponsors_help') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      const b = String(p.b || '').toLowerCase();
      let backCb = `a:gw_step_sponsors|ws:${wsId}`;
      if (b === 'folder') backCb = `a:gw_sponsors_from_folder|ws:${wsId}`;
      if (b === 'step') backCb = `a:gw_step_sponsors|ws:${wsId}`;

      await ctx.answerCallbackQuery();
      await ctx.editMessageText(
        `🧭 Каналы-спонсоры (подписки)

Это список каналов, на которые участник должен подписаться.
Дальше участник жмёт «🔄 Проверить», и бот проверяет подписки на каждый канал.

Как подготовить (по-уму):
1) Добавь бота админом в каналы-спонсоры (иначе Telegram не даст проверить).
2) В «Мои каналы» создай папку и добавь туда нужные каналы.

Дальше: выбери папку → «➡️ Дальше» → дедлайн → превью → публикация.`,
        { reply_markup: new InlineKeyboard().text('⬅️ Назад', backCb) }
      );
      return;
    }
// Giveaways: load sponsors from folder
    if (p.a === 'a:gw_sponsors_from_folder') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      const folders = await db.listChannelFolders(wsId);
      const kb = new InlineKeyboard();
      for (const f of folders.slice(0, 20)) {
        kb.text(`📁 ${String(f.title).slice(0, 32)} (${Number(f.items_count || 0)})`, `a:gw_sponsors_use_folder|ws:${wsId}|f:${f.id}`).row();
      }
      kb.text('🧭 Как это работает', `a:gw_sponsors_help|ws:${wsId}|b:folder`).row();
      kb.text('⬅️ Назад', `a:gw_step_sponsors|ws:${wsId}`);

      await ctx.answerCallbackQuery();
      await ctx.editMessageText(`📁 Спонсоры из папки

Выбери папку — каналы из неё станут спонсорами (подписки) для конкурса.
Если папок нет — создай папку в «Мои каналы» → «Папки».`, { reply_markup: kb });
      return;
    }

    if (p.a === 'a:gw_sponsors_use_folder') {
      const wsId = Number(p.ws);
      const folderId = Number(p.f);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      const folder = await db.getChannelFolder(folderId);
      if (!folder || Number(folder.workspace_id) !== Number(wsId)) return ctx.answerCallbackQuery({ text: 'Папка не найдена.' });

      const items = await db.listChannelFolderItems(folderId);
      const isPro = await db.isWorkspacePro(wsId);
      const max = isPro ? CFG.GIVEAWAY_SPONSORS_MAX_PRO : CFG.GIVEAWAY_SPONSORS_MAX_FREE;
      if (items.length > max) {
        await ctx.answerCallbackQuery();
        await ctx.editMessageText(`⚠️ В этой папке <b>${items.length}</b> каналов, а лимит спонсоров — <b>${max}</b>.\n\nУменьши папку или включи ⭐️ PRO.`, {
          parse_mode: 'HTML',
          reply_markup: new InlineKeyboard().text('⭐️ PRO', `a:ws_pro|ws:${wsId}`).row().text('⬅️ Назад', `a:gw_sponsors_from_folder|ws:${wsId}`)
        });
        return;
      }

      const sponsors = items.map(i => i.channel_username);
      const draft = (await getDraft(ctx.from.id)) || {};
      draft.wsId = wsId;
      draft.sponsors = sponsors;
      await setDraft(ctx.from.id, draft);

      const list = sponsors.map(x => `• ${escapeHtml(String(x))}`).join('\n');
      await ctx.answerCallbackQuery({ text: 'Готово.' });
      await ctx.editMessageText(
        `✅ Спонсоры: <b>${sponsors.length}</b>
${list}

Эти каналы появятся в конкурсе как обязательные подписки.
Дальше жми «➡️ Дальше» и выбери дедлайн.

⚠️ Чтобы «Проверить» работало, добавь бота админом в каналы-спонсоры.`,
        { parse_mode: 'HTML', reply_markup: gwSponsorsReviewKb(wsId) }
      );
      return;
    }

    // GIVEAWAYS list
    if (p.a === 'a:gw_list') {
      await ctx.answerCallbackQuery();
      await maybeSendBanner(ctx, 'giveaway', CFG.GIVEAWAY_BANNER_FILE_ID);
      await renderGwList(ctx, u.id, null);
      return;
    }
    if (p.a === 'a:gw_new_pick') {
      await ctx.answerCallbackQuery();
      await renderGwNewWorkspacePicker(ctx, u.id, 'a:gw_list');
      return;
    }
    if (p.a === 'a:gw_list_ws') {
      await ctx.answerCallbackQuery();
      await maybeSendBanner(ctx, 'giveaway', CFG.GIVEAWAY_BANNER_FILE_ID);
      await renderGwList(ctx, u.id, Number(p.ws));
      return;
    }
    if (p.a === 'a:gw_open') {
      await ctx.answerCallbackQuery();
      await renderGwOpen(ctx, u.id, Number(p.i));
      return;
    }
    if (p.a === 'a:gw_stats') {
      await ctx.answerCallbackQuery();
      await renderGwStats(ctx, u.id, Number(p.i));
      return;
    }
    if (p.a === 'a:gw_log') {
      await ctx.answerCallbackQuery();
      const gwId = Number(p.i);
      const isPub = String(p.pub || '') === '1';
      await renderGwLog(ctx, isPub ? null : u.id, gwId);
      return;
    }

    if (p.a === 'a:gw_del_q') {
      const gwId = Number(p.i);
      const g = await db.getGiveawayForOwner(gwId, u.id);
      if (!g) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }
      await ctx.answerCallbackQuery();
      const kb = new InlineKeyboard()
        .text('✅ Да, удалить', `a:gw_del_do|i:${gwId}|ws:${g.workspace_id}`)
        .row()
        .text('⬅️ Назад', `a:gw_open|i:${gwId}`)
        .row()
        .text('🏠 Меню', 'a:menu');

      await ctx.editMessageText(
        `🗑 <b>Удалить конкурс #${gwId}?</b>

Это действие необратимо (удалятся спонсоры/участники/победители).
Если нужно просто остановить — используй «🏁 Завершить сейчас».`,
        { parse_mode: 'HTML', reply_markup: kb }
      );
      return;
    }
    if (p.a === 'a:gw_del_do') {
      const gwId = Number(p.i);
      // Owner-gated hard delete
      const deleted = await db.deleteGiveawayForOwner(gwId, u.id);
      if (!deleted) {
        await ctx.answerCallbackQuery({ text: 'Не найдено / нет доступа.' });
        await renderGwList(ctx, u.id, null);
        return;
      }
      try {
        await db.auditWorkspace(deleted.workspace_id, u.id, 'gw.deleted', { giveaway_id: gwId });
      } catch {}

      await ctx.answerCallbackQuery({ text: 'Удалено.' });
      await renderGwList(ctx, u.id, null);
      return;
    }

    if (p.a === 'a:gw_publish_results') {
      const gwId = Number(p.i);
      const g = await db.getGiveawayForOwner(gwId, u.id);
      if (!g) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      if (g.results_message_id && Number(g.results_message_id) !== -1) {
        await ctx.answerCallbackQuery({ text: 'Итоги уже опубликованы.' });
        await renderGwOpen(ctx, u.id, gwId);
        return;
      }
      if (Number(g.results_message_id) === -1) {
        await ctx.answerCallbackQuery({ text: 'Итоги уже публикуются…' });
        return;
      }
      if (String(g.status || '').toUpperCase() !== 'WINNERS_DRAWN') {
        await ctx.answerCallbackQuery({ text: 'Сначала выбери победителей.' });
        await renderGwOpen(ctx, u.id, gwId);
        return;
      }
      if (!g.published_chat_id) {
        await ctx.answerCallbackQuery({ text: 'Не вижу куда публиковать.' });
        await renderGwOpen(ctx, u.id, gwId);
        return;
      }

      // Idempotency: lock per giveaway
      const lockKey = k(['lock', 'gw_publish', gwId]);
      const locked = await redis.set(lockKey, { by: u.id }, { nx: true, ex: 30 });
      if (!locked) {
        await ctx.answerCallbackQuery({ text: 'Секунду… уже публикуется.' });
        return;
      }

      try {
        // Strong idempotency: reserve in DB (results_message_id=-1)
        const reserved = await db.reserveGiveawayPublish(gwId, u.id);
        if (!reserved) {
          await ctx.answerCallbackQuery({ text: 'Уже публикуется / опубликовано.' });
          await renderGwOpen(ctx, u.id, gwId);
          return;
        }

        const winners = await db.exportGiveawayWinnersForPublish(gwId, u.id);
        if (!winners || !winners.length) {
          await db.releaseGiveawayPublish(gwId, u.id);
          await ctx.answerCallbackQuery({ text: 'Нет победителей.' });
          return;
        }

        const winnersList = winners
          .map(w => {
            const name = w.username ? '@' + escapeHtml(String(w.username)) : `<a href="tg://user?id=${Number(w.tg_id)}">участник</a>`;
            return `${Number(w.place)}. ${name}`;
          })
          .join('\n');

        const prize = (g.prize_value_text || '').trim() || '—';
        const body =
`🎉 <b>Итоги конкурса #${g.id}</b>

🎁 Приз: <b>${escapeHtml(prize)}</b>
🏆 Победители:

${winnersList}

🧾 Проверить лог: открой бота → /start gw_${g.id} → “🧾 Лог конкурса”`;

        const url = `https://t.me/${CFG.BOT_USERNAME}?start=gw_${g.id}`;
        const sent = await ctx.api.sendMessage(g.published_chat_id, body, {
          parse_mode: 'HTML',
          disable_web_page_preview: true,
          reply_markup: new InlineKeyboard().url('🧾 Проверить в боте', url)
        });

        await db.finalizeGiveawayPublish(gwId, u.id, sent.message_id);
        await db.auditGiveaway(gwId, g.workspace_id, u.id, 'gw.results_published', { message_id: sent.message_id });

        await ctx.answerCallbackQuery({ text: 'Опубликовано.' });
        await renderGwOpen(ctx, u.id, gwId);
      } catch (e) {
        try { await db.releaseGiveawayPublish(gwId, u.id); } catch {}
        await ctx.answerCallbackQuery({ text: 'Ошибка публикации.' });
      } finally {
        // best-effort unlock
        try { await redis.del(lockKey); } catch {}
      }
      return;
    }

    // Public open (participants)
    if (p.a === 'a:gw_open_public') {
      await ctx.answerCallbackQuery();
      await renderGwOpenPublic(ctx, Number(p.i), u.id);
      return;
    }

    // Export
    if (p.a === 'a:gw_export') {
      const gwId = Number(p.i);
      const t = p.t;
      await ctx.answerCallbackQuery();
      if (t === 'winners') {
        const winners = await db.exportGiveawayWinnersForPublish(gwId, u.id);
        if (!winners || !winners.length) return ctx.reply('Победителей пока нет.');
        const lines = winners.map(w => {
          const name = w.username ? '@' + String(w.username) : `id:${Number(w.tg_id)}`;
          return `${Number(w.place)}. ${name}`;
        });
        return ctx.reply(lines.join('\n'));
      }
      if (t === 'eligible') {
        const list = await db.exportGiveawayParticipantsUsernames(gwId, u.id, true);
        return ctx.reply(list.length ? list.map(x => '@' + x).join('\n') : 'Пока нет eligible.');
      }
      const list = await db.exportGiveawayParticipantsUsernames(gwId, u.id, null);
      return ctx.reply(list.length ? list.map(x => '@' + x).join('\n') : 'Пока нет участников.');
    }

    // 🧩 Access
    if (p.a === 'a:gw_access') {
      await renderGwAccess({ ctx, gwId: Number(p.i), ownerUserId: u.id, redis, db, forceRecheck: false });
      return;
    }
    if (p.a === 'a:gw_access_recheck') {
      await renderGwAccess({ ctx, gwId: Number(p.i), ownerUserId: u.id, redis, db, forceRecheck: true });
      return;
    }

    // ✅ Preflight readiness (owner)
    if (p.a === 'a:gw_preflight') {
      await ctx.answerCallbackQuery();
      await renderGwPreflight(ctx, u.id, Number(p.i), { forceRecheck: String(p.r || '') === '1' });
      return;
    }

    // ℹ️ Why not eligible (owner)
    if (p.a === 'a:gw_why') {
      await ctx.answerCallbackQuery();
      await renderGwWhyMenu(ctx, u.id, Number(p.i));
      return;
    }
    if (p.a === 'a:gw_why_enter') {
      const gwId = Number(p.i);
      await ctx.answerCallbackQuery();
      await ctx.editMessageText(
        'ℹ️ <b>Почему не прошёл</b>\n\nПришли <b>user_id</b> участника (цифрами).\n\nПодсказка: участник может узнать свой id командой /whoami.',
        { parse_mode: 'HTML', reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:gw_stats|i:${gwId}`) }
      );
      await setExpectText(ctx.from.id, { type: 'gw_why_userid', gwId });
      return;
    }
    if (p.a === 'a:gw_why_forward') {
      const gwId = Number(p.i);
      await ctx.answerCallbackQuery();
      await ctx.editMessageText(
        'ℹ️ <b>Почему не прошёл</b>\n\nПерешли сюда сообщение участника (forward).\n\nВажно: если у участника включена “Forward privacy”, бот не увидит user_id — тогда используй “Ввести ID”.',
        { parse_mode: 'HTML', reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:gw_why|i:${gwId}`) }
      );
      await setExpectText(ctx.from.id, { type: 'gw_why_forward', gwId });
      return;
    }
    if (p.a === 'a:gw_why_recheck') {
      await ctx.answerCallbackQuery();
      await renderGwWhyResult(ctx, u.id, Number(p.i), Number(p.tu), { forceRecheck: true });
      return;
    }


    // Create giveaway
    if (p.a === 'a:gw_new') {
      const wsId = Number(p.ws);
      db.trackEvent('gw_new_open', { userId: u.id, wsId, meta: {} });
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await clearDraft(ctx.from.id);
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('🎁 <b>Новый конкурс</b>\n\nВыбери тип приза:', { parse_mode: 'HTML', reply_markup: gwNewStepPrizeKb(wsId) });
      return;
    }

    
    if (p.a === 'a:gw_preset_home') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      await ctx.editMessageText(
        '🧩 <b>Пресеты конкурса</b>\n\nВыбери вариант — мы подготовим тип приза и текст. Потом выберешь количество мест, спонсоров и дедлайн.',
        { parse_mode: 'HTML', reply_markup: gwPresetKb(wsId) }
      );
      return;
    }

    if (p.a === 'a:gw_preset_apply') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const presetId = String(p.id || '');
      const preset = GW_PRESETS.find((x) => x.id === presetId);
      if (!preset) return ctx.answerCallbackQuery({ text: 'Пресет не найден.' });
      await ctx.answerCallbackQuery();
      await clearDraft(ctx.from.id);
      await setDraft(ctx.from.id, { wsId, prize_type: preset.prize_type, prize_value_text: preset.prize_value_text });
      await ctx.editMessageText(
        `✅ Пресет применён.\n\n<b>Приз:</b> <code>${escapeHtml(preset.prize_value_text)}</code>\n\nТеперь выбери количество призовых мест:`,
        { parse_mode: 'HTML', reply_markup: gwNewStepWinnersKb(wsId) }
      );
      return;
    }

if (p.a === 'a:gw_prize') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const type = p.t;
      await ctx.answerCallbackQuery();
      await ctx.editMessageText(gwPrizePrompt(type), {
        parse_mode: 'HTML',
        reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:gw_new|ws:${wsId}`)
      });
      await setDraft(ctx.from.id, { wsId, prize_type: type });
      await setExpectText(ctx.from.id, { type: 'gw_prize_text', wsId });
      return;
    }

    if (p.a === 'a:gw_winners') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const n = Number(p.n);
      const draft = (await getDraft(ctx.from.id)) || { wsId };
      draft.winners_count = n;
      await setDraft(ctx.from.id, draft);
      await ctx.answerCallbackQuery();
      const isPro = await db.isWorkspacePro(wsId);
      const max = isPro ? CFG.GIVEAWAY_SPONSORS_MAX_PRO : CFG.GIVEAWAY_SPONSORS_MAX_FREE;
      const kb = new InlineKeyboard()
        .text('✅ Без спонсоров (соло)', `a:gw_sponsors_skip|ws:${wsId}`)
        .row()
        .text('✍️ Ввести списком', `a:gw_sponsors_enter|ws:${wsId}`)
        .row()
        .text('📁 Из папки', `a:gw_sponsors_from_folder|ws:${wsId}`)
        .row()
        .text('🧭 Что такое спонсоры?', `a:gw_sponsors_help|ws:${wsId}`)
        .row()
        .text('⬅️ Назад', `a:gw_new|ws:${wsId}`);
      await ctx.editMessageText(
        `Спонсоры (необязательно, до ${max}).\n\n` +
        `Если это соло — нажми «✅ Без спонсоров (соло)».\n` +
        `Если есть партнёры — нажми «✍️ Ввести списком» и пришли список @каналов или t.me ссылками (можно просто прислать).`,
        { reply_markup: kb }
      );
      await setExpectText(ctx.from.id, { type: 'gw_sponsors_text', wsId });
      return;
    }

    if (p.a === 'a:gw_winners_custom') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('Введи число призовых мест (1..50):', {
        reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:gw_new|ws:${wsId}`)
      });
      await setExpectText(ctx.from.id, { type: 'gw_winners_custom', wsId });
      return;
    }

    if (p.a === 'a:gw_step_sponsors') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      const isPro = await db.isWorkspacePro(wsId);
      const max = isPro ? CFG.GIVEAWAY_SPONSORS_MAX_PRO : CFG.GIVEAWAY_SPONSORS_MAX_FREE;
      const kb = new InlineKeyboard()
        .text('✅ Без спонсоров (соло)', `a:gw_sponsors_skip|ws:${wsId}`)
        .row()
        .text('✍️ Ввести списком', `a:gw_sponsors_enter|ws:${wsId}`)
        .row()
        .text('📁 Из папки', `a:gw_sponsors_from_folder|ws:${wsId}`)
        .row()
        .text('🧭 Что такое спонсоры?', `a:gw_sponsors_help|ws:${wsId}`)
        .row()
        .text('⬅️ Назад', `a:gw_new|ws:${wsId}`);
      await ctx.editMessageText(
        `Спонсоры (необязательно, до ${max}).\n\n` +
        `Если соло — нажми «✅ Без спонсоров (соло)».\n` +
        `Если есть партнёры — нажми «✍️ Ввести списком» и пришли список @каналов или t.me ссылками (можно просто прислать).`,
        { reply_markup: kb }
      );
      await setExpectText(ctx.from.id, { type: 'gw_sponsors_text', wsId });
      return;
    }

    if (p.a === 'a:gw_step_deadline') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('Выбери дедлайн:', { reply_markup: gwNewStepDeadlineKb(wsId) });
      return;
    }

    if (p.a === 'a:gw_deadline') {
      const wsId = Number(p.ws);
      const mins = Number(p.m);

      if (!Number.isFinite(mins) || mins < 5 || mins > 30 * 24 * 60) {
        await ctx.answerCallbackQuery({ text: 'Некорректный дедлайн.' });
        return;
      }

      const draft = (await getDraft(ctx.from.id)) || { wsId };
      draft.ends_at = addMinutes(new Date(), mins).toISOString();
      await setDraft(ctx.from.id, draft);
      await ctx.answerCallbackQuery();
      await renderGwMediaStep(ctx, wsId, { edit: true });
      return;
    }

    if (p.a === 'a:gw_deadline_custom') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('Введи дедлайн в формате DD.MM HH:MM (МСК). Пример: 20.01 18:00', {
        reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:gw_step_deadline|ws:${wsId}`)
      });
      await setExpectText(ctx.from.id, { type: 'gw_deadline_custom', wsId });
      return;
    }


    if (p.a === 'a:gw_media_step') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      await clearExpectText(ctx.from.id);
      await renderGwMediaStep(ctx, wsId, { edit: true });
      return;
    }

    if (p.a === 'a:gw_media_skip') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      await clearExpectText(ctx.from.id);
      await renderGwConfirm(ctx, wsId, { edit: true });
      return;
    }

    if (p.a === 'a:gw_media_clear') {
      const wsId = Number(p.ws);
      await clearExpectText(ctx.from.id);
      const draft = (await getDraft(ctx.from.id)) || { wsId };
      delete draft.media_type;
      delete draft.media_file_id;
      await setDraft(ctx.from.id, draft);
      await ctx.answerCallbackQuery({ text: 'Убрано' });
      await renderGwMediaStep(ctx, wsId, { edit: true });
      return;
    }

    if (p.a === 'a:gw_media_photo') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      await setExpectText(ctx.from.id, { type: 'gw_media_photo', wsId });
      const kb = new InlineKeyboard().text('⬅️ Назад', `a:gw_media_step|ws:${wsId}`);
      await ctx.editMessageText('🖼 Пришли <b>картинку</b> одним сообщением.\n\n(Можно пропустить этот шаг)', {
        parse_mode: 'HTML',
        reply_markup: kb
      });
      return;
    }

    if (p.a === 'a:gw_media_gif') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      await setExpectText(ctx.from.id, { type: 'gw_media_gif', wsId });
      const kb = new InlineKeyboard().text('⬅️ Назад', `a:gw_media_step|ws:${wsId}`);
      await ctx.editMessageText('🎞 Пришли <b>GIF</b> (анимацию) одним сообщением.\n\n(Можно пропустить этот шаг)', {
        parse_mode: 'HTML',
        reply_markup: kb
      });
      return;
    }
    if (p.a === 'a:gw_media_video') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      await setExpectText(ctx.from.id, { type: 'gw_media_video', wsId });
      const kb = new InlineKeyboard().text('⬅️ Назад', `a:gw_media_step|ws:${wsId}`);
      await ctx.editMessageText(`🎥 Пришли <b>видео</b> одним сообщением.\n\n(Поддержка: mp4. Можно отправить как видео или как файл.)`, {
        parse_mode: 'HTML',
        reply_markup: kb
      });
      return;
    }

    if (p.a === 'a:gw_preview') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      const draft = (await getDraft(ctx.from.id)) || { wsId };

      const prize = (draft.prize_value_text || '').trim() || '—';
      const winners = Number(draft.winners_count || 0) || 1;
      const ends = draft.ends_at ? fmtTs(draft.ends_at) : '—';
      const sponsorsCount = normalizeSponsorsList(draft.sponsors).map(fmtSponsorHandle).filter(Boolean).length;
      const sponsorsLine = sponsorsCount
        ? `👥 Условие: ${sponsorsCountText(draft.sponsors)}
${sponsorsBulletText(draft.sponsors, 5)}
Проверка подписки — в боте (кнопка «🔄 Проверить»).`
        : `👥 Спонсоры: <b>нет</b> (соло).`;

      const text =
`🎀 <b>РОЗЫГРЫШ</b>

🎁 Приз: <b>${escapeHtml(prize)}</b>
🏆 Мест: <b>${winners}</b>
⏳ Итоги: <b>${escapeHtml(String(ends))}</b>

${sponsorsLine}

🤖 Открой бота → 🎟 Участвовать → 🔄 Проверить.

<i>Это превью. Для публикации нажми “📣 Опубликовать” ниже.</i>`;

      // Add action buttons прямо в превью, чтобы не было ощущения “надо переслать”.
      // IMPORTANT: callback приходит из превью-сообщения (медиа), поэтому “назад” делаем как «показать черновик ещё раз».
      const previewKb = new InlineKeyboard()
        .text('📣 Опубликовать', `a:gw_publish|ws:${wsId}`)
        .row()
        .text('⬅️ Назад к черновику', `a:gw_confirm_push|ws:${wsId}`);

      try {
        if (draft.media_file_id && String(draft.media_type) === 'photo') {
          await ctx.replyWithPhoto(draft.media_file_id, { caption: text, parse_mode: 'HTML', reply_markup: previewKb });
        } else if (draft.media_file_id && String(draft.media_type) === 'animation') {
          await ctx.replyWithAnimation(draft.media_file_id, { caption: text, parse_mode: 'HTML', reply_markup: previewKb });
        } else if (draft.media_file_id && String(draft.media_type) === 'video') {
          await ctx.replyWithVideo(draft.media_file_id, { caption: text, parse_mode: 'HTML', reply_markup: previewKb });
        } else {
          await ctx.reply(text, { parse_mode: 'HTML', disable_web_page_preview: true, reply_markup: previewKb });
        }
      } catch (_) {
        await ctx.reply('Не удалось отправить превью. Попробуй ещё раз или убери медиа.');
      }

      // Keep user in confirm screen
      await renderGwConfirm(ctx, wsId, { edit: true });
      return;
    }

    // “Назад” из превью конкурса: присылаем черновик ещё раз (не пытаемся редактировать медиа-сообщение).
    if (p.a === 'a:gw_confirm_push') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      await clearExpectText(ctx.from.id);
      await renderGwConfirm(ctx, wsId, { edit: false });
      return;
    }



    if (p.a === 'a:gw_publish') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const draft = (await getDraft(ctx.from.id)) || {};
      if (!draft.prize_value_text || !draft.winners_count || !draft.sponsors || !draft.ends_at) {
        await ctx.answerCallbackQuery({ text: 'Черновик не полный.' });
        return;
      }

      db.trackEvent('gw_publish_attempt', { userId: u.id, wsId, meta: { winners: Number(draft.winners_count || 0) } });

      // create in DB
      const created = await db.createGiveaway({
        workspaceId: wsId,
        prizeValueText: draft.prize_value_text,
        winnersCount: Number(draft.winners_count),
        endsAt: draft.ends_at,
        autoDraw: false,
        autoPublish: false
      });
      await db.replaceGiveawaySponsors(created.id, draft.sponsors);

      // publish post
      const botUsername = CFG.BOT_USERNAME;
      const deepLinkOpen = `https://t.me/${botUsername}?start=gw_${created.id}`;

      const sponsorsCount = normalizeSponsorsList(draft.sponsors).map(fmtSponsorHandle).filter(Boolean).length;
      const sponsorsLine = sponsorsCount
        ? `
👥 Условие: ${sponsorsCountText(draft.sponsors)}
${sponsorsBulletText(draft.sponsors, 5)}`
        : '';
      const actionHint = sponsorsCount
        ? '🤖 Открой бота → подпишись → 🔄 Проверить.'
        : '🤖 Открой бота → 🎟 Участвовать → 🔄 Проверить.';

      const text =
`🎀 <b>РОЗЫГРЫШ</b>

🎁 Приз: <b>${escapeHtml(draft.prize_value_text)}</b>
🏆 Мест: <b>${Number(draft.winners_count)}</b>
⏳ Итоги: <b>${escapeHtml(fmtTs(draft.ends_at))}</b>${sponsorsLine}

${actionHint}`;

      const kb = {
        inline_keyboard: [
          [{ text: '🤖 Открыть бота', url: deepLinkOpen }]
        ]
      };

      try {
        let sent;
        if (draft.media_file_id && String(draft.media_type) === 'photo') {
          sent = await ctx.api.sendPhoto(ws.channel_id, draft.media_file_id, {
            caption: text,
            parse_mode: 'HTML',
            reply_markup: kb
          });
        } else if (draft.media_file_id && String(draft.media_type) === 'animation') {
          sent = await ctx.api.sendAnimation(ws.channel_id, draft.media_file_id, {
            caption: text,
            parse_mode: 'HTML',
            reply_markup: kb
          });
        } else if (draft.media_file_id && String(draft.media_type) === 'video') {
          sent = await ctx.api.sendVideo(ws.channel_id, draft.media_file_id, {
            caption: text,
            parse_mode: 'HTML',
            reply_markup: kb
          });
        } else {
          sent = await ctx.api.sendMessage(ws.channel_id, text, {
            parse_mode: 'HTML',
            reply_markup: kb,
            disable_web_page_preview: true
          });
        }

        await db.updateGiveaway(created.id, {
          status: 'ACTIVE',
          published_chat_id: ws.channel_id,
          published_message_id: sent.message_id
        });
        await db.auditGiveaway(created.id, wsId, u.id, 'gw.published', { chat_id: ws.channel_id, message_id: sent.message_id });
        db.trackEvent('gw_published', { userId: u.id, wsId, meta: { giveawayId: created.id, chatId: ws.channel_id, messageId: sent.message_id } });

        await clearDraft(ctx.from.id);
        await ctx.answerCallbackQuery({ text: 'Опубликовано ✅' });
        await renderGwOpen(ctx, u.id, created.id);
      } catch (e) {
        await ctx.answerCallbackQuery({ text: 'Не удалось опубликовать.' });
        await ctx.editMessageText(
          `⚠️ Не удалось отправить пост в канал.\n\nПроверь: бот админ в канале, есть право писать.\n\nОшибка: ${escapeHtml(String(e?.message || e))}`,
          { parse_mode: 'HTML', reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:ws_open|ws:${wsId}`) }
        );
      }
      return;
    }

    // Join / Check
    if (p.a === 'a:gw_join') {
      const gwId = Number(p.i);
      const pub = String(p.pub || '') === '1';
      const g = await db.getGiveawayInfoForUser(gwId);
      if (!g) return ctx.answerCallbackQuery({ text: 'Конкурс не найден.' });

      // Ensure entry exists
      await db.upsertGiveawayEntry(gwId, u.id);
      await db.auditGiveaway(gwId, g.workspace_id, u.id, 'gw.joined', { from: 'button' });

      const sponsors = await db.listGiveawaySponsors(gwId);
      const entryNow = await db.getEntryStatus(gwId, u.id);

      await ctx.answerCallbackQuery({ text: '🎟 Участие записано' });

      const screen = renderParticipantScreen(g, entryNow, { hint: true, sponsors });
      const kb = participantKb(gwId, entryNow, { pub });

      try {
        await ctx.editMessageText(screen, { parse_mode: 'HTML', reply_markup: kb });
      } catch {
        await ctx.reply(screen, { parse_mode: 'HTML', reply_markup: kb });
      }
      return;
    }

    if (p.a === 'a:gw_check') {
      const gwId = Number(p.i);
      const isPub = String(p.pub || '') === '1';
      const g = await db.getGiveawayInfoForUser(gwId);
      if (!g) return ctx.answerCallbackQuery({ text: 'Конкурс не найден.' });

      // Ensure entry exists
      await db.upsertGiveawayEntry(gwId, u.id);
      const entry0 = await db.getEntryStatus(gwId, u.id);
      const sponsors = await db.listGiveawaySponsors(gwId);

      // Instant feedback (perceived speed)
      await ctx.answerCallbackQuery({ text: '⏳ Проверяю…' });
      try {
        const text0 = renderParticipantScreen(g, entry0, { checking: true, sponsors });
        await ctx.editMessageText(text0, { parse_mode: 'HTML', reply_markup: participantKb(gwId, entry0, { pub: isPub }) });
      } catch {
        // ignore edit errors
      }

      const check = await doEligibilityCheck(ctx, gwId, ctx.from.id);
      await db.setEntryEligibility(gwId, u.id, check.isEligible);
      await db.auditGiveaway(gwId, g.workspace_id, u.id, 'gw.checked', { isEligible: check.isEligible, unknown: check.unknown, results: check.results });

      try {
        const entry = await db.getEntryStatus(gwId, u.id);
        const text1 = renderParticipantScreen(g, entry, { hint: true, sponsors, elig: check });
        await ctx.editMessageText(text1, { parse_mode: 'HTML', reply_markup: participantKb(gwId, entry, { pub: isPub, blocker: check.firstBlocker, firstBlockerHandle: check.firstBlockerHandle }) });
      } catch {
        const msg = check.isEligible ? '✅ Участие подтверждено!' : '⚠️ Пока не подтверждено.';
        await ctx.reply(msg + (check.unknown ? '\n\n💡 Если бот не может проверить — попроси админа добавить бота в канал-спонсор.' : ''));
      }
      return;
    }

    // Reminder
    if (p.a === 'a:gw_remind_q') {
      const gwId = Number(p.i);
      const g = await db.getGiveawayForOwner(gwId, u.id);
      if (!g) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      if (String(g.status).toUpperCase() === 'ENDED') return ctx.answerCallbackQuery({ text: 'Уже завершен.' });

      const kb = new InlineKeyboard()
        .text('✅ Да, отправить', `a:gw_remind_send|i:${gwId}`)
        .text('❌ Отмена', `a:gw_open|i:${gwId}`);

      await ctx.answerCallbackQuery();
      await ctx.editMessageText('📣 Отправить напоминание в канал конкурса?\n\nЭто поднимет Eligible %.', { reply_markup: kb });
      return;
    }

    if (p.a === 'a:gw_remind_send') {
      const gwId = Number(p.i);
      const g = await db.getGiveawayForOwner(gwId, u.id);
      if (!g) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      if (!g.published_chat_id) return ctx.answerCallbackQuery({ text: 'Конкурс не опубликован?' });

      const rlKey = k(['rl', 'gw_remind', gwId]);
      const ok = await redis.set(rlKey, '1', { nx: true, ex: 30 * 60 });
      if (!ok) return ctx.answerCallbackQuery({ text: 'Уже отправляли недавно.' });

      const sponsors = await db.listGiveawaySponsors(gwId);
      const hasSponsors = Array.isArray(sponsors) && sponsors.length > 0;

      // Use a direct "check" deep-link so the channel button always works and takes the user straight to eligibility check.
      const link = `https://t.me/${CFG.BOT_USERNAME}?start=gw_${gwId}`;
      const line1 = hasSponsors
        ? '1) Подпишись на канал конкурса (этот канал) и на все каналы-спонсоры'
        : '1) Подпишись на канал конкурса (этот канал)';
      const text =
`📣 <b>Напоминание участникам</b>\n\nЧтобы участие засчиталось ✅\n${line1}\n2) Открой бота и нажми <b>«Проверить»</b>\n\n🤖 Бот: ${escapeHtml(link)}`;

      try {
        const sent = await ctx.api.sendMessage(Number(g.published_chat_id), text, {
          parse_mode: 'HTML',
          disable_web_page_preview: true,
          reply_markup: { inline_keyboard: [[{ text: '🤖 Открыть бота', url: link }]] }
        });
        await db.auditGiveaway(gwId, g.workspace_id, u.id, 'gw.reminder_posted', { chat_id: g.published_chat_id, message_id: sent.message_id });
        await ctx.answerCallbackQuery({ text: 'Отправлено ✅' });
        // Go back to the giveaway card to avoid leaving a "success" message hanging in the chat.
        await renderGwOpen(ctx, u.id, gwId);
      } catch (e) {
        await redis.del(rlKey);
        await ctx.answerCallbackQuery({ text: 'Не удалось.' });
        await ctx.editMessageText(`⚠️ Ошибка отправки: ${escapeHtml(String(e?.message || e))}`, { parse_mode: 'HTML', reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:gw_open|i:${gwId}`) });
      }
      return;
    }

    // End now
    if (p.a === 'a:gw_end_now') {
      const gwId = Number(p.i);
      const g = await db.getGiveawayForOwner(gwId, u.id);
      if (!g) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const kb = new InlineKeyboard()
        .text('✅ Завершить', `a:gw_end_do|i:${gwId}`)
        .text('❌ Отмена', `a:gw_open|i:${gwId}`);
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('🏁 Завершить конкурс сейчас?', { reply_markup: kb });
      return;
    }

    if (p.a === 'a:gw_end_do') {
      const gwId = Number(p.i);
      const g = await db.getGiveawayForOwner(gwId, u.id);
      if (!g) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.updateGiveaway(gwId, { status: 'ENDED' });
      await db.auditGiveaway(gwId, g.workspace_id, u.id, 'gw.ended', { manual: true });
      await ctx.answerCallbackQuery({ text: 'Завершен' });
      await renderGwOpen(ctx, u.id, gwId);
      return;
    }

    // Fallback
    await ctx.answerCallbackQuery({ text: 'Неизвестное действие.' });
  });

  BOT = bot;
  return bot;
  }

// -----------------------------
// Verification (feature-flag)
// -----------------------------

async function renderVerifyInfo(ctx) {
  const kb = new InlineKeyboard()
    .text('⬅️ Назад', 'a:verify_home')
    .text('⬅️ Меню', 'a:menu');

  const text = `✅ <b>Верификация</b>

Зачем это нужно:
• ✅ знак повышает доверие в ленте
• брендам проще писать блогерам
• меньше спама и фейков

Как получить:
1) Подай заявку (1 сообщение)
2) Модератор проверит
3) Получишь ответ в этом чате`;

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderVerifyHome(ctx, userRow) {
  const v = await safeUserVerifications(() => db.getUserVerification(userRow.id), async () => null);
  const status = String(v?.status || 'NONE').toUpperCase();
  const kind = String(v?.kind || 'creator');

  const verifiedLimit = Math.max(0, Number(CFG.INTRO_DAILY_LIMIT || 0));
  const unverifiedLimit = Math.max(0, Number(CFG.INTRO_DAILY_LIMIT_UNVERIFIED || 0));
  const brandLimitLine = (verifiedLimit > unverifiedLimit && verifiedLimit > 0)
    ? `• Лимит интро в день: <b>${unverifiedLimit}</b> → <b>${verifiedLimit}</b>`
    : `• Более высокий лимит интро (после одобрения)`;
  const benefits = kind === 'brand'
    ? `

<b>Преимущества</b>:
${brandLimitLine}
• Больше доверия и выше шанс ответа
`
    : `

<b>Преимущества</b>:
• Бейдж ✅ рядом с каналом в ленте офферов и в диалогах
• Больше доверия со стороны брендов
`;

  let statusLine = '';
  if (status === 'APPROVED') statusLine = '✅ <b>Верифицирован(а)</b>';
  else if (status === 'PENDING') statusLine = '⏳ <b>На проверке</b>';
  else if (status === 'REJECTED') statusLine = '❌ <b>Отклонено</b>';
  else statusLine = '—';

  const kb = new InlineKeyboard();
  if (!v) {
    kb.text('🧑‍🎨 Я Creator', 'a:verify_kind|k:creator').row();
    kb.text('🏷 Я Brand', 'a:verify_kind|k:brand').row();
  } else if (status === 'REJECTED') {
    kb.text('🔁 Подать заново', `a:verify_kind|k:${kind}`).row();
  }
  kb.text('ℹ️ Как это работает', 'a:verify_info').row();
  kb.text('⬅️ Меню', 'a:menu');

  const reason = status === 'REJECTED' && v?.rejection_reason ? `

Причина:
${escapeHtml(v.rejection_reason)}` : '';
  const submitted = v?.submitted_at ? fmtTs(v.submitted_at) : null;
  const submittedLine = v ? `
Заявка: <tg-spoiler>${escapeHtml(submitted || '—')}</tg-spoiler>` : '';

  const text = `✅ <b>Верификация</b>

Статус: ${statusLine}
Тип: <b>${escapeHtml(kind)}</b>${submittedLine}${reason}

${benefits}
Чтобы отправить заявку — выбери роль и пришли 1 сообщение с пруфами.`;

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

// -----------------------------
// Admin helpers (payments + moderators)
// -----------------------------

async function renderAdminHome(ctx) {
  // Access is checked in the callback handler via isSuperAdminTg().

  let text = '👑 Админ-панель\n\n';
  text += '• Платежи: manual/apply\n';
  text += '• Метрики: DAU/MAU, конверсии, воронки\n';
  if (CFG.OFFICIAL_PUBLISH_ENABLED) text += '• Офиц.канал: очередь публикаций\n';

  const kb = new InlineKeyboard()
    .text('💰 Платежи', 'a:admin_payments')
    .row()
    .text('📈 Метрики', 'a:admin_metrics|d:14')
    .row();

  if (CFG.OFFICIAL_PUBLISH_ENABLED) {
    kb.text('📣 Офиц.канал', 'a:off_queue|p:0').row();
  }

  kb.text('➕ Добавить модератора', 'a:admin_mod_add')
    .row()
    .text('📋 Модераторы', 'a:admin_mod_list')
    .row()
    .text('⬅️ Назад', 'a:menu');

  await ctx.editMessageText(text, { reply_markup: kb });
}


async function renderAdminMetrics(ctx, days = 14) {
  const d = Math.max(1, Math.min(90, Number(days) || 14));
  const snap = await db.getAdminMetricsSnapshot(d);

  const usersTotal = snap?.users_total ?? '—';
  const wsTotal = snap?.workspaces_total ?? '—';
  const gwTotal = snap?.giveaways_total ?? '—';
  const gwActive = snap?.giveaways_active ?? '—';
  const offersTotal = snap?.offers_total ?? '—';
  const offersActive = snap?.offers_active ?? '—';

  let text = `📈 <b>Метрики</b> · окно <b>${d}д</b>

`;
  text += `👥 Пользователи: <b>${escapeHtml(String(usersTotal))}</b>
`;
  text += `📣 Каналы: <b>${escapeHtml(String(wsTotal))}</b>
`;
  text += `🎁 Конкурсы: <b>${escapeHtml(String(gwActive))}</b> активн. / <b>${escapeHtml(String(gwTotal))}</b> всего
`;
  text += `📦 Офферы: <b>${escapeHtml(String(offersActive))}</b> активн. / <b>${escapeHtml(String(offersTotal))}</b> всего
`;

  // Payments summary
  const pays = Array.isArray(snap?.payments) ? snap.payments : [];
  if (pays.length) {
    const byCurrency = new Map();
    for (const r of pays) {
      const cur = String(r.currency || '');
      const status = String(r.status || '');
      const key = `${status}::${cur}`;
      const prev = byCurrency.get(key) || { cnt: 0, amount_sum: 0 };
      byCurrency.set(key, { cnt: prev.cnt + Number(r.cnt || 0), amount_sum: prev.amount_sum + Number(r.amount_sum || 0) });
    }

    text += `
💳 <b>Payments</b> (за ${d}д)
`;
    for (const [key, v] of byCurrency.entries()) {
      const [status, cur] = key.split('::');
      text += `• ${escapeHtml(status)}: <b>${escapeHtml(String(v.cnt))}</b> / <b>${escapeHtml(String(v.amount_sum))} ${escapeHtml(cur)}</b>
`;
    }
  }

  // Optional analytics
  const topline = snap?.analytics_topline || null;
  if (topline) {
    text += `
📊 <b>Активность</b>
`;
    text += `DAU(24h): <b>${escapeHtml(String(topline.dau_24h ?? 0))}</b> · `;
    text += `WAU(7d): <b>${escapeHtml(String(topline.wau_7d ?? 0))}</b> · `;
    text += `MAU(30d): <b>${escapeHtml(String(topline.mau_30d ?? 0))}</b>
`;

    // Show last 7 days table (if available)
    const daily = Array.isArray(snap?.analytics_daily) ? snap.analytics_daily : [];
    if (daily.length) {
      const rows = daily.slice(0, 7);
      text += `
📅 Последние дни (MSK)
`;
      for (const r of rows) {
        const day = escapeHtml(String(r.day || '')); // already date
        text += `• ${day}: DAU ${escapeHtml(String(r.dau ?? 0))}, starts ${escapeHtml(String(r.starts ?? 0))}, ws ${escapeHtml(String(r.ws_created ?? 0))}, gw ${escapeHtml(String(r.gw_published ?? 0))}
`;
      }
    }
  } else {
    text += `
ℹ️ Analytics выключены (ANALYTICS_ENABLED=false) — показываю базовые счётчики.`;
  }

  const kb = new InlineKeyboard()
    .text('7д', 'a:admin_metrics|d:7')
    .text('14д', 'a:admin_metrics|d:14')
    .row()
    .text('30д', 'a:admin_metrics|d:30')
    .text('90д', 'a:admin_metrics|d:90')
    .row()
    .text('📋 Модераторы', 'a:admin_mod_list')
    .row()
    .text('⬅️ Админка', 'a:admin_home');

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderAdminModerators(ctx) {
  const rows = await db.listNetworkModerators();

  let text = `📋 <b>Модераторы</b>

`;
  if (!rows.length) {
    text += 'Пока нет модераторов.';
  } else {
    for (const r of rows) {
      const who = r.tg_username ? '@' + r.tg_username : 'id ' + r.tg_id;
      const when = r.created_at ? new Date(r.created_at).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : '—';
      text += `• <b>${escapeHtml(who)}</b> · ${escapeHtml(when)}
`;
    }
  }

  const kb = new InlineKeyboard()
    .text('➕ Добавить модератора', 'a:admin_mod_add')
    .row();

  // Remove buttons
  for (const r of rows) {
    const who = r.tg_username ? '@' + r.tg_username : 'id ' + r.tg_id;
    kb.text(`🗑 ${who}`, `a:admin_mod_rm|uid:${r.user_id}`).row();
  }

  kb.text('⬅️ Админка', 'a:admin_home');

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderAdminPayments(ctx, statusRaw = 'ORPHANED', page = 0) {
  const status = String(statusRaw || 'ORPHANED').toUpperCase();
  const limit = 10;
  const offset = Math.max(0, Number(page) || 0) * limit;

  const rows = await db.listPaymentsByStatus(status, limit, offset);
  const lines = rows
    .map((r) => {
      const when = new Date(r.created_at).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' });
      const who = r.username ? '@' + r.username : 'id ' + r.tg_id;
      return `#${r.id} • ${r.kind} • ${who} • ${r.total_amount} ${r.currency} • ${when}`;
    })
    .join('\n') || 'Платежей нет.';

  const kb = new InlineKeyboard();
  for (const r of rows) {
    kb.text(`#${r.id} • ${r.kind}`, `a:admin_pay_view|id:${r.id}|st:${status}|p:${Math.max(0, Number(page) || 0)}`).row();
  }
  if ((Number(page) || 0) > 0) kb.text('⬅️ Назад', `a:admin_payments|st:${status}|p:${Number(page) - 1}`);
  if (rows.length === limit) kb.text('➡️ Далее', `a:admin_payments|st:${status}|p:${Number(page) + 1}`);
    kb.row().text('⬅️ Админка', 'a:admin_home');

  await ctx.editMessageText(
    `💳 <b>Payments</b> • <b>${escapeHtml(status)}</b>

${escapeHtml(lines)}`,
    { parse_mode: 'HTML', reply_markup: kb }
  );
}

async function renderAdminPaymentView(ctx, paymentId, backStatus = 'ORPHANED', page = 0) {
  const p = await db.getPaymentById(Number(paymentId));
  if (!p) {
    await ctx.editMessageText('⚠️ Платеж не найден.', { reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:admin_payments|st:${backStatus}|p:${page}`) });
    return;
  }

  const payload = String(p.invoice_payload || '');
  const when = new Date(p.created_at).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' });
  const who = p.username ? '@' + p.username : 'id ' + p.tg_id;
  const canApply = (p.status === 'ORPHANED' || p.status === 'ERROR' || p.status === 'RECEIVED') &&
    (payload.startsWith('pro_') || payload.startsWith('brand_') || payload.startsWith('bplan_') || payload.startsWith('offpub_'));

  const kb = new InlineKeyboard();
  if (canApply) kb.text('✅ Apply (manual)', `a:admin_pay_apply|id:${p.id}|st:${backStatus}|p:${page}`).row();
  kb.text('⬅️ К списку', `a:admin_payments|st:${backStatus}|p:${page}`).row();
  kb.text('⬅️ Админка', 'a:admin_home');

  const text = `💳 <b>Payment #${p.id}</b>

Status: <b>${escapeHtml(p.status)}</b>
Kind: <b>${escapeHtml(p.kind)}</b>
User: <b>${escapeHtml(who)}</b>
Amount: <b>${p.total_amount} ${escapeHtml(p.currency)}</b>
Created: <b>${escapeHtml(when)}</b>

Charge:
<tg-spoiler>${escapeHtml(String(p.telegram_payment_charge_id || '—'))}</tg-spoiler>

Payload:
<tg-spoiler>${escapeHtml(payload)}</tg-spoiler>

Note:
<tg-spoiler>${escapeHtml(String(p.note || '—'))}</tg-spoiler>`;

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function adminApplyPayment(ctx, adminUserRow, paymentId, backStatus = 'ORPHANED', page = 0) {
  const row = await db.getPaymentById(Number(paymentId));
  if (!row) {
    await ctx.answerCallbackQuery({ text: 'Платеж не найден.', show_alert: true });
    await renderAdminPayments(ctx, backStatus, page);
    return;
  }

  if (row.status === 'APPLIED') {
    await ctx.answerCallbackQuery({ text: 'Уже применён ✅', show_alert: true });
    await renderAdminPaymentView(ctx, row.id, backStatus, page);
    return;
  }

  const payload = String(row.invoice_payload || '');
  try {
    if (payload.startsWith('pro_')) {
      const parts = payload.split('_');
      const wsId = Number(parts[1]);
      if (!wsId) throw new Error('Bad wsId');
      await db.activateWorkspacePro(wsId, CFG.PRO_DURATION_DAYS);
      await db.auditWorkspace(wsId, adminUserRow.id, 'pro.activated.manual', {
        payment_id: row.id,
        telegram_payment_charge_id: row.telegram_payment_charge_id
      });
      await db.markPaymentApplied(row.id, adminUserRow.id, 'manual_apply_pro');
      await ctx.answerCallbackQuery({ text: 'PRO применён ✅', show_alert: true });
      await renderAdminPaymentView(ctx, row.id, backStatus, page);
      return;
    }

    if (payload.startsWith('brand_')) {
      const parts = payload.split('_');
      const userId = Number(parts[1]);
      const packId = Number(parts[2]);
      const pack = getBrandPack(packId);
      if (!userId || !pack) throw new Error('Bad userId/pack');
      await db.addBrandCredits(userId, Number(pack.credits));
      await db.markPaymentApplied(row.id, adminUserRow.id, `manual_apply_brand_pass:+${pack.credits}`);
      await ctx.answerCallbackQuery({ text: 'Brand Pass применён ✅', show_alert: true });
      await renderAdminPaymentView(ctx, row.id, backStatus, page);
      return;
    }

    if (payload.startsWith('bplan_')) {
      const parts = payload.split('_');
      const userId = Number(parts[1]);
      const plan = String(parts[2] || 'basic').toLowerCase();
      if (!userId) throw new Error('Bad userId');
      await db.activateBrandPlan(userId, plan, CFG.BRAND_PLAN_DURATION_DAYS);
      await db.markPaymentApplied(row.id, adminUserRow.id, `manual_apply_brand_plan:${plan}`);
      await ctx.answerCallbackQuery({ text: 'Brand Plan применён ✅', show_alert: true });
      await renderAdminPaymentView(ctx, row.id, backStatus, page);
      return;
    }

    if (payload.startsWith('offpub_')) {
      const parts = payload.split('_');
      const offerId = Number(parts[2]);
      const days = Number(parts[3] || CFG.OFFICIAL_MANUAL_DEFAULT_DAYS);
      if (!CFG.OFFICIAL_PUBLISH_ENABLED) throw new Error('Official publishing disabled');
      if (!offerId) throw new Error('Bad offerId');
      await publishOfferToOfficialChannel(ctx.api, offerId, {
        placementType: 'PAID',
        paymentId: row.id,
        days,
        publishedByUserId: adminUserRow.id,
        keepExpiry: false
      });
      await db.markPaymentApplied(row.id, adminUserRow.id, `manual_apply_official_publish:${offerId}:${days}d`);
      await ctx.answerCallbackQuery({ text: 'Опубликовано ✅', show_alert: true });
      await renderAdminPaymentView(ctx, row.id, backStatus, page);
      return;
    }

    // match/feat or unknown
    await ctx.answerCallbackQuery({ text: 'Эта услуга не поддерживает apply.', show_alert: true });
    await renderAdminPaymentView(ctx, row.id, backStatus, page);
    return;
  } catch (e) {
    const msg = String(e?.message || e);
    try {
      await db.setPaymentStatus(row.id, 'ERROR', `manual_apply_error: ${msg.slice(0, 160)}`);
    } catch {
      // ignore
    }
    await ctx.answerCallbackQuery({ text: `Ошибка apply: ${msg.slice(0, 64)}`, show_alert: true });
    await renderAdminPaymentView(ctx, row.id, backStatus, page);
  }
}

// -----------------------------
// Moderation render helpers (v1.0.0)
// -----------------------------

async function renderModHome(ctx) {
  const kb = new InlineKeyboard()
    .text('🚩 Жалобы/споры', 'a:mod_reports');

  if (CFG.VERIFICATION_ENABLED) {
    const pending = await safeUserVerifications(() => db.countPendingVerifications(), async () => 0);
    kb.row().text(`✅ Верификации (${pending})`, 'a:mod_verifs');
  }

  if (CFG.OFFICIAL_PUBLISH_ENABLED) {
    kb.row().text('📣 Офиц.канал', 'a:off_queue|p:0');
  }

  kb.row().text('⬅️ Меню', 'a:menu');
  await ctx.editMessageText('🛡 <b>Модерация</b>\n\nВыбери действие:', { parse_mode: 'HTML', reply_markup: kb });
}

async function renderModReports(ctx, page = 0) {
  const limit = 10;
  const offset = page * limit;
  const rows = await db.listOpenBarterReports(limit, offset);

  const lines = rows.map((r) => {
    const kind = r.thread_id ? 'thread' : 'offer';
    const who = r.reporter_username ? '@' + r.reporter_username : 'id ' + r.reporter_tg_id;
    const when = new Date(r.created_at).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' });
    return `#${r.id} • ${kind} • ${who} • ${when}`;
  }).join('\n') || 'Пока нет открытых жалоб.';

  const kb = new InlineKeyboard();
  for (const r of rows) {
    kb.text(`#${r.id}`, `a:mod_report|r:${r.id}`).row();
  }
  if (page > 0) kb.text('⬅️ Назад', `a:mod_reports|p:${page - 1}`);
  if (rows.length === limit) kb.text('➡️ Далее', `a:mod_reports|p:${page + 1}`);
    kb.row().text('⬅️ Модерация', 'a:mod_home');

  await ctx.editMessageText(`🚩 <b>Очередь жалоб</b>\n\n${escapeHtml(lines)}`, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderModReportView(ctx, reportId) {
  const r = await db.getBarterReport(reportId);
  if (!r) {
    await ctx.editMessageText('Жалоба не найдена.', { reply_markup: new InlineKeyboard().text('⬅️ Назад', 'a:mod_reports') });
    return;
  }
  const who = r.reporter_username ? '@' + r.reporter_username : 'id ' + r.reporter_tg_id;
  const created = new Date(r.created_at).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' });

  const kb = new InlineKeyboard();
  if (r.offer_id) kb.text('⛔️ Заморозить оффер', `a:mod_r_freeze|r:${r.id}`).row();
  if (r.thread_id) kb.text('🔒 Закрыть тред', `a:mod_r_close|r:${r.id}`).row();
  kb.text('✅ Закрыть жалобу', `a:mod_r_resolve|r:${r.id}`).row();
  kb.text('⬅️ К очереди', 'a:mod_reports').row();

  const text = `🚩 <b>Жалоба #${r.id}</b>\n\n` +
    `От: ${escapeHtml(who)}\n` +
    `Когда: ${escapeHtml(created)}\n` +
    `Статус: ${escapeHtml(r.status)}\n` +
    (r.offer_id ? `Оффер: #${r.offer_id}\n` : '') +
    (r.thread_id ? `Тред: #${r.thread_id}\n` : '') +
    `\nПричина:\n${escapeHtml(r.reason || '—')}`;

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}


async function renderModVerifs(ctx, page = 0) {
  const limit = 10;
  const offset = page * limit;
  const rows = await safeUserVerifications(() => db.listPendingVerifications(limit, offset), async () => []);
  const total = await safeUserVerifications(() => db.countPendingVerifications(), async () => 0);

  const kb = new InlineKeyboard();
  for (const r of rows) {
    const who = r.tg_username ? '@' + r.tg_username : ('tg:' + r.tg_id);
    const kind = String(r.kind || 'creator');
    kb.text(`👀 ${who} · ${kind}`, `a:mod_verif_view|uid:${r.user_id}|p:${page}`).row();
  }

  const hasPrev = page > 0;
  const hasNext = offset + rows.length < total;
  if (hasPrev) kb.text('⬅️ Назад', `a:mod_verifs|p:${page - 1}`);
  if (hasNext) kb.text('➡️ Далее', `a:mod_verifs|p:${page + 1}`);
    kb.row().text('⬅️ Модерация', 'a:mod_home');

  const text = `✅ <b>Верификации</b>

Ожидают: <b>${total}</b>

` + (rows.length
    ? rows.map((r) => {
      const who = r.tg_username ? '@' + r.tg_username : ('tg:' + r.tg_id);
      const when = r.submitted_at ? fmtTs(r.submitted_at) : '—';
      const kind = String(r.kind || 'creator');
      return `• <b>${escapeHtml(who)}</b> · ${escapeHtml(kind)} · <tg-spoiler>${escapeHtml(when)}</tg-spoiler>`;
    }).join('\n')
    : 'Пока нет заявок.');

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderModVerifView(ctx, userId, page = 0) {
  const v = await safeUserVerifications(() => db.getUserVerification(userId), async () => null);
  if (!v) return ctx.answerCallbackQuery({ text: 'Заявка не найдена.' });

  const who = v.tg_username ? '@' + v.tg_username : ('tg:' + v.tg_id);
  const when = v.submitted_at ? fmtTs(v.submitted_at) : '—';
  const kind = String(v.kind || 'creator');
  const text = `✅ <b>Заявка на верификацию</b>

` +
    `Пользователь: <b>${escapeHtml(who)}</b>
` +
    `Тип: <b>${escapeHtml(kind)}</b>
` +
    `Когда: <tg-spoiler>${escapeHtml(when)}</tg-spoiler>

` +
    `<b>Текст заявки:</b>
${escapeHtml(v.submitted_text || '—')}`;

  const kb = new InlineKeyboard()
    .text('✅ Approve', `a:mod_verif_approve|uid:${userId}|p:${page}`)
    .text('❌ Reject', `a:mod_verif_reject|uid:${userId}|p:${page}`)
    .row()
    .text('⬅️ К очереди', `a:mod_verifs|p:${page}`)
    .text('⬅️ Модерация', 'a:mod_home');

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

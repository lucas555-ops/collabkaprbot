import { Bot, InlineKeyboard } from 'grammy';
import { CFG, assertEnv } from '../lib/config.js';
import { redis, k, rateLimit } from '../lib/redis.js';
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

// Runtime toggles (stored in Redis, editable from Admin)
const SYS_KEYS = {
  pay_accept: k(['sys', 'pay_accept']),
  pay_auto_apply: k(['sys', 'pay_auto_apply'])
};

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

async function getRoleFlags(userRow, tgId) {
  const isAdmin = isSuperAdminTg(tgId);
  const isModerator = isAdmin || (userRow ? await db.isNetworkModerator(userRow.id) : false);
  const isFolderEditor = userRow ? await db.hasAnyWorkspaceEditorRole(userRow.id) : false;
  return { isAdmin, isModerator, isFolderEditor };
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


function mainMenuKb(flags = {}) {
  const { isModerator = false, isAdmin = false, isFolderEditor = false } = flags;
  const kb = new InlineKeyboard()
    .text('🚀 Подключить канал', 'a:setup')
    .row()
    .text('📣 Мои каналы', 'a:ws_list')
    .row();
  if (isFolderEditor) kb.text('📁 Папки', 'a:folders_my').row();
  kb
    .text('🎁 Мои конкурсы', 'a:gw_list')
    .row()
    .text('🤝 Бартер-биржа', 'a:bx_home')
    .row()
    .text('🏷 Brand Mode', 'a:bx_open|ws:0');
  if (CFG.VERIFICATION_ENABLED) kb.row().text('✅ Верификация', 'a:verify_home');
  if (isModerator) kb.row().text('🛡 Модерация', 'a:mod_home');
  if (isAdmin) kb.row().text('👑 Админка', 'a:admin_home');
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

async function setActiveWorkspace(tgId, wsId) {
  await redis.set(k(['active_ws', tgId]), String(wsId), { ex: 30 * 24 * 3600 });
}
async function getActiveWorkspace(tgId) {
  const v = await redis.get(k(['active_ws', tgId]));
  const n = Number(v);
  return n > 0 ? n : null;
}

function wsMenuKb(wsId) {
  return new InlineKeyboard()
    .text('➕ Новый конкурс', `a:gw_new|ws:${wsId}`)
    .row()
    .text('🎁 Конкурсы канала', `a:gw_list_ws|ws:${wsId}`)
    .row()
    .text('🤝 Бартер-биржа', `a:bx_open|ws:${wsId}`)
    .row()
    .text('📁 Папки', `a:folders_home|ws:${wsId}`)
    .row()
    .text('👤 Профиль', `a:ws_profile|ws:${wsId}`)
    .text('⭐️ PRO', `a:ws_pro|ws:${wsId}`)
    .row()
    .text('⚙️ Настройки', `a:ws_settings|ws:${wsId}`)
    .row()
    .text('🧾 История', `a:ws_history|ws:${wsId}`)
    .row()
    .text('⬅️ Назад', 'a:ws_list');
}

function wsSettingsKb(wsId, s) {
  const net = s.network_enabled ? '✅ Сеть: ВКЛ' : '🌐 Сеть: ВЫКЛ';
  const cur = s.curator_enabled ? '🛡 Куратор: ВКЛ' : '🛡 Куратор: ВЫКЛ';
  return new InlineKeyboard()
    .text(net, `a:ws_toggle_net|ws:${wsId}`)
    .row()
    .text(cur, `a:ws_toggle_cur|ws:${wsId}`)
    .row()
    .text('👤 Пригласить куратора', `a:cur_invite|ws:${wsId}`)
    .row()
    .text('➕ Добавить куратора по @username', `a:cur_add_username|ws:${wsId}`)
    .row()
    .text('👥 Список кураторов', `a:cur_list|ws:${wsId}`)
    .row()
    .text('⬅️ Назад', `a:ws_open|ws:${wsId}`);
}

function curListKb(wsId, curators) {
  const kb = new InlineKeyboard();
  for (const c of curators) {
    const label = c.tg_username ? `@${c.tg_username}` : `id:${c.tg_id}`;
    kb.text(`🗑 ${label}`, `a:cur_rm_q|ws:${wsId}|u:${c.user_id}`).row();
  }
  kb.text('⬅️ Назад', `a:ws_settings|ws:${wsId}`);
  return kb;
}

// -----------------------------
// Barters Marketplace (v0.9.1)
// -----------------------------

function bxMenuKb(wsId) {
  const kb = new InlineKeyboard()
    .text('🛍 Лента', `a:bx_feed|ws:${wsId}|p:0`)
    .text('🎛 Фильтры', `a:bx_filters|ws:${wsId}`)
    .row()
    .text('📨 Inbox', `a:bx_inbox|ws:${wsId}|p:0`)
    .row()
    .text('➕ Разместить оффер', `a:bx_new|ws:${wsId}`)
    .row()
    .text('📦 Мои офферы', `a:bx_my|ws:${wsId}|p:0`)
    .row()
    .text('🏷 Brand Mode', 'a:bx_open|ws:0');

  if (CFG.VERIFICATION_ENABLED) kb.row().text('✅ Верификация', 'a:verify_home');

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
    .row()
    .text(`🎫 Brand Pass: ${credits}${retry ? ' · 🎟' + retry : ''}`, `a:brand_pass|ws:${wsId}`)
    .row()
    .text(`⭐️ Brand Plan: ${planLabel}`, `a:brand_plan|ws:${wsId}`)
    .row()
    .text('🎯 Smart Matching', `a:match_home|ws:${wsId}`)
    .text('🔥 Featured', `a:feat_home|ws:${wsId}`);

  if (CFG.VERIFICATION_ENABLED) kb.row().text('✅ Верификация', 'a:verify_home');

  kb.row().text('⬅️ Назад', 'a:menu');
  return kb;
}

function bxNeedNetworkKb(wsId) {
  return new InlineKeyboard()
    .text('✅ Включить “Сеть”', `a:bx_enable_net|ws:${wsId}`)
    .row()
    .text('⬅️ Назад', `a:ws_open|ws:${wsId}`);
}

function bxCategoryKb(wsId) {
  return new InlineKeyboard()
    .text('💄 Косметика', `a:bx_cat|ws:${wsId}|c:cosmetics`)
    .row()
    .text('🧴 Уход', `a:bx_cat|ws:${wsId}|c:skincare`)
    .row()
    .text('🎀 Аксессуары', `a:bx_cat|ws:${wsId}|c:accessories`)
    .row()
    .text('✨ Другое', `a:bx_cat|ws:${wsId}|c:other`)
    .row()
    .text('🧩 Шаблоны', `a:bx_preset_home|ws:${wsId}`)
    .row()
    .text('⬅️ Отмена', `a:bx_open|ws:${wsId}`);
}

const BX_PRESETS = [
  {
    id: 'review_barter_skincare',
    title: '🎥 Обзор/распаковка за бартер (уход)',
    category: 'skincare',
    offer_type: 'review',
    compensation_type: 'barter',
    example:
      'Заголовок: Ищу магазин уходовой косметики для обзора\n\nУсловия: обзор + 3 сторис. Аудитория: 500–2k. Гео: РФ. Хочу: бартер (продукты для обзора). Контакт: @myname'
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
    kb.text('💄 Косметика', `a:bx_fset|ws:${wsId}|k:cat|v:cosmetics|p:${page}`).row();
    kb.text('🧴 Уход', `a:bx_fset|ws:${wsId}|k:cat|v:skincare|p:${page}`).row();
    kb.text('🎀 Аксессуары', `a:bx_fset|ws:${wsId}|k:cat|v:accessories|p:${page}`).row();
    kb.text('✨ Другое', `a:bx_fset|ws:${wsId}|k:cat|v:other|p:${page}`).row();
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
    .text('🤝 Бартер', `a:gw_prize|ws:${wsId}|t:barter`)
    .text('🎟 Сертификат', `a:gw_prize|ws:${wsId}|t:cert`)
    .row()
    .text('💸 ₽', `a:gw_prize|ws:${wsId}|t:rub`)
    .text('⭐️ Звезды', `a:gw_prize|ws:${wsId}|t:stars`)
    .row()
    .text('✍️ Другое', `a:gw_prize|ws:${wsId}|t:other`)
    .row()
    .text('🧩 Пресеты', `a:gw_preset_home|ws:${wsId}`)
    .row()
    .text('⬅️ Отмена', `a:ws_open|ws:${wsId}`);
}

const GW_PRESETS = [
  {
    id: 'product_barter',
    title: '🎁 Розыгрыш продукта (бартер)',
    prize_type: 'barter',
    prize_value_text: 'Розыгрыш продукта от спонсора (бартер). Доставка/условия — уточняем в треде.'
  },
  {
    id: 'cert_discount',
    title: '🎟 Сертификат / скидка',
    prize_type: 'cert',
    prize_value_text: 'Сертификат/скидка от магазина (условия и номинал — в описании/в треде).'
  },
  {
    id: 'cash_rub',
    title: '💸 Денежный приз (₽)',
    prize_type: 'rub',
    prize_value_text: 'Денежный приз в ₽. Сумма и способ выплаты — указать в описании.'
  }
];

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
    .text('📣 Опубликовать', `a:gw_publish|ws:${wsId}`)
    .row()
    .text('⬅️ Назад', `a:gw_step_deadline|ws:${wsId}`);
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
    ;

  if (String(g.status || '').toUpperCase() === 'WINNERS_DRAWN' && !g.results_message_id && g.published_chat_id) {
    kb.text('📣 Опубликовать итоги', `a:gw_publish_results|i:${gwId}`).row();
  }

  kb
    .text('🏁 Завершить сейчас', `a:gw_end_now|i:${gwId}`)
    .row()
    .text('⬅️ Назад', 'a:gw_list');
  return kb;
}

function participantKb(gwId) {
  return new InlineKeyboard()
    .text('🔄 Проверить', `a:gw_check|i:${gwId}`)
    .row()
    .text('✅ Участвовать', `a:gw_join|i:${gwId}`)
    .row()
    .text('🧾 Лог конкурса', `a:gw_log|i:${gwId}`);
}


function gwEntriesKb(gwId, page, total, pageSize) {
  const p = Math.max(0, Number(page || 0));
  const t = Math.max(0, Number(total || 0));
  const ps = Math.max(1, Number(pageSize || 10));
  const maxPage = Math.max(0, Math.ceil(t / ps) - 1);

  const kb = new InlineKeyboard();

  if (p > 0) kb.text('⬅️ Назад', `a:gw_entries|i:${gwId}|p:${p - 1}`);
  if (p < maxPage) kb.text('➡️ Вперёд', `a:gw_entries|i:${gwId}|p:${p + 1}`);
  if (p > 0 || p < maxPage) kb.row();

  kb
    .text('🔄 Обновить статусы (страница)', `a:gw_entries_refresh|i:${gwId}|p:${p}`)
    .row()
    .text('⬅️ К статистике', `a:gw_stats|i:${gwId}`);

  return kb;
}

function renderParticipantScreen(g, entry) {
  const prize = (g.prize_value_text || '').trim() || '—';
  const ends = g.ends_at ? fmtTs(g.ends_at) : '—';
  const st = String(g.status || '').toUpperCase();

  let stLine;
  if (!entry) stLine = 'Статус: ⛔ <b>не участвуешь</b>';
  else if (entry.is_eligible === true) stLine = 'Статус: ✅ <b>участие подтверждено</b>';
  else if (!entry.last_checked_at) stLine = 'Статус: ⏳ <b>нужно проверить</b>';
  else stLine = 'Статус: ⚠️ <b>пока не подтверждено</b>';

  return (
`🎁 <b>Конкурс #${g.id}</b>

🎁 Приз: <b>${escapeHtml(prize)}</b>
🏆 Мест: <b>${Number(g.winners_count || 1)}</b>
⏳ Итоги: <b>${escapeHtml(ends)}</b>

${stLine}
Статус конкурса: <b>${st}</b>

Нажми “🔄 Проверить”, чтобы подтвердить подписки на каналы.

💡 Если бот не может проверить каналы — попроси админа добавить бота в канал-спонсор.`
  );
}

async function sendSafeDM(ctx, tgId, text, extra = {}) {
  try {
    await ctx.api.sendMessage(tgId, text, { parse_mode: 'HTML', ...extra });
    return true;
  } catch {
    return false;
  }
}

async function ensureWorkspaceForOwner(ctx, ownerUserId) {
  const wsList = await db.listWorkspaces(ownerUserId);
  if (!wsList.length) {
    const u = await db.upsertUser(ctx.from.id, ctx.from.username ?? null);
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
  kb.text('⬅️ В меню', 'a:menu');
  await ctx.editMessageText(`📣 <b>Мои каналы</b>

Выбери канал:`, { parse_mode: 'HTML', reply_markup: kb });
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
  const ws = await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  await db.ensureWorkspaceSettings(wsId);
  const s = await db.getWorkspace(ownerUserId, wsId);
  const settings = {
    network_enabled: s.network_enabled,
    curator_enabled: s.curator_enabled
  };
  await ctx.editMessageText(`⚙️ <b>Настройки</b>

Канал: <b>${escapeHtml(ws.channel_username ? '@' + ws.channel_username : ws.title)}</b>`, {
    parse_mode: 'HTML',
    reply_markup: wsSettingsKb(wsId, settings)
  });
}

async function renderWsHistory(ctx, ownerUserId, wsId) {
  const ws = await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  const items = await db.listWorkspaceAudit(wsId, 20);
  const lines = items.map(i => `• <b>${escapeHtml(i.action)}</b> — ${fmtTs(i.created_at)}`);
  const text = `🧾 <b>История действий</b>

${lines.length ? lines.join('\n') : 'Пока пусто.'}`;
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:ws_open|ws:${wsId}`) });
}


async function renderWsProfile(ctx, ownerUserId, wsId) {
  const ws = await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
  await db.ensureWorkspaceSettings(wsId);
  const s = await db.getWorkspace(ownerUserId, wsId);
  const isPro = await db.isWorkspacePro(wsId);
  const name = s.profile_title || (ws.channel_username ? '@' + ws.channel_username : ws.title);
  const niche = s.profile_niche || '—';
  const contact = s.profile_contact || '—';
  const geo = s.profile_geo || '—';
  const proLine = isPro ? '⭐️ PRO: <b>активен</b>' : '⭐️ PRO: <b>free</b>';

  const text = `👤 <b>Профиль</b>

Канал: <b>${escapeHtml(ws.channel_username ? '@' + ws.channel_username : ws.title)}</b>
${proLine}

Название/витрина: <b>${escapeHtml(name)}</b>
Ниша: <b>${escapeHtml(niche)}</b>
Контакт: <b>${escapeHtml(contact)}</b>
Гео: <b>${escapeHtml(geo)}</b>

💡 Профиль показывается в бирже и помогает брендам быстрее написать тебе.`;

  const kb = new InlineKeyboard()
    .text('✏️ Название', `a:ws_prof_edit|ws:${wsId}|f:title`)
    .text('✏️ Ниша', `a:ws_prof_edit|ws:${wsId}|f:niche`)
    .row()
    .text('✏️ Контакт', `a:ws_prof_edit|ws:${wsId}|f:contact`)
    .text('✏️ Гео', `a:ws_prof_edit|ws:${wsId}|f:geo`)
    .row()
    .text('⬅️ Назад', `a:ws_open|ws:${wsId}`);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderWsPro(ctx, ownerUserId, wsId) {
  const ws = await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
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
  const ws = await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Канал не найден.' });
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

function bxCategoryLabel(c) {
  switch (c) {
    case 'cosmetics': return '💄 Косметика';
    case 'skincare': return '🧴 Уход';
    case 'accessories': return '🎀 Аксессуары';
    default: return '✨ Другое';
  }
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

const BX_CATS = [null, 'cosmetics', 'skincare', 'accessories', 'other'];
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
  return v || { category: null, offerType: null, compensationType: null };
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
      `🏷 <b>Brand Mode</b>

Здесь бренд может пользоваться бартер-биржей без подключения канала.

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
      `🤝 <b>Бартер-биржа</b>

Это “мини-биржа” офферов для микроблогеров (косметика/уход/аксессуары).

Чтобы видеть ленту и публиковать офферы, включи “🌐 Сеть”.`,
      { parse_mode: 'HTML', reply_markup: bxNeedNetworkKb(wsNum) }
    );
    return;
  }

  await ctx.editMessageText(
    `🤝 <b>Бартер-биржа</b>

Канал: <b>${escapeHtml(ws.channel_username ? '@' + ws.channel_username : ws.title)}</b>

• Лента — офферы от участников сети
• Разместить — твой оффер попадет в ленту
• Мои офферы — пауза/удаление`,
    { parse_mode: 'HTML', reply_markup: bxMenuKb(wsNum) }
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
  for (const o of rows) {
    const st = String(o.status || 'ACTIVE').toUpperCase();
    const stEmoji = st === 'ACTIVE' ? '✅' : (st === 'PAUSED' ? '⏸' : '⛔');
    kb.text(`${stEmoji} #${o.id} · ${o.title}`, `a:bx_view|ws:${wsId}|o:${o.id}|back:my`).row();
  }
  kb.text('⬅️ Назад', `a:bx_open|ws:${wsId}`);

  await ctx.editMessageText(
    `📦 <b>Мои офферы</b>

Нажми оффер, чтобы поставить паузу/удалить.`,
    { parse_mode: 'HTML', reply_markup: kb }
  );
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

<b>${escapeHtml(o.title)}</b>

${escapeHtml(o.description)}${partnerBlock}

${contact ? `Контакт: <b>${escapeHtml(contact)}</b>` : ''}`;

  const kb = new InlineKeyboard();
  if (st === 'ACTIVE') {
    kb.text('⬆️ Поднять', `a:bx_bump|ws:${wsId}|o:${o.id}`).row();

    kb.text(partnerBtnLabel, `a:bx_partner_folder_pick|ws:${wsId}|o:${o.id}`).row();

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
  kb.text('🗑 Удалить', `a:bx_del_q|ws:${wsId}|o:${o.id}`).row();
  kb.text('⬅️ Назад', back === 'my' ? `a:bx_my|ws:${wsId}|p:0` : `a:bx_feed|ws:${wsId}|p:0`);

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
    kb.row().text('⬅️ Назад', `a:bx_feed|ws:${wsId}|p:${page}`);

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

async function buildOfficialOfferPost(offerRow) {
  const offerId = Number(offerRow.id);
  const ch = offerRow.channel_username ? `@${offerRow.channel_username}` : (offerRow.ws_title || 'канал');
  const contact = (offerRow.contact || '').trim();
  const link = offerDeepLink(offerId);

  const title = escapeHtml(String(offerRow.title || ''));
  const desc = escapeHtml(truncateText(offerRow.description || '', 900));
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

  const offer = CFG.VERIFICATION_ENABLED
    ? await safeUserVerifications(() => db.getBarterOfferPublicWithVerified(offerId), () => db.getBarterOfferPublic(offerId))
    : await db.getBarterOfferPublic(offerId);
  if (!offer) throw new Error('Offer not found');
  if (String(offer.status || '').toUpperCase() !== 'ACTIVE') throw new Error('Offer is not active');
  if (!offer.network_enabled) throw new Error('Offer is not in network');

  const { text, kb } = await buildOfficialOfferPost(offer);

  const existing = await safeOfficialPosts(() => db.getOfficialPostByOfferId(offerId), async () => null);
  const keepExpiry = Boolean(opts.keepExpiry);
  const placementType = String(opts.placementType || (existing?.placement_type || 'MANUAL')).toUpperCase();

  // Decide expiry
  const days = Math.max(1, Number(opts.days || existing?.slot_days || CFG.OFFICIAL_MANUAL_DEFAULT_DAYS || 3));
  const expiresAt = keepExpiry && existing?.slot_expires_at
    ? new Date(existing.slot_expires_at).toISOString()
    : new Date(Date.now() + days * 24 * 60 * 60 * 1000).toISOString();

  let messageId = null;
  if (existing && existing.message_id && Number(existing.channel_chat_id) === channelId && String(existing.status || '').toUpperCase() === 'ACTIVE') {
    try {
      await api.editMessageText(channelId, Number(existing.message_id), text, { parse_mode: 'HTML', reply_markup: kb });
      messageId = Number(existing.message_id);
    } catch (_) {
      messageId = null;
    }
  }

  if (!messageId) {
    const msg = await api.sendMessage(channelId, text, { parse_mode: 'HTML', reply_markup: kb });
    messageId = Number(msg.message_id);
  }

  await safeOfficialPosts(
    () => db.setOfficialPostActive(offerId, {
            channelChatId: channelId,
      messageId,
      placementType,
      paymentId: opts.paymentId || existing?.payment_id || null,
      slotDays: days,
      slotExpiresAt: expiresAt,
      publishedByUserId: opts.publishedByUserId || null,
    }),
    async () => null,
  );

  return { channelId, messageId, expiresAt, days };
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
      await api.editMessageText(channelId, msgId, text, { parse_mode: 'HTML' });
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

async function renderBxInbox(ctx, userId, wsId, page = 0) {

  const limit = CFG.BARTER_INBOX_PAGE_SIZE;
  const offset = page * limit;
  const rows = CFG.VERIFICATION_ENABLED
    ? await safeUserVerifications(() => db.listBarterThreadsForUserWithVerified(userId, limit, offset), () => db.listBarterThreadsForUser(userId, limit, offset))
    : await db.listBarterThreadsForUser(userId, limit, offset);

  const header = `📨 <b>Inbox</b>

Диалоги по офферам (бренд ↔ блогер).`;
  const kb = new InlineKeyboard();
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

Пополняй, чтобы открывать новые диалоги с микро-каналами.`,
    { parse_mode: 'HTML', reply_markup: kb }
  );
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
async function renderGwList(ctx, ownerUserId, wsId = null) {
  const items = await db.listGiveaways(ownerUserId, 25);
  const filtered = wsId ? items.filter(x => x.workspace_id === wsId) : items;
  if (!filtered.length) {
    await ctx.editMessageText(`🎁 Конкурсов пока нет.

Нажми “➕ Новый конкурс” в канале.`, { reply_markup: new InlineKeyboard().text('⬅️ Назад', wsId ? `a:ws_open|ws:${wsId}` : 'a:menu') });
    return;
  }
  const kb = new InlineKeyboard();
  for (const g of filtered) {
    kb.text(`#${g.id} · ${String(g.status).toUpperCase()}`, `a:gw_open|i:${g.id}`).row();
  }
  kb.text('⬅️ Назад', wsId ? `a:ws_open|ws:${wsId}` : 'a:menu');
  await ctx.editMessageText(`🎁 <b>Мои конкурсы</b>

Выбери конкурс:`, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderGwOpen(ctx, ownerUserId, gwId) {
  const g = await db.getGiveawayForOwner(gwId, ownerUserId);
  if (!g) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  const sponsors = await db.listGiveawaySponsors(gwId);
  const sponsorLines = sponsors.map(s => `• ${escapeHtml(s.sponsor_text)}`).join('\n') || '—';
  const text = `🎁 <b>Конкурс #${g.id}</b>

Статус: <b>${escapeHtml(String(g.status).toUpperCase())}</b>
Приз: <b>${escapeHtml(g.prize_value_text || '—')}</b>
Мест: <b>${g.winners_count}</b>
Дедлайн: <b>${g.ends_at ? escapeHtml(fmtTs(g.ends_at)) : '—'}</b>

Спонсоры:\n${sponsorLines}`;
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: gwOpenKb(g, { isAdmin: isSuperAdminTg(ctx.from?.id) }) });
}

async function renderGwStats(ctx, ownerUserId, gwId) {
  const st = await db.getGiveawayStats(gwId, ownerUserId);
  if (!st) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  if (typeof db.listGiveawayEntriesPage !== 'function') {
    await ctx.editMessageText('⚠️ Нужен апдейт базы: обнови <b>src/db/queries.js</b> (патч listGiveawayEntriesPage).', { parse_mode: 'HTML', reply_markup: new InlineKeyboard().text('⬅️ К статистике', `a:gw_stats|i:${gwId}`) });
    return;
  }
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
  .text('👥 Участники', `a:gw_entries|i:${gwId}|p:0`)
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

async function renderGwEntries(ctx, ownerUserId, gwId, page = 0) {
  const st = await db.getGiveawayStats(gwId, ownerUserId);
  if (!st) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const total = Number(st.entries_total || 0);
  const pageSize = 10;
  const maxPage = Math.max(0, Math.ceil(total / pageSize) - 1);
  const p = Math.min(maxPage, Math.max(0, Number(page || 0)));

  const offset = p * pageSize;
  const rows = await db.listGiveawayEntriesPage(gwId, ownerUserId, pageSize, offset);

  const lines = rows.map((r) => {
    const icon = r.is_eligible ? '✅' : (r.last_checked_at ? '⚠️' : '⏳');
    const who = r.tg_username ? `@${escapeHtml(String(r.tg_username))}` : `<code>${Number(r.tg_id)}</code>`;
    const chk = r.last_checked_at ? escapeHtml(fmtTs(r.last_checked_at)) : '—';
    return `${icon} ${who} <i>chk:</i> ${chk}`;
  });

  const text =
`👥 <b>Участники конкурса #${gwId}</b>

Страница: <b>${p + 1}</b>/<b>${maxPage + 1}</b> · Всего: <b>${total}</b>

${lines.length ? lines.join('
') : 'Пока пусто.'}

Легенда: ✅ подтверждено · ⚠️ не подтверждено · ⏳ не проверяли
💡 Нажми на участника, чтобы открыть “почему не прошёл”.`;

  const kb = new InlineKeyboard();

  // Per-participant buttons (click to open detail)
  for (const r of rows) {
    const icon = r.is_eligible ? '✅' : (r.last_checked_at ? '⚠️' : '⏳');
    const who = r.tg_username ? `@${String(r.tg_username)}` : String(r.tg_id);
    const label = `${icon} ${who}`.slice(0, 60);
    kb.text(label, `a:gw_entry|i:${gwId}|uid:${r.user_id}|u:${r.tg_id}|p:${p}`).row();
  }

  // Paging controls
  if (p > 0) kb.text('⬅️ Назад', `a:gw_entries|i:${gwId}|p:${p - 1}`);
  if (p < maxPage) kb.text('➡️ Вперёд', `a:gw_entries|i:${gwId}|p:${p + 1}`);
  if (p > 0 || p < maxPage) kb.row();

  kb
    .text('🔄 Обновить статусы (страница)', `a:gw_entries_refresh|i:${gwId}|p:${p}`)
    .row()
    .text('⬅️ К статистике', `a:gw_stats|i:${gwId}`);

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
  const text = renderParticipantScreen(g, entry);
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: participantKb(gwId) });
}

async function ensureBotId(ctx) {
  if (CFG.BOT_ID) return CFG.BOT_ID;
  const me = await ctx.api.getMe();
  return me.id;
}

async function doEligibilityCheck(ctx, gwId, userTgId) {
  // Always check the main giveaway channel (where the post is published), plus optional sponsor channels.
  let mainChat = null;
  try {
    const g = await db.getGiveawayInfoForUser(gwId);
    mainChat = g?.published_chat_id ?? g?.published_chat ?? g?.channel_id ?? null;
  } catch {}

  const sponsors = await db.listGiveawaySponsors(gwId);
  const sponsorChats = sponsors.map(s => sponsorToChatId(s.sponsor_text)).filter(Boolean);

  const chats = [...new Set([mainChat, ...sponsorChats].filter(Boolean).map((x) => String(x)))];
  const results = [];
  let unknown = false;

  for (const chat of chats) {
    const cacheKey = k(['cm', chat, userTgId]);
    const cached = await redis.get(cacheKey);
    if (cached) {
      results.push({ chat, status: cached });
      if (cached === 'unknown') unknown = true;
      continue;
    }

    try {
      const cm = await ctx.api.getChatMember(chat, userTgId);
      const st = String(cm.status || '');
      const ok = (st === 'member' || st === 'administrator' || st === 'creator');
      const val = ok ? 'ok' : 'no';
      await redis.set(cacheKey, val, { ex: 10 * 60 });
      results.push({ chat, status: val });
    } catch {
      unknown = true;
      await redis.set(cacheKey, 'unknown', { ex: 5 * 60 });
      results.push({ chat, status: 'unknown' });
    }
  }

  const isEligible = results.every(r => r.status === 'ok') && !unknown;
  return { isEligible, unknown, results };
}

async function getChatLabelCached(ctx, chatId) {
  const id = String(chatId);
  const cacheKey = k(['chatmeta', id]);
  try {
    const cached = await redis.get(cacheKey);
    if (cached) return String(cached);
  } catch {}

  try {
    const chat = await ctx.api.getChat(id);
    const label = chat?.username ? `@${chat.username}` : (chat?.title ? String(chat.title) : id);
    try { await redis.set(cacheKey, label, { ex: 24 * 3600 }); } catch {}
    return label;
  } catch {
    // If bot has no access to the chat, just show the raw id / @username.
    return id;
  }
}

function gwEntryDetailKb(gwId, page, entryUserId, entryTgId) {
  const kb = new InlineKeyboard()
    .text('🔄 Проверить сейчас', `a:gw_entry_check|i:${gwId}|uid:${entryUserId}|u:${entryTgId}|p:${page}`)
    .row()
    .text('⬅️ К участникам', `a:gw_entries|i:${gwId}|p:${page}`)
    .row()
    .text('⬅️ К статистике', `a:gw_stats|i:${gwId}`);
  return kb;
}

async function renderGwEntryDetail(ctx, ownerUserId, gwId, entryUserId, entryTgId, page = 0) {
  const gOwner = await db.getGiveawayForOwner(gwId, ownerUserId);
  if (!gOwner) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const userMeta = await db.getUserTgIdByUserId(Number(entryUserId));
  const who = userMeta?.tg_username ? `@${escapeHtml(String(userMeta.tg_username))}` : `<code>${Number(entryTgId)}</code>`;

  const entry = await db.getEntryStatus(gwId, Number(entryUserId));
  const lastChk = entry?.last_checked_at ? escapeHtml(fmtTs(entry.last_checked_at)) : '—';
  const stored =
    !entry ? '⛔ не участвует'
      : entry.is_eligible ? '✅ подтверждено (в базе)'
      : entry.last_checked_at ? '⚠️ не подтверждено (в базе)'
      : '⏳ не проверяли (в базе)';

  // Build ordered list of required channels: main first, then sponsors
  let mainChat = null;
  try {
    const gPublic = await db.getGiveawayInfoForUser(gwId);
    mainChat = gPublic?.published_chat_id ?? gPublic?.published_chat ?? gPublic?.channel_id ?? null;
  } catch {}

  const sponsors = await db.listGiveawaySponsors(gwId);
  const sponsorPairs = sponsors
    .map(s => ({ text: String(s.sponsor_text || '').trim(), chat: sponsorToChatId(s.sponsor_text) }))
    .filter(x => x.chat);

  const required = [];
  if (mainChat) required.push({ kind: 'main', chat: String(mainChat), text: null });
  for (const sp of sponsorPairs) required.push({ kind: 'sponsor', chat: String(sp.chat), text: sp.text });

  // Run check (uses Redis cache for getChatMember results)
  const check = await doEligibilityCheck(ctx, gwId, Number(entryTgId));
  const statusByChat = new Map(check.results.map(r => [String(r.chat), String(r.status)]));

  const rows = [];
  for (const item of required) {
    const st = statusByChat.get(String(item.chat)) || 'unknown';
    const icon = st === 'ok' ? '✅' : (st === 'no' ? '❌' : '❔');
    let label = '';
    if (item.kind === 'main') {
      const nm = await getChatLabelCached(ctx, item.chat);
      label = `Основной канал: ${escapeHtml(nm)}`;
    } else {
      label = `Спонсор: ${escapeHtml(item.text || item.chat)}`;
    }
    const reason = st === 'ok' ? 'подписан' : (st === 'no' ? 'нет подписки' : 'не могу проверить');
    rows.push(`${icon} ${label} — <i>${reason}</i>`);
  }

  const hasUnknown = check.unknown || rows.some(x => x.includes('❔'));
  const headline = check.isEligible && !hasUnknown ? '✅ <b>Проходит условия</b>' : (hasUnknown ? '⚠️ <b>Не могу проверить все каналы</b>' : '⚠️ <b>Не проходит условия</b>');

  const tips = hasUnknown
    ? `

💡 <b>Что делать:</b>
• Добавь бота в админы проблемных каналов/спонсоров
• Затем нажми “🔄 Проверить сейчас”`
    : '';

  const text =
`👤 <b>Участник</b>: ${who}
🎁 Конкурс: <b>#${gwId}</b>

${headline}
🕒 Последняя проверка (в базе): <b>${lastChk}</b>
📌 Статус (в базе): <b>${stored}</b>

<b>Проверка по каналам:</b>
${rows.length ? rows.join('
') : '—'}${tips}`;

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: gwEntryDetailKb(gwId, Number(page || 0), Number(entryUserId), Number(entryTgId)) });
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

  bot.on('message:text', async (ctx, next) => {
    const text = String(ctx.message?.text || '');
    const isCommand = text.startsWith('/') &&
      Array.isArray(ctx.message?.entities) &&
      ctx.message.entities.some((e) => e.type === 'bot_command' && e.offset === 0);

    const exp = await getExpectText(ctx.from.id);
    if (!exp) return next(); // allow commands like /start to reach bot.command()

    // If user sends a command while мы ждали ввод — не блокируем команду.
    if (isCommand) {
      await clearExpectText(ctx.from.id);
      return next();
    }

    const u = await db.upsertUser(ctx.from.id, ctx.from.username ?? null);
    await clearExpectText(ctx.from.id);

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
      await ctx.reply(`✅ Куратор @${username} добавлен.

Включи 🛡 Куратор: ВКЛ, если хочешь чтобы он мог модерировать споры.`);
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



    // Workspace profile edit
    if (exp.type === 'ws_profile_edit') {
      const wsId = Number(exp.wsId);
      const field = String(exp.field || '');
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) { await ctx.reply('Нет доступа к этому каналу.'); return; }
      const v = String(ctx.message.text || '').trim().slice(0, 120);
      if (!v || v.length < 2) { await ctx.reply('Слишком коротко. Введи ещё раз.'); await setExpectText(ctx.from.id, exp); return; }
      const patch = {};
      if (field === 'title') patch.profile_title = v;
      if (field === 'niche') patch.profile_niche = v;
      if (field === 'contact') patch.profile_contact = v;
      if (field === 'geo') patch.profile_geo = v;
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
      const m = txt.match(/^@?([a-zA-Z0-9_]{5,})$/);
      if (!m) { await ctx.reply('Введи @username (пример: @user)'); await setExpectText(ctx.from.id, exp); return; }
      const username = m[1];
      try {
        const chat = await ctx.api.getChat('@' + username);
        if (!chat || !chat.id) throw new Error('no chat');
        const u2 = await db.upsertUser(chat.id, chat.username ?? username);
        await db.addNetworkModerator(u2.id, u.id);
        await ctx.reply(`✅ Модератор добавлен: @${u2.username || username}`);
      } catch {
        await ctx.reply('Не смог найти пользователя по @username. Проверь написание или попроси его написать боту /start.');
      }
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
      const title = (lines[0] || '').trim().slice(0, 80);
      const description = (lines.slice(1).join('\n') || '').trim().slice(0, 2000);

      if (!wsId || !draft.category || !draft.offer_type || !draft.compensation_type) {
        await ctx.reply('Черновик оффера потерян. Начни заново: 🤝 Бартер-биржа → ➕ Разместить оффер');
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
      await db.auditBarterOffer(offer.id, wsId, u.id, 'bx.offer_created', { category: draft.category, offerType: draft.offer_type, compensationType: draft.compensation_type });
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
      const body = String(ctx.message.text || '').trim().slice(0, 800);
      if (!threadId || !body) {
        await ctx.reply('Пустое сообщение.');
        return;
      }

      if (CFG.RATE_LIMIT_ENABLED) {
        try {
          const rl = await rateLimit(
            k(['rl', 'bxmsg', u.id, threadId]),
            { limit: CFG.BX_MSG_RATE_LIMIT, windowSec: CFG.BX_MSG_RATE_WINDOW_SEC }
          );
          if (!rl.allowed) {
            await ctx.reply(`⏳ Слишком часто. Подожди ${fmtWait(rl.resetSec)} и отправь ещё раз.`);
            // we cleared expectation at the start of message router; restore it for retry
            await setExpectText(ctx.from.id, exp);
            return;
          }
        } catch {}
      }

      const built = await buildBxThreadView(u.id, threadId);
      if (!built) {
        await ctx.reply('Диалог не найден.');
        return;
      }
      const { thread } = built;
      if (String(thread.status || '').toUpperCase() !== 'OPEN') {
        await ctx.reply('Диалог закрыт.');
        return;
      }

      await db.addBarterMessage(threadId, u.id, body);
      await db.auditBarterOffer(thread.offer_id, thread.workspace_id, u.id, 'bx.thread_message', { threadId });
      db.trackEvent('thread_message_sent', { userId: u.id, wsId: Number(thread.workspace_id) || null, meta: { threadId, offerId: Number(thread.offer_id) } });

      // notify other side (best-effort)
      const otherUserId = Number(thread.buyer_user_id) == Number(u.id) ? Number(thread.seller_user_id) : Number(thread.buyer_user_id);
      try {
        const otherTgId = await db.getUserTgIdByUserId(otherUserId);
        if (otherTgId) {
          const link = `https://t.me/${CFG.BOT_USERNAME}?start=bxth_${threadId}`;
          await ctx.api.sendMessage(otherTgId, `📨 Новое сообщение по офферу #${thread.offer_id}

Открыть: ${link}`);
        }
      } catch {}

      // show updated thread in reply
      const again = await buildBxThreadView(u.id, threadId);
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

      const raw = String(ctx.message.text || '').trim();
      // allow bare t.me, https links, or @channel/... patterns
      const ok = raw.length >= 8 && raw.length <= 500 && (/^https?:\/\//i.test(raw) || /t\.me\//i.test(raw) || /^@?[a-zA-Z0-9_]{5,}/.test(raw));
      if (!ok) {
        await ctx.reply('Нужна ссылка на пост (пример: https://t.me/...)');
        await setExpectText(ctx.from.id, { type: 'bx_proof_link', wsId, threadId, back, offerId, page });
        return;
      }

      try {
        await db.addBarterThreadProofLink(threadId, u.id, raw);
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
        `✅ Спонсоры: <b>${sponsors.length}</b>\n${list}\n\nВыбери действие:`,
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
      await ctx.reply('Проверь и опубликуй:', { reply_markup: gwConfirmKb(exp.wsId) });
      return;
    }
  });

  // Proofs: screenshot (photo)
  bot.on('message:photo', async (ctx, next) => {
    const exp = await getExpectText(ctx.from.id);
    if (!exp || String(exp.type) !== 'bx_proof_photo') return next();

    const u = await db.upsertUser(ctx.from.id, ctx.from.username ?? null);
    await clearExpectText(ctx.from.id);

    const wsId = Number(exp.wsId);
    const threadId = Number(exp.threadId);
    const back = exp.back ? String(exp.back) : 'inbox';
    const offerId = exp.offerId ? Number(exp.offerId) : null;
    const page = Number(exp.page || 0);

    const photos = ctx.message.photo || [];
    const last = photos.length ? photos[photos.length - 1] : null;
    const fileId = last?.file_id;
    if (!fileId) {
      await ctx.reply('Не вижу фото. Пришли скрин как картинку (не файл).');
      await setExpectText(ctx.from.id, { type: 'bx_proof_photo', wsId, threadId, back, offerId, page });
      return;
    }

    try {
      await db.addBarterThreadProofScreenshot(threadId, u.id, fileId);
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
  });

  // --- Commands ---
  bot.command('start', async (ctx) => {
    const u = await db.upsertUser(ctx.from.id, ctx.from.username ?? null);
    const payload = parseStartPayload(ctx.message?.text || '');
    db.trackEvent('start', { userId: u.id, meta: { payloadType: payload?.type || null, hasPayload: !!payload } });
    if (payload?.type === 'gw') {
      const g = await db.getGiveawayInfoForUser(payload.id);
      if (!g) return ctx.reply('Конкурс не найден.');
      const entry = await db.getEntryStatus(payload.id, u.id);
      const text = renderParticipantScreen(g, entry);
      return ctx.reply(text, { parse_mode: 'HTML', reply_markup: participantKb(payload.id) });
    }
    if (payload?.type === 'gwo') {
      const g = await db.getGiveawayForOwner(payload.id, u.id);
      if (!g) return ctx.reply('Нет доступа к этому конкурсу.');
      const sponsors = await db.listGiveawaySponsors(payload.id);
      const sponsorLines = sponsors.map(s => `• ${escapeHtml(s.sponsor_text)}`).join('\n') || '—';
      const text = `🎁 <b>Конкурс #${g.id}</b>\n\nСтатус: <b>${escapeHtml(String(g.status).toUpperCase())}</b>\nПриз: <b>${escapeHtml(g.prize_value_text || '—')}</b>\nМест: <b>${g.winners_count}</b>\nДедлайн: <b>${g.ends_at ? escapeHtml(fmtTs(g.ends_at)) : '—'}</b>\n\nСпонсоры:\n${sponsorLines}`;
      return ctx.reply(text, { parse_mode: 'HTML', reply_markup: gwOpenKb(g, { isAdmin: isSuperAdminTg(ctx.from?.id) }) });
    }
    if (payload?.type === 'cur') {
      // curator invite flow
      const key = k(['cur_invite', payload.wsId, payload.token]);
      const val = await redis.get(key);
      if (!val) return ctx.reply('Ссылка устарела или недействительна.');
      const ownerUserId = Number(val.ownerUserId || val.owner_user_id || val.owner || 0);
      const added = await db.addCurator(payload.wsId, u.id, ownerUserId || u.id);
      await redis.del(key);
      await ctx.reply('✅ Ты добавлен как куратор. Теперь попроси владельца включить “🛡 Куратор: ВКЛ” в настройках канала.');
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
    if (CFG.ONBOARDING_V2_ENABLED) {
      await ctx.reply('Привет! 👋\n\nВыбери роль — и я покажу быстрый старт:', { reply_markup: onboardingKb(flags) });
      return;
    }
    await ctx.reply(`Привет! Это бот для конкурсов микроблогеров.\n\nВыбери действие:`, { reply_markup: mainMenuKb(flags) });
  });

  bot.command('whoami', async (ctx) => {
    const me = await ctx.api.getMe();
    await ctx.reply(`BOT_ID=${me.id}\nBOT_USERNAME=@${me.username}`);
  });

  bot.command('paysupport', async (ctx) => {
    // Telegram expects bots that accept payments to provide a support contact via /paysupport.
    const fallback = [
      '💬 Support for payments / billing:',
      '— Write to the admin of this bot (add PAY_SUPPORT_TEXT in env for your contact).',
      '',
      'When you write, include:',
      '• what you bought (PRO / Brand Pass / Plan / Featured / Matching)',
      '• approximate time of payment',
      '• screenshot of the receipt (if available)'
    ].join('\n');

    const msg = (CFG.PAY_SUPPORT_TEXT && String(CFG.PAY_SUPPORT_TEXT).trim())
      ? String(CFG.PAY_SUPPORT_TEXT).trim()
      : fallback;

    await ctx.reply(msg);
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
    const p = parseCb(ctx.callbackQuery.data);
    const u = await db.upsertUser(ctx.from.id, ctx.from.username ?? null);

    // MENU
    if (p.a === 'a:menu') {
      await ctx.answerCallbackQuery();
      const flags = await getRoleFlags(u, ctx.from.id);
      await ctx.editMessageText('Меню:', { reply_markup: mainMenuKb(flags) });
      return;
    }

    // ONBOARDING V2 (feature-flag)
    if (p.a === 'a:onb_creator') {
      await ctx.answerCallbackQuery();
      const text =
        '✨ <b>Creator / Канал</b>\n\n' +
        '1) Подключи канал\n' +
        '2) Создай конкурс или оффер\n' +
        '3) Получай спонсоров и коллаборации\n\n' +
        'Давай начнём:';
      const kb = new InlineKeyboard()
        .text('🚀 Подключить канал', 'a:setup')
        .row()
        .text('📣 Мои каналы', 'a:ws_list')
        .row()
        .text('🤝 Бартер-биржа', 'a:bx_home')
        .row()
        .text('⬅️ Меню', 'a:menu');
      await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

    if (p.a === 'a:onb_brand') {
      await ctx.answerCallbackQuery();
      const text =
        '🏷 <b>Brand / Бренд</b>\n\n' +
        '• Смотри ленту офферов\n' +
        '• Открывай диалоги через <b>Brand Pass</b> (анти-спам)\n' +
        '• Веди переписки в Inbox\n\n' +
        'Открыть бренд-режим:';
      const kb = new InlineKeyboard()
        .text('🏷 Brand Mode', 'a:bx_open|ws:0')
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
    if (p.a === 'a:ws_prof_edit') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const field = String(p.f || 'title');
      const prompts = {
        title: '✍️ Введи название профиля (показывается в бирже).',
        niche: '✍️ Введи нишу (например: косметика, уход, лайфстайл).',
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
      try {
        await ctx.api.sendInvoice(
          ctx.chat.id,
          'MicroGiveaways PRO',
          'PRO на 30 дней: чаще bump, больше офферов, пин в ленте, расширенная аналитика.',
          payload,
          '',
          'XTR',
          [{ label: 'PRO 30 days', amount: CFG.PRO_STARS_PRICE }]
        );
      } catch (e) {
        await ctx.reply('Не удалось отправить инвойс. Проверь, что бот может принимать Stars.', { reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:ws_pro|ws:${wsId}`) });
      }
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
      try {
        await ctx.api.sendInvoice(
          ctx.chat.id,
          `Brand Pass · ${pack.credits} контактов`,
          'Кредиты нужны только для открытия НОВОГО диалога. Переписка внутри диалога — бесплатна.',
          payload,
          '',
          'XTR',
          [{ label: `Brand Pass ${pack.id}`, amount: pack.stars }]
        );
      } catch (e) {
        const back = offerId ? `a:bx_pub|ws:${wsId}|o:${offerId}|p:${page}` : `a:brand_pass|ws:${wsId}`;
        await ctx.reply('Не удалось отправить инвойс. Проверь, что бот может принимать Stars.', {
          reply_markup: new InlineKeyboard().text('⬅️ Назад', back)
        });
      }
      return;
    }



    // Brand Mode tools
    if (p.a === 'a:brand_pass') {
      await ctx.answerCallbackQuery();
      await renderBrandPassTopup(ctx, u.id, Number(p.ws || 0));
      return;
    }

    if (p.a === 'a:brand_plan') {
      await ctx.answerCallbackQuery();
      await renderBrandPlan(ctx, u.id, Number(p.ws || 0));
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
      try {
        await ctx.api.sendInvoice(
          ctx.chat.id,
          `Brand Plan · ${label} · ${CFG.BRAND_PLAN_DURATION_DAYS} дней`,
          'Подписка на инструменты бренда: CRM стадии, расширенная воронка, удобный менеджмент диалогов.',
          payload,
          '',
          'XTR',
          [{ label: `Brand Plan ${label}`, amount: stars }]
        );
      } catch (e) {
        await ctx.reply('Не удалось отправить инвойс. Проверь, что бот может принимать Stars.', {
          reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:brand_plan|ws:${wsId}`)
        });
      }
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
      try {
        await ctx.api.sendInvoice(
          ctx.chat.id,
          `Smart Matching · ${tier.title}`,
          'Подбор подходящих микро-каналов под твой бриф. После оплаты отправь бриф одним сообщением.',
          payload,
          '',
          'XTR',
          [{ label: `${tier.title}`, amount: tier.stars }]
        );
      } catch (e) {
        await ctx.reply('Не удалось отправить инвойс. Проверь, что бот может принимать Stars.', {
          reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:match_home|ws:${wsId}`)
        });
      }
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
      try {
        await ctx.api.sendInvoice(
          ctx.chat.id,
          `Featured · ${d.title}`,
          'Твой блок появится сверху в ленте у всех (бренд + блогеры). После оплаты отправь контент.',
          payload,
          '',
          'XTR',
          [{ label: `Featured ${d.title}`, amount: d.stars }]
        );
      } catch (e) {
        await ctx.reply('Не удалось отправить инвойс. Проверь, что бот может принимать Stars.', {
          reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:feat_home|ws:${wsId}`)
        });
      }
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
      await ctx.answerCallbackQuery();
      await db.removeNetworkModerator(Number(p.uid));
      await ctx.editMessageText('✅ Удалено.', { reply_markup: new InlineKeyboard().text('⬅️ Назад', 'a:admin_home') });
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
        await db.closeBarterThread(rep.thread_id, rep.owner_user_id || u.id);
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
      await ctx.answerCallbackQuery();
      await renderBxOpen(ctx, u.id, Number(p.ws));
      return;
    }

    if (p.a === 'a:bx_enable_net') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.setWorkspaceSetting(wsId, { network_enabled: true });
      await db.auditWorkspace(wsId, u.id, 'ws.network_enabled', { enabled: true, source: 'barter' });
      await ctx.answerCallbackQuery();
      await renderBxOpen(ctx, u.id, wsId);
      return;
    }

    if (p.a === 'a:bx_feed') {
      await ctx.answerCallbackQuery();
      await renderBxFeed(ctx, u.id, Number(p.ws), Number(p.p || 0), p.c || null);
      return;
    }

    if (p.a === 'a:bx_filters') {
      await ctx.answerCallbackQuery();
      await renderBxFilters(ctx, u.id, Number(p.ws), Number(p.p || 0));
      return;
    }

    if (p.a === 'a:bx_fpick') {
      await ctx.answerCallbackQuery();
      await renderBxFilterPick(ctx, u.id, Number(p.ws), String(p.k || ''), Number(p.p || 0));
      return;
    }

    if (p.a === 'a:bx_fset') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const key = String(p.k || '');
      const v = (p.v === 'all' || p.v === 'null') ? null : (p.v || null);
      const patch = {};
      if (key === 'cat') patch.category = v;
      if (key === 'type') patch.offerType = v;
      if (key === 'comp') patch.compensationType = v;
      await setBxFilter(ctx.from.id, wsId, patch);
      await renderBxFilters(ctx, u.id, wsId, Number(p.p || 0));
      return;
    }

    if (p.a === 'a:bx_freset') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      await setBxFilter(ctx.from.id, wsId, { category: null, offerType: null, compensationType: null });
      await renderBxFilters(ctx, u.id, wsId, Number(p.p || 0));
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
      await ctx.api.sendInvoice(
        ctx.from.id,
        title,
        description,
        `offpub_${u.id}_${offerId}_${d.days}_${token}`,
        CFG.STARS_PROVIDER_TOKEN,
        'XTR',
        [{ label: title, amount: d.price }]
      );

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

      if (CFG.RATE_LIMIT_ENABLED) {
        try {
          const rl = await rateLimit(
            k(['rl', 'intro', u.id]),
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
      db.trackEvent('intro_attempt', { userId: u.id, wsId: wsId || null, meta: { offerId, brandMode: wsId === 0 } });

      // Pricing / limits (configurable)
      const cost = Math.max(1, Number(CFG.INTRO_COST_PER_INTRO || 1));
      const trialCredits = Math.max(0, Number(CFG.INTRO_TRIAL_CREDITS || 0));

      let isVerified = false;
      if (CFG.VERIFICATION_ENABLED) {
        const v = await safeUserVerifications(() => db.getUserVerification(u.id), async () => null);
        isVerified = String(v?.status || '').toUpperCase() === 'APPROVED';
      }
      const dailyLimit = Math.max(0, Number(isVerified ? CFG.INTRO_DAILY_LIMIT : CFG.INTRO_DAILY_LIMIT_UNVERIFIED));

      const res = await db.getOrCreateBarterThreadWithCredits(
        offerId,
        u.id,
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
        db.trackEvent('intro_blocked_daily_limit', { userId: u.id, wsId: wsId || null, meta: { offerId, lim, used } });
        await ctx.answerCallbackQuery({ text: `Лимит интро на сегодня: ${lim} (использовано: ${used}). Попробуй завтра.`, show_alert: true });
        return;
      }

      if (res.needPaywall) {
        db.trackEvent('paywall_shown', { userId: u.id, wsId: wsId || null, meta: { offerId, cost, balance: Number(res.balance ?? 0), usedToday: Number(res.dailyUsed ?? 0), dailyLimit: Number(res.dailyLimit ?? dailyLimit ?? 0) } });
        await renderBrandPaywall(ctx, u.id, wsId, offerId, Number(p.p || 0));
        return;
      }

      if (!res.ok || !res.thread) {
        return ctx.answerCallbackQuery({ text: 'Не получилось открыть диалог. Возможно оффер закрыт.' });
      }

      db.trackEvent('thread_opened', { userId: u.id, wsId: wsId || null, meta: { offerId, threadId: res.thread.id, charged: !!res.charged, chargedAmount: Number(res.chargedAmount || cost || 1) } });

      if (res.charged) {
        const left = Number(res.balance ?? 0);
        const amt = Number(res.chargedAmount || cost || 1);
        const bonus = res.trialGranted ? '🎁 Бонус активирован. ' : '';
        await ctx.answerCallbackQuery({ text: `${bonus}✅ Диалог открыт. -${amt} кредит(ов). Осталось: ${left}`, show_alert: true });
      }
      else if (res.retryUsed) {
        await ctx.answerCallbackQuery({ text: `🎟 Диалог открыт. Использован Retry credit.`, show_alert: true });
      }

      await renderBxThread(ctx, u.id, wsId, res.thread.id, { back: 'offer', offerId, page: Number(p.p || 0) });
      return;
    }

    if (p.a === 'a:bx_inbox') {
      await ctx.answerCallbackQuery();
      await renderBxInbox(ctx, u.id, Number(p.ws), Number(p.p || 0));
      return;
    }

    if (p.a === 'a:bx_thread') {
      await ctx.answerCallbackQuery();
      const back = p.b ? String(p.b) : 'inbox';
      const offerId = p.o ? Number(p.o) : null;
      await renderBxThread(ctx, u.id, Number(p.ws), Number(p.t), { back, offerId, page: Number(p.p || 0) });
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
      await renderBxProofs(ctx, u.id, wsId, threadId, { back, offerId, page: Number(p.p || 0) });
      return;
    }

    if (p.a === 'a:bx_proof_link') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const threadId = Number(p.t);
      const back = p.b ? String(p.b) : 'inbox';
      const offerId = p.o ? Number(p.o) : null;

      await ctx.editMessageText('🔗 Пришли ссылку на пост (пример: https://t.me/... )', {
        reply_markup: new InlineKeyboard().text('⬅️ Отмена', `a:bx_proofs|ws:${wsId}|t:${threadId}|p:${Number(p.p || 0)}${offerId ? `|o:${offerId}` : ''}|b:${back}`)
      });
      await setExpectText(ctx.from.id, { type: 'bx_proof_link', wsId, threadId, back, offerId, page: Number(p.p || 0) });
      return;
    }

    if (p.a === 'a:bx_proof_photo') {
      await ctx.answerCallbackQuery();
      const wsId = Number(p.ws);
      const threadId = Number(p.t);
      const back = p.b ? String(p.b) : 'inbox';
      const offerId = p.o ? Number(p.o) : null;

      await ctx.editMessageText('📎 Пришли скрин как <b>фото</b> (обычная картинка).', {
        parse_mode: 'HTML',
        reply_markup: new InlineKeyboard().text('⬅️ Отмена', `a:bx_proofs|ws:${wsId}|t:${threadId}|p:${Number(p.p || 0)}${offerId ? `|o:${offerId}` : ''}|b:${back}`)
      });
      await setExpectText(ctx.from.id, { type: 'bx_proof_photo', wsId, threadId, back, offerId, page: Number(p.p || 0) });
      return;
    }

    if (p.a === 'a:bx_stage') {
      const wsId = Number(p.ws);
      const threadId = Number(p.t);
      const stage = String(p.s || '');
      const back = String(p.b || 'inbox');
      const page = Number(p.p || 0);
      const offerId = p.o ? Number(p.o) : null;

      const stageOk = CRM_STAGES.some((x) => x.id === stage);
      if (!stageOk) {
        await ctx.answerCallbackQuery({ text: 'Стадия не найдена.' });
        return;
      }

      const hasPlan = await db.isBrandPlanActive(u.id);
      if (!hasPlan) {
        await ctx.answerCallbackQuery({ text: 'CRM стадии доступны в Brand Plan.', show_alert: true });
        return;
      }

      const updated = await db.setBarterThreadBuyerStage(threadId, u.id, stage);
      if (!updated) {
        await ctx.answerCallbackQuery({ text: 'Нет доступа.' });
        return;
      }

      await ctx.answerCallbackQuery({ text: '✅' });
      await renderBxThread(ctx, u.id, wsId, threadId, { back, offerId, page });
      return;
    }

    if (p.a === 'a:bx_thread_reply') {
      await ctx.answerCallbackQuery();
      const threadId = Number(p.t);
      await ctx.editMessageText('✍️ Напиши сообщение одним текстом (без медиа).', {
        reply_markup: new InlineKeyboard().text('⬅️ Отмена', `a:bx_thread|ws:${Number(p.ws)}|t:${threadId}|p:${Number(p.p || 0)}`)
      });
      await setExpectText(ctx.from.id, { type: 'bx_thread_msg', threadId, wsId: Number(p.ws) });
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
      await ctx.answerCallbackQuery({ text: 'Закрыто.' });
      const closed = await db.closeBarterThread(Number(p.t), u.id);
      if (!closed) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await renderBxInbox(ctx, u.id, Number(p.ws), 0);
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
      await ctx.editMessageText('➕ <b>Новый оффер</b>\n\nВыбери категорию:', {
        parse_mode: 'HTML',
        reply_markup: bxCategoryKb(wsId)
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

if (p.a === 'a:bx_cat') {
      const wsId = Number(p.ws);
      await ctx.answerCallbackQuery();
      const draft = (await getDraft(ctx.from.id)) || {};
      draft.wsId = wsId;
      draft.category = p.c;
      await setDraft(ctx.from.id, draft);
      await ctx.editMessageText('Шаг 2/4: выбери формат размещения:', {
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
      await ctx.editMessageText('Шаг 3/4: выбери тип оплаты:', {
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

      const example = 'Заголовок: Ищу бартер с магазином уходовой косметики\n\nУсловия: пост+сторис, аудитория 500, Уфа. Хочу: бартер или сертификат. Контакт: @myname';
      await ctx.editMessageText(
        `Шаг 4/4: отправь одним сообщением\n\n1-я строка — <b>заголовок</b>\nсо 2-й строки — <b>детали</b> (условия/гео/что хочешь получить).\n\nПример:\n<code>${escapeHtml(example)}</code>`,
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

    if (p.a === 'a:bx_del_q') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const o = await db.getBarterOfferForOwner(u.id, offerId);
      if (!o) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      const kb = new InlineKeyboard()
        .text('✅ Удалить', `a:bx_del_do|ws:${wsId}|o:${offerId}`)
        .text('❌ Отмена', `a:bx_view|ws:${wsId}|o:${offerId}|back:my`);
      await ctx.answerCallbackQuery();
      await ctx.editMessageText(`Удалить оффер <b>#${offerId}</b>?`, { parse_mode: 'HTML', reply_markup: kb });
      return;
    }

    if (p.a === 'a:bx_del_do') {
      const wsId = Number(p.ws);
      const offerId = Number(p.o);
      const o = await db.getBarterOfferForOwner(u.id, offerId);
      if (!o) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.updateBarterOfferStatus(offerId, 'CLOSED');
      await db.auditBarterOffer(offerId, wsId, u.id, 'bx.offer_deleted', {});
      await ctx.answerCallbackQuery({ text: 'Удалено.' });
      await renderBxMy(ctx, u.id, wsId, 0);
      return;
    }

    if (p.a === 'a:ws_toggle_net') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await db.setWorkspaceSetting(wsId, { network_enabled: !ws.network_enabled });
      await db.auditWorkspace(wsId, u.id, 'ws.network_toggled', { enabled: !ws.network_enabled });
      await renderWsSettings(ctx, u.id, wsId);
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
    if (p.a === 'a:cur_invite') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      const token = randomToken(8);
      const key = k(['cur_invite', wsId, token]);
      await redis.set(key, { ownerUserId: u.id }, { ex: 10 * 60 });

      const link = `https://t.me/${CFG.BOT_USERNAME}?start=cur_${wsId}_${token}`;
      const text = `👤 <b>Приглашение куратора</b>\n\nСсылка на 10 минут:\n${escapeHtml(link)}\n\nКуратор должен открыть ссылку в Telegram.`;
      await ctx.answerCallbackQuery();
      await ctx.editMessageText(text, {
        parse_mode: 'HTML',
        disable_web_page_preview: true,
        reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:ws_settings|ws:${wsId}`)
      });
      return;
    }

    if (p.a === 'a:cur_add_username') {
      const wsId = Number(p.ws);
      const ws = await db.getWorkspace(u.id, wsId);
      if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
      await ctx.answerCallbackQuery();
      await ctx.editMessageText('➕ Введи @username куратора (он должен уже запускать бота /start).', {
        reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:ws_settings|ws:${wsId}`)
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
      await ctx.editMessageText(`👥 <b>Кураторы</b>\n\n${lines.length ? lines.join('\n') : 'Пока нет.'}`, {
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
      await ctx.answerCallbackQuery({ text: 'Удалено' });
      // refresh list
      const curators = await db.listCurators(wsId);
      const lines = curators.map(c => `• ${c.tg_username ? '@' + escapeHtml(c.tg_username) : 'id:' + c.tg_id}`);
      await ctx.editMessageText(`👥 <b>Кураторы</b>\n\n${lines.length ? lines.join('\n') : 'Пока нет.'}`, {
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
      kb.text('⬅️ Назад', `a:gw_step_sponsors|ws:${wsId}`);

      await ctx.answerCallbackQuery();
      await ctx.editMessageText('📁 Выбери папку — каналы из неё станут спонсорами конкурса:', { reply_markup: kb });
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
        `✅ Спонсоры: <b>${sponsors.length}</b>\n${list}\n\nВыбери действие:`,
        { parse_mode: 'HTML', reply_markup: gwSponsorsReviewKb(wsId) }
      );
      return;
    }

    // GIVEAWAYS list
    if (p.a === 'a:gw_list') {
      await ctx.answerCallbackQuery();
      await renderGwList(ctx, u.id, null);
      return;
    }
    if (p.a === 'a:gw_list_ws') {
      await ctx.answerCallbackQuery();
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

    if (p.a === 'a:gw_entries') {
      await ctx.answerCallbackQuery();
      await renderGwEntries(ctx, u.id, Number(p.i), Number(p.p || 0));
      return;
    }

    if (p.a === 'a:gw_entry') {
      await ctx.answerCallbackQuery();
      await renderGwEntryDetail(ctx, u.id, Number(p.i), Number(p.uid), Number(p.u), Number(p.p || 0));
      return;
    }
    if (p.a === 'a:gw_entry_check') {
      const gwId = Number(p.i);
      const entryUserId = Number(p.uid);
      const entryTgId = Number(p.u);
      const page = Number(p.p || 0);

      // Owner-only
      const g = await db.getGiveawayForOwner(gwId, u.id);
      if (!g) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      await ctx.answerCallbackQuery({ text: 'Проверяю…' });

      const check = await doEligibilityCheck(ctx, gwId, entryTgId);
      await db.setEntryEligibility(gwId, entryUserId, check.isEligible);
      await db.auditGiveaway(gwId, g.workspace_id, u.id, 'gw.owner_check_one', { entryUserId, entryTgId, isEligible: check.isEligible, unknown: check.unknown });

      await renderGwEntryDetail(ctx, u.id, gwId, entryUserId, entryTgId, page);
      return;
    }

    if (p.a === 'a:gw_entries_refresh') {
      const gwId = Number(p.i);
      const page = Number(p.p || 0);

      // Owner-only
      const g = await db.getGiveawayForOwner(gwId, u.id);
      if (!g) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

      // Anti-spam: 1 refresh per 30s per giveaway per owner
      const rl = await rateLimit(k(['gw', 'entries_refresh', gwId, u.id]), { limit: 1, windowSec: 30 });
      if (!rl.allowed) return ctx.answerCallbackQuery({ text: `Подожди ${fmtWait(rl.resetSec)}.` });

      await ctx.answerCallbackQuery({ text: 'Проверяю…' });

      // Refresh only current page (uses Redis membership cache inside doEligibilityCheck)
      const st = await db.getGiveawayStats(gwId, u.id);
      const total = Number(st?.entries_total || 0);
      const pageSize = 10;
      const maxPage = Math.max(0, Math.ceil(total / pageSize) - 1);
      const pPage = Math.min(maxPage, Math.max(0, page));
      const offset = pPage * pageSize;

      if (typeof db.listGiveawayEntriesPage !== 'function') {
        await ctx.answerCallbackQuery({ text: 'Нужен апдейт src/db/queries.js (listGiveawayEntriesPage).' });
        return;
      }

      const rows = await db.listGiveawayEntriesPage(gwId, u.id, pageSize, offset);

      let okCount = 0;
      let unknownCount = 0;

      for (const r of rows) {
        const check = await doEligibilityCheck(ctx, gwId, Number(r.tg_id));
        await db.setEntryEligibility(gwId, Number(r.user_id), check.isEligible);
        if (check.isEligible) okCount += 1;
        if (check.unknown) unknownCount += 1;
      }

      await db.auditGiveaway(gwId, g.workspace_id, u.id, 'gw.owner_refresh_page', { page: pPage, pageSize, okCount, unknownCount });

      await renderGwEntries(ctx, u.id, gwId, pPage);
      return;
    }
    if (p.a === 'a:gw_log') {
      await ctx.answerCallbackQuery();
      await renderGwLog(ctx, u.id, Number(p.i));
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
      await ctx.editMessageText('✍️ Опиши приз одним сообщением (коротко и понятно).', {
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
      await ctx.editMessageText('Проверь и опубликуй:', { reply_markup: gwConfirmKb(wsId) });
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
      const deepLink = `https://t.me/${botUsername}?start=gw_${created.id}`;
      const text =
`🎀 <b>РОЗЫГРЫШ</b>\n\n🎁 Приз: <b>${escapeHtml(draft.prize_value_text)}</b>\n🏆 Мест: <b>${Number(draft.winners_count)}</b>\n⏳ Итоги: <b>${escapeHtml(fmtTs(draft.ends_at))}</b>\n\n✅ Нажми “Участвовать”, затем “Проверить” в боте.`;

      const kb = {
        inline_keyboard: [
          [
            { text: '✅ Участвовать', callback_data: `a:gw_join|i:${created.id}` },
            { text: '🔍 Проверить', url: deepLink }
          ]
        ]
      };

      try {
        const sent = await ctx.api.sendMessage(ws.channel_id, text, {
          parse_mode: 'HTML',
          reply_markup: kb,
          disable_web_page_preview: true
        });

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
      const g = await db.getGiveawayPublic(gwId);
      if (!g) return ctx.answerCallbackQuery({ text: 'Конкурс не найден.' });
      await db.upsertGiveawayEntry(gwId, u.id);
      await db.auditGiveaway(gwId, g.workspace_id, u.id, 'gw.joined', { from: 'button' });

      const dmText = `✅ Ты участвуешь в конкурсе #${gwId}.\n\nНажми “Проверить”, чтобы подтвердить подписки.`;
      const ok = await sendSafeDM(ctx, ctx.from.id, dmText, { reply_markup: participantKb(gwId) });

      if (!ok) {
        const link = `https://t.me/${CFG.BOT_USERNAME}?start=gw_${gwId}`;
        await ctx.answerCallbackQuery({ text: 'Открой бота для проверки', show_alert: true });
        return;
      }

      await ctx.answerCallbackQuery({ text: 'Участие записано ✅' });
      return;
    }

    if (p.a === 'a:gw_check') {
      const gwId = Number(p.i);
      const g = await db.getGiveawayInfoForUser(gwId);
      if (!g) return ctx.answerCallbackQuery({ text: 'Конкурс не найден.' });
      await db.upsertGiveawayEntry(gwId, u.id);

      const check = await doEligibilityCheck(ctx, gwId, ctx.from.id);
      await db.setEntryEligibility(gwId, u.id, check.isEligible);
      await db.auditGiveaway(gwId, g.workspace_id, u.id, 'gw.checked', { isEligible: check.isEligible, unknown: check.unknown, results: check.results });

      let msg = check.isEligible ? '✅ Участие подтверждено!' : '⚠️ Пока не подтверждено.';
      if (check.unknown) {
        msg += '\n\n💡 Если бот не может проверить — попроси админа добавить бота в канал-спонсор.';
      }

      await ctx.answerCallbackQuery({ text: check.isEligible ? '✅ Eligible' : 'Проверь подписки' });
      try {
        const entry = await db.getEntryStatus(gwId, u.id);
        const text = renderParticipantScreen(g, entry);
        await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: participantKb(gwId) });
      } catch {
        await ctx.reply(msg);
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

      const link = `https://t.me/${CFG.BOT_USERNAME}?start=gw_${gwId}`;
      const line1 = hasSponsors
        ? '1) Подпишись на канал конкурса (этот канал) и на все каналы-спонсоры'
        : '1) Подпишись на канал конкурса (этот канал)';
      const text =
`📣 <b>Напоминание участникам</b>\n\nЧтобы участие засчиталось ✅\n${line1}\n2) Нажми <b>«Проверить участие»</b> в боте\n\n🔍 Проверка: ${escapeHtml(link)}`;

      try {
        const sent = await ctx.api.sendMessage(Number(g.published_chat_id), text, {
          parse_mode: 'HTML',
          disable_web_page_preview: true,
          reply_markup: { inline_keyboard: [[{ text: '🔍 Проверить участие', url: link }]] }
        });
        await db.auditGiveaway(gwId, g.workspace_id, u.id, 'gw.reminder_posted', { chat_id: g.published_chat_id, message_id: sent.message_id });
        await ctx.answerCallbackQuery({ text: 'Отправлено ✅' });
        await ctx.editMessageText('✅ Напоминание опубликовано.', { reply_markup: new InlineKeyboard().text('⬅️ Назад', `a:gw_open|i:${gwId}`) });
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

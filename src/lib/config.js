import 'dotenv/config';

function parseIntSafe(v, d) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function parseBoolSafe(v, d = false) {
  if (v === undefined || v === null || v === '') return d;
  const s = String(v).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(s)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(s)) return false;
  return d;
}

function parseCsvNums(v) {
  if (!v) return [];
  return String(v)
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => Number(s))
    .filter((n) => Number.isFinite(n) && n > 0);
}

const DEFAULT_SUPER_ADMINS = '';

export const CFG = {
  APP_ENV: process.env.APP_ENV || 'dev',

  // Variant (for copy+paste deployments on one core)
  BOT_VARIANT: process.env.BOT_VARIANT || 'collab_girls',

  // Telegram bot
  BOT_TOKEN: process.env.BOT_TOKEN || '',
  BOT_USERNAME: (process.env.BOT_USERNAME || '').replace(/^@/, ''),
  BOT_ID: process.env.BOT_ID ? Number(process.env.BOT_ID) : 0,

  // Security
  WEBHOOK_SECRET_TOKEN: process.env.WEBHOOK_SECRET_TOKEN || '',
  CRON_SECRET: process.env.CRON_SECRET || '',

  // DB
  DATABASE_URL: process.env.DATABASE_URL || '',

  // Access checks
  TG_ACCESS_CHECK_CONCURRENCY: parseIntSafe(process.env.TG_ACCESS_CHECK_CONCURRENCY, 4),

  // Barters
  BARTER_FEED_PAGE_SIZE: parseIntSafe(process.env.BARTER_FEED_PAGE_SIZE, 5),
  BARTER_INBOX_PAGE_SIZE: parseIntSafe(process.env.BARTER_INBOX_PAGE_SIZE, 10),

  // Backwards compat (v0.9.x)
  BARTER_BUMP_COOLDOWN_HOURS: parseIntSafe(process.env.BARTER_BUMP_COOLDOWN_HOURS, 6),

  // Free / Pro tuning
  BARTER_MAX_ACTIVE_OFFERS_FREE: parseIntSafe(process.env.BARTER_MAX_ACTIVE_OFFERS_FREE, 3),
  BARTER_MAX_ACTIVE_OFFERS_PRO: parseIntSafe(process.env.BARTER_MAX_ACTIVE_OFFERS_PRO, 10),
  BARTER_BUMP_COOLDOWN_HOURS_FREE: parseIntSafe(process.env.BARTER_BUMP_COOLDOWN_HOURS_FREE, 24),
  BARTER_BUMP_COOLDOWN_HOURS_PRO: parseIntSafe(process.env.BARTER_BUMP_COOLDOWN_HOURS_PRO, 6),

  // Monetization (microbloggers)
  PRO_STARS_PRICE: parseIntSafe(process.env.PRO_STARS_PRICE, 299),
  PRO_DURATION_DAYS: parseIntSafe(process.env.PRO_DURATION_DAYS, 30),
  PRO_PAYMENT_URL: process.env.PRO_PAYMENT_URL || '',
  PAY_SUPPORT_TEXT: process.env.PAY_SUPPORT_TEXT || '',

  // Monetization (brands)
  BRAND_PLAN_BASIC_PRICE: parseIntSafe(process.env.BRAND_PLAN_BASIC_PRICE, 999),
  BRAND_PLAN_MAX_PRICE: parseIntSafe(process.env.BRAND_PLAN_MAX_PRICE, 2999),
  BRAND_PLAN_DURATION_DAYS: parseIntSafe(process.env.BRAND_PLAN_DURATION_DAYS, 30),

  MATCH_S_PRICE: parseIntSafe(process.env.MATCH_S_PRICE, 699),
  MATCH_M_PRICE: parseIntSafe(process.env.MATCH_M_PRICE, 1499),
  MATCH_L_PRICE: parseIntSafe(process.env.MATCH_L_PRICE, 2999),
  MATCH_S_COUNT: parseIntSafe(process.env.MATCH_S_COUNT, 10),
  MATCH_M_COUNT: parseIntSafe(process.env.MATCH_M_COUNT, 30),
  MATCH_L_COUNT: parseIntSafe(process.env.MATCH_L_COUNT, 75),

  FEATURED_1D_PRICE: parseIntSafe(process.env.FEATURED_1D_PRICE, 399),
  FEATURED_7D_PRICE: parseIntSafe(process.env.FEATURED_7D_PRICE, 999),
  FEATURED_30D_PRICE: parseIntSafe(process.env.FEATURED_30D_PRICE, 2999),
  FEATURED_MAX_SLOTS: parseIntSafe(process.env.FEATURED_MAX_SLOTS, 5),

  // Official channel publishing
  OFFICIAL_PUBLISH_ENABLED: parseBoolSafe(process.env.OFFICIAL_PUBLISH_ENABLED, false),
  OFFICIAL_PUBLISH_MODE: String(process.env.OFFICIAL_PUBLISH_MODE || 'manual').trim().toLowerCase(), // manual | paid | mixed
  OFFICIAL_CHANNEL_ID: process.env.OFFICIAL_CHANNEL_ID || '',
  OFFICIAL_CHANNEL_USERNAME: (process.env.OFFICIAL_CHANNEL_USERNAME || '').replace(/^@/, ''),
  OFFICIAL_MANUAL_DEFAULT_DAYS: parseIntSafe(process.env.OFFICIAL_MANUAL_DEFAULT_DAYS, 3),
  OFFICIAL_1D_PRICE: parseIntSafe(process.env.OFFICIAL_1D_PRICE, 199),
  OFFICIAL_7D_PRICE: parseIntSafe(process.env.OFFICIAL_7D_PRICE, 499),
  OFFICIAL_30D_PRICE: parseIntSafe(process.env.OFFICIAL_30D_PRICE, 1299),

  // Intro credits & anti-spam
  INTRO_TRIAL_CREDITS: parseIntSafe(process.env.INTRO_TRIAL_CREDITS, 3),
  INTRO_COST_PER_INTRO: parseIntSafe(process.env.INTRO_COST_PER_INTRO, 1),
  INTRO_DAILY_LIMIT: parseIntSafe(process.env.INTRO_DAILY_LIMIT, 20),
  INTRO_DAILY_LIMIT_UNVERIFIED: parseIntSafe(process.env.INTRO_DAILY_LIMIT_UNVERIFIED, 10),
  // Intro retry credits (fairness): no reply → retry credit
  INTRO_RETRY_ENABLED: parseBoolSafe(process.env.INTRO_RETRY_ENABLED, false),
  INTRO_RETRY_AFTER_HOURS: parseIntSafe(process.env.INTRO_RETRY_AFTER_HOURS, 24),
  INTRO_RETRY_EXPIRES_DAYS: parseIntSafe(process.env.INTRO_RETRY_EXPIRES_DAYS, 7),
  INTRO_RETRY_NOTIFY: parseBoolSafe(process.env.INTRO_RETRY_NOTIFY, true),


  // Giveaways
  GIVEAWAY_SPONSORS_MAX_FREE: parseIntSafe(process.env.GIVEAWAY_SPONSORS_MAX_FREE, 10),
  GIVEAWAY_SPONSORS_MAX_PRO: parseIntSafe(process.env.GIVEAWAY_SPONSORS_MAX_PRO, 30),

  // Workspace channel folders
  WORKSPACE_FOLDER_MAX_ITEMS_FREE: parseIntSafe(process.env.WORKSPACE_FOLDER_MAX_ITEMS_FREE, 10),
  WORKSPACE_FOLDER_MAX_ITEMS_PRO: parseIntSafe(process.env.WORKSPACE_FOLDER_MAX_ITEMS_PRO, 30),
  WORKSPACE_EDITOR_INVITE_TTL_MIN: parseIntSafe(process.env.WORKSPACE_EDITOR_INVITE_TTL_MIN, 10),

  // Payments toggles (runtime override via admin → stored in Redis)
  PAYMENTS_ACCEPT_DEFAULT: parseBoolSafe(process.env.PAYMENTS_ACCEPT_DEFAULT, true),
  PAYMENTS_AUTO_APPLY_DEFAULT: parseBoolSafe(process.env.PAYMENTS_AUTO_APPLY_DEFAULT, true),

  // Feature flags
  ANALYTICS_ENABLED: parseBoolSafe(process.env.ANALYTICS_ENABLED, false),

  // Onboarding v2
  ONBOARDING_V2_ENABLED: parseBoolSafe(process.env.ONBOARDING_V2_ENABLED, false),

  // Rate limiting
  RATE_LIMIT_ENABLED: parseBoolSafe(process.env.RATE_LIMIT_ENABLED, false),
  BX_MSG_RATE_LIMIT: parseIntSafe(process.env.BX_MSG_RATE_LIMIT, 12),
  BX_MSG_RATE_WINDOW_SEC: parseIntSafe(process.env.BX_MSG_RATE_WINDOW_SEC, 60),
  INTRO_RATE_LIMIT: parseIntSafe(process.env.INTRO_RATE_LIMIT, 6),
  INTRO_RATE_WINDOW_SEC: parseIntSafe(process.env.INTRO_RATE_WINDOW_SEC, 3600),
  BRAND_LEAD_RATE_LIMIT: parseIntSafe(process.env.BRAND_LEAD_RATE_LIMIT, 1),
  BRAND_LEAD_RATE_WINDOW_SEC: parseIntSafe(process.env.BRAND_LEAD_RATE_WINDOW_SEC, 600),

  VERIFICATION_ENABLED: parseBoolSafe(process.env.VERIFICATION_ENABLED, false),
  // Brand profile (Brand Mode)
  BRAND_PROFILE_REQUIRED: parseBoolSafe(process.env.BRAND_PROFILE_REQUIRED, true),
  BRAND_VERIFY_REQUIRES_EXTENDED: parseBoolSafe(process.env.BRAND_VERIFY_REQUIRES_EXTENDED, true),


  // Moderation
  SUPER_ADMIN_TG_IDS: parseCsvNums(process.env.SUPER_ADMIN_TG_IDS || DEFAULT_SUPER_ADMINS),

  // Upstash Redis REST
  UPSTASH_REDIS_REST_URL: process.env.UPSTASH_REDIS_REST_URL || '',
  UPSTASH_REDIS_REST_TOKEN: process.env.UPSTASH_REDIS_REST_TOKEN || '',


  // UI banners (optional)
  MENU_BANNER_FILE_ID: process.env.MENU_BANNER_FILE_ID || '',
  GUIDE_BANNER_FILE_ID: process.env.GUIDE_BANNER_FILE_ID || '',
  BRAND_BANNER_FILE_ID: process.env.BRAND_BANNER_FILE_ID || '',
  GIVEAWAY_BANNER_FILE_ID: process.env.GIVEAWAY_BANNER_FILE_ID || '',
  BANNER_COOLDOWN_HOURS: parseIntSafe(process.env.BANNER_COOLDOWN_HOURS, 24),

};

export function assertEnv() {
  const missing = [];
  if (!CFG.BOT_TOKEN) missing.push('BOT_TOKEN');
  if (!CFG.BOT_USERNAME) missing.push('BOT_USERNAME');
  if (!CFG.DATABASE_URL) missing.push('DATABASE_URL');
  if (!CFG.UPSTASH_REDIS_REST_URL) missing.push('UPSTASH_REDIS_REST_URL');
  if (!CFG.UPSTASH_REDIS_REST_TOKEN) missing.push('UPSTASH_REDIS_REST_TOKEN');

  if (CFG.OFFICIAL_PUBLISH_ENABLED) {
    if (!CFG.OFFICIAL_CHANNEL_ID) missing.push('OFFICIAL_CHANNEL_ID');
  }

  // Fail-fast safety in prod
  if (CFG.APP_ENV === 'prod') {
    if (!CFG.WEBHOOK_SECRET_TOKEN) missing.push('WEBHOOK_SECRET_TOKEN');
    if (!CFG.CRON_SECRET) missing.push('CRON_SECRET');
    if (!CFG.SUPER_ADMIN_TG_IDS?.length) missing.push('SUPER_ADMIN_TG_IDS');
  }

  if (missing.length) {
    throw new Error(`Missing env: ${missing.join(', ')}`);
  }
}

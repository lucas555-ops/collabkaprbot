# Collabka Bot (CollabGirlsBot) (v1.2.9)

## Release v1.2.9
- Hotfix: moved dev polling out of `/api` into `scripts/dev-polling.js` (prevents serverless endpoint exposure).
- Added P1 improvements: analytics events + **👑 Админка → 📈 Метрики**, rate limits (flag), onboarding v2 (flag), presets/templates, verification CTA in paywall.
- Recommended: generate `package-lock.json` for deterministic installs (`npm install --package-lock-only`).

Telegram bot for creators (channels) + brands (barter marketplace + giveaways):
- Workspaces = your channels
- Giveaways with sponsors, join + eligibility check, stats
- Curator model (optional) + invite links (10 min)
- Audit trail (workspace + giveaways)
- Deterministic PRNG (seedHash) for transparency
- Cron tick endpoint for auto-end + auto-draw (lock in Redis)
- Mini barter exchange (cosmetics/under care/accessories) + inbox
- Pro plan (Stars) for bigger limits, faster bump, pinned offer
- Brand Pass (Stars) for brands: credits to open first contact (new inbox thread)
- Moderator panel (reports queue) with audit

> Stack: Node.js (ESM) + grammY + Postgres + Upstash Redis (REST). Designed for Vercel.

## 1) Env vars
Copy `.env.example` and set these in Vercel:
- `APP_ENV` = `prod` or `dev`
- `BOT_TOKEN`, `BOT_USERNAME`
- `DATABASE_URL`
- `UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN`
- required for prod hardening: `WEBHOOK_SECRET_TOKEN`, `CRON_SECRET`, `SUPER_ADMIN_TG_IDS`
- payments support: `PAY_SUPPORT_TEXT` (shown on `/paysupport`)
- optional rate limiting (infra): `RATE_LIMIT_ENABLED`, `BX_MSG_RATE_LIMIT`, `BX_MSG_RATE_WINDOW_SEC`, `INTRO_RATE_LIMIT`, `INTRO_RATE_WINDOW_SEC`
- optional: `BOT_ID`, `PRO_STARS_PRICE`, `PRO_DURATION_DAYS`

## 2) DB migration
Run once (locally) or via a one-off script in your environment:

```bash
npm i
npm run migrate
```

SQL is in `migrations/*.sql` (run all).

## 3) Deploy to Vercel
1) Push repo to GitHub
2) Import to Vercel
3) Set env vars

Webhook endpoint:
- `POST /api/webhook`

Test health:
- `GET /api/health` returns `{ ok: true }`

## 4) Set Telegram webhook
After deploy:

```bash
curl -s "https://api.telegram.org/bot$BOT_TOKEN/setWebhook" \
  -d "url=https://YOUR_VERCEL_DOMAIN/api/webhook" \
  -d "secret_token=$WEBHOOK_SECRET_TOKEN"
```

Webhook protection is required in prod. Use `secret_token` on setWebhook and validate it in `api/webhook.js`.

## 5) Cron tick
Endpoint:
- `POST /api/cron/giveaways-tick`

Auth (prod):
- `Authorization: Bearer $CRON_SECRET`

Example:
```bash
curl -s -X POST "https://YOUR_VERCEL_DOMAIN/api/cron/giveaways-tick" \
  -H "Authorization: Bearer $CRON_SECRET"
```

Configure Vercel Cron (dashboard) to call it every minute or every 2–5 minutes.

What it does:
- Ends giveaways when `ends_at <= now()`
- If `auto_draw` is ON: draws winners deterministically among eligible participants
- Sends owner a preview notification in DM (safe)

## 6) Bot UX (MVP)
- Add workspace: connect a channel by forwarding any post from that channel (bot must be admin)
- New giveaway: prize → winners count → sponsors (up to 10) → deadline → publish
- Participant: `/start gw_<id>` shows status + “🔄 Check”
- Owner: stats + export usernames + access check (bot admin?) + audit logs

## Dev mode (local polling)
```bash
npm i
node api/dev-polling.js
```

## Files
- `src/bot/bot.js` — handlers, menus, create flow, eligibility, stats
- `src/bot/cron.js` — tick logic + lock
- `src/bot/prng.js` — deterministic PRNG + seedHash
- `src/db/*.js` — Postgres pool + queries
- `src/lib/redis.js` — Upstash Redis REST client
- `api/webhook.js` — Vercel webhook
- `api/cron/giveaways-tick.js` — Vercel cron route

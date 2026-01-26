import { pool } from './pool.js';
import { CFG } from '../lib/config.js';

// Users
export async function upsertUser(tgId, username) {
  const r = await pool.query(
    `insert into users (tg_id, tg_username)
     values ($1, $2)
     on conflict (tg_id)
     do update set tg_username = coalesce(excluded.tg_username, users.tg_username), updated_at = now()
     returning id, tg_id, tg_username`,
    [tgId, username || null]
  );
  return r.rows[0];
}


// -----------------------------
// Analytics events (optional)
// -----------------------------
export async function trackEvent(name, { userId = null, wsId = null, meta = {} } = {}) {
  if (!CFG.ANALYTICS_ENABLED) return null;
  const n = String(name || '').trim();
  if (!n) return null;
  try {
    await pool.query(
      `insert into events (user_id, ws_id, name, meta) values ($1, $2, $3, $4::jsonb)`,
      [userId ? Number(userId) : null, wsId ? Number(wsId) : null, n, JSON.stringify(meta || {})]
    );
    return true;
  } catch (e) {
    // Never break bot UX if analytics table/migration is missing.
    return null;
  }
}


// -----------------------------
// Analytics aggregates (optional)
// -----------------------------
export async function getAnalyticsTopline({ windowDays = 14 } = {}) {
  if (!CFG.ANALYTICS_ENABLED) return null;
  const wd = Math.max(1, Number(windowDays) || 14);

  try {
    const r1 = await pool.query(
      `select
         count(distinct user_id) filter (where user_id is not null and ts >= now() - interval '1 day')::int as dau_24h,
         count(distinct user_id) filter (where user_id is not null and ts >= now() - interval '7 day')::int as wau_7d,
         count(distinct user_id) filter (where user_id is not null and ts >= now() - interval '30 day')::int as mau_30d
       from events`
    );

    const r2 = await pool.query(
      `select
         count(*) filter (where name='paywall_shown')::int as paywall_events,
         count(distinct user_id) filter (where name='paywall_shown' and user_id is not null)::int as paywall_users,

         count(*) filter (
           where name='payment_success'
             and ((meta->>'payload') like 'brand_%' or (meta->>'payload') like 'bplan_%')
         )::int as brandpay_events,
         count(distinct user_id) filter (
           where name='payment_success'
             and user_id is not null
             and ((meta->>'payload') like 'brand_%' or (meta->>'payload') like 'bplan_%')
         )::int as brandpay_users,

         count(*) filter (where name='ws_created')::int as ws_events,
         count(distinct user_id) filter (where name='ws_created' and user_id is not null)::int as ws_users,

         count(*) filter (where name='gw_published')::int as gw_events,
         count(distinct user_id) filter (where name='gw_published' and user_id is not null)::int as gw_users
       from events
       where ts >= now() - ($1::int || ' days')::interval`,
      [wd]
    );

    return { ...(r1.rows[0] || {}), ...(r2.rows[0] || {}), window_days: wd };
  } catch {
    // Missing table/migration or any DB error should not break admin UX.
    return null;
  }
}

export async function getAnalyticsDaily(days = 14) {
  if (!CFG.ANALYTICS_ENABLED) return [];
  const d = Math.max(1, Math.min(90, Number(days) || 14));

  try {
    const r = await pool.query(
      `select
         (ts at time zone 'Europe/Moscow')::date as day,
         count(distinct user_id) filter (where user_id is not null)::int as dau,
         count(*) filter (where name='start')::int as starts,
         count(*) filter (where name='ws_created')::int as ws_created,
         count(*) filter (where name='gw_published')::int as gw_published,
         count(*) filter (where name='bx_offer_published')::int as bx_offer_published,
         count(*) filter (where name='intro_attempt')::int as intro_attempt,
         count(*) filter (where name='paywall_shown')::int as paywall_shown,
         count(*) filter (
           where name='payment_success'
             and ((meta->>'payload') like 'brand_%' or (meta->>'payload') like 'bplan_%')
         )::int as brand_purchases
       from events
       where ts >= now() - ($1::int || ' days')::interval
       group by day
       order by day desc`,
      [d]
    );
    return r.rows || [];
  } catch {
    return [];
  }
}


export async function findUserByUsername(username) {
  const u = String(username || '').replace(/^@/, '').toLowerCase();
  const r = await pool.query(`select id, tg_id, tg_username from users where lower(tg_username)= $1 limit 1`, [u]);
  return r.rows[0] || null;
}

export async function getUserTgIdByUserId(userId) {
  const r = await pool.query(`select tg_id, tg_username from users where id=$1`, [userId]);
  return r.rows[0] || null;
}

// -----------------------------
// Brand Pass credits (brands pay for first contact)
// -----------------------------

export async function getBrandCredits(userId) {
  const r = await pool.query(`select brand_credits from users where id=$1`, [userId]);
  return Number(r.rows[0]?.brand_credits ?? 0);
}

export async function getBrandIntroMeta(userId) {
  const r = await pool.query(
    `select brand_credits, brand_trial_granted, brand_trial_granted_at
     from users
     where id=$1`,
    [userId]
  );
  const row = r.rows[0] || null;
  if (!row) return null;
  return {
    brand_credits: Number(row.brand_credits ?? 0),
    brand_trial_granted: !!row.brand_trial_granted,
    brand_trial_granted_at: row.brand_trial_granted_at || null
  };
}

export async function getIntroDailyUsage(userId, day = null) {
  const r = await pool.query(
    `select used_count from intro_daily_usage
     where user_id=$1 and day = coalesce($2::date, now()::date)`,
    [userId, day]
  );
  return Number(r.rows[0]?.used_count ?? 0);
}

export async function addBrandCredits(userId, credits) {
  const r = await pool.query(
    `update users
       set brand_credits = brand_credits + $2,
           brand_credits_updated_at = now()
     where id=$1
     returning brand_credits`,
    [userId, Number(credits || 0)]
  );
  return Number(r.rows[0]?.brand_credits ?? 0);
}

// -----------------------------
// Intro retry credits (fairness)
// -----------------------------

export async function countAvailableBrandRetryCredits(userId) {
  const r = await pool.query(
    `select count(*)::int as c
     from brand_retry_credits
     where user_id=$1 and status='AVAILABLE' and expires_at>now()`,
    [userId]
  );
  return Number(r.rows[0]?.c ?? 0);
}

// Transaction helper: take 1 retry credit (oldest expiry) with row-level lock
async function takeAvailableRetryCreditForUpdate(client, userId) {
  const r = await client.query(
    `select id
     from brand_retry_credits
     where user_id=$1 and status='AVAILABLE' and expires_at>now()
     order by expires_at asc
     limit 1
     for update`,
    [userId]
  );
  return r.rows[0]?.id || null;
}

async function redeemRetryCredit(client, retryId, redeemedThreadId) {
  await client.query(
    `update brand_retry_credits
     set status='REDEEMED', redeemed_at=now(), redeemed_thread_id=$2
     where id=$1 and status='AVAILABLE'`,
    [retryId, redeemedThreadId]
  );
}

export async function listIntroThreadsForRetry(limit = 50, afterHours = 24) {
  // Note: these columns are added in migration 019. Caller can catch undefined_column for rolling upgrades.
  const r = await pool.query(
    `select t.id as thread_id, t.buyer_user_id, t.offer_id, t.buyer_first_msg_at
     from barter_threads t
     where t.intro_charge_source='CREDITS'
       and t.intro_charged_at is not null
       and t.buyer_first_msg_at is not null
       and t.seller_first_reply_at is null
       and t.retry_issued_at is null
       and t.buyer_first_msg_at < (now() - (($2::text || ' hours')::interval))
     order by t.buyer_first_msg_at asc
     limit $1`,
    [Number(limit || 50), Number(afterHours || 24)]
  );
  return r.rows || [];
}

export async function issueRetryCreditForThread(threadId, userId, expiresDays = 7, reason = 'no_reply') {
  const r = await pool.query(
    `insert into brand_retry_credits (user_id, source_thread_id, expires_at, reason)
     values ($1,$2, now() + (($3::text || ' days')::interval), $4)
     on conflict (source_thread_id) do nothing
     returning id`,
    [userId, threadId, Number(expiresDays || 7), String(reason || 'no_reply')]
  );
  if (r.rowCount > 0) {
    await pool.query(
      `update barter_threads set retry_issued_at=now(), updated_at=now()
       where id=$1 and retry_issued_at is null`,
      [threadId]
    );
    return { issued: true, id: r.rows[0].id };
  }
  return { issued: false, id: null };
}

export async function expireRetryCredits(limit = 200) {
  const r = await pool.query(
    `update brand_retry_credits
     set status='EXPIRED'
     where id in (
       select id from brand_retry_credits
       where status='AVAILABLE' and expires_at < now()
       order by expires_at asc
       limit $1
     )
     returning id`,
    [Number(limit || 200)]
  );
  return r.rowCount || 0;
}

// Minimal rule: "brand" is a user without any workspaces.
export async function userHasWorkspace(userId) {
  const r = await pool.query(`select 1 from workspaces where owner_user_id=$1 limit 1`, [userId]);
  return r.rowCount > 0;
}

// Workspaces
export async function createWorkspace({ ownerUserId, title, channelId, channelUsername }) {
  const r = await pool.query(
    `insert into workspaces (owner_user_id, title, channel_id, channel_username)
     values ($1,$2,$3,$4)
     on conflict (owner_user_id, channel_id)
     do update set title = excluded.title, channel_username = excluded.channel_username
     returning *`,
    [ownerUserId, title, channelId, channelUsername || null]
  );
  const ws = r.rows[0];
  await ensureWorkspaceSettings(ws.id);
  return ws;
}

export async function ensureWorkspaceSettings(workspaceId) {
  await pool.query(
    `insert into workspace_settings (workspace_id)
     values ($1)
     on conflict (workspace_id) do nothing`,
    [workspaceId]
  );
}

export async function listWorkspaces(ownerUserId) {
  const r = await pool.query(
    `select ws.*, s.network_enabled, s.curator_enabled, s.auto_draw_default, s.auto_publish_default,
            s.plan, s.pro_until, s.pro_pinned_offer_id,
            s.profile_title, s.profile_niche, s.profile_contact, s.profile_geo
     from workspaces ws
     join workspace_settings s on s.workspace_id = ws.id
     where ws.owner_user_id=$1
     order by ws.created_at desc`,
    [ownerUserId]
  );
  return r.rows;
}

export async function getWorkspace(ownerUserId, workspaceId) {
  const r = await pool.query(
    `select ws.*, s.network_enabled, s.curator_enabled, s.auto_draw_default, s.auto_publish_default,
            s.plan, s.pro_until, s.pro_pinned_offer_id,
            s.profile_title, s.profile_niche, s.profile_contact, s.profile_geo
     from workspaces ws
     join workspace_settings s on s.workspace_id = ws.id
     where ws.owner_user_id=$1 and ws.id=$2`,
    [ownerUserId, workspaceId]
  );
  return r.rows[0] || null;
}



// Workspaces (admin/helpers)
export async function getWorkspaceAny(workspaceId) {
  const r = await pool.query(
    `select ws.*, s.network_enabled, s.curator_enabled, s.auto_draw_default, s.auto_publish_default,
            s.plan, s.pro_until, s.pro_pinned_offer_id,
            s.profile_title, s.profile_niche, s.profile_contact, s.profile_geo
     from workspaces ws
     join workspace_settings s on s.workspace_id = ws.id
     where ws.id=$1`,
    [workspaceId]
  );
  return r.rows[0] || null;
}

export async function findWorkspaceByChannelUsername(channelUsername) {
  const u = String(channelUsername || '').replace(/^@/, '').toLowerCase();
  const r = await pool.query(
    `select ws.*, s.network_enabled, s.curator_enabled, s.auto_draw_default, s.auto_publish_default,
            s.plan, s.pro_until, s.pro_pinned_offer_id,
            s.profile_title, s.profile_niche, s.profile_contact, s.profile_geo
     from workspaces ws
     join workspace_settings s on s.workspace_id = ws.id
     where lower(ws.channel_username)= $1
     limit 1`,
    [u]
  );
  return r.rows[0] || null;
}

export async function isWorkspacePro(workspaceId) {
  const r = await pool.query(
    `select plan, pro_until from workspace_settings where workspace_id=$1`,
    [workspaceId]
  );
  const row = r.rows[0];
  if (!row) return false
  const plan = String(row.plan || 'free');
  const until = row.pro_until;
  if (plan !== 'pro') return false;
  if (!until) return true;
  return new Date(until).getTime() > Date.now();
}

export async function activateWorkspacePro(workspaceId, days) {
  await pool.query(
    `update workspace_settings
     set plan='pro',
         pro_until = coalesce(pro_until, now()) + ($2::int || ' days')::interval,
         updated_at=now()
     where workspace_id=$1`,
    [workspaceId, Number(days || 30)]
  );
}

export async function setWorkspacePinnedOffer(workspaceId, offerId) {
  await pool.query(
    `update workspace_settings
     set pro_pinned_offer_id=$2, updated_at=now()
     where workspace_id=$1`,
    [workspaceId, offerId]
  );
}

export async function countActiveBarterOffers(workspaceId) {
  const r = await pool.query(
    `select count(*)::int as cnt from barter_offers where workspace_id=$1 and status='ACTIVE'`,
    [workspaceId]
  );
  return Number(r.rows[0]?.cnt || 0);
}
export async function setWorkspaceSetting(workspaceId, patch) {
  const keys = Object.keys(patch);
  if (!keys.length) return;
  const sets = keys.map((k, i) => `${k}=$${i + 2}`);
  const vals = keys.map(k => patch[k]);
  await pool.query(
    `update workspace_settings set ${sets.join(', ')}, updated_at=now() where workspace_id=$1`,
    [workspaceId, ...vals]
  );
}

// Curators
export async function addCurator(workspaceId, curatorUserId, addedByUserId) {
  const r = await pool.query(
    `insert into workspace_curators (workspace_id, user_id, added_by_user_id)
     values ($1,$2,$3)
     on conflict (workspace_id, user_id) do nothing
     returning id`,
    [workspaceId, curatorUserId, addedByUserId]
  );
  return r.rowCount ? r.rows[0] : null;
}

export async function listCurators(workspaceId) {
  const r = await pool.query(
    `select c.id, u.id as user_id, u.tg_id, u.tg_username, c.created_at
     from workspace_curators c
     join users u on u.id = c.user_id
     where c.workspace_id=$1
     order by c.created_at desc`,
    [workspaceId]
  );
  return r.rows;
}

export async function removeCurator(workspaceId, curatorUserId) {
  await pool.query(
    `delete from workspace_curators where workspace_id=$1 and user_id=$2`,
    [workspaceId, curatorUserId]
  );
}

// Workspace audit
export async function auditWorkspace(workspaceId, actorUserId, action, payload = {}) {
  await pool.query(
    `insert into workspace_audit (workspace_id, actor_user_id, action, payload)
     values ($1,$2,$3,$4::jsonb)`,
    [workspaceId, actorUserId, action, JSON.stringify(payload || {})]
  );
}

export async function listWorkspaceAudit(workspaceId, limit = 30) {
  const r = await pool.query(
    `select action, payload, created_at
     from workspace_audit
     where workspace_id=$1
     order by created_at desc
     limit $2`,
    [workspaceId, limit]
  );
  return r.rows;
}

// Giveaways
export async function createGiveaway({ workspaceId, prizeValueText, winnersCount, endsAt, autoDraw, autoPublish }) {
  const r = await pool.query(
    `insert into giveaways (workspace_id, prize_value_text, winners_count, ends_at, auto_draw, auto_publish)
     values ($1,$2,$3,$4,$5,$6)
     returning *`,
    [workspaceId, prizeValueText || null, winnersCount || 1, endsAt || null, !!autoDraw, !!autoPublish]
  );
  return r.rows[0];
}

export async function updateGiveaway(giveawayId, patch) {
  const keys = Object.keys(patch);
  if (!keys.length) return;
  const sets = keys.map((k, i) => `${k}=$${i + 2}`);
  const vals = keys.map(k => patch[k]);
  await pool.query(
    `update giveaways set ${sets.join(', ')}, updated_at=now() where id=$1`,
    [giveawayId, ...vals]
  );
}

export async function getGiveawayForOwner(giveawayId, ownerUserId) {
  const r = await pool.query(
    `select g.*, ws.owner_user_id, ws.channel_id, ws.channel_username, ws.title as workspace_title
     from giveaways g
     join workspaces ws on ws.id = g.workspace_id
     where g.id=$1 and ws.owner_user_id=$2`,
    [giveawayId, ownerUserId]
  );
  return r.rows[0] || null;
}

export async function deleteGiveawayForOwner(giveawayId, ownerUserId) {
  // Hard delete (cascades: sponsors/entries/winners/audit).
  // We owner-gate via workspaces join.
  const r = await pool.query(
    `delete from giveaways g
     using workspaces ws
     where g.id=$1
       and g.workspace_id = ws.id
       and ws.owner_user_id = $2
     returning g.id, g.workspace_id`,
    [Number(giveawayId), Number(ownerUserId)]
  );
  return r.rows[0] || null;
}

export async function getGiveawayPublic(giveawayId) {
  const r = await pool.query(
    `select id, workspace_id, status, ends_at, published_chat_id
     from giveaways
     where id=$1`,
    [giveawayId]
  );
  return r.rows[0] || null;
}

export async function getGiveawayInfoForUser(giveawayId) {
  const r = await pool.query(
    `select id, status, ends_at, prize_value_text, winners_count, published_chat_id
     from giveaways
     where id=$1`,
    [giveawayId]
  );
  return r.rows[0] || null;
}

export async function listGiveaways(ownerUserId, limit = 20) {
  const r = await pool.query(
    `select g.*, ws.title as workspace_title
     from giveaways g
     join workspaces ws on ws.id = g.workspace_id
     where ws.owner_user_id=$1
     order by g.created_at desc
     limit $2`,
    [ownerUserId, limit]
  );
  return r.rows;
}

// Sponsors
export async function replaceGiveawaySponsors(giveawayId, sponsorTexts) {
  await pool.query(`delete from giveaway_sponsors where giveaway_id=$1`, [giveawayId]);
  let pos = 1;
  for (const s of sponsorTexts) {
    await pool.query(
      `insert into giveaway_sponsors (giveaway_id, position, sponsor_text)
       values ($1,$2,$3)`,
      [giveawayId, pos++, s]
    );
  }
}

export async function listGiveawaySponsors(giveawayId) {
  const r = await pool.query(
    `select sponsor_text, position
     from giveaway_sponsors
     where giveaway_id=$1
     order by position asc`,
    [giveawayId]
  );
  return r.rows;
}

// Entries
export async function upsertGiveawayEntry(giveawayId, userId) {
  await pool.query(
    `insert into giveaway_entries (giveaway_id, user_id, joined_at, is_eligible)
     values ($1,$2, now(), false)
     on conflict (giveaway_id, user_id)
     do update set joined_at = giveaway_entries.joined_at`,
    [giveawayId, userId]
  );
}

export async function getEntryStatus(giveawayId, userId) {
  const r = await pool.query(
    `select is_eligible, joined_at, last_checked_at
     from giveaway_entries
     where giveaway_id=$1 and user_id=$2`,
    [giveawayId, userId]
  );
  return r.rows[0] || null;
}

export async function setEntryEligibility(giveawayId, userId, isEligible) {
  await pool.query(
    `update giveaway_entries
     set is_eligible=$3, last_checked_at=now()
     where giveaway_id=$1 and user_id=$2`,
    [giveawayId, userId, !!isEligible]
  );
}

export async function getGiveawayStats(giveawayId, ownerUserId) {
  const r = await pool.query(
    `select
        count(*)::int as entries_total,
        sum(case when e.is_eligible then 1 else 0 end)::int as eligible_count,
        sum(case when not e.is_eligible then 1 else 0 end)::int as not_eligible_count,
        max(e.last_checked_at) as last_checked_at,
        max(e.joined_at) as last_joined_at
     from giveaway_entries e
     join giveaways g on g.id = e.giveaway_id
     join workspaces ws on ws.id = g.workspace_id
     where g.id=$1 and ws.owner_user_id=$2`,
    [giveawayId, ownerUserId]
  );
  return r.rows[0] || null;
}


export async function listGiveawayEntriesPage(giveawayId, ownerUserId, limit = 10, offset = 0) {
  const lim = Math.max(1, Math.min(50, Number(limit || 10)));
  const off = Math.max(0, Number(offset || 0));

  const r = await pool.query(
    `select
        e.user_id,
        u.tg_id,
        u.tg_username,
        e.is_eligible,
        e.last_checked_at,
        e.joined_at
     from giveaway_entries e
     join giveaways g on g.id = e.giveaway_id
     join workspaces ws on ws.id = g.workspace_id
     join users u on u.id = e.user_id
     where g.id=$1 and ws.owner_user_id=$2
     order by e.joined_at desc nulls last, e.user_id desc
     limit $3 offset $4`,
    [giveawayId, ownerUserId, lim, off]
  );

  return r.rows || [];
}


export async function exportGiveawayParticipantsUsernames(giveawayId, ownerUserId, onlyEligible = null) {
  const cond = onlyEligible === null ? '' : 'and e.is_eligible = ' + (onlyEligible ? 'true' : 'false');
  const r = await pool.query(
    `select u.tg_username
     from giveaway_entries e
     join giveaways g on g.id = e.giveaway_id
     join workspaces ws on ws.id = g.workspace_id
     join users u on u.id = e.user_id
     where g.id=$1 and ws.owner_user_id=$2
       and u.tg_username is not null and u.tg_username <> ''
       ${cond}
     order by lower(u.tg_username) asc`,
    [giveawayId, ownerUserId]
  );
  return r.rows.map(x => String(x.tg_username));
}

export async function exportGiveawayWinnersUsernames(giveawayId, ownerUserId) {
  const r = await pool.query(
    `select w.place, u.tg_username
     from giveaway_winners w
     join giveaways g on g.id = w.giveaway_id
     join workspaces ws on ws.id = g.workspace_id
     join users u on u.id = w.user_id
     where w.giveaway_id=$1 and ws.owner_user_id=$2
       and u.tg_username is not null and u.tg_username <> ''
     order by w.place asc`,
    [giveawayId, ownerUserId]
  );
  return r.rows.map(x => ({ place: Number(x.place), username: String(x.tg_username) }));
}

// Winners for publish/preview (include tg_id for mentions when username missing)
export async function exportGiveawayWinnersForPublish(giveawayId, ownerUserId) {
  const r = await pool.query(
    `select w.place, u.tg_id, u.tg_username
     from giveaway_winners w
     join giveaways g on g.id = w.giveaway_id
     join workspaces ws on ws.id = g.workspace_id
     join users u on u.id = w.user_id
     where w.giveaway_id=$1 and ws.owner_user_id=$2
     order by w.place asc`,
    [giveawayId, ownerUserId]
  );
  return r.rows.map(x => ({ place: Number(x.place), tg_id: Number(x.tg_id), username: x.tg_username ? String(x.tg_username) : null }));
}

// Winners
export async function setWinners(giveawayId, winners) {
  // winners: [{user_id, place}]
  await pool.query(`delete from giveaway_winners where giveaway_id=$1`, [giveawayId]);
  for (const w of winners) {
    await pool.query(
      `insert into giveaway_winners (giveaway_id, user_id, place)
       values ($1,$2,$3)`,
      [giveawayId, (w.user_id ?? w.userId), w.place]
    );
  }
}

export async function markGiveawayResultsPublished(giveawayId, ownerUserId, resultsMessageId) {
  const q = {
    text: `
      update giveaways g
      set status='RESULTS_PUBLISHED', results_message_id=$3, results_published_at=now(), updated_at=now()
      from workspaces w
      where g.id=$1 and g.workspace_id=w.id and w.owner_user_id=$2 and g.results_message_id is null
      returning g.id
    `,
    values: [giveawayId, ownerUserId, resultsMessageId],
  };
  const r = await pool.query(q);
  return r.rows[0] || null;
}

// Reserve publish to avoid double posts across retries (sets results_message_id=-1)
export async function reserveGiveawayPublish(giveawayId, ownerUserId) {
  const r = await pool.query(
    `update giveaways g
     set results_message_id = -1, updated_at=now()
     from workspaces w
     where g.id=$1 and g.workspace_id=w.id and w.owner_user_id=$2
       and g.results_message_id is null
       and upper(g.status)='WINNERS_DRAWN'
     returning g.id`,
    [giveawayId, ownerUserId]
  );
  return r.rows[0] || null;
}

export async function finalizeGiveawayPublish(giveawayId, ownerUserId, resultsMessageId) {
  const r = await pool.query(
    `update giveaways g
     set status='RESULTS_PUBLISHED', results_message_id=$3, results_published_at=now(), updated_at=now()
     from workspaces w
     where g.id=$1 and g.workspace_id=w.id and w.owner_user_id=$2
       and g.results_message_id = -1
     returning g.id`,
    [giveawayId, ownerUserId, resultsMessageId]
  );
  return r.rows[0] || null;
}

export async function releaseGiveawayPublish(giveawayId, ownerUserId) {
  const r = await pool.query(
    `update giveaways g
     set results_message_id=null, updated_at=now()
     from workspaces w
     where g.id=$1 and g.workspace_id=w.id and w.owner_user_id=$2
       and g.results_message_id = -1
     returning g.id`,
    [giveawayId, ownerUserId]
  );
  return r.rows[0] || null;
}

// Giveaway audit
export async function auditGiveaway(giveawayId, workspaceId, actorUserId, action, payload = {}) {
  await pool.query(
    `insert into giveaway_audit (giveaway_id, workspace_id, actor_user_id, action, payload)
     values ($1,$2,$3,$4,$5::jsonb)`,
    [giveawayId, workspaceId, actorUserId || null, action, JSON.stringify(payload || {})]
  );
}

export async function listGiveawayAudit(giveawayId, limit = 30) {
  const r = await pool.query(
    `select action, payload, created_at
     from giveaway_audit
     where giveaway_id=$1
     order by created_at desc
     limit $2`,
    [giveawayId, limit]
  );
  return r.rows;
}

// Worker queries
export async function listGiveawaysToEnd(limit = 50) {
  const r = await pool.query(
    `select id, workspace_id, ends_at, status, auto_draw, auto_publish, published_chat_id
     from giveaways
     where status in ('PUBLISHED','RUNNING')
       and ends_at is not null
       and ends_at <= now()
     order by ends_at asc
     limit $1`,
    [limit]
  );
  return r.rows;
}

export async function listEndedGiveawaysToDraw(limit = 50) {
  const r = await pool.query(
    `select g.id, g.workspace_id, g.winners_count, g.auto_publish, g.auto_draw, g.published_chat_id, g.results_message_id, g.ends_at, w.owner_user_id
     from giveaways g
     join workspaces w on w.id = g.workspace_id
     where g.status='ENDED'
       and g.winners_drawn_at is null
     order by g.updated_at asc
     limit $1`,
    [limit]
  );
  return r.rows;
}

export async function listDrawnGiveawaysToPublish(limit = 50) {
  const r = await pool.query(
    `select id, workspace_id, published_chat_id, results_message_id
     from giveaways
     where status='WINNERS_DRAWN'
       and auto_publish=true
       and results_message_id is null
     order by updated_at asc
     limit $1`,
    [limit]
  );
  return r.rows;
}

export async function listEligibleUserIdsForGiveaway(giveawayId) {
  const r = await pool.query(
    `select user_id from giveaway_entries where giveaway_id=$1 and is_eligible=true`,
    [giveawayId]
  );
  return r.rows.map(x => Number(x.user_id));
}

export async function listAllUserIdsForGiveaway(giveawayId) {
  const r = await pool.query(
    `select user_id from giveaway_entries where giveaway_id=$1`,
    [giveawayId]
  );
  return r.rows.map(x => Number(x.user_id));
}

export async function listEntriesToCheck(limit = 50) {
  const r = await pool.query(
    `select e.giveaway_id, e.user_id
     from giveaway_entries e
     join giveaways g on g.id = e.giveaway_id
     where g.status <> 'ENDED'
       and e.is_eligible=false
       and (e.last_checked_at is null or e.last_checked_at < now() - interval '10 minutes')
     order by coalesce(e.last_checked_at, e.joined_at) asc
     limit $1`,
    [limit]
  );
  return r.rows;
}

// -----------------------------
// Barters marketplace (v0.9.1)
// -----------------------------

export async function createBarterOffer(input) {
  const {
    workspaceId,
    creatorUserId,
    category,
    offerType,
    compensationType,
    title,
    description,
    partnerFolderId,
    contact,
  } = input;

  const r = await pool.query(
    `insert into barter_offers
      (workspace_id, creator_user_id, category, offer_type, compensation_type, title, description, partner_folder_id, contact)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     returning *`,
    [workspaceId, creatorUserId || null, category, offerType, compensationType, title, description, partnerFolderId || null, contact || null]
  );
  return r.rows[0];
}

export async function listNetworkBarterOffers(opts = {}) {
  const { category = null, offerType = null, compensationType = null, limit = 5, offset = 0 } = opts;
  const r = await pool.query(
    `select o.*, w.title as ws_title, w.channel_username, w.channel_id
     from barter_offers o
     join workspaces w on w.id = o.workspace_id
     join workspace_settings s on s.workspace_id = w.id
     where o.status='ACTIVE'
       and s.network_enabled=true
       and ($1::text is null or o.category=$1)
       and ($2::text is null or o.offer_type=$2)
       and ($3::text is null or o.compensation_type=$3)
     order by case when s.plan='pro' and (s.pro_until is null or s.pro_until>now()) and s.pro_pinned_offer_id = o.id then 0 else 1 end,
              o.bump_at desc
     limit $4 offset $5`,
    [category, offerType, compensationType, limit, offset]
  );
  return r.rows;
}

export async function countNetworkBarterOffers(opts = {}) {
  const { category = null, offerType = null, compensationType = null } = opts;
  const r = await pool.query(
    `select count(*)::int as cnt
     from barter_offers o
     join workspaces w on w.id = o.workspace_id
     join workspace_settings s on s.workspace_id = w.id
     where o.status='ACTIVE'
       and s.network_enabled=true
       and ($1::text is null or o.category=$1)
       and ($2::text is null or o.offer_type=$2)
       and ($3::text is null or o.compensation_type=$3)`,
    [category, offerType, compensationType]
  );
  return Number(r.rows[0]?.cnt || 0);
}

export async function listBarterOffersForOwnerWorkspace(ownerUserId, workspaceId, limit = 10, offset = 0) {
  // owner gate
  const ws = await pool.query(
    `select id from workspaces where id=$1 and owner_user_id=$2`,
    [workspaceId, ownerUserId]
  );
  if (!ws.rows.length) return [];
  const r = await pool.query(
    `select *
     from barter_offers
     where workspace_id=$1
       and coalesce(status,'ACTIVE') <> 'CLOSED'
     order by created_at desc
     limit $2 offset $3`,
    [workspaceId, limit, offset]
  );
  return r.rows;
}

export async function listArchivedBarterOffersForOwnerWorkspace(ownerUserId, workspaceId, limit = 10, offset = 0) {
  // owner gate
  const ws = await pool.query(
    `select id from workspaces where id=$1 and owner_user_id=$2`,
    [workspaceId, ownerUserId]
  );
  if (!ws.rows.length) return [];
  const r = await pool.query(
    `select *
     from barter_offers
     where workspace_id=$1
       and coalesce(status,'ACTIVE') = 'CLOSED'
     order by updated_at desc, created_at desc
     limit $2 offset $3`,
    [workspaceId, limit, offset]
  );
  return r.rows;
}

export async function restoreBarterOfferForOwner(offerId, ownerUserId) {
  const r = await pool.query(
    `update barter_offers o
     set status='ACTIVE', bump_at=now(), updated_at=now()
     from workspaces w
     where o.id=$1
       and o.workspace_id = w.id
       and w.owner_user_id = $2
     returning o.*`,
    [Number(offerId), Number(ownerUserId)]
  );
  return r.rows[0] || null;
}

export async function getBarterOfferForOwner(ownerUserId, offerId) {
  const r = await pool.query(
    `select o.*, w.owner_user_id
     from barter_offers o
     join workspaces w on w.id = o.workspace_id
     where o.id=$1`,
    [offerId]
  );
  const row = r.rows[0];
  if (!row) return null;
  if (Number(row.owner_user_id) !== Number(ownerUserId)) return null;
  return row;
}

export async function updateBarterOffer(offerId, patch) {
  const fields = [];
  const vals = [];
  let idx = 1;

  const allowed = ['status', 'title', 'description', 'contact', 'bump_at', 'partner_folder_id'];
  for (const k2 of allowed) {
    if (patch[k2] === undefined) continue;
    fields.push(`${k2}=$${idx++}`);
    vals.push(patch[k2]);
  }
  fields.push(`updated_at=now()`);
  vals.push(offerId);
  const sql = `update barter_offers set ${fields.join(', ')} where id=$${idx} returning *`;
  const r = await pool.query(sql, vals);
  return r.rows[0];
}

export async function auditBarterOffer(offerId, workspaceId, actorUserId, action, payload = {}) {
  await pool.query(
    `insert into barter_offer_audit (offer_id, workspace_id, actor_user_id, action, payload)
     values ($1,$2,$3,$4,$5::jsonb)`,
    [offerId, workspaceId, actorUserId || null, action, JSON.stringify(payload || {})]
  );
}

export async function updateBarterOfferStatus(offerId, status) {
  return await updateBarterOffer(offerId, { status });
}

export async function bumpBarterOffer(offerId) {
  const r = await pool.query(
    `update barter_offers
     set bump_at=now(), bump_count=bump_count+1, updated_at=now()
     where id=$1
     returning *`,
    [offerId]
  );
  return r.rows[0];
}

export async function getBarterOfferPublic(offerId) {
  const r = await pool.query(
    `select o.*, w.title as ws_title, w.channel_username, w.channel_id, w.owner_user_id,
            s.network_enabled
     from barter_offers o
     join workspaces w on w.id = o.workspace_id
     join workspace_settings s on s.workspace_id = w.id
     where o.id=$1`,
    [offerId]
  );
  return r.rows[0] || null;
}

// -----------------------------
// Barters inbox / mini-deals (v0.9.2)
// -----------------------------

export async function getOrCreateBarterThread(offerId, buyerUserId) {
  const rOffer = await pool.query(
    `select o.id as offer_id, o.workspace_id, w.owner_user_id as seller_user_id, s.network_enabled, o.status
     from barter_offers o
     join workspaces w on w.id=o.workspace_id
     join workspace_settings s on s.workspace_id=w.id
     where o.id=$1`,
    [offerId]
  );
  const off = rOffer.rows[0];
  if (!off) return null;
  if (String(off.status).toUpperCase() !== 'ACTIVE') return null;
  if (!off.network_enabled) return null;

  const r = await pool.query(
    `insert into barter_threads (offer_id, workspace_id, buyer_user_id, seller_user_id, last_message_at)
     values ($1,$2,$3,$4, now())
     on conflict (offer_id, buyer_user_id)
     do update set updated_at=now()
     returning *`,
    [offerId, off.workspace_id, buyerUserId, off.seller_user_id]
  );
  return r.rows[0];
}

/**
 * Idempotent first-contact charging:
 * - If thread already exists => charged=false (no credits spent)
 * - If new thread created by this call => charged=true only when buyer is a "brand" (no workspaces)
 * - If brand has 0 credits => needPaywall=true
 */
export async function getOrCreateBarterThreadWithCredits(offerId, buyerUserId, opts = {}) {
  const forceBrand = !!opts.forceBrand;
  const cost = Number(opts.cost ?? 1);
  const trialCredits = Number(opts.trialCredits ?? 0);
  const dailyLimitRaw = opts.dailyLimit;
  const dailyLimit = dailyLimitRaw === null || dailyLimitRaw === undefined ? null : Number(dailyLimitRaw);

  const client = await pool.connect();
  try {
    await client.query('begin');

    // load offer
    const offerRes = await client.query(
      `select o.id, o.workspace_id, o.creator_user_id
       from barter_offers o
       where o.id=$1`,
      [offerId]
    );
    const offer = offerRes.rows[0];
    if (!offer) {
      await client.query('rollback');
      return { ok: false, error: 'offer_not_found' };
    }

    // prevent self-message (creator cannot open thread to own offer)
    if (Number(offer.creator_user_id) === Number(buyerUserId)) {
      await client.query('rollback');
      return { ok: false, self: true };
    }

    // check existing thread
    const existingRes = await client.query(
      `select * from barter_threads where offer_id=$1 and buyer_user_id=$2`,
      [offerId, buyerUserId]
    );
    if (existingRes.rows.length) {
      await client.query('commit');
      return { ok: true, thread: existingRes.rows[0], charged: false, chargedAmount: 0 };
    }

    // who is buyer
    const buyer = await client.query(
      `select id, brand_credits, brand_trial_granted
       from users
       where id=$1
       for update`,
      [buyerUserId]
    );
    const b = buyer.rows[0];
    if (!b) {
      await client.query('rollback');
      return { ok: false, error: 'buyer_not_found' };
    }

    // Minimal rule: a "brand" is a user without any workspaces.
    // We also allow forcing brand-mode explicitly (e.g., user opens Brand menu even if they have a workspace).
    let buyerIsBrand = forceBrand;
    if (!buyerIsBrand) {
      const hasWs = await client.query(
        `select 1 from workspaces where owner_user_id=$1 limit 1`,
        [buyerUserId]
      );
      buyerIsBrand = hasWs.rowCount === 0;
    }

    const requireCredits = forceBrand || buyerIsBrand;

    // Daily limit + trial (brands only)
    let dailyUsed = null;
    let trialGranted = false;
    let balance = requireCredits ? Number(b.brand_credits || 0) : null;

if (requireCredits) {
  const lim = Number.isFinite(dailyLimit) ? dailyLimit : null;
  if (lim !== null && lim > 0) {
    const usage = await client.query(
      `select used_count
       from intro_daily_usage
       where user_id=$1 and day=now()::date
       for update`,
      [buyerUserId]
    );
    dailyUsed = Number(usage.rows[0]?.used_count || 0);
    if (dailyUsed >= lim) {
      await client.query('rollback');
      return { ok: false, limitReached: true, dailyUsed, dailyLimit: lim };
    }
  }

  const normalizedCost = Number.isFinite(cost) && cost > 0 ? Math.floor(cost) : 1;
  const normalizedTrial = Number.isFinite(trialCredits) && trialCredits > 0 ? Math.floor(trialCredits) : 0;

  // Retry credits (fairness): use before paid credits (they expire).
  const retryEnabled = !!opts.retryEnabled;
  let retryUsed = false;
  let retryId = null;

  if (retryEnabled) {
    try {
      retryId = await takeAvailableRetryCreditForUpdate(client, buyerUserId);
      retryUsed = !!retryId;
    } catch (e) {
      // Rolling upgrade safety: if table doesn't exist yet, ignore.
      if (!(e && (e.code === '42P01' || String(e.message || '').includes('brand_retry_credits')))) throw e;
    }
  }

  if (!retryUsed) {
    if (balance < normalizedCost) {
      if (!b.brand_trial_granted && normalizedTrial > 0) {
        const t = await client.query(
          `update users
           set brand_credits = brand_credits + $2,
               brand_trial_granted=true,
               brand_trial_granted_at=now(),
               updated_at=now()
           where id=$1
           returning brand_credits`,
          [buyerUserId, normalizedTrial]
        );
        balance = Number(t.rows[0]?.brand_credits || balance);
        trialGranted = true;
      } else {
        await client.query('rollback');
        return { ok: false, needPaywall: true, balance };
      }
    }

    // re-check after possible trial
    if (balance < normalizedCost) {
      await client.query('rollback');
      return { ok: false, needPaywall: true, balance };
    }
  }

  // create thread (store intro payment meta if migration is applied)
  const chargeSource = retryUsed ? 'RETRY' : 'CREDITS';
  let ins;
  try {
    ins = await client.query(
      `insert into barter_threads (offer_id, workspace_id, buyer_user_id, seller_user_id, status, last_message_at, intro_cost, intro_charge_source, intro_charged_at)
       values ($1,$2,$3,$4,'OPEN',now(),$5,$6,now())
       on conflict (offer_id, buyer_user_id) do nothing
       returning *`,
      [offerId, offer.workspace_id, buyerUserId, offer.creator_user_id, normalizedCost, chargeSource]
    );
  } catch (e) {
    // 42703 = undefined_column (rolling upgrades)
    if (e && e.code === '42703') {
      ins = await client.query(
        `insert into barter_threads (offer_id, workspace_id, buyer_user_id, seller_user_id, status, last_message_at)
         values ($1,$2,$3,$4,'OPEN',now())
         on conflict (offer_id, buyer_user_id) do nothing
         returning *`,
        [offerId, offer.workspace_id, buyerUserId, offer.creator_user_id]
      );
      // best-effort backfill if columns exist
      try {
        if (ins.rows[0]?.id) {
          await client.query(
            `update barter_threads
             set intro_cost=$2, intro_charge_source=$3, intro_charged_at=now(), updated_at=now()
             where id=$1`,
            [ins.rows[0].id, normalizedCost, chargeSource]
          );
        }
      } catch {}
    } else {
      throw e;
    }
  }

  if (!ins.rows.length) {
    // someone else created concurrently
    const again = await client.query(
      `select * from barter_threads where offer_id=$1 and buyer_user_id=$2`,
      [offerId, buyerUserId]
    );
    await client.query('commit');
    return {
      ok: true,
      thread: again.rows[0] || null,
      charged: false,
      chargedAmount: 0,
      retryUsed: false,
      balance,
      trialGranted,
      dailyUsed,
      dailyLimit: Number.isFinite(dailyLimit) ? dailyLimit : null
    };
  }

  // Apply payment: retry credit OR paid credits
  if (retryUsed && retryId) {
    await redeemRetryCredit(client, retryId, ins.rows[0].id);
  } else {
    const chargedRes = await client.query(
      `update users
       set brand_credits = greatest(0, brand_credits - $2),
           brand_credits_spent = brand_credits_spent + $2,
           updated_at=now()
       where id=$1
       returning brand_credits`,
      [buyerUserId, normalizedCost]
    );
    balance = Number(chargedRes.rows[0]?.brand_credits || 0);
  }

  // track daily usage (counts retry too: it's still an intro attempt)
  const usageUp = await client.query(
    `insert into intro_daily_usage (user_id, day, used_count, updated_at)
     values ($1, now()::date, 1, now())
     on conflict (user_id, day)
     do update set used_count = intro_daily_usage.used_count + 1, updated_at=now()
     returning used_count`,
    [buyerUserId]
  );
  dailyUsed = Number(usageUp.rows[0]?.used_count || dailyUsed);

  await client.query('commit');
  return {
    ok: true,
    thread: ins.rows[0],
    charged: !retryUsed,
    chargedAmount: retryUsed ? 0 : normalizedCost,
    retryUsed,
    balance,
    trialGranted,
    dailyUsed,
    dailyLimit: Number.isFinite(dailyLimit) ? dailyLimit : null
  };
}

    // Non-brand path: create thread without credits
    const ins = await client.query(
      `insert into barter_threads (offer_id, workspace_id, buyer_user_id, seller_user_id, status, last_message_at)
       values ($1,$2,$3,$4,'OPEN',now())
       on conflict (offer_id, buyer_user_id) do nothing
       returning *`,
      [offerId, offer.workspace_id, buyerUserId, offer.creator_user_id]
    );

    if (!ins.rows.length) {
      const again = await client.query(
        `select * from barter_threads where offer_id=$1 and buyer_user_id=$2`,
        [offerId, buyerUserId]
      );
      await client.query('commit');
      return { ok: true, thread: again.rows[0] || null, charged: false, chargedAmount: 0 };
    }

    await client.query('commit');
    return { ok: true, thread: ins.rows[0], charged: false, chargedAmount: 0 };
  } catch (e) {
    try { await client.query('rollback'); } catch {}
    throw e;
  } finally {
    client.release();
  }
}

export async function countBarterThreadProofs(threadId) {
  const r = await pool.query(
    `select count(*)::int as c
     from barter_thread_proofs
     where thread_id=$1`,
    [threadId]
  );
  return Number(r.rows[0]?.c ?? 0);
}

export async function listBarterThreadProofs(threadId, limit = 10) {
  const r = await pool.query(
    `select id, thread_id, kind, url, tg_file_id, tg_file_unique_id, added_by_user_id, created_at
     from barter_thread_proofs
     where thread_id=$1
     order by created_at desc
     limit $2`,
    [threadId, Number(limit || 10)]
  );
  return r.rows;
}

async function assertThreadAccess(client, threadId, userId) {
  const r = await client.query(
    `select 1
     from barter_threads
     where id=$1 and (buyer_user_id=$2 or seller_user_id=$2)
     limit 1`,
    [threadId, userId]
  );
  if (!r.rowCount) throw new Error('NO_THREAD_ACCESS');
}

export async function addBarterThreadProofLink(threadId, userId, url) {
  const client = await pool.connect();
  try {
    await client.query('begin');
    await assertThreadAccess(client, threadId, userId);
    const r = await client.query(
      `insert into barter_thread_proofs (thread_id, kind, url, added_by_user_id)
       values ($1, 'LINK', $3, $2)
       returning *`,
      [threadId, userId, String(url || '').trim()]
    );
    await client.query('commit');
    return r.rows[0] || null;
  } catch (e) {
    await client.query('rollback');
    throw e;
  } finally {
    client.release();
  }
}

export async function addBarterThreadProofScreenshot(threadId, userId, fileId, fileUniqueId = null) {
  const client = await pool.connect();
  try {
    await client.query('begin');
    await assertThreadAccess(client, threadId, userId);
    const r = await client.query(
      `insert into barter_thread_proofs (thread_id, kind, tg_file_id, tg_file_unique_id, added_by_user_id)
       values ($1, 'SCREENSHOT', $3, $4, $2)
       returning *`,
      [threadId, userId, String(fileId || ''), fileUniqueId ? String(fileUniqueId) : null]
    );
    await client.query('commit');
    return r.rows[0] || null;
  } catch (e) {
    await client.query('rollback');
    throw e;
  } finally {
    client.release();
  }
}

export async function addBarterMessage(threadId, senderUserId, body) {
  const r = await pool.query(
    `insert into barter_messages (thread_id, sender_user_id, body)
     values ($1,$2,$3)
     returning *`,
    [threadId, senderUserId, body]
  );

  // Rolling upgrade safe: newer columns may not exist yet on some deployments.
  try {
    await pool.query(
      `update barter_threads
       set last_message_at=now(),
           updated_at=now(),
           buyer_first_msg_at = case when buyer_first_msg_at is null and buyer_user_id=$2 then now() else buyer_first_msg_at end,
           seller_first_reply_at = case when seller_first_reply_at is null and seller_user_id=$2 then now() else seller_first_reply_at end
       where id=$1`,
      [threadId, senderUserId]
    );
  } catch (e) {
    // 42703 = undefined_column
    if (e && (e.code === '42703' || String(e.message || '').includes('does not exist'))) {
      await pool.query(
        `update barter_threads
         set last_message_at=now(), updated_at=now()
         where id=$1`,
        [threadId]
      );
    } else {
      throw e;
    }
  }

  return r.rows[0];
}

export async function closeBarterThread(threadId, userId) {
  const r = await pool.query(
    `update barter_threads
     set status='CLOSED', updated_at=now()
     where id=$1 and (buyer_user_id=$2 or seller_user_id=$2)
     returning *`,
    [threadId, userId]
  );
  return r.rows[0] || null;
}

// -----------------------------
// Moderation (v1.0.0)
// -----------------------------

export async function addNetworkModerator(userId, addedByUserId) {
  const r = await pool.query(
    `insert into network_moderators (user_id, added_by_user_id)
     values ($1,$2)
     on conflict (user_id) do update set added_by_user_id=excluded.added_by_user_id
     returning user_id`,
    [userId, addedByUserId]
  );
  return r.rows[0] || null;
}

export async function removeNetworkModerator(userId) {
  await pool.query(`delete from network_moderators where user_id=$1`, [userId]);
}

export async function listNetworkModerators() {
  const r = await pool.query(
    `select m.user_id, u.tg_id, u.tg_username, m.created_at
     from network_moderators m
     join users u on u.id=m.user_id
     order by m.created_at desc`
  );
  return r.rows;
}

export async function isNetworkModerator(userId) {
  const r = await pool.query(`select 1 from network_moderators where user_id=$1`, [userId]);
  return r.rows.length > 0;
}

export async function createBarterReport(input) {
  const { workspaceId, offerId=null, threadId=null, reporterUserId=null, reason, details=null } = input;
  const r = await pool.query(
    `insert into barter_reports (workspace_id, offer_id, thread_id, reporter_user_id, reason, details)
     values ($1,$2,$3,$4,$5,$6)
     returning *`,
    [workspaceId, offerId, threadId, reporterUserId, reason, details]
  );
  return r.rows[0];
}

export async function listOpenBarterReports(limit=20, offset=0) {
  const r = await pool.query(
    `select r.*, u.tg_username as reporter_username,
            o.title as offer_title,
            w.title as ws_title, w.channel_username
     from barter_reports r
     join workspaces w on w.id=r.workspace_id
     left join users u on u.id=r.reporter_user_id
     left join barter_offers o on o.id=r.offer_id
     where r.status='OPEN'
     order by r.created_at desc
     limit $1 offset $2`,
    [limit, offset]
  );
  return r.rows;
}

export async function countOpenBarterReports() {
  const r = await pool.query(`select count(*)::int as cnt from barter_reports where status='OPEN'`);
  return Number(r.rows[0]?.cnt || 0);
}

export async function getBarterReport(reportId) {
  const r = await pool.query(
    `select r.*, u.tg_username as reporter_username,
            o.title as offer_title, o.status as offer_status,
            w.title as ws_title, w.channel_username
     from barter_reports r
     join workspaces w on w.id=r.workspace_id
     left join users u on u.id=r.reporter_user_id
     left join barter_offers o on o.id=r.offer_id
     where r.id=$1`,
    [reportId]
  );
  return r.rows[0] || null;
}

export async function resolveBarterReport(reportId, resolverUserId) {
  const r = await pool.query(
    `update barter_reports
     set status='RESOLVED', resolved_by_user_id=$2, resolved_at=now(), updated_at=now()
     where id=$1
     returning *`,
    [reportId, resolverUserId]
  );
  return r.rows[0] || null;
}

export async function moderatorCloseBarterThread(threadId) {
  const r = await pool.query(
    `update barter_threads set status='CLOSED', updated_at=now()
     where id=$1
     returning *`,
    [threadId]
  );
  return r.rows[0] || null;
}

export async function moderatorFreezeBarterOffer(offerId) {
  const r = await pool.query(
    `update barter_offers set status='PAUSED', updated_at=now() where id=$1 returning *`,
    [offerId]
  );
  return r.rows[0] || null;
}

// -----------------------------
// Brand Plan (tools subscription)
// -----------------------------

export async function getBrandPlan(userId) {
  const r = await pool.query(`select brand_plan, brand_plan_until from users where id=$1`, [userId]);
  return r.rows[0] || { brand_plan: null, brand_plan_until: null };
}

export async function isBrandPlanActive(userId) {
  const row = await getBrandPlan(userId);
  const plan = String(row.brand_plan || '').toLowerCase();
  if (!plan || plan === 'none') return false;
  const until = row.brand_plan_until;
  if (!until) return true;
  return new Date(until).getTime() > Date.now();
}

export async function activateBrandPlan(userId, plan, days) {
  const r = await pool.query(
    `update users
        set brand_plan = $2,
            brand_plan_until = (
              case
                when brand_plan_until is null or brand_plan_until < now() then now()
                else brand_plan_until
              end
            ) + ($3::int || ' days')::interval,
            brand_plan_updated_at = now(),
            updated_at = now()
      where id = $1
      returning brand_plan, brand_plan_until`,
    [userId, String(plan || 'basic'), Number(days || 30)]
  );
  return r.rows[0] || null;
}

// -----------------------------
// CRM stage (buyer-side) in barter threads
// -----------------------------

export async function setBarterThreadBuyerStage(threadId, buyerUserId, stage) {
  const r = await pool.query(
    `update barter_threads
        set buyer_stage = $3,
            updated_at = now()
      where id = $1 and buyer_user_id = $2
      returning buyer_stage`,
    [threadId, buyerUserId, stage || null]
  );
  return r.rowCount ? (r.rows[0]?.buyer_stage ?? null) : null;
}

// -----------------------------
// Smart Matching
// -----------------------------

export async function createMatchingRequest(userId, tier, starsPaid) {
  const r = await pool.query(
    `insert into matching_requests (user_id, tier, stars_paid, status)
     values ($1,$2,$3,'PAID')
     returning *`,
    [userId, String(tier || 'S'), Number(starsPaid || 0)]
  );
  return r.rows[0];
}

export async function setMatchingBrief(requestId, userId, brief) {
  await pool.query(
    `update matching_requests
        set brief = $3,
            updated_at = now()
      where id = $1 and user_id = $2`,
    [requestId, userId, brief || null]
  );
}

export async function completeMatchingRequest(requestId, userId, offerIds) {
  await pool.query(
    `update matching_requests
        set result_offer_ids = $3::jsonb,
            status = 'DONE',
            updated_at = now()
      where id = $1 and user_id = $2`,
    [requestId, userId, JSON.stringify(offerIds || [])]
  );
}

export async function getMatchingRequest(requestId, userId) {
  const r = await pool.query(
    `select * from matching_requests where id=$1 and user_id=$2`,
    [requestId, userId]
  );
  return r.rows[0] || null;
}

// naive keyword search over active network offers
export async function searchNetworkBarterOffersByBrief(brief, limit = 10) {
  const q = String(brief || '').trim().toLowerCase();
  if (!q) return [];

  const raw = q
    .replace(/[^\p{L}\p{N}\s@._-]+/gu, ' ')
    .split(/\s+/)
    .map((s) => s.trim())
    .filter((s) => s.length >= 3)
    .slice(0, 8);

  if (!raw.length) return [];

  const terms = [...new Set(raw)];
  const conds = [];
  const params = [];
  let idx = 1;
  for (const t of terms) {
    const pat = `%${t}%`;
    params.push(pat, pat);
    conds.push(`(lower(o.title) like $${idx} or lower(o.description) like $${idx + 1})`);
    idx += 2;
  }

  params.push(Number(limit));

  const sql = `
    select o.*, w.title as ws_title, w.channel_username
      from barter_offers o
      join workspaces w on w.id = o.workspace_id
      join workspace_settings s on s.workspace_id = w.id
     where s.network_enabled = true
       and upper(o.status) = 'ACTIVE'
       and (${conds.join(' or ')})
     order by coalesce(o.bump_at, o.created_at) desc
     limit $${idx}
  `;

  const r = await pool.query(sql, params);
  return r.rows;
}

// -----------------------------
// Featured placements
// -----------------------------

export async function createFeaturedPlacement(userId, durationDays, starsPaid) {
  const r = await pool.query(
    `insert into featured_placements (user_id, duration_days, stars_paid, status)
     values ($1,$2,$3,'WAIT_CONTENT')
     returning *`,
    [userId, Number(durationDays || 1), Number(starsPaid || 0)]
  );
  return r.rows[0];
}

export async function activateFeaturedPlacementWithContent(id, userId, title, body, contact) {
  const r = await pool.query(
    `update featured_placements
        set title = $3,
            body = $4,
            contact = $5,
            status = 'ACTIVE',
            starts_at = now(),
            ends_at = now() + (duration_days::int * interval '1 day'),
            updated_at = now()
      where id = $1 and user_id = $2
      returning *`,
    [id, userId, title || null, body || null, contact || null]
  );
  return r.rows[0] || null;
}

export async function stopFeaturedPlacement(id, userId) {
  const r = await pool.query(
    `update featured_placements
        set status = 'STOPPED',
            ends_at = now(),
            updated_at = now()
      where id = $1 and user_id = $2
      returning id`,
    [id, userId]
  );
  return r.rowCount > 0;
}

export async function getFeaturedPlacement(id) {
  const r = await pool.query(`select * from featured_placements where id=$1`, [id]);
  return r.rows[0] || null;
}

export async function listActiveFeatured(limit = 5) {
  const r = await pool.query(
    `select *
       from featured_placements
      where status='ACTIVE'
        and (starts_at is null or starts_at <= now())
        and (ends_at is null or ends_at > now())
      order by coalesce(ends_at, now()) desc, created_at desc
      limit $1`,
    [Number(limit || 5)]
  );
  return r.rows;
}

export async function listFeaturedForUser(userId, limit = 10) {
  const r = await pool.query(
    `select * from featured_placements where user_id=$1 order by created_at desc limit $2`,
    [userId, Number(limit || 10)]
  );
  return r.rows;
}


// -----------------------------
// -----------------------------
// Official channel posts
// -----------------------------

export async function getOfficialPostByOfferId(offerId) {
  const r = await pool.query(`select * from official_posts where offer_id=$1`, [Number(offerId)]);
  return r.rows[0] || null;
}

export async function upsertOfficialPostDraft(input = {}) {
  const offerId = Number(input.offerId);
  const channelChatId = Number(input.channelChatId);
  const placementType = String(input.placementType || 'MANUAL');
  const paymentId = input.paymentId ? Number(input.paymentId) : null;
  const slotDays = input.slotDays ? Number(input.slotDays) : null;
  const slotExpiresAt = input.slotExpiresAt || null;

  const r = await pool.query(
    `insert into official_posts (offer_id, channel_chat_id, status, placement_type, payment_id, slot_days, slot_expires_at, updated_at)
     values ($1,$2,'PENDING',$3,$4,$5,$6, now())
     on conflict (offer_id)
     do update set channel_chat_id=excluded.channel_chat_id,
                   status='PENDING',
                   placement_type=excluded.placement_type,
                   payment_id=coalesce(excluded.payment_id, official_posts.payment_id),
                   slot_days=coalesce(excluded.slot_days, official_posts.slot_days),
                   slot_expires_at=coalesce(excluded.slot_expires_at, official_posts.slot_expires_at),
                   updated_at=now()
     returning *`,
    [offerId, channelChatId, placementType, paymentId, slotDays, slotExpiresAt]
  );
  return r.rows[0] || null;
}

export async function setOfficialPostActive(offerId, input = {}) {
  const channelChatId = Number(input.channelChatId);
  const messageId = input.messageId ? Number(input.messageId) : null;
  const placementType = String(input.placementType || 'MANUAL');
  const paymentId = input.paymentId ? Number(input.paymentId) : null;
  const slotDays = input.slotDays ? Number(input.slotDays) : null;
  const slotExpiresAt = input.slotExpiresAt || null;
  const publishedByUserId = input.publishedByUserId ? Number(input.publishedByUserId) : null;

  const r = await pool.query(
    `insert into official_posts (offer_id, channel_chat_id, message_id, status, placement_type, payment_id, slot_days, slot_expires_at, published_by_user_id, updated_at)
     values ($1,$2,$3,'ACTIVE',$4,$5,$6,$7,$8, now())
     on conflict (offer_id)
     do update set channel_chat_id=excluded.channel_chat_id,
                   message_id=excluded.message_id,
                   status='ACTIVE',
                   placement_type=excluded.placement_type,
                   payment_id=coalesce(excluded.payment_id, official_posts.payment_id),
                   slot_days=coalesce(excluded.slot_days, official_posts.slot_days),
                   slot_expires_at=coalesce(excluded.slot_expires_at, official_posts.slot_expires_at),
                   published_by_user_id=coalesce(excluded.published_by_user_id, official_posts.published_by_user_id),
                   last_error=null,
                   updated_at=now()
     returning *`,
    [Number(offerId), channelChatId, messageId, placementType, paymentId, slotDays, slotExpiresAt, publishedByUserId]
  );
  return r.rows[0] || null;
}

export async function setOfficialPostStatus(offerId, status, input = {}) {
  const st = String(status || '').toUpperCase();
  const lastError = input.lastError ? String(input.lastError).slice(0, 2000) : null;
  const r = await pool.query(
    `update official_posts
        set status=$2,
            last_error=coalesce($3, last_error),
            updated_at=now()
      where offer_id=$1
      returning *`,
    [Number(offerId), st, lastError]
  );
  return r.rows[0] || null;
}

export async function listOfficialPending(limit = 20, offset = 0) {
  const r = await pool.query(
    `select op.*, o.title as offer_title, w.title as ws_title, w.channel_username
       from official_posts op
       join barter_offers o on o.id=op.offer_id
       join workspaces w on w.id=o.workspace_id
      where op.status='PENDING'
      order by op.updated_at desc
      limit $1 offset $2`,
    [Number(limit), Number(offset)]
  );
  return r.rows;
}

export async function countOfficialPending() {
  const r = await pool.query(`select count(*)::int as c from official_posts where status='PENDING'`);
  return (r.rows[0] && Number(r.rows[0].c)) || 0;
}

export async function listOfficialToExpire(limit = 50) {
  const r = await pool.query(
    `select *
       from official_posts
      where status='ACTIVE'
        and slot_expires_at is not null
        and slot_expires_at <= now()
      order by slot_expires_at asc
      limit $1`,
    [Number(limit)]
  );
  return r.rows;
}



// --------------------------------------------
// Backfilled exports (compat): v1.3.2+ runtime safety
// --------------------------------------------


function isMissingRelationError(e, relation) {
  return Boolean(e) && String(e.code || '') === '42P01' && String(e.message || '').includes(String(relation || ''));
}

export async function recordStarsPayment(input = {}) {
  const userId = Number(input.userId);
  const kind = String(input.kind || '');
  const invoicePayload = String(input.invoicePayload || '');
  const currency = String(input.currency || '');
  const totalAmount = Number(input.totalAmount || 0);
  const telegramPaymentChargeId = String(input.telegramPaymentChargeId || '');
  const providerPaymentChargeId = input.providerPaymentChargeId ? String(input.providerPaymentChargeId) : null;
  const raw = input.raw || null;

  if (!userId || !telegramPaymentChargeId) {
    return { inserted: true, ledger: 'skipped_missing_fields' };
  }

  try {
    const r = await pool.query(
      `insert into stars_payments
         (user_id, kind, invoice_payload, currency, total_amount, telegram_payment_charge_id, provider_payment_charge_id, raw)
       values ($1,$2,$3,$4,$5,$6,$7,$8)
       on conflict (telegram_payment_charge_id) do nothing
       returning id`,
      [userId, kind, invoicePayload, currency, totalAmount, telegramPaymentChargeId, providerPaymentChargeId, raw]
    );
    if (r.rowCount > 0) return { inserted: true, id: r.rows[0].id };
    return { inserted: false, reason: 'duplicate_charge_id' };
  } catch (e) {
    // Duplicate invoice payload (unique index)
    if (String(e.code || '') === '23505') {
      return { inserted: false, reason: 'duplicate' };
    }
    if (isMissingRelationError(e, 'stars_payments')) {
      return { inserted: true, ledger: 'missing_table' };
    }
    throw e;
  }
}



export async function insertPayment(input = {}) {
  const userId = Number(input.userId);
  const kind = String(input.kind || 'unknown');
  const invoicePayload = String(input.invoicePayload || '');
  const currency = String(input.currency || '');
  const totalAmount = Number(input.totalAmount || 0);
  const telegramPaymentChargeId = String(input.telegramPaymentChargeId || '');
  const providerPaymentChargeId = input.providerPaymentChargeId ? String(input.providerPaymentChargeId) : null;
  const raw = input.raw || null;
  const status = String(input.status || 'RECEIVED');

  if (!userId || !telegramPaymentChargeId || !invoicePayload) {
    return { inserted: true, ledger: 'skipped_missing_fields' };
  }

  try {
    const r = await pool.query(
      `insert into payments
         (user_id, kind, invoice_payload, currency, total_amount, telegram_payment_charge_id, provider_payment_charge_id, status, raw)
       values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       on conflict (telegram_payment_charge_id) do nothing
       returning id`,
      [userId, kind, invoicePayload, currency, totalAmount, telegramPaymentChargeId, providerPaymentChargeId, status, raw]
    );
    if (r.rowCount > 0) return { inserted: true, id: r.rows[0].id };
    return { inserted: false, reason: 'duplicate_charge_id' };
  } catch (e) {
    if (String(e.code || '') === '23505') {
      return { inserted: false, reason: 'duplicate' };
    }
    if (isMissingRelationError(e, 'payments')) {
      return { inserted: true, ledger: 'missing_table' };
    }
    throw e;
  }
}


export async function getPaymentById(paymentId) {
  const r = await pool.query(`select * from payments where id=$1`, [Number(paymentId)]);
  return r.rows[0] || null;
}

export async function setPaymentStatus(paymentId, status, note = null) {
  const st = String(status || 'ORPHANED').toUpperCase();
  const r = await pool.query(
    `update payments
        set status=$2,
            note=$3,
            updated_at=now()
      where id=$1
      returning *`,
    [Number(paymentId), st, note]
  );
  return r.rows[0] || null;
}

export async function markPaymentApplied(paymentId, appliedByUserId, note = null) {
  const r = await pool.query(
    `update payments
        set status='APPLIED',
            applied_by_user_id=$2,
            applied_at=now(),
            note=coalesce($3, note),
            updated_at=now()
      where id=$1
      returning *`,
    [Number(paymentId), Number(appliedByUserId), note]
  );
  return r.rows[0] || null;
}

// -----------------------------
// Workspace channel folders + editors (v1.1.2)
// -----------------------------

export async function listPaymentsByStatus(status, limit = 10, offset = 0) {
  const st = String(status || 'ORPHANED').toUpperCase();
  const r = await pool.query(
    `select p.*, u.tg_id, u.tg_username
     from payments p
     left join users u on u.id = p.user_id
     where p.status=$1
     order by p.created_at desc
     limit $2 offset $3`,
    [st, Number(limit), Number(offset)]
  );
  return r.rows;
}

export async function getUserVerification(userId) {
  const r = await pool.query(
    `select uv.*, u.tg_id, u.tg_username
     from user_verifications uv
     join users u on u.id = uv.user_id
     where uv.user_id=$1`,
    [userId]
  );
  return r.rows[0] || null;
}

export async function upsertVerificationRequest(userId, input = {}) {
  const kind = (input.kind || 'creator').toString();
  const submittedText = (input.submittedText || '').toString();
  const r = await pool.query(
    `insert into user_verifications (user_id, kind, status, submitted_text, submitted_at, updated_at)
     values ($1, $2, 'PENDING', $3, now(), now())
     on conflict (user_id)
     do update set kind=excluded.kind,
                   status='PENDING',
                   submitted_text=excluded.submitted_text,
                   submitted_at=now(),
                   reviewed_at=null,
                   reviewed_by_user_id=null,
                   rejection_reason=null,
                   updated_at=now()
     returning *`,
    [userId, kind, submittedText]
  );
  return r.rows[0] || null;
}

export async function setVerificationStatus(userId, status, reviewedByUserId, rejectionReason = null) {
  const st = String(status || '').toUpperCase();
  const rr = rejectionReason ? String(rejectionReason).slice(0, 2000) : null;
  const r = await pool.query(
    `update user_verifications
        set status=$2,
            reviewed_at=now(),
            reviewed_by_user_id=$3,
            rejection_reason=$4,
            updated_at=now()
      where user_id=$1
      returning *`,
    [userId, st, reviewedByUserId || null, rr]
  );
  return r.rows[0] || null;
}

// Verified joins for barter UI

export async function listPendingVerifications(limit = 20, offset = 0) {
  const r = await pool.query(
    `select uv.user_id, uv.kind, uv.status, uv.submitted_text, uv.submitted_at,
            u.tg_id, u.tg_username
     from user_verifications uv
     join users u on u.id=uv.user_id
     where uv.status='PENDING'
     order by uv.submitted_at desc
     limit $1 offset $2`,
    [Number(limit || 20), Number(offset || 0)]
  );
  return r.rows;
}

export async function countPendingVerifications() {
  const r = await pool.query(`select count(*)::int as cnt from user_verifications where status='PENDING'`);
  return Number(r.rows[0]?.cnt || 0);
}

export async function getWorkspaceById(workspaceId) {
  const r = await pool.query(`select * from workspaces where id=$1`, [Number(workspaceId)]);
  return r.rows[0] || null;
}

export async function hasAnyWorkspaceEditorRole(userId) {
  const r = await pool.query(`select 1 from workspace_editors where user_id=$1 limit 1`, [Number(userId)]);
  return r.rows.length > 0;
}

export async function isWorkspaceEditor(workspaceId, userId) {
  const r = await pool.query(
    `select 1 from workspace_editors where workspace_id=$1 and user_id=$2 limit 1`,
    [Number(workspaceId), Number(userId)]
  );
  return r.rows.length > 0;
}

export async function addWorkspaceEditor(workspaceId, userId, addedByUserId) {
  const r = await pool.query(
    `insert into workspace_editors (workspace_id, user_id, added_by_user_id)
     values ($1,$2,$3)
     on conflict (workspace_id, user_id) do update set added_by_user_id=excluded.added_by_user_id
     returning *`,
    [Number(workspaceId), Number(userId), Number(addedByUserId)]
  );
  return r.rows[0] || null;
}

export async function removeWorkspaceEditor(workspaceId, userId) {
  const r = await pool.query(
    `delete from workspace_editors where workspace_id=$1 and user_id=$2 returning *`,
    [Number(workspaceId), Number(userId)]
  );
  return r.rows[0] || null;
}

export async function listWorkspaceEditors(workspaceId) {
  const r = await pool.query(
    `select e.user_id, u.tg_id, u.tg_username, u.created_at as user_created_at, e.created_at as added_at
       from workspace_editors e
       join users u on u.id = e.user_id
      where e.workspace_id=$1
      order by e.created_at desc`,
    [Number(workspaceId)]
  );
  return r.rows;
}

export async function listWorkspaceEditorWorkspaces(userId) {
  const r = await pool.query(
    `select w.id, w.title, w.channel_username, w.channel_id
       from workspace_editors e
       join workspaces w on w.id = e.workspace_id
      where e.user_id=$1
      order by lower(w.title) asc`,
    [Number(userId)]
  );
  return r.rows;
}

export async function createChannelFolder(workspaceId, createdByUserId, title) {
  const r = await pool.query(
    `insert into channel_folders (workspace_id, created_by_user_id, title)
     values ($1,$2,$3)
     returning *`,
    [Number(workspaceId), Number(createdByUserId), String(title)]
  );
  return r.rows[0] || null;
}

export async function renameChannelFolder(folderId, title) {
  const r = await pool.query(
    `update channel_folders set title=$2 where id=$1 returning *`,
    [Number(folderId), String(title)]
  );
  return r.rows[0] || null;
}

export async function deleteChannelFolder(folderId) {
  const r = await pool.query(`delete from channel_folders where id=$1 returning *`, [Number(folderId)]);
  return r.rows[0] || null;
}

export async function getChannelFolder(folderId) {
  const r = await pool.query(
    `select f.*, (
        select count(*)::int from channel_folder_items i where i.folder_id=f.id
      ) as items_count
     from channel_folders f
     where f.id=$1`,
    [Number(folderId)]
  );
  return r.rows[0] || null;
}

export async function listChannelFolders(workspaceId) {
  const r = await pool.query(
    `select f.*, (
        select count(*)::int from channel_folder_items i where i.folder_id=f.id
      ) as items_count
     from channel_folders f
     where f.workspace_id=$1
     order by lower(f.title) asc`,
    [Number(workspaceId)]
  );
  return r.rows;
}

export async function listChannelFolderItems(folderId) {
  const r = await pool.query(
    `select id, channel_username, created_at
       from channel_folder_items
      where folder_id=$1
      order by lower(channel_username) asc`,
    [Number(folderId)]
  );
  return r.rows;
}

export async function addChannelFolderItems(folderId, usernames = []) {
  const norm = (x) => {
    const t = String(x || '').trim();
    if (!t) return null;

    let u = t;
    const m1 = u.match(/^https?:\/\/t\.me\/([a-zA-Z0-9_]{5,})/i);
    if (m1) u = '@' + m1[1];

    const m2 = u.match(/^@?([a-zA-Z0-9_]{5,})$/);
    if (m2) u = '@' + m2[1];

    return String(u).toLowerCase();
  };

  const cleaned = (usernames || [])
    .map(norm)
    .filter(Boolean);

  // unique preserve order
  const unique = [];
  const seen = new Set();
  for (const u of cleaned) {
    if (seen.has(u)) continue;
    seen.add(u);
    unique.push(u);
  }

  if (!unique.length) return { added: 0 };

  const values = unique.map((_, i) => `($1, $${i + 2})`).join(',');
  const params = [Number(folderId), ...unique];

  const r = await pool.query(
    `insert into channel_folder_items (folder_id, channel_username)
     values ${values}
     on conflict do nothing`,
    params
  );
  return { added: r.rowCount };
}

export async function removeChannelFolderItems(folderId, usernames = []) {
  const norm = (x) => {
    const t = String(x || '').trim();
    if (!t) return null;

    let u = t;
    const m1 = u.match(/^https?:\/\/t\.me\/([a-zA-Z0-9_]{5,})/i);
    if (m1) u = '@' + m1[1];

    const m2 = u.match(/^@?([a-zA-Z0-9_]{5,})$/);
    if (m2) u = '@' + m2[1];

    return String(u).toLowerCase();
  };

  const cleaned = (usernames || [])
    .map(norm)
    .filter(Boolean);
  if (!cleaned.length) return { removed: 0 };

  // unique
  const unique = [];
  const seen = new Set();
  for (const u of cleaned) {
    if (seen.has(u)) continue;
    seen.add(u);
    unique.push(u);
  }

  const placeholders = unique.map((_, i) => `$${i + 2}`).join(',');
  const params = [Number(folderId), ...unique];

  const r = await pool.query(
    `delete from channel_folder_items
      where folder_id=$1
        and lower(channel_username) in (${placeholders})`,
    params
  );
  return { removed: r.rowCount };
}

export async function clearChannelFolder(folderId) {
  const r = await pool.query(`delete from channel_folder_items where folder_id=$1`, [Number(folderId)]);
  return { removed: r.rowCount };
}

export async function getBarterOfferPublicWithVerified(offerId) {
  const r = await pool.query(
    `select o.*, w.title as ws_title, w.channel_username, w.channel_id,
            s.network_enabled,
            (case when uv.status='APPROVED' then true else false end) as creator_verified
     from barter_offers o
     join workspaces w on w.id=o.workspace_id
     join workspace_settings s on s.workspace_id=w.id
     left join user_verifications uv on uv.user_id=o.creator_user_id and uv.status='APPROVED'
     where o.id=$1`,
    [offerId]
  );
  return r.rows[0] || null;
}

export async function listNetworkBarterOffersWithVerified(opts = {}) {
  const { category = null, offerType = null, compensationType = null, limit = 5, offset = 0 } = opts;
  const r = await pool.query(
    `select o.*, w.title as ws_title, w.channel_username, w.channel_id,
            (case when uv.status='APPROVED' then true else false end) as creator_verified
     from barter_offers o
     join workspaces w on w.id = o.workspace_id
     join workspace_settings s on s.workspace_id = w.id
     left join user_verifications uv on uv.user_id=o.creator_user_id and uv.status='APPROVED'
     where o.status='ACTIVE'
       and s.network_enabled=true
       and ($1::text is null or o.category=$1)
       and ($2::text is null or o.offer_type=$2)
       and ($3::text is null or o.compensation_type=$3)
     order by case when s.plan='pro' and (s.pro_until is null or s.pro_until>now()) and s.pro_pinned_offer_id = o.id then 0 else 1 end,
              o.bump_at desc
     limit $4 offset $5`,
    [category, offerType, compensationType, limit, offset]
  );
  return r.rows;
}

export async function listBarterThreadsForUserWithVerified(userId, limit = 20, offset = 0) {
  const r = await pool.query(
    `select t.*, o.title as offer_title, o.category, o.offer_type, o.compensation_type,
            w.channel_username, w.title as ws_title,
            case when t.buyer_user_id=$1 then t.seller_user_id else t.buyer_user_id end as other_user_id,
            uo.tg_username as other_username,
            (case when uvo.status='APPROVED' then true else false end) as other_verified,
            lm.body as last_body, lm.created_at as last_created_at
     from barter_threads t
     join barter_offers o on o.id=t.offer_id
     join workspaces w on w.id=t.workspace_id
     left join users uo on uo.id = (case when t.buyer_user_id=$1 then t.seller_user_id else t.buyer_user_id end)
     left join user_verifications uvo on uvo.user_id = (case when t.buyer_user_id=$1 then t.seller_user_id else t.buyer_user_id end) and uvo.status='APPROVED'
     left join lateral (
       select m.body, m.created_at
       from barter_messages m
       where m.thread_id=t.id
       order by m.created_at desc
       limit 1
     ) lm on true
     where (t.buyer_user_id=$1 or t.seller_user_id=$1)
     order by coalesce(t.last_message_at, t.created_at) desc
     limit $2 offset $3`,
    [userId, limit, offset]
  );
  return r.rows;
}


export async function listBarterThreadsForUser(userId, limit = 20, offset = 0) {
  const r = await pool.query(
    `select t.*, o.title as offer_title, o.category, o.offer_type, o.compensation_type,
            w.channel_username, w.title as ws_title,
            case when t.buyer_user_id=$1 then t.seller_user_id else t.buyer_user_id end as other_user_id,
            uo.tg_username as other_username,
            false as other_verified,
            lm.body as last_body, lm.created_at as last_created_at
     from barter_threads t
     join barter_offers o on o.id=t.offer_id
     join workspaces w on w.id=t.workspace_id
     left join users uo on uo.id = (case when t.buyer_user_id=$1 then t.seller_user_id else t.buyer_user_id end)
     left join lateral (
       select m.body, m.created_at
       from barter_messages m
       where m.thread_id=t.id
       order by m.created_at desc
       limit 1
     ) lm on true
     where (t.buyer_user_id=$1 or t.seller_user_id=$1)
     order by coalesce(t.last_message_at, t.created_at) desc
     limit $2 offset $3`,
    [Number(userId), Number(limit || 20), Number(offset || 0)]
  );
  return r.rows || [];
}

export async function getBarterThreadForUserWithVerified(threadId, userId) {
  const r = await pool.query(
    `select t.*, o.title as offer_title, o.category, o.offer_type, o.compensation_type,
            w.channel_username, w.title as ws_title,
            ub.tg_username as buyer_username, us.tg_username as seller_username,
            (case when uvb.status='APPROVED' then true else false end) as buyer_verified,
            (case when uvs.status='APPROVED' then true else false end) as seller_verified
     from barter_threads t
     join barter_offers o on o.id=t.offer_id
     join workspaces w on w.id=t.workspace_id
     left join users ub on ub.id=t.buyer_user_id
     left join users us on us.id=t.seller_user_id
     left join user_verifications uvb on uvb.user_id=t.buyer_user_id and uvb.status='APPROVED'
     left join user_verifications uvs on uvs.user_id=t.seller_user_id and uvs.status='APPROVED'
     where t.id=$1 and (t.buyer_user_id=$2 or t.seller_user_id=$2)`,
    [threadId, userId]
  );
  return r.rows[0] || null;
}




export async function getBarterThreadForUser(threadId, userId) {
  const r = await pool.query(
    `select t.*, o.title as offer_title, o.category, o.offer_type, o.compensation_type,
            w.channel_username, w.title as ws_title,
            ub.tg_username as buyer_username, us.tg_username as seller_username
     from barter_threads t
     join barter_offers o on o.id=t.offer_id
     join workspaces w on w.id=t.workspace_id
     left join users ub on ub.id=t.buyer_user_id
     left join users us on us.id=t.seller_user_id
     where t.id=$1 and (t.buyer_user_id=$2 or t.seller_user_id=$2)`,
    [Number(threadId), Number(userId)]
  );
  return r.rows[0] || null;
}


export async function listBarterMessages(threadId, limit = 20) {
  const r = await pool.query(
    `select id, thread_id, sender_user_id, body, created_at
     from barter_messages
     where thread_id=$1
     order by created_at desc
     limit $2`,
    [Number(threadId), Number(limit || 20)]
  );
  return r.rows || [];
}


export async function listBarterOffersForWorkspace(ownerUserId, workspaceId, limit = 10, offset = 0) {
  // Backwards-compat alias: owner-gated list for workspace
  return await listBarterOffersForOwnerWorkspace(ownerUserId, workspaceId, limit, offset);
}


export async function listMyBarterOffers(workspaceId, limit = 100, offset = 0) {
  const r = await pool.query(
    `select *
     from barter_offers
     where workspace_id=$1
     order by created_at desc
     limit $2 offset $3`,
    [Number(workspaceId), Number(limit || 100), Number(offset || 0)]
  );
  return r.rows || [];
}


export async function getUserById(userId) {
  const r = await pool.query(`select * from users where id=$1`, [Number(userId)]);
  return r.rows[0] || null;
}


export async function auditBarterThread(threadId, actorUserId, action, payload = {}) {
  // We don't have a dedicated thread_audit table; log into workspace_audit for traceability.
  const r = await pool.query(`select workspace_id from barter_threads where id=$1`, [Number(threadId)]);
  const wsId = r.rows[0]?.workspace_id;
  if (!wsId) return;
  await pool.query(
    `insert into workspace_audit (workspace_id, actor_user_id, action, payload)
     values ($1,$2,$3,$4::jsonb)`,
    [Number(wsId), actorUserId || null, String(action || 'thread.audit'), JSON.stringify({ threadId: Number(threadId), ...(payload || {}) })]
  );
}


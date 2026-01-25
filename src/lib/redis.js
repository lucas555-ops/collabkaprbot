import { Redis } from '@upstash/redis';
import { CFG } from './config.js';

export const redis = new Redis({
  url: CFG.UPSTASH_REDIS_REST_URL,
  token: CFG.UPSTASH_REDIS_REST_TOKEN
});

export function k(parts) {
  // namespaced keys
  return [
    'mg',
    CFG.APP_ENV,
    ...parts.map((p) => String(p))
  ].join(':');
}

// Simple rate limiter (good enough for Upstash REST Redis in our use-cases):
// - INCR key
// - if first hit => EXPIRE key
// Returns: { allowed, remaining, limit, current, resetSec }
// NOTE: this is infra-only in Commit 4; enforcement is wired in Commit 5.
export async function rateLimit(key, { limit = 0, windowSec = 60 } = {}) {
  const lim = Number(limit);
  const win = Number(windowSec);
  if (!Number.isFinite(lim) || lim <= 0) {
    return {
      allowed: true,
      remaining: Number.POSITIVE_INFINITY,
      limit: lim,
      current: 0,
      resetSec: win
    };
  }

  const current = await redis.incr(key);
  if (current === 1) {
    // set window TTL on first hit
    await redis.expire(key, win);
  }

  let ttl = null;
  try {
    ttl = await redis.ttl(key);
    if (ttl !== null && ttl < 0) ttl = null;
  } catch {
    ttl = null;
  }

  const remaining = Math.max(0, lim - current);
  return {
    allowed: current <= lim,
    remaining,
    limit: lim,
    current,
    resetSec: ttl ?? win
  };
}

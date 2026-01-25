import { redis, k } from '../lib/redis.js';

export async function setExpectText(tgId, payload, ttlSec = 15 * 60) {
  try {
    await redis.set(k(['expectText', tgId]), payload, { ex: ttlSec });
  } catch (e) {
    console.error('[REDIS] setExpectText failed', String(e?.message || e));
  }
}

export async function getExpectText(tgId) {
  try {
    return await redis.get(k(['expectText', tgId]));
  } catch (e) {
    console.error('[REDIS] getExpectText failed', String(e?.message || e));
    return null;
  }
}

export async function clearExpectText(tgId) {
  try {
    await redis.del(k(['expectText', tgId]));
  } catch (e) {
    console.error('[REDIS] clearExpectText failed', String(e?.message || e));
  }
}

export async function setDraft(tgId, draft, ttlSec = 60 * 60) {
  try {
    await redis.set(k(['draft', tgId]), draft, { ex: ttlSec });
  } catch (e) {
    console.error('[REDIS] setDraft failed', String(e?.message || e));
  }
}

export async function getDraft(tgId) {
  try {
    return await redis.get(k(['draft', tgId]));
  } catch (e) {
    console.error('[REDIS] getDraft failed', String(e?.message || e));
    return null;
  }
}

export async function clearDraft(tgId) {
  try {
    await redis.del(k(['draft', tgId]));
  } catch (e) {
    console.error('[REDIS] clearDraft failed', String(e?.message || e));
  }
}

import { redis, k } from '../lib/redis.js';

export async function setExpectText(tgId, payload, ttlSec = 15 * 60) {
  await redis.set(k(['expectText', tgId]), payload, { ex: ttlSec });
}

export async function getExpectText(tgId) {
  return await redis.get(k(['expectText', tgId]));
}

export async function clearExpectText(tgId) {
  await redis.del(k(['expectText', tgId]));
}

export async function setDraft(tgId, draft, ttlSec = 60 * 60) {
  await redis.set(k(['draft', tgId]), draft, { ex: ttlSec });
}

export async function getDraft(tgId) {
  return await redis.get(k(['draft', tgId]));
}

export async function clearDraft(tgId) {
  await redis.del(k(['draft', tgId]));
}

import { CFG } from '../src/lib/config.js';

// Simple health endpoint (no secrets).
export default async function handler(_req, res) {
  res.status(200).json({ ok: true, ts: new Date().toISOString(), env: CFG.APP_ENV });
}

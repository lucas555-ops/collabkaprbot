import pg from 'pg';
import { CFG } from '../lib/config.js';

const { Pool } = pg;

export const pool = new Pool({
  connectionString: CFG.DATABASE_URL,
  ssl: CFG.DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false }
});

export async function pingDb() {
  const r = await pool.query('select 1 as ok');
  return r.rows?.[0]?.ok === 1;
}

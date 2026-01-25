import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { pool } from '../src/db/pool.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function run() {
  const files = fs.readdirSync(__dirname)
    .filter(f => f.match(/^\d+_.*\.sql$/))
    .sort();

  for (const f of files) {
    const sql = fs.readFileSync(path.join(__dirname, f), 'utf8');
    console.log('Running', f);
    await pool.query(sql);
  }

  console.log('Migrations complete.');
  await pool.end();
}

run().catch(async (e) => {
  console.error(e);
  try { await pool.end(); } catch {}
  process.exit(1);
});

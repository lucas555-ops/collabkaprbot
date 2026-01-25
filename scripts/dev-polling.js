import { getBot } from '../src/bot/bot.js';
import { assertEnv } from '../src/lib/config.js';

// Local dev runner (long-polling). Not used on Vercel.

async function main() {
  assertEnv();
  const bot = getBot();
  console.log('Starting polling...');
  await bot.start({ drop_pending_updates: true });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

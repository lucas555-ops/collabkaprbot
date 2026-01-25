#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="/mnt/data/CollabGirlsBot-PROD-FINAL-v1.2.9.zip"
cd "$ROOT"
rm -f "$OUT"
zip -r "$OUT" . -x "node_modules/*" ".vercel/*" "*.zip" ".env" ".env.local" ".env.prod" ".env.production" ".env.dev" ".env.development" >/dev/null
printf "Created %s\n" "$OUT"

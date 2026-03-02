#!/usr/bin/env bash
set -euo pipefail

# Two-phase strategy:
# 1) Core variables for product-critical history (faster and safer)
# 2) Full variables for deep historical archive (long-running lane)

BASE_URL="${BASE_URL:-https://orion-api-126886725893.europe-west1.run.app}"
API_KEY="${API_KEY:-}"

if [[ -z "$API_KEY" ]]; then
  echo "API_KEY is required. Example: API_KEY=orion-dev-key-2024 $0"
  exit 1
fi

cd "$(dirname "$0")/.."

echo "[PHASE P0] core variables, 2010-01..2026-12"
python scripts/era5_full_backfill.py \
  --base-url "$BASE_URL" \
  --api-key "$API_KEY" \
  --profile core \
  --start-month 2010-01 \
  --end-month 2026-12 \
  --group-size 5 \
  --max-inflight 2 \
  --poll-seconds 15 \
  --max-retries 5 \
  --state-file era5_core_state.json

echo "[PHASE P1] full variables, 1950-01..2009-12"
python scripts/era5_full_backfill.py \
  --base-url "$BASE_URL" \
  --api-key "$API_KEY" \
  --profile full \
  --start-month 1950-01 \
  --end-month 2009-12 \
  --group-size 10 \
  --max-inflight 1 \
  --poll-seconds 20 \
  --max-retries 5 \
  --state-file era5_full_archive_state.json

echo "Done."


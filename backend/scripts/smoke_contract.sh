#!/usr/bin/env bash
set -euo pipefail

: "${BASE_URL:?Set BASE_URL, e.g. https://orion-api-xxx.run.app}"
: "${API_KEY:?Set API_KEY}"

echo "[1/6] GET /portfolios"
curl -s "${BASE_URL}/portfolios" \
  -H "Authorization: Bearer ${API_KEY}" | jq '.[0]'

echo "[2/6] GET /portfolios/{id}/risk-summary"
curl -s "${BASE_URL}/portfolios/demo-10-assets/risk-summary?start=2024-01-01&end=2024-01-31" \
  -H "Authorization: Bearer ${API_KEY}" | jq '{portfolio_id,period,bands,trend_first:(.trend[0] // null)}'

echo "[3/6] POST /scores/batch"
curl -s -X POST "${BASE_URL}/scores/batch" \
  -H "Authorization: Bearer ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "assets":[{"asset_id":"a1","lat":41.01,"lon":28.97}],
    "start_date":"2024-01-01",
    "end_date":"2024-01-03",
    "climatology_version":"v1_test_2024_jan",
    "include_perils":["heat","rain","wind","drought"]
  }' | jq '{run_id,climatology_version,sample:(.results[0].series[0] // null)}'

echo "[4/6] GET /notifications"
curl -s "${BASE_URL}/notifications?portfolio_id=demo-10-assets" \
  -H "Authorization: Bearer ${API_KEY}" | jq '.[0] // {}'

echo "[5/6] POST /notifications/{id}/ack (optional)"
NOTIF_ID="$(curl -s "${BASE_URL}/notifications?portfolio_id=demo-10-assets" -H "Authorization: Bearer ${API_KEY}" | jq -r '.[0].id // empty')"
if [[ -n "${NOTIF_ID}" ]]; then
  curl -s -X POST "${BASE_URL}/notifications/${NOTIF_ID}/ack" \
    -H "Authorization: Bearer ${API_KEY}" | jq
else
  echo "No notification to ack; skipping"
fi

echo "[6/6] POST /export/portfolio"
curl -s -X POST "${BASE_URL}/export/portfolio" \
  -H "Authorization: Bearer ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "portfolio_id":"demo-10-assets",
    "start_date":"2024-01-01",
    "end_date":"2024-01-31",
    "format":"csv",
    "include_drivers":true
  }' | jq '{export_id,status,path,download_url}'

echo "Contract smoke checks completed."


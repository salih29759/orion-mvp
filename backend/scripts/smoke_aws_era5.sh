#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://localhost:8000}"
API_KEY="${API_KEY:-${2:-orion-dev-key-2024}}"

auth=(-H "Authorization: Bearer ${API_KEY}")
json=(-H "Content-Type: application/json")

echo "[1/5] catalog sync"
SYNC=$(curl -sS -X POST "${BASE_URL}/jobs/aws-era5/catalog/sync" "${auth[@]}" "${json[@]}" -d '{"max_keys_per_prefix":200}')
echo "$SYNC" | jq '{job_id,status,type,progress}'

SYNC_JOB_ID=$(echo "$SYNC" | jq -r '.job_id')

echo "[2/6] sample key listing (catalog parser)"
python - <<'PY'
from pipeline.aws_era5_catalog import list_sample_keys
keys = list_sample_keys("e5.oper.an.sfc/", limit=3)
print({"sample_keys": keys})
PY

echo "[3/6] catalog latest"
LATEST=$(curl -sS "${BASE_URL}/jobs/aws-era5/catalog/latest" "${auth[@]}")
echo "$LATEST" | jq '{latest_common_month,latest_by_variable}'

echo "[4/6] start small aws backfill (province points)"
BACKFILL=$(curl -sS -X POST "${BASE_URL}/jobs/aws-era5/backfill" "${auth[@]}" "${json[@]}" -d '{
  "start":"2024-01-01",
  "end":"2024-01-31",
  "mode":"points",
  "points_set":"provinces",
  "bbox":{"north":42,"west":26,"south":36,"east":45},
  "variables":["2m_temperature","total_precipitation","10m_u_component_of_wind","10m_v_component_of_wind","volumetric_soil_water_layer_1"],
  "concurrency":1,
  "force":false
}')
echo "$BACKFILL" | jq '{job_id,status,type,progress}'
JOB_ID=$(echo "$BACKFILL" | jq -r '.job_id')

echo "[5/6] poll parent job once"
curl -sS "${BASE_URL}/jobs/${JOB_ID}" "${auth[@]}" | jq '{job_id,status,type,progress}'

echo "[6/6] sample scored output check"
curl -sS -X POST "${BASE_URL}/scores/batch" "${auth[@]}" "${json[@]}" -d '{
  "assets":[{"asset_id":"a1","lat":41.01,"lon":28.97}],
  "start_date":"2024-01-01",
  "end_date":"2024-01-02",
  "climatology_version":"v1_baseline_2015_2024",
  "include_perils":["heat","rain","wind","drought"]
}' | jq '{run_id,climatology_version,result_count:(.results|length)}'

echo "done"

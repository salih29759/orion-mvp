#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-https://orion-api-126886725893.europe-west1.run.app}"
API_KEY="${2:-orion-dev-key-2024}"
YEAR="${3:-2024}"
MONTH="${4:-01}"

if [[ ${#MONTH} -eq 1 ]]; then
  MONTH="0${MONTH}"
fi

submit_job() {
  local start_date="$1"
  local end_date="$2"
  curl -s -X POST "${BASE_URL}/jobs/era5/ingest" \
    -H "Authorization: Bearer ${API_KEY}" \
    -H "Content-Type: application/json" \
    -d "{
      \"dataset\":\"era5-land\",
      \"variables\":[
        \"2m_temperature\",
        \"total_precipitation\",
        \"10m_u_component_of_wind\",
        \"10m_v_component_of_wind\",
        \"volumetric_soil_water_layer_1\"
      ],
      \"start_date\":\"${start_date}\",
      \"end_date\":\"${end_date}\",
      \"bbox\":{\"north\":42,\"west\":26,\"south\":36,\"east\":45},
      \"format\":\"netcdf\"
    }"
}

if [[ "${MONTH}" == "01" ]]; then
  ranges=(
    "${YEAR}-01-01 ${YEAR}-01-07"
    "${YEAR}-01-08 ${YEAR}-01-14"
    "${YEAR}-01-15 ${YEAR}-01-21"
    "${YEAR}-01-22 ${YEAR}-01-28"
    "${YEAR}-01-29 ${YEAR}-01-31"
  )
else
  echo "Only month=01 is currently pre-configured. Edit ranges in this script for other months."
  exit 1
fi

for r in "${ranges[@]}"; do
  s="$(echo "${r}" | awk '{print $1}')"
  e="$(echo "${r}" | awk '{print $2}')"
  echo "Submitting ${s} -> ${e}"
  submit_job "${s}" "${e}"
  echo
  sleep 1
done

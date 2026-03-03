#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOB_TEMPLATE="${SCRIPT_DIR}/job.json"
TMP_CONFIG="$(mktemp "${TMPDIR:-/tmp}/orion-era5-batch-job.XXXXXX.json")"
trap 'rm -f "${TMP_CONFIG}"' EXIT

require_env() {
  local key="$1"
  if [[ -z "${!key:-}" ]]; then
    echo "Missing required env var: ${key}" >&2
    exit 1
  fi
}

escape_sed() {
  printf '%s' "$1" | sed -e 's/[\/&]/\\&/g'
}

FORCE="${FORCE:-false}"
if [[ "${FORCE}" != "true" && "${FORCE}" != "false" ]]; then
  echo "FORCE must be true or false (current: ${FORCE})" >&2
  exit 1
fi
FORCE_FLAG=""
if [[ "${FORCE}" == "true" ]]; then
  FORCE_FLAG="--force"
fi

JOB_NAME="${JOB_NAME:-era5-backfill-$(date +%Y%m%d-%H%M%S)}"
BACKFILL_START="${BACKFILL_START:-1950-01-01}"
BACKFILL_END="${BACKFILL_END:-2026-12-31}"
WORKERS="${WORKERS:-14}"
POINTS_SET="${POINTS_SET:-assets+provinces}"
PROCESSING_MODE="${PROCESSING_MODE:-streaming}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-8}"
CPU_MILLI="${CPU_MILLI:-8000}"
MEMORY_MIB="${MEMORY_MIB:-32768}"
MAX_RUN_DURATION="${MAX_RUN_DURATION:-172800s}"
MAX_RETRY_COUNT="${MAX_RETRY_COUNT:-1}"

require_env PROJECT_ID
require_env REGION
require_env DATABASE_URL
require_env ERA5_GCS_BUCKET
require_env ORION_BACKEND_API_KEY
require_env BACKFILL_IMAGE

SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_EMAIL:-batch-backfill@${PROJECT_ID}.iam.gserviceaccount.com}"

sed \
  -e "s|__BACKFILL_IMAGE__|$(escape_sed "${BACKFILL_IMAGE}")|g" \
  -e "s|__DATABASE_URL__|$(escape_sed "${DATABASE_URL}")|g" \
  -e "s|__ERA5_GCS_BUCKET__|$(escape_sed "${ERA5_GCS_BUCKET}")|g" \
  -e "s|__ORION_BACKEND_API_KEY__|$(escape_sed "${ORION_BACKEND_API_KEY}")|g" \
  -e "s|__BACKFILL_START__|$(escape_sed "${BACKFILL_START}")|g" \
  -e "s|__BACKFILL_END__|$(escape_sed "${BACKFILL_END}")|g" \
  -e "s|__WORKERS__|$(escape_sed "${WORKERS}")|g" \
  -e "s|__POINTS_SET__|$(escape_sed "${POINTS_SET}")|g" \
  -e "s|__PROCESSING_MODE__|$(escape_sed "${PROCESSING_MODE}")|g" \
  -e "s|__FORCE_FLAG__|$(escape_sed "${FORCE_FLAG}")|g" \
  -e "s|__MACHINE_TYPE__|$(escape_sed "${MACHINE_TYPE}")|g" \
  -e "s|__CPU_MILLI__|$(escape_sed "${CPU_MILLI}")|g" \
  -e "s|__MEMORY_MIB__|$(escape_sed "${MEMORY_MIB}")|g" \
  -e "s|__MAX_RUN_DURATION__|$(escape_sed "${MAX_RUN_DURATION}")|g" \
  -e "s|__MAX_RETRY_COUNT__|$(escape_sed "${MAX_RETRY_COUNT}")|g" \
  -e "s|__SERVICE_ACCOUNT_EMAIL__|$(escape_sed "${SERVICE_ACCOUNT_EMAIL}")|g" \
  "${JOB_TEMPLATE}" > "${TMP_CONFIG}"

if [[ "${DRY_RUN:-false}" == "true" ]]; then
  cat "${TMP_CONFIG}"
  exit 0
fi

gcloud batch jobs submit "${JOB_NAME}" \
  --project "${PROJECT_ID}" \
  --location "${REGION}" \
  --config "${TMP_CONFIG}"

echo "Submitted Cloud Batch job: ${JOB_NAME}"
echo "Inspect: gcloud batch jobs describe ${JOB_NAME} --project ${PROJECT_ID} --location ${REGION}"

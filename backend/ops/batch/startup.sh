#!/usr/bin/env bash
set -euo pipefail

METADATA_HEADER="Metadata-Flavor: Google"
INSTANCE_METADATA_URL="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
PROJECT_METADATA_URL="http://metadata.google.internal/computeMetadata/v1/project/project-id"

read_metadata() {
  local key="$1"
  curl -fsS -H "${METADATA_HEADER}" "${INSTANCE_METADATA_URL}/${key}" 2>/dev/null || true
}

PROJECT_ID="$(curl -fsS -H "${METADATA_HEADER}" "${PROJECT_METADATA_URL}")"

DATABASE_URL="${DATABASE_URL:-$(read_metadata DATABASE_URL)}"
ERA5_GCS_BUCKET="${ERA5_GCS_BUCKET:-$(read_metadata ERA5_GCS_BUCKET)}"
ORION_BACKEND_API_KEY="${ORION_BACKEND_API_KEY:-$(read_metadata ORION_BACKEND_API_KEY)}"
BACKFILL_IMAGE="${BACKFILL_IMAGE:-$(read_metadata BACKFILL_IMAGE)}"
BACKFILL_START="${BACKFILL_START:-$(read_metadata BACKFILL_START)}"
BACKFILL_END="${BACKFILL_END:-$(read_metadata BACKFILL_END)}"
WORKERS="${WORKERS:-$(read_metadata WORKERS)}"
POINTS_SET="${POINTS_SET:-$(read_metadata POINTS_SET)}"
PROCESSING_MODE="${PROCESSING_MODE:-$(read_metadata PROCESSING_MODE)}"
FORCE="${FORCE:-$(read_metadata FORCE)}"
AUTO_SHUTDOWN="${AUTO_SHUTDOWN:-$(read_metadata AUTO_SHUTDOWN)}"

BACKFILL_IMAGE="${BACKFILL_IMAGE:-us-central1-docker.pkg.dev/${PROJECT_ID}/orion/backend:latest}"
BACKFILL_START="${BACKFILL_START:-1950-01-01}"
BACKFILL_END="${BACKFILL_END:-2026-12-31}"
WORKERS="${WORKERS:-14}"
POINTS_SET="${POINTS_SET:-assets+provinces}"
PROCESSING_MODE="${PROCESSING_MODE:-streaming}"
FORCE="${FORCE:-false}"
AUTO_SHUTDOWN="${AUTO_SHUTDOWN:-true}"

for required in DATABASE_URL ERA5_GCS_BUCKET ORION_BACKEND_API_KEY; do
  if [[ -z "${!required:-}" ]]; then
    echo "Missing required env var or metadata attribute: ${required}" >&2
    exit 1
  fi
done

if [[ "${FORCE}" != "true" && "${FORCE}" != "false" ]]; then
  echo "FORCE must be true or false (current: ${FORCE})" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  apt-get update
  apt-get install -y docker.io
fi
systemctl enable --now docker

FORCE_FLAG=""
if [[ "${FORCE}" == "true" ]]; then
  FORCE_FLAG="--force"
fi

cmd=(
  python -m pipeline.aws_era5_parallel
  --start "${BACKFILL_START}"
  --end "${BACKFILL_END}"
  --workers "${WORKERS}"
  --points-set "${POINTS_SET}"
  --mode "${PROCESSING_MODE}"
)
if [[ -n "${FORCE_FLAG}" ]]; then
  cmd+=("${FORCE_FLAG}")
fi

quoted_cmd=""
for arg in "${cmd[@]}"; do
  quoted_cmd+=" $(printf '%q' "${arg}")"
done

docker pull "${BACKFILL_IMAGE}"
docker run --rm \
  -e DATABASE_URL="${DATABASE_URL}" \
  -e ERA5_GCS_BUCKET="${ERA5_GCS_BUCKET}" \
  -e ORION_BACKEND_API_KEY="${ORION_BACKEND_API_KEY}" \
  "${BACKFILL_IMAGE}" \
  /bin/bash -lc "cd /app &&${quoted_cmd}"

if [[ "${AUTO_SHUTDOWN}" == "true" ]]; then
  shutdown -h now
fi

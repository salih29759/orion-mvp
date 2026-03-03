#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-orion-labs-mvp}"
ZONE="${ZONE:-us-central1-a}"
VM_NAME="${VM_NAME:-orion-backfill-vm}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-highcpu-16}"
DISK_SIZE_GB="${DISK_SIZE_GB:-200}"
GCS_BUCKET="${GCS_BUCKET:-orion-labs-mvp-era5-126886725893}"
BACKFILL_START="${BACKFILL_START:-1950-01-01}"
BACKFILL_END="${BACKFILL_END:-2026-12-31}"
N_WORKERS="${N_WORKERS:-14}"
POINTS_SET="${POINTS_SET:-provinces}"
REPO_URL="${REPO_URL:-}"
DATABASE_URL="${DATABASE_URL:-}"
INSTANCE_CONNECTION_NAME="${INSTANCE_CONNECTION_NAME:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STARTUP_SCRIPT="${SCRIPT_DIR}/startup_script.sh"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ ! -f "${STARTUP_SCRIPT}" ]]; then
  echo "startup script not found: ${STARTUP_SCRIPT}" >&2
  exit 1
fi

if [[ -z "${REPO_URL}" ]]; then
  REPO_URL="$(git -C "${REPO_ROOT}" config --get remote.origin.url || true)"
fi

if [[ -z "${REPO_URL}" ]]; then
  echo "REPO_URL is empty. Export REPO_URL before running this script." >&2
  exit 1
fi

metadata_pairs=(
  "REPO_URL=${REPO_URL}"
  "GCS_BUCKET=${GCS_BUCKET}"
  "BACKFILL_START=${BACKFILL_START}"
  "BACKFILL_END=${BACKFILL_END}"
  "N_WORKERS=${N_WORKERS}"
  "POINTS_SET=${POINTS_SET}"
)
if [[ -n "${DATABASE_URL}" ]]; then
  metadata_pairs+=("DATABASE_URL=${DATABASE_URL}")
fi
if [[ -n "${INSTANCE_CONNECTION_NAME}" ]]; then
  metadata_pairs+=("INSTANCE_CONNECTION_NAME=${INSTANCE_CONNECTION_NAME}")
fi
METADATA_CSV="$(IFS=,; echo "${metadata_pairs[*]}")"

echo "Creating VM ${VM_NAME} in ${PROJECT_ID}/${ZONE}..."
gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  --boot-disk-size="${DISK_SIZE_GB}GB" \
  --boot-disk-type="pd-ssd" \
  --image-family="debian-12" \
  --image-project="debian-cloud" \
  --scopes="cloud-platform" \
  --metadata="${METADATA_CSV}" \
  --metadata-from-file="startup-script=${STARTUP_SCRIPT}"

echo ""
echo "VM created."
echo "SSH:"
echo "gcloud compute ssh ${VM_NAME} --project ${PROJECT_ID} --zone ${ZONE}"
echo ""
echo "Progress JSON:"
echo "gs://${GCS_BUCKET}/backfill-status/progress.json"
echo ""
echo "Metadata:"
echo "REPO_URL=${REPO_URL}"
echo "BACKFILL_START=${BACKFILL_START}"
echo "BACKFILL_END=${BACKFILL_END}"
echo "N_WORKERS=${N_WORKERS}"
echo "POINTS_SET=${POINTS_SET}"

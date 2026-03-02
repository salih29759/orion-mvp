#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-orion-labs-mvp}"
ZONE="${ZONE:-us-central1-a}"
VM_NAME="${VM_NAME:-orion-backfill-vm}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-highcpu-16}"
DISK_SIZE_GB="${DISK_SIZE_GB:-200}"
GCS_BUCKET="${GCS_BUCKET:-orion-labs-mvp-era5-126886725893}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STARTUP_SCRIPT="${SCRIPT_DIR}/startup_script.sh"

if [[ ! -f "${STARTUP_SCRIPT}" ]]; then
  echo "startup script not found: ${STARTUP_SCRIPT}" >&2
  exit 1
fi

echo "Creating VM ${VM_NAME} in ${PROJECT_ID}/${ZONE}..."
gcloud compute instances create "${VM_NAME}" \
  --project="${PROJECT_ID}" \
  --zone="${ZONE}" \
  --machine-type="${MACHINE_TYPE}" \
  --boot-disk-size="${DISK_SIZE_GB}GB" \
  --boot-disk-type="pd-ssd" \
  --image-family="debian-12" \
  --image-project="debian-cloud" \
  --scopes="storage-full" \
  --metadata-from-file="startup-script=${STARTUP_SCRIPT}"

echo ""
echo "VM created."
echo "SSH:"
echo "gcloud compute ssh ${VM_NAME} --project ${PROJECT_ID} --zone ${ZONE}"
echo ""
echo "Progress JSON:"
echo "gs://${GCS_BUCKET}/backfill-status/progress.json"

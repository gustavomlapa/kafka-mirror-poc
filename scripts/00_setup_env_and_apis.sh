#!/usr/bin/env bash
# ==============================================================================
# Step 00: Verify Environment and Enable Required Google Cloud APIs
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

check_prerequisites

log_info "Setting active Google Cloud project to: ${PROJECT_ID}"
gcloud config set project "${PROJECT_ID}" --quiet

log_info "Enabling required Google Cloud APIs..."
SERVICES=(
    "managedkafka.googleapis.com"
    "run.googleapis.com"
    "artifactregistry.googleapis.com"
    "cloudbuild.googleapis.com"
    "compute.googleapis.com"
    "iam.googleapis.com"
    "logging.googleapis.com"
    "monitoring.googleapis.com"
)

gcloud services enable "${SERVICES[@]}" --project="${PROJECT_ID}"

log_success "All required Google Cloud APIs successfully enabled for project '${PROJECT_ID}'."

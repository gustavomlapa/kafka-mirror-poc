#!/usr/bin/env bash
# ==============================================================================
# Step 99: Clean Up / Teardown All GCP Resources Created for the POC
# Avoids unnecessary ongoing costs after completing validation.
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

check_prerequisites

if [[ "$1" != "--force" && "$1" != "-y" ]]; then
    echo -e "${RED}[CAUTION] This script will permanently delete all POC resources created in project '${PROJECT_ID}':${NC}"
    echo "  - Cloud Run Services (${PRODUCER_SERVICE_NAME}, ${CONSUMER_SERVICE_NAME})"
    echo "  - Compute Engine VM (${MM2_VM_NAME})"
    echo "  - Managed Kafka Clusters (${CLUSTER1_NAME}, ${CLUSTER2_NAME})"
    echo "  - Artifact Registry Repository (${ARTIFACT_REPO_NAME})"
    echo "  - Subnet & VPC Network (${SUBNET_NAME}, ${VPC_NAME})"
    echo "  - Service Account (${SERVICE_ACCOUNT_EMAIL})"
    echo ""
    read -p "Are you sure you want to proceed? (type 'yes' to confirm): " CONFIRM
    if [[ "${CONFIRM}" != "yes" ]]; then
        log_info "Teardown cancelled by user."
        exit 0
    fi
fi

log_info "1. Deleting Cloud Run services..."
gcloud run services delete "${PRODUCER_SERVICE_NAME}" --region="${REGION}" --project="${PROJECT_ID}" --quiet || true
gcloud run services delete "${CONSUMER_SERVICE_NAME}" --region="${REGION}" --project="${PROJECT_ID}" --quiet || true

log_info "2. Deleting MirrorMaker 2 Compute Engine VM..."
gcloud compute instances delete "${MM2_VM_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}" --quiet || true

log_info "3. Deleting Managed Kafka Clusters (Cluster 1 & Cluster 2)..."
gcloud managed-kafka clusters delete "${CLUSTER1_NAME}" --location="${REGION}" --project="${PROJECT_ID}" --quiet || true
gcloud managed-kafka clusters delete "${CLUSTER2_NAME}" --location="${REGION}" --project="${PROJECT_ID}" --quiet || true

log_info "4. Deleting Artifact Registry repository..."
gcloud artifacts repositories delete "${ARTIFACT_REPO_NAME}" --location="${REGION}" --project="${PROJECT_ID}" --quiet || true

log_info "5. Deleting Firewall Rules..."
gcloud compute firewall-rules delete "${VPC_NAME}-allow-internal" --project="${PROJECT_ID}" --quiet || true
gcloud compute firewall-rules delete "${VPC_NAME}-allow-iap-ssh" --project="${PROJECT_ID}" --quiet || true

log_info "6. Deleting Subnet and VPC Network..."
gcloud compute networks subnets delete "${SUBNET_NAME}" --region="${REGION}" --project="${PROJECT_ID}" --quiet || true
gcloud compute networks delete "${VPC_NAME}" --project="${PROJECT_ID}" --quiet || true

log_info "7. Deleting Service Account..."
gcloud iam service-accounts delete "${SERVICE_ACCOUNT_EMAIL}" --project="${PROJECT_ID}" --quiet || true

# Clean up local temporary state
rm -f "${SCRIPT_DIR}/.cluster_state.env" "${SCRIPT_DIR}/connect-mirror-maker.properties"

log_success "All POC resources have been completely torn down and cleaned up."

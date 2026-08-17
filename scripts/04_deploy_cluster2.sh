#!/usr/bin/env bash
# ==============================================================================
# Step 04: Provision Target Managed Kafka Cluster (cluster2)
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

check_prerequisites

SUBNET_RESOURCE="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${SUBNET_NAME}"

log_info "1. Checking if Target Kafka Cluster '${CLUSTER2_NAME}' exists in region '${REGION}'..."
CLUSTER_STATE=$(gcloud managed-kafka clusters describe "${CLUSTER2_NAME}" --location="${REGION}" --project="${PROJECT_ID}" --format="value(state)" 2>/dev/null || echo "NOT_FOUND")

if [[ "${CLUSTER_STATE}" == "NOT_FOUND" ]]; then
    log_info "Creating Target Managed Kafka Cluster '${CLUSTER2_NAME}'..."
    log_info "CPU: ${CLUSTER2_CPU} | Memory: ${CLUSTER2_MEMORY} | Subnet: ${SUBNET_RESOURCE}"
    
    gcloud managed-kafka clusters create "${CLUSTER2_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --cpu="${CLUSTER2_CPU}" \
        --memory="${CLUSTER2_MEMORY}" \
        --subnets="${SUBNET_RESOURCE}"
    
    log_success "Target Cluster '${CLUSTER2_NAME}' creation command completed."
else
    log_info "Cluster '${CLUSTER2_NAME}' already exists with state: ${CLUSTER_STATE}"
fi

log_info "2. Retrieving Bootstrap Server address for '${CLUSTER2_NAME}'..."
CLUSTER2_BOOTSTRAP=$(gcloud managed-kafka clusters describe "${CLUSTER2_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --format="value(bootstrapAddress)" 2>/dev/null || echo "")

if [[ -z "${CLUSTER2_BOOTSTRAP}" ]]; then
    CLUSTER2_BOOTSTRAP="bootstrap.${CLUSTER2_NAME}.${REGION}.managedkafka.${PROJECT_ID}.cloud.goog:9092"
fi

log_success "Cluster 2 Bootstrap Server: ${CLUSTER2_BOOTSTRAP}"

# Append or update cluster state
touch "${SCRIPT_DIR}/.cluster_state.env"
# Remove old CLUSTER2_BOOTSTRAP if present
sed -i.bak '/CLUSTER2_BOOTSTRAP/d' "${SCRIPT_DIR}/.cluster_state.env" 2>/dev/null || true
rm -f "${SCRIPT_DIR}/.cluster_state.env.bak" 2>/dev/null || true
echo "export CLUSTER2_BOOTSTRAP='${CLUSTER2_BOOTSTRAP}'" >> "${SCRIPT_DIR}/.cluster_state.env"

log_success "Step 04 completed. Target Cluster '${CLUSTER2_NAME}' is ready."

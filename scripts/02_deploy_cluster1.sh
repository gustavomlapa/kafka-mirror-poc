#!/usr/bin/env bash
# ==============================================================================
# Step 02: Provision Source Managed Kafka Cluster (cluster1) and Create Topic
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

check_prerequisites

SUBNET_RESOURCE="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${SUBNET_NAME}"

log_info "1. Checking if Source Kafka Cluster '${CLUSTER1_NAME}' exists in region '${REGION}'..."
CLUSTER_STATE=$(gcloud managed-kafka clusters describe "${CLUSTER1_NAME}" --location="${REGION}" --project="${PROJECT_ID}" --format="value(state)" 2>/dev/null || echo "NOT_FOUND")

if [[ "${CLUSTER_STATE}" == "NOT_FOUND" ]]; then
    log_info "Creating Source Managed Kafka Cluster '${CLUSTER1_NAME}'..."
    log_info "CPU: ${CLUSTER1_CPU} | Memory: ${CLUSTER1_MEMORY} | Subnet: ${SUBNET_RESOURCE}"
    
    gcloud managed-kafka clusters create "${CLUSTER1_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --cpu="${CLUSTER1_CPU}" \
        --memory="${CLUSTER1_MEMORY}" \
        --subnets="${SUBNET_RESOURCE}"
    
    log_success "Cluster '${CLUSTER1_NAME}' creation command completed."
else
    log_info "Cluster '${CLUSTER1_NAME}' already exists with state: ${CLUSTER_STATE}"
fi

log_info "2. Retrieving Bootstrap Server address for '${CLUSTER1_NAME}'..."
CLUSTER1_BOOTSTRAP=$(gcloud managed-kafka clusters describe "${CLUSTER1_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --format="value(bootstrapAddress)" 2>/dev/null || echo "")

if [[ -z "${CLUSTER1_BOOTSTRAP}" ]]; then
    # Fallback to standard bootstrap address format if not populated directly
    CLUSTER1_BOOTSTRAP="bootstrap.${CLUSTER1_NAME}.${REGION}.managedkafka.${PROJECT_ID}.cloud.goog:9092"
fi

log_success "Cluster 1 Bootstrap Server: ${CLUSTER1_BOOTSTRAP}"

log_info "3. Checking / Creating Topic '${TOPIC_NAME}' in '${CLUSTER1_NAME}'..."
if ! gcloud managed-kafka topics describe "${TOPIC_NAME}" --cluster="${CLUSTER1_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud managed-kafka topics create "${TOPIC_NAME}" \
        --cluster="${CLUSTER1_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --partitions="${TOPIC_PARTITIONS}" \
        --replication-factor="${TOPIC_REPLICATION_FACTOR}"
    log_success "Topic '${TOPIC_NAME}' created successfully on cluster '${CLUSTER1_NAME}'."
else
    log_info "Topic '${TOPIC_NAME}' already exists on cluster '${CLUSTER1_NAME}'."
fi

# Export to a temporary state file for subsequent scripts
echo "export CLUSTER1_BOOTSTRAP='${CLUSTER1_BOOTSTRAP}'" > "${SCRIPT_DIR}/.cluster_state.env"

log_success "Step 02 completed. Source Cluster '${CLUSTER1_NAME}' is ready."

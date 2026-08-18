#!/usr/bin/env bash
# ==============================================================================
# Step 09 (Optional): Reset POC Data and Environment
# Cleans topics, resets offsets, stops MM2, and repoints apps to Cluster 1
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

check_prerequisites

# Load bootstrap servers if saved
if [[ -f "${SCRIPT_DIR}/.cluster_state.env" ]]; then
    # shellcheck source=/dev/null
    source "${SCRIPT_DIR}/.cluster_state.env"
fi

if [[ -z "${CLUSTER1_BOOTSTRAP}" ]]; then
    CLUSTER1_BOOTSTRAP=$(gcloud managed-kafka clusters describe "${CLUSTER1_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --format="value(bootstrapAddress)" 2>/dev/null || echo "bootstrap.${CLUSTER1_NAME}.${REGION}.managedkafka.${PROJECT_ID}.cloud.goog:9092")
fi

log_info "Starting full data and state reset for Kafka MirrorMaker POC..."

# 1. Stop MirrorMaker 2 on the VM
log_info "1. Stopping MirrorMaker 2.0 daemon and cleaning logs on VM '${MM2_VM_NAME}'..."
gcloud compute ssh "${MM2_VM_NAME}" \
    --zone="${ZONE}" \
    --project="${PROJECT_ID}" \
    --tunnel-through-iap \
    --command="sudo pkill -f 'connect-mirror-maker' || true && sudo rm -f /var/log/mirrormaker.log" 2>/dev/null || log_warn "Could not connect to VM (VM might be stopped or unreachable)."

# 2. Reset Topic on Cluster 1 (Source)
log_info "2. Resetting topic '${TOPIC_NAME}' on Source Cluster '${CLUSTER1_NAME}'..."
if gcloud managed-kafka topics describe "${TOPIC_NAME}" --cluster="${CLUSTER1_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud managed-kafka topics delete "${TOPIC_NAME}" \
        --cluster="${CLUSTER1_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --quiet
    log_info "Deleted existing topic '${TOPIC_NAME}' on '${CLUSTER1_NAME}'."
fi

gcloud managed-kafka topics create "${TOPIC_NAME}" \
    --cluster="${CLUSTER1_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --partitions="${TOPIC_PARTITIONS}" \
    --replication-factor="${TOPIC_REPLICATION_FACTOR}"
log_success "Recreated clean topic '${TOPIC_NAME}' on '${CLUSTER1_NAME}'."

# 3. Reset Topics on Cluster 2 (Target)
log_info "3. Cleaning topics and replication metadata on Target Cluster '${CLUSTER2_NAME}'..."
if gcloud managed-kafka clusters describe "${CLUSTER2_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    # Fetch list of topics on cluster2
    TOPICS_C2=$(gcloud managed-kafka topics list --cluster="${CLUSTER2_NAME}" --location="${REGION}" --project="${PROJECT_ID}" --format="value(name)" 2>/dev/null || true)
    
    for T in ${TOPICS_C2}; do
        TOPIC_ID=$(basename "${T}")
        log_info "Deleting topic '${TOPIC_ID}' from '${CLUSTER2_NAME}'..."
        gcloud managed-kafka topics delete "${TOPIC_ID}" \
            --cluster="${CLUSTER2_NAME}" \
            --location="${REGION}" \
            --project="${PROJECT_ID}" \
            --quiet 2>/dev/null || true
    done
    log_success "Target Cluster '${CLUSTER2_NAME}' is now clean."
else
    log_info "Target Cluster '${CLUSTER2_NAME}' does not exist yet. Skipping."
fi

# 4. Point Cloud Run Apps back to Cluster 1 and reset runtime state
log_info "4. Resetting Cloud Run Producer and Consumer to point to Cluster 1..."
RESET_TIMESTAMP=$(date +%s)

gcloud run services update "${PRODUCER_SERVICE_NAME}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --update-env-vars="KAFKA_BOOTSTRAP_SERVERS=${CLUSTER1_BOOTSTRAP},KAFKA_TOPIC=${TOPIC_NAME},RESET_EPOCH=${RESET_TIMESTAMP}" \
    --quiet 2>/dev/null || log_warn "Producer Cloud Run service not updated."

PRODUCER_URL=$(gcloud run services describe "${PRODUCER_SERVICE_NAME}" --region="${REGION}" --project="${PROJECT_ID}" --format="value(status.url)" 2>/dev/null || echo "")

gcloud run services update "${CONSUMER_SERVICE_NAME}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --update-env-vars="KAFKA_BOOTSTRAP_SERVERS=${CLUSTER1_BOOTSTRAP},KAFKA_TOPIC=${TOPIC_NAME},RESET_EPOCH=${RESET_TIMESTAMP}" \
    --quiet 2>/dev/null || log_warn "Consumer Cloud Run service not updated."

CONSUMER_URL=$(gcloud run services describe "${CONSUMER_SERVICE_NAME}" --region="${REGION}" --project="${PROJECT_ID}" --format="value(status.url)" 2>/dev/null || echo "")

echo ""
log_success "================================================================="
log_success "POC Environment Reset Completed Successfully!"
log_success "1. MirrorMaker 2.0 stopped and logs purged."
log_success "2. Topics recreated cleanly with 0 messages."
log_success "3. Producer and Consumer repointed to Cluster 1 (${CLUSTER1_NAME})."
if [[ -n "${PRODUCER_URL}" ]]; then
    log_success "Producer URL : ${PRODUCER_URL}"
fi
if [[ -n "${CONSUMER_URL}" ]]; then
    log_success "Consumer URL : ${CONSUMER_URL}"
    log_success "Verify Consumer initial status: curl -s ${CONSUMER_URL}/status | jq ."
fi
log_success "You can now re-run steps 03 through 07 to replay the migration!"
log_success "================================================================="

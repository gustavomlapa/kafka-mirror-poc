#!/usr/bin/env bash
# ==============================================================================
# Step 08 (Optional): Application Failback / Rollback to Cluster 1 (cluster1)
# Reconfigures Producer and Consumer on Cloud Run to point back to cluster1
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

log_info "Source (Cluster 1) Bootstrap Server: ${CLUSTER1_BOOTSTRAP}"
log_info "Target Topic: ${TOPIC_NAME}"

# Optional: Stop MirrorMaker forward replication to prevent replication feedback
log_info "1. Stopping forward MirrorMaker 2.0 replication on VM (if running)..."
gcloud compute ssh "${MM2_VM_NAME}" \
    --zone="${ZONE}" \
    --project="${PROJECT_ID}" \
    --tunnel-through-iap \
    --command="sudo pkill -f 'connect-mirror-maker' || true" 2>/dev/null || log_warn "Could not stop MM2 on VM (VM might be stopped or unreachable)."

log_info "2. Updating Producer Service on Cloud Run to point back to Cluster 1..."
gcloud run services update "${PRODUCER_SERVICE_NAME}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --update-env-vars="KAFKA_BOOTSTRAP_SERVERS=${CLUSTER1_BOOTSTRAP},KAFKA_TOPIC=${TOPIC_NAME}" \
    --quiet

PRODUCER_URL=$(gcloud run services describe "${PRODUCER_SERVICE_NAME}" --region="${REGION}" --project="${PROJECT_ID}" --format="value(status.url)")
log_success "Producer successfully rolled back to Cluster 1! URL: ${PRODUCER_URL}"

log_info "3. Updating Consumer Service on Cloud Run to point back to Cluster 1..."
gcloud run services update "${CONSUMER_SERVICE_NAME}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --update-env-vars="KAFKA_BOOTSTRAP_SERVERS=${CLUSTER1_BOOTSTRAP},KAFKA_TOPIC=${TOPIC_NAME}" \
    --quiet

CONSUMER_URL=$(gcloud run services describe "${CONSUMER_SERVICE_NAME}" --region="${REGION}" --project="${PROJECT_ID}" --format="value(status.url)")
log_success "Consumer successfully rolled back to Cluster 1! URL: ${CONSUMER_URL}"

echo ""
log_success "================================================================="
log_success "Failback Completed Successfully!"
log_success "Both Producer and Consumer are now operating directly on Cluster 1 (${CLUSTER1_NAME})."
log_success "Verify Consumer live status: curl -s ${CONSUMER_URL}/status | jq ."
log_success "================================================================="

#!/usr/bin/env bash
# ==============================================================================
# Step 07: Application Cutover to Target Managed Kafka Cluster (cluster2)
# Reconfigures Producer and Consumer on Cloud Run to point to cluster2
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

if [[ -z "${CLUSTER2_BOOTSTRAP}" ]]; then
    CLUSTER2_BOOTSTRAP=$(gcloud managed-kafka clusters describe "${CLUSTER2_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --format="value(bootstrapAddress)" 2>/dev/null || echo "bootstrap.${CLUSTER2_NAME}.${REGION}.managedkafka.${PROJECT_ID}.cloud.goog:9092")
fi

TARGET_TOPIC="${TOPIC_NAME}"
if [[ "$1" == "default-policy" ]]; then
    TARGET_TOPIC="source.${TOPIC_NAME}"
    log_info "Cutover with Default Policy: Target topic is '${TARGET_TOPIC}'"
else
    log_info "Cutover with Identity Policy: Target topic is '${TARGET_TOPIC}'"
fi

log_info "Target (Cluster 2) Bootstrap Server: ${CLUSTER2_BOOTSTRAP}"

log_info "1. Updating Producer Service on Cloud Run to point to Cluster 2..."
gcloud run services update "${PRODUCER_SERVICE_NAME}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --update-env-vars="KAFKA_BOOTSTRAP_SERVERS=${CLUSTER2_BOOTSTRAP},KAFKA_TOPIC=${TARGET_TOPIC}" \
    --quiet

PRODUCER_URL=$(gcloud run services describe "${PRODUCER_SERVICE_NAME}" --region="${REGION}" --project="${PROJECT_ID}" --format="value(status.url)")
log_success "Producer successfully cut over to Cluster 2! URL: ${PRODUCER_URL}"

log_info "2. Updating Consumer Service on Cloud Run to point to Cluster 2..."
gcloud run services update "${CONSUMER_SERVICE_NAME}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --update-env-vars="KAFKA_BOOTSTRAP_SERVERS=${CLUSTER2_BOOTSTRAP},KAFKA_TOPIC=${TARGET_TOPIC}" \
    --quiet

CONSUMER_URL=$(gcloud run services describe "${CONSUMER_SERVICE_NAME}" --region="${REGION}" --project="${PROJECT_ID}" --format="value(status.url)")
log_success "Consumer successfully cut over to Cluster 2! URL: ${CONSUMER_URL}"

echo ""
log_success "================================================================="
log_success "Cutover Completed!"
log_success "Both Producer and Consumer are now operating directly on Cluster 2 (${CLUSTER2_NAME})."
log_success "Verify Consumer live status: curl -s ${CONSUMER_URL}/status | jq ."
log_success "================================================================="

#!/usr/bin/env bash
# ==============================================================================
# Step 03: Build Container Images and Deploy Producer / Consumer to Cloud Run
# Uses Cloud Run Direct VPC Egress to privately communicate with Managed Kafka
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

check_prerequisites

# Load bootstrap servers if saved from Step 02
if [[ -f "${SCRIPT_DIR}/.cluster_state.env" ]]; then
    # shellcheck source=/dev/null
    source "${SCRIPT_DIR}/.cluster_state.env"
fi

if [[ -z "${CLUSTER1_BOOTSTRAP}" ]]; then
    log_info "Fetching Cluster 1 bootstrap server address..."
    CLUSTER1_BOOTSTRAP=$(gcloud managed-kafka clusters describe "${CLUSTER1_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --format="value(bootstrapAddress)" 2>/dev/null || echo "bootstrap.${CLUSTER1_NAME}.${REGION}.managedkafka.${PROJECT_ID}.cloud.goog:9092")
fi

log_info "1. Checking / Creating Artifact Registry repository '${ARTIFACT_REPO_NAME}'..."
if ! gcloud artifacts repositories describe "${ARTIFACT_REPO_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud artifacts repositories create "${ARTIFACT_REPO_NAME}" \
        --repository-format=docker \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --description="Docker repository for Kafka POC microservices"
    log_success "Artifact Registry repository '${ARTIFACT_REPO_NAME}' created."
else
    log_info "Artifact Registry repository '${ARTIFACT_REPO_NAME}' already exists."
fi

REPO_URL="${REGION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPO_NAME}"
PRODUCER_IMAGE="${REPO_URL}/kafka-producer:latest"
CONSUMER_IMAGE="${REPO_URL}/kafka-consumer:latest"

log_info "2. Building Producer container image using Google Cloud Build..."
gcloud builds submit "${ROOT_DIR}" \
    --config=<(cat <<EOF
steps:
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build', '-t', '${PRODUCER_IMAGE}', '-f', 'apps/producer/Dockerfile', '.']
images:
  - '${PRODUCER_IMAGE}'
EOF
) \
    --project="${PROJECT_ID}"

log_success "Producer image built: ${PRODUCER_IMAGE}"

log_info "3. Building Consumer container image using Google Cloud Build..."
gcloud builds submit "${ROOT_DIR}" \
    --config=<(cat <<EOF
steps:
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build', '-t', '${CONSUMER_IMAGE}', '-f', 'apps/consumer/Dockerfile', '.']
images:
  - '${CONSUMER_IMAGE}'
EOF
) \
    --project="${PROJECT_ID}"

log_success "Consumer image built: ${CONSUMER_IMAGE}"

log_info "4. Deploying Producer Service to Cloud Run with Direct VPC Egress..."
gcloud run deploy "${PRODUCER_SERVICE_NAME}" \
    --image="${PRODUCER_IMAGE}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --network="${VPC_NAME}" \
    --subnet="${SUBNET_NAME}" \
    --vpc-egress="all-traffic" \
    --service-account="${SERVICE_ACCOUNT_EMAIL}" \
    --set-env-vars="KAFKA_BOOTSTRAP_SERVERS=${CLUSTER1_BOOTSTRAP},KAFKA_TOPIC=${TOPIC_NAME},KAFKA_AUTH_TYPE=OAUTHBEARER,AUTO_STREAM=true,STREAM_INTERVAL_SECONDS=3,SERVICE_ACCOUNT_EMAIL=${SERVICE_ACCOUNT_EMAIL},GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL=${SERVICE_ACCOUNT_EMAIL}" \
    --min-instances=1 \
    --max-instances=2 \
    --port=8080 \
    --cpu-boost \
    --no-cpu-throttling \
    --timeout=300s \
    --allow-unauthenticated

PRODUCER_URL=$(gcloud run services describe "${PRODUCER_SERVICE_NAME}" --region="${REGION}" --project="${PROJECT_ID}" --format="value(status.url)")
log_success "Producer deployed: ${PRODUCER_URL}"

log_info "5. Deploying Consumer Service to Cloud Run with Direct VPC Egress..."
gcloud run deploy "${CONSUMER_SERVICE_NAME}" \
    --image="${CONSUMER_IMAGE}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --network="${VPC_NAME}" \
    --subnet="${SUBNET_NAME}" \
    --vpc-egress="all-traffic" \
    --service-account="${SERVICE_ACCOUNT_EMAIL}" \
    --set-env-vars="KAFKA_BOOTSTRAP_SERVERS=${CLUSTER1_BOOTSTRAP},KAFKA_TOPIC=${TOPIC_NAME},KAFKA_GROUP_ID=${CONSUMER_GROUP},KAFKA_AUTH_TYPE=OAUTHBEARER,KAFKA_AUTO_OFFSET_RESET=earliest,SERVICE_ACCOUNT_EMAIL=${SERVICE_ACCOUNT_EMAIL},GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL=${SERVICE_ACCOUNT_EMAIL}" \
    --min-instances=1 \
    --max-instances=1 \
    --port=8080 \
    --cpu-boost \
    --no-cpu-throttling \
    --timeout=300s \
    --allow-unauthenticated

CONSUMER_URL=$(gcloud run services describe "${CONSUMER_SERVICE_NAME}" --region="${REGION}" --project="${PROJECT_ID}" --format="value(status.url)")
log_success "Consumer deployed: ${CONSUMER_URL}"

echo ""
log_success "================================================================="
log_success "Producer App URL : ${PRODUCER_URL}"
log_success "Consumer App URL : ${CONSUMER_URL}"
log_success "Check Consumer status endpoint: curl -s ${CONSUMER_URL}/status | jq ."
log_success "================================================================="

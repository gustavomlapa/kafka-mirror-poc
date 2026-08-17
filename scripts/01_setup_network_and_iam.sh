#!/usr/bin/env bash
# ==============================================================================
# Step 01: Setup VPC Network, Subnet, Firewall Rules, and IAM Service Account
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

check_prerequisites

log_info "1. Checking / Creating VPC Network: ${VPC_NAME}"
if ! gcloud compute networks describe "${VPC_NAME}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud compute networks create "${VPC_NAME}" \
        --project="${PROJECT_ID}" \
        --subnet-mode=custom \
        --description="VPC for Kafka Managed Service and Cloud Run Direct VPC Egress"
    log_success "VPC '${VPC_NAME}' created."
else
    log_info "VPC '${VPC_NAME}' already exists."
fi

log_info "2. Checking / Creating Subnet: ${SUBNET_NAME} (${SUBNET_CIDR}) in region ${REGION}"
if ! gcloud compute networks subnets describe "${SUBNET_NAME}" --region="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud compute networks subnets create "${SUBNET_NAME}" \
        --project="${PROJECT_ID}" \
        --network="${VPC_NAME}" \
        --region="${REGION}" \
        --range="${SUBNET_CIDR}" \
        --enable-private-ip-google-access
    log_success "Subnet '${SUBNET_NAME}' created."
else
    log_info "Subnet '${SUBNET_NAME}' already exists."
fi

log_info "3. Configuring Firewall Rules for VPC: ${VPC_NAME}"
# Internal traffic between Kafka, VM, and Cloud Run VPC Egress
if ! gcloud compute firewall-rules describe "${VPC_NAME}-allow-internal" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud compute firewall-rules create "${VPC_NAME}-allow-internal" \
        --project="${PROJECT_ID}" \
        --network="${VPC_NAME}" \
        --allow=tcp:9092,tcp:8080,tcp:22,icmp \
        --source-ranges="${SUBNET_CIDR}" \
        --description="Allow internal Kafka and app traffic"
    log_success "Internal firewall rule '${VPC_NAME}-allow-internal' created."
fi

# IAP SSH access for secure VM access without public IP
if ! gcloud compute firewall-rules describe "${VPC_NAME}-allow-iap-ssh" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud compute firewall-rules create "${VPC_NAME}-allow-iap-ssh" \
        --project="${PROJECT_ID}" \
        --network="${VPC_NAME}" \
        --allow=tcp:22 \
        --source-ranges=35.235.240.0/20 \
        --description="Allow Cloud IAP SSH traffic"
    log_success "IAP firewall rule '${VPC_NAME}-allow-iap-ssh' created."
fi

log_info "4. Checking / Creating Service Account: ${SERVICE_ACCOUNT_EMAIL}"
if ! gcloud iam service-accounts describe "${SERVICE_ACCOUNT_EMAIL}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud iam service-accounts create "${SERVICE_ACCOUNT_NAME}" \
        --project="${PROJECT_ID}" \
        --display-name="Kafka POC Service Account for Clients and MM2"
    log_success "Service account '${SERVICE_ACCOUNT_EMAIL}' created."
else
    log_info "Service account '${SERVICE_ACCOUNT_EMAIL}' already exists."
fi

log_info "5. Granting IAM Roles to Service Account..."
ROLES=(
    "roles/managedkafka.client"
    "roles/run.invoker"
    "roles/artifactregistry.reader"
    "roles/logging.logWriter"
    "roles/monitoring.metricWriter"
)

for ROLE in "${ROLES[@]}"; do
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
        --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
        --role="${ROLE}" \
        --condition=None \
        --quiet &>/dev/null
    log_info "Granted role: ${ROLE}"
done

log_success "Network, Firewall, and IAM configurations completed successfully."

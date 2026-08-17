#!/usr/bin/env bash
# ==============================================================================
# Step 06: Configure and Start MirrorMaker 2.0 Replication on GCE VM
# According to: https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/move-kafka-mirrormaker#set-up-mirrormaker
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

if [[ -z "${CLUSTER2_BOOTSTRAP}" ]]; then
    CLUSTER2_BOOTSTRAP=$(gcloud managed-kafka clusters describe "${CLUSTER2_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --format="value(bootstrapAddress)" 2>/dev/null || echo "bootstrap.${CLUSTER2_NAME}.${REGION}.managedkafka.${PROJECT_ID}.cloud.goog:9092")
fi

log_info "Source (Cluster 1) Bootstrap: ${CLUSTER1_BOOTSTRAP}"
log_info "Target (Cluster 2) Bootstrap: ${CLUSTER2_BOOTSTRAP}"

# Choose replication template (Default: IdentityReplicationPolicy for direct topic name migration)
POLICY_MODE="${1:-identity}"

if [[ "${POLICY_MODE}" == "default" ]]; then
    TEMPLATE_FILE="${ROOT_DIR}/mm2/connect-mirror-maker.properties.template"
    log_info "Using Default Replication Policy (Replicated topic will be: 'source.${TOPIC_NAME}')"
else
    TEMPLATE_FILE="${ROOT_DIR}/mm2/connect-mirror-maker-identity.properties.template"
    log_info "Using Identity Replication Policy (Replicated topic will preserve exact name: '${TOPIC_NAME}')"
fi

# Generate concrete properties file
GENERATED_PROPS="${SCRIPT_DIR}/connect-mirror-maker.properties"
sed \
    -e "s|\${SOURCE_BOOTSTRAP_SERVERS}|${CLUSTER1_BOOTSTRAP}|g" \
    -e "s|\${TARGET_BOOTSTRAP_SERVERS}|${CLUSTER2_BOOTSTRAP}|g" \
    "${TEMPLATE_FILE}" > "${GENERATED_PROPS}"

# Create a clean standalone execution script to run on the VM
RUN_SCRIPT="${SCRIPT_DIR}/run_mm2_service.sh"
cat << 'EOF' > "${RUN_SCRIPT}"
#!/usr/bin/env bash
set -e

echo "[1/4] Moving configuration to /opt/kafka/config..."
mkdir -p /opt/kafka/config
mv /tmp/connect-mirror-maker.properties /opt/kafka/config/connect-mirror-maker.properties

echo "[2/4] Stopping any existing MirrorMaker process..."
pkill -f 'connect-mirror-maker' || true
sleep 1

echo "[3/4] Launching MirrorMaker 2.0 process in the background..."
nohup /opt/kafka/bin/connect-mirror-maker.sh /opt/kafka/config/connect-mirror-maker.properties </dev/null >/var/log/mirrormaker.log 2>&1 &

echo "[4/4] Verifying process startup..."
sleep 3
if pgrep -f 'connect-mirror-maker' >/dev/null; then
    echo "MirrorMaker 2.0 daemon is RUNNING (PID: $(pgrep -f 'connect-mirror-maker' | tr '\n' ' '))"
else
    echo "WARNING: MirrorMaker 2.0 process not detected in process list. Check logs below:"
fi
echo "--- Initial log lines (/var/log/mirrormaker.log) ---"
head -n 25 /var/log/mirrormaker.log 2>/dev/null || true
echo "---------------------------------------------------"
EOF
chmod +x "${RUN_SCRIPT}"

log_info "1. Uploading MirrorMaker 2.0 configuration and runner script to VM '${MM2_VM_NAME}'..."
gcloud compute scp "${GENERATED_PROPS}" "${RUN_SCRIPT}" "${MM2_VM_NAME}:/tmp/" \
    --zone="${ZONE}" \
    --project="${PROJECT_ID}" \
    --tunnel-through-iap

log_info "2. Starting MirrorMaker 2.0 daemon on VM '${MM2_VM_NAME}'..."
gcloud compute ssh "${MM2_VM_NAME}" \
    --zone="${ZONE}" \
    --project="${PROJECT_ID}" \
    --tunnel-through-iap \
    --command="sudo bash /tmp/run_mm2_service.sh"

log_success "MirrorMaker 2.0 replication command executed successfully!"

echo ""
log_info "To view live MirrorMaker 2 replication logs, run:"
echo -e "${YELLOW}gcloud compute ssh ${MM2_VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} --tunnel-through-iap --command=\"sudo tail -f /var/log/mirrormaker.log\"${NC}"

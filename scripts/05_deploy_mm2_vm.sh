#!/usr/bin/env bash
# ==============================================================================
# Step 05: Provision Compute Engine VM for MirrorMaker 2.0
# According to: https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/move-kafka-mirrormaker#set-up-mirrormaker
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

check_prerequisites

log_info "1. Checking if MirrorMaker 2 VM '${MM2_VM_NAME}' already exists..."
VM_EXISTS=$(gcloud compute instances describe "${MM2_VM_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}" --format="value(name)" 2>/dev/null || echo "NOT_FOUND")

if [[ "${VM_EXISTS}" != "NOT_FOUND" ]]; then
    log_info "VM '${MM2_VM_NAME}' already exists."
else
    log_info "Creating Compute Engine VM '${MM2_VM_NAME}' in zone '${ZONE}'..."

    # Write startup script to a file to avoid gcloud comma/dictionary syntax parsing issues
    STARTUP_SCRIPT_FILE="${SCRIPT_DIR}/.mm2_startup_script.sh"
    cat << EOF > "${STARTUP_SCRIPT_FILE}"
#!/usr/bin/env bash
set -e

apt-get update
apt-get install -y openjdk-17-jre-headless maven wget curl tar

KAFKA_DIR="/opt/kafka"
if [ ! -d "\${KAFKA_DIR}" ]; then
    mkdir -p "\${KAFKA_DIR}"
    cd /tmp
    wget -q https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz
    tar -xzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C "\${KAFKA_DIR}" --strip-components=1
    rm kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz
fi

# Download Google Cloud Managed Kafka Auth Login Handler and all transitive dependencies
mkdir -p /tmp/kafka-auth-deps
cat << 'POM' > /tmp/kafka-auth-deps/pom.xml
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.poc</groupId>
  <artifactId>kafka-auth-downloader</artifactId>
  <version>1.0</version>
  <dependencies>
    <dependency>
      <groupId>com.google.cloud.hosted.kafka</groupId>
      <artifactId>managed-kafka-auth-login-handler</artifactId>
      <version>${GCP_AUTH_HANDLER_VERSION}</version>
    </dependency>
  </dependencies>
</project>
POM

mvn -f /tmp/kafka-auth-deps/pom.xml dependency:copy-dependencies -DoutputDirectory="\${KAFKA_DIR}/libs" -q

echo "MirrorMaker 2 VM setup completed at \$(date)" >> /var/log/mm2-startup.log
EOF

    gcloud compute instances create "${MM2_VM_NAME}" \
        --project="${PROJECT_ID}" \
        --zone="${ZONE}" \
        --machine-type="${MM2_MACHINE_TYPE}" \
        --network="${VPC_NAME}" \
        --subnet="${SUBNET_NAME}" \
        --service-account="${SERVICE_ACCOUNT_EMAIL}" \
        --scopes="https://www.googleapis.com/auth/cloud-platform" \
        --image-family="debian-12" \
        --image-project="debian-cloud" \
        --metadata-from-file=startup-script="${STARTUP_SCRIPT_FILE}" \
        --description="Compute Engine VM dedicated to running Apache Kafka MirrorMaker 2.0"

    rm -f "${STARTUP_SCRIPT_FILE}"

    log_success "Compute Engine VM '${MM2_VM_NAME}' created."
    log_info "Waiting 30 seconds for startup script to finish downloading Kafka and GCP dependencies..."
    sleep 30
fi

log_success "MirrorMaker 2 VM is ready."

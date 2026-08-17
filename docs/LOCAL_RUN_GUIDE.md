# Local Execution & Testing Guide

This guide explains how to run and test the Python Producer and Consumer applications on your local workstation.

---

## 1. Prerequisites

1. **Python 3.10+**
2. **librdkafka** (installed via Homebrew on macOS or apt on Linux):
   ```bash
   # On macOS
   brew install librdkafka

   # On Ubuntu / Debian
   sudo apt-get install -y librdkafka-dev
   ```
3. **Google Cloud SDK (`gcloud`)**:
   Authenticated with Application Default Credentials (ADC):
   ```bash
   gcloud auth login
   gcloud auth application-default login
   ```

---

## 2. Setting Up Local Python Environment

```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies for both Producer and Consumer
pip install -r apps/producer/requirements.txt
```

---

## 3. Running Applications Against a Managed Kafka Cluster

> [!NOTE]
> Managed Service for Apache Kafka is accessible within its VPC. To connect directly from a local workstation, you can use:
> 1. An SSH SOCKS5 proxy or port forward through a bastion / GCE VM in the VPC:
>    ```bash
>    gcloud compute ssh kafka-mm2-vm --zone=us-central1-a -- -N -L 9092:bootstrap.kafka-cluster1-source.us-central1.managedkafka.PROJECT_ID.cloud.goog:9092
>    ```
> 2. Or test locally with PLAINTEXT against a local Docker Kafka container.

### Running with GCP OAuth Authentication:
```bash
export KAFKA_BOOTSTRAP_SERVERS="bootstrap.kafka-cluster1-source.us-central1.managedkafka.YOUR_PROJECT_ID.cloud.goog:9092"
export KAFKA_TOPIC="orders-poc"
export KAFKA_AUTH_TYPE="OAUTHBEARER"
export PORT=8081

# Run Producer
python apps/producer/producer.py
```

In a separate terminal:
```bash
source .venv/bin/activate
export KAFKA_BOOTSTRAP_SERVERS="bootstrap.kafka-cluster1-source.us-central1.managedkafka.YOUR_PROJECT_ID.cloud.goog:9092"
export KAFKA_TOPIC="orders-poc"
export KAFKA_GROUP_ID="order-processing-group"
export KAFKA_AUTH_TYPE="OAUTHBEARER"
export PORT=8082

# Run Consumer
python apps/consumer/consumer.py
```

---

## 4. Testing Endpoints Locally

### Check Producer Health & Stream Status
```bash
curl -s http://localhost:8081/health | jq .
```

### Trigger a Batch of 5 Custom Mock Orders
```bash
curl -X POST http://localhost:8081/produce \
  -H "Content-Type: application/json" \
  -d '{"count": 5}' | jq .
```

### Check Consumer Consumed Records & Current Offsets
```bash
curl -s http://localhost:8082/status | jq .
```

# Kafka MirrorMaker 2.0 POC Implementation Plan (Bash / gcloud / Cloud Run)

- [x] In `.gitignore`, add ignore patterns for GCP secrets, service account JSON files (`*.json`), `.env`, `env.sh`, and Python/Docker artifacts.
- [x] In `env.sh.example`, create central environment variables template (`PROJECT_ID`, `REGION`, `VPC_NAME`, `SUBNET_NAME`, `CLUSTER1_NAME`, `CLUSTER2_NAME`, `VM_NAME`, `SERVICE_ACCOUNT_NAME`, `TOPIC_NAME`, etc.).
- [x] In `apps/auth/gcp_token_provider.py`, implement Google Cloud IAM OAuth token provider for Python Kafka clients (confluent-kafka / kafka-python).
- [x] In `apps/producer/mock_data.py`, implement realistic mock data generator for e-commerce orders, items, amounts, and sequential message IDs.
- [x] In `apps/producer/producer.py`, `apps/producer/requirements.txt`, and `apps/producer/Dockerfile`, implement the Producer application with continuous mock stream and REST API triggers.
- [x] In `apps/consumer/consumer.py`, `apps/consumer/requirements.txt`, and `apps/consumer/Dockerfile`, implement the Consumer application with offset logging, group tracking, and health status endpoint.
- [x] In `mm2/`, create MirrorMaker 2.0 configuration templates (`connect-mirror-maker.properties.template` and `connect-mirror-maker-identity.properties.template`) with Google Cloud Managed Kafka SASL/OAUTHBEARER handler.
- [x] In `scripts/00_setup_env_and_apis.sh` and `scripts/01_setup_network_and_iam.sh`, create scripts to validate environment, enable GCP APIs, create VPC, Subnet, Firewall rules, and Service Account with `roles/managedkafka.client`.
- [x] In `scripts/02_deploy_cluster1.sh`, create script to provision Source Managed Kafka cluster (`cluster1`) and create the initial topic.
- [x] In `scripts/03_deploy_apps_cloudrun.sh`, create script to create Artifact Registry repo, build container images with Cloud Build, and deploy Producer and Consumer to Cloud Run with Direct VPC Egress.
- [x] In `scripts/04_deploy_cluster2.sh`, create script to provision Target Managed Kafka cluster (`cluster2`).
- [x] In `scripts/05_deploy_mm2_vm.sh`, create script to provision GCE VM with Java, Kafka binaries, and Google Cloud Managed Kafka Auth library.
- [x] In `scripts/06_start_replication.sh`, create script to upload properties and launch MirrorMaker 2.0 replication.
- [x] In `scripts/07_cutover_apps.sh`, create script to update Cloud Run environment variables for Producer and Consumer to target `cluster2`.
- [x] In `scripts/99_teardown.sh`, create script to clean up all GCP resources created in the POC.
- [x] In `docs/MIGRATION_GUIDE.md` and `docs/LOCAL_RUN_GUIDE.md`, document the deep-dive migration concepts and local execution instructions.
- [x] In `README.md`, provide an exhaustive walkthrough in Portuguese and English explaining the POC architecture, manual step-by-step execution, and what changes in apps during cutover.

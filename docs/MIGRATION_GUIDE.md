# Kafka Data Migration Guide with MirrorMaker 2.0 on Google Cloud

This guide explains the architecture, offset translation mechanisms, topic naming strategies, and application cutover steps when migrating data between Kafka clusters using **Apache Kafka MirrorMaker 2.0 (MM2)** and **Google Cloud Managed Service for Apache Kafka (MSAK)**.

---

## 1. Architecture Overview

```
                      +-------------------------------------------------+
                      |              Single Google Cloud Project        |
                      |                                                 |
                      |   +------------------+                          |
                      |   | Cloud Run / App  |                          |
                      |   | (Producer)       |                          |
                      |   +--------+---------+                          |
                      |            | (Step 1: Write to Cluster 1)       |
                      |            v                                    |
                      |   +-----------------------+                     |
                      |   | Source Managed Kafka  | (cluster1)          |
                      |   | Topic: 'orders-poc'   |                     |
                      |   +-----------+-----------+                     |
                      |               |                                 |
                      |               | (Step 4: Continuous Mirroring)  |
                      |               v                                 |
                      |   +-----------------------+                     |
                      |   | MirrorMaker 2.0 GCE VM| (mm2-vm)            |
                      |   | - MirrorSourceTask    |                     |
                      |   | - MirrorCheckpointTask|                     |
                      |   | - MirrorHeartbeatTask |                     |
                      |   +-----------+-----------+                     |
                      |               |                                 |
                      |               v                                 |
                      |   +-----------------------+                     |
                      |   | Target Managed Kafka  | (cluster2)          |
                      |   | Topic: 'orders-poc'   |                     |
                      |   +-----------^-----------+                     |
                      |               |                                 |
                      |            | (Step 5: Cutover - App connects)   |
                      |   +--------+---------+                          |
                      |   | Cloud Run / App  |                          |
                      |   | (Consumer)       |                          |
                      |   +------------------+                          |
                      +-------------------------------------------------+
```

---

## 2. Authentication: Google Cloud IAM & SASL/OAUTHBEARER

Google Cloud Managed Service for Apache Kafka does not use static username/password pairs. Instead, it natively integrates with **Google Cloud IAM** via `SASL/OAUTHBEARER`.

### On the MirrorMaker 2.0 VM (Java)
The VM uses the `com.google.cloud.hosted.kafka:managed-kafka-auth-login-handler` library:
```properties
security.protocol = SASL_SSL
sasl.mechanism = OAUTHBEARER
sasl.login.callback.handler.class = com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler
sasl.jaas.config = org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
```
The login callback handler automatically extracts OAuth2 access tokens using the Compute Engine VM's attached Service Account (`sa-kafka-poc`) via Application Default Credentials (ADC).

### In Python Applications (confluent-kafka / Cloud Run)
In Python, the `gcp_token_provider.py` module uses `google.auth.default()` to dynamically generate and refresh Google IAM bearer tokens:
```python
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'OAUTHBEARER',
    'oauth_cb': confluent_oauth_callback,
}
```

---

## 3. Topic Replication Policies

MirrorMaker 2.0 supports two main topic replication policies:

### Policy A: IdentityReplicationPolicy (Recommended for Cluster Migration)
- **Configuration:**
  ```properties
  replication.policy.class = org.apache.kafka.connect.mirror.IdentityReplicationPolicy
  ```
- **Behavior:** Topics on the target cluster preserve the **exact same name** as the source (e.g., `orders-poc` on source is replicated as `orders-poc` on target).
- **Advantage:** When cutting over applications (producers/consumers), you only need to change the `KAFKA_BOOTSTRAP_SERVERS` address; no topic name modifications are required in application code.

### Policy B: DefaultReplicationPolicy (Standard Multi-Cluster / Active-Active)
- **Configuration:**
  ```properties
  replication.policy.class = org.apache.kafka.connect.mirror.DefaultReplicationPolicy
  ```
- **Behavior:** Topics on the target cluster are prefixed with the source cluster alias (e.g., `source.orders-poc`).
- **Advantage:** Prevents cyclic replication loops in bidirectional active-active multi-cluster environments.

---

## 4. Consumer Group Offset Translation & Checkpoints

When migrating data, offsets in the target cluster usually **do not match** offsets in the source cluster because partition compaction, retention expiration, or replication timing causes offset shifts.

MirrorMaker 2 solves this via **`MirrorCheckpointConnector`**:
1. **`emit.checkpoints.enabled = true`**: Emits offset checkpoint records mapping `(source_partition, source_offset)` to `(target_partition, target_offset)`.
2. **`sync.group.offsets.enabled = true`**: Directly commits translated offsets into the target cluster's `__consumer_offsets` topic for all synchronized consumer groups.
3. **Seamless Consumer Switch:** When the consumer group connects to the target cluster, Kafka delivers messages starting from the **translated committed offset**, ensuring no messages are skipped or reprocessed unnecessarily.

---

## 5. Application Cutover Strategy (Step-by-Step)

To achieve a clean migration with zero data loss:

### Phase 1: Replication Active & Steady State
1. Both `cluster1` and `cluster2` are active.
2. MirrorMaker 2 is continuously running on `mm2-vm`.
3. Producer writes to `cluster1`.
4. Consumer reads from `cluster1`.
5. MM2 replicates all historical and real-time records and syncs consumer group offsets to `cluster2`.

### Phase 2: Producer Cutover
1. Update Producer configuration:
   ```bash
   KAFKA_BOOTSTRAP_SERVERS=<CLUSTER2_BOOTSTRAP>
   ```
2. New messages are now written directly to `cluster2`.
3. MirrorMaker continues replicating any remaining in-flight messages from `cluster1` to `cluster2`.

### Phase 3: Consumer Cutover
1. Wait until consumer lag on `cluster1` reaches zero (all messages produced to `cluster1` have been processed or replicated).
2. Update Consumer configuration:
   ```bash
   KAFKA_BOOTSTRAP_SERVERS=<CLUSTER2_BOOTSTRAP>
   ```
3. The consumer connects to `cluster2`, picks up its committed offset synchronized by MirrorMaker 2, and processes new messages seamlessly.

### Phase 4: Decommission
1. Stop MirrorMaker 2.0 on `mm2-vm`.
2. Decommission `cluster1`.

---

## 6. What Needs to Change in Your Applications

| Component | Setting Before Migration (Cluster 1) | Setting After Migration (Cluster 2) |
|---|---|---|
| **Producer** | `KAFKA_BOOTSTRAP_SERVERS=bootstrap.cluster1...` | `KAFKA_BOOTSTRAP_SERVERS=bootstrap.cluster2...` |
| **Producer Topic** | `KAFKA_TOPIC=orders-poc` | `KAFKA_TOPIC=orders-poc` (Identity) or `source.orders-poc` (Default) |
| **Consumer** | `KAFKA_BOOTSTRAP_SERVERS=bootstrap.cluster1...` | `KAFKA_BOOTSTRAP_SERVERS=bootstrap.cluster2...` |
| **Consumer Topic** | `KAFKA_TOPIC=orders-poc` | `KAFKA_TOPIC=orders-poc` (Identity) or `source.orders-poc` (Default) |
| **Consumer Group** | `KAFKA_GROUP_ID=order-processing-group` | `KAFKA_GROUP_ID=order-processing-group` (Unchanged) |
| **Authentication** | `SASL/OAUTHBEARER` (Unchanged) | `SASL/OAUTHBEARER` (Unchanged) |

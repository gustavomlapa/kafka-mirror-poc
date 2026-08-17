# Kafka MirrorMaker 2.0 POC on Google Cloud Managed Service for Apache Kafka

[![GCP Managed Service for Apache Kafka](https://img.shields.io/badge/GCP-Managed%20Kafka-4285F4?logo=google-cloud&logoColor=white)](https://cloud.google.com/managed-service-for-apache-kafka/docs)
[![MirrorMaker 2.0](https://img.shields.io/badge/Apache-MirrorMaker%202.0-231F20?logo=apache-kafka&logoColor=white)](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/move-kafka-mirrormaker)
[![Cloud Run](https://img.shields.io/badge/Cloud%20Run-Direct%20VPC%20Egress-4285F4?logo=google-cloud&logoColor=white)](https://cloud.google.com/run/docs/configuring/vpc-direct-vpc)

Repositório de Prova de Conceito (POC) para validar e demonstrar a migração de dados e sincronização de offsets entre dois clusters do **Google Cloud Managed Service for Apache Kafka (MSAK)** em um único projeto GCP utilizando **Apache Kafka MirrorMaker 2.0 (MM2)**.

Acompanha microsserviços de exemplo em Python (**Producer** com gerador de dados mock e **Consumer**) prontos para execução no **Cloud Run** com **Direct VPC Egress** e autenticação nativa **Google Cloud IAM (SASL/OAUTHBEARER)**.

---

## 📐 Arquitetura da Solução

```
+---------------------------------------------------------------------------------------------------+
| Google Cloud Project                                                                              |
|                                                                                                   |
|  VPC: kafka-poc-vpc (Subnet: 10.0.0.0/24)                                                        |
|                                                                                                   |
|   +------------------------------------+          +------------------------------------+          |
|   | Cloud Run: kafka-producer-service  |          | Cloud Run: kafka-consumer-service  |          |
|   | - Direct VPC Egress                |          | - Direct VPC Egress                |          |
|   | - Mock Data Generator              |          | - Consumer Group Offset Tracker    |          |
|   | - IAM OAuthBearer Auth             |          | - IAM OAuthBearer Auth             |          |
|   +-----------------+------------------+          +-----------------+------------------+          |
|                     | (1. Produz mensagens)                         | (2. Consome mensagens)      |
|                     v                                               v                             |
|   +------------------------------------------------------------------------------------+          |
|   | Cluster 1 (Origem): kafka-cluster1-source                                          |          |
|   | Tópico: 'orders-poc' (Partições: 3, Replication Factor: 3)                         |          |
|   +---------------------------------+--------------------------------------------------+          |
|                                     |                                                             |
|                                     | (3. Replicação contínua & Tradução de Offsets)              |
|                                     v                                                             |
|   +------------------------------------------------------------------------------------+          |
|   | Compute Engine VM: kafka-mm2-vm                                                    |          |
|   | - Apache Kafka 3.7.x Binaries                                                      |          |
|   | - Google Cloud Managed Kafka Auth Login Handler (GcpLoginCallbackHandler)          |          |
|   | - MirrorMaker 2.0 (MirrorSourceConnector + MirrorCheckpointConnector)             |          |
|   +---------------------------------+--------------------------------------------------+          |
|                                     |                                                             |
|                                     | (4. Dados replicados + Checkpoints sincronizados)           |
|                                     v                                                             |
|   +------------------------------------------------------------------------------------+          |
|   | Cluster 2 (Destino): kafka-cluster2-target                                         |          |
|   | Tópico: 'orders-poc' (Criado automaticamente pelo MM2 ou pré-criado)               |          |
|   +------------------------------------------------------------------------------------+          |
|                                     ^                                                             |
|                                     | (5. Cutover: Apps apontam para Cluster 2)                   |
|                                     +-------------------------------------------------------------+
+---------------------------------------------------------------------------------------------------+
```

---

## 📁 Estrutura do Repositório

```
kafka-mirror-poc/
├── .gitignore                                    # Proteção contra commit de chaves, .env e credenciais
├── env.sh.example                                # Template central de variáveis de ambiente
├── README.md                                     # Guia completo da POC
├── PLAN.md                                       # Checklist operacional
├── mm2/
│   ├── connect-mirror-maker.properties.template  # Template MM2 com DefaultReplicationPolicy (source.orders-poc)
│   └── connect-mirror-maker-identity.properties.template # Template MM2 com IdentityReplicationPolicy (orders-poc)
├── scripts/
│   ├── common.sh                                 # Funções auxiliares e logger
│   ├── 00_setup_env_and_apis.sh                 # Habilita APIs do Google Cloud
│   ├── 01_setup_network_and_iam.sh              # Cria VPC, Subnet, Firewall e Service Account
│   ├── 02_deploy_cluster1.sh                    # Cria Cluster 1 (Origem) e Tópico inicial
│   ├── 03_deploy_apps_cloudrun.sh               # Constrói containers e deploya Producer/Consumer no Cloud Run
│   ├── 04_deploy_cluster2.sh                    # Cria Cluster 2 (Destino)
│   ├── 05_deploy_mm2_vm.sh                      # Cria VM GCE com Java, Kafka e GCP Auth Handler
│   ├── 06_start_replication.sh                  # Inicia MirrorMaker 2.0 na VM
│   ├── 07_cutover_apps.sh                       # Executa o cutover das apps no Cloud Run para o Cluster 2
│   └── 99_teardown.sh                           # Deleta todos os recursos criados para evitar custos
├── apps/
│   ├── auth/
│   │   └── gcp_token_provider.py                # Provider de token OAuthBearer para Python Kafka clients
│   ├── producer/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── mock_data.py                         # Gerador de dados de pedidos/pedidos realistas
│   │   └── producer.py                          # Producer FastAPI + stream em background
│   └── consumer/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── consumer.py                          # Consumer FastAPI com rastreamento de partições e offsets
└── docs/
    ├── MIGRATION_GUIDE.md                       # Teoria aprofundada de MM2, offset translation e cutover
    └── LOCAL_RUN_GUIDE.md                       # Como executar Producer e Consumer na máquina local
```

---

## 🚀 Passo a Passo de Execução da POC

### 0. Configuração Inicial do Ambiente

1. Copie o arquivo de exemplo de variáveis:
   ```bash
   cp env.sh.example env.sh
   ```
2. Edite o arquivo `env.sh` configurando o seu **`PROJECT_ID`** do GCP:
   ```bash
   export PROJECT_ID="meu-projeto-gcp-id"
   export REGION="us-central1"
   ```

---

### Passo 1: Habilitar APIs e Criar Infraestrutura de Rede & IAM

Execute os scripts de preparação:
```bash
# 1.1 Habilitar APIs necessárias (managedkafka, run, artifactregistry, compute, etc.)
./scripts/00_setup_env_and_apis.sh

# 1.2 Criar VPC, Subnet, Regras de Firewall e Service Account com permissões 'roles/managedkafka.client'
./scripts/01_setup_network_and_iam.sh
```

---

### Passo 2: Provisionar o Cluster 1 (Origem) e Tópico

Crie o cluster Kafka gerenciado de origem e o tópico de teste:
```bash
./scripts/02_deploy_cluster1.sh
```
> O script cria o cluster `kafka-cluster1-source` e o tópico `orders-poc` com 3 partições e fator de replicação 3.

---

### Passo 3: Deploy das Aplicações no Cloud Run

Compile as imagens de container via Google Cloud Build e faça o deploy no Cloud Run:
```bash
./scripts/03_deploy_apps_cloudrun.sh
```
Ao final, o script exibirá as URLs públicas dos serviços:
- **Producer App URL:** `https://kafka-producer-service-xyz.run.app`
- **Consumer App URL:** `https://kafka-consumer-service-xyz.run.app`

#### Validando o fluxo inicial no Cluster 1:
- O Producer inicia gerando automaticamente 1 pedido mock a cada 3 segundos.
- Para verificar o status e mensagens consumidas em tempo real:
  ```bash
  curl -s <CONSUMER_URL>/status | jq .
  ```
- Para disparar pedidos sob demanda:
  ```bash
  curl -X POST <PRODUCER_URL>/produce \
    -H "Content-Type: application/json" \
    -d '{"count": 5}' | jq .
  ```

---

### Passo 4: Provisionar o Cluster 2 (Destino)

Crie o segundo cluster Kafka gerenciado que receberá os dados migrados:
```bash
./scripts/04_deploy_cluster2.sh
```

---

### Passo 5: Provisionar a VM do MirrorMaker 2.0

Crie a instância Compute Engine (`kafka-mm2-vm`) na mesma VPC e Subnet com o conector de autenticação GCP:
```bash
./scripts/05_deploy_mm2_vm.sh
```
> Conforme a documentação oficial da GCP ([move-kafka-mirrormaker](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/move-kafka-mirrormaker#set-up-mirrormaker)), a VM é provisionada com Java 17, os binários do Apache Kafka 3.7.x e o conector `managed-kafka-auth-login-handler` no diretório `/opt/kafka/libs`.

---

### Passo 6: Iniciar a Replicação com MirrorMaker 2.0

Inicie o processo de espelhamento e sincronização de offsets:
```bash
# Recomendado: IdentityReplicationPolicy (mantém o nome exato 'orders-poc' no cluster de destino)
./scripts/06_start_replication.sh identity
```

#### Para acompanhar os logs de replicação do MirrorMaker 2:
```bash
gcloud compute ssh kafka-mm2-vm --zone=us-central1-a --tunnel-through-iap --command="tail -f /var/log/mirrormaker.log"
```

O MirrorMaker 2.0 irá:
1. Criar o tópico `orders-poc` no Cluster 2 (com a mesma quantidade de partições).
2. Replicar todas as mensagens existentes e as novas mensagens em tempo real.
3. Emitir checkpoints e sincronizar os offsets do consumer group (`order-processing-group`).

---

### Passo 7: Cutover das Aplicações (Virada de Chave)

Quando os dados estiverem sincronizados e o lag de replicação estiver zerado, execute a virada de chave das aplicações para o **Cluster 2**:

```bash
./scripts/07_cutover_apps.sh
```

#### O que o script de cutover faz:
1. **Producer:** Atualiza a variável `KAFKA_BOOTSTRAP_SERVERS` para o endereço do Cluster 2. Os novos pedidos passam a ser gravados diretamente no Cluster 2.
2. **Consumer:** Atualiza a variável `KAFKA_BOOTSTRAP_SERVERS` para o Cluster 2. Graças à sincronização de offsets (`sync.group.offsets.enabled = true`), o consumidor retoma o processamento no Cluster 2 **exatamente do ponto onde parou no Cluster 1**, sem duplicar ou pular mensagens!

#### Verifique o status pós-migração:
```bash
curl -s <CONSUMER_URL>/status | jq .
```
Você verá o `bootstrap_servers` apontando para o Cluster 2 e a contagem de mensagens continuando de forma contínua e transparente.

---

### Passo 8: Limpeza de Recursos (Teardown)

Após concluir todos os testes da POC, execute o script de limpeza para evitar custos residuais no projeto GCP:
```bash
./scripts/99_teardown.sh
```

---

## ⚙️ O que precisa modificar nas Aplicações durante a Migração?

Ao migrar workloads do Kafka de origem para o Kafka de destino, as alterações necessárias nas aplicações são estritamente de configuração:

| Configuração | Aplicação Origem (Cluster 1) | Aplicação Destino (Cluster 2) | Observações |
|---|---|---|---|
| `bootstrap.servers` | `bootstrap.kafka-cluster1-source...:9092` | `bootstrap.kafka-cluster2-target...:9092` | **Obrigatório:** Aponta para o novo cluster |
| `topic` | `orders-poc` | `orders-poc` | Inalterado ao usar `IdentityReplicationPolicy` |
| `group.id` | `order-processing-group` | `order-processing-group` | **Inalterado:** O MM2 sincroniza os offsets deste grupo |
| `security.protocol` | `SASL_SSL` | `SASL_SSL` | Inalterado |
| `sasl.mechanism` | `OAUTHBEARER` | `OAUTHBEARER` | Inalterado (IAM nativo do Google Cloud) |

Para mais detalhes técnicos sobre o funcionamento do `MirrorCheckpointConnector` e estratégias de replicação, consulte o [Guia de Migração Detalhado](file:///Users/gustavolapa/dev/github/kafka-mirror-poc/docs/MIGRATION_GUIDE.md).

---

## 🔒 Segurança e Gestão de Credenciais

- **Zero Secrets em Código:** Este repositório não contém senhas, chaves estáticas ou tokens hardcoded.
- **Autenticação Baseada em IAM:** Utiliza **Application Default Credentials (ADC)** e Service Accounts gerenciadas pelo Google Cloud.
- **Git Ignore Rígido:** O arquivo `.gitignore` bloqueia a adição inadvertida de arquivos `.env`, `env.sh`, chaves de service account (`*.json`), certificados (`*.pem`, `*.key`) e arquivos de estado.

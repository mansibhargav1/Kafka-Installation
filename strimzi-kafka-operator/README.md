# Strimzi Kafka on Kubernetes — Installation Guide
# Strimzi Kafka Operator — Deployment Guide
Deploy **Apache Kafka** on Kubernetes using the [Strimzi Kafka Operator](https://strimzi.io/).
A complete guide for deploying **Apache Kafka** on Kubernetes using the [Strimzi Kafka Operator](https://strimzi.io/). This repository packages the Helm chart, Custom Resource Definitions (CRDs), Kafka cluster manifests, and client connectivity assets required for a production-ready installation.
| Component        | Version |
|------------------|---------|
| Strimzi Operator | 0.46.1  |
| Apache Kafka     | 4.0.0   |
| Mode             | KRaft (dual broker/controller roles) |
---
> **Note:** Strimzi 0.46.x requires KRaft. ZooKeeper-based clusters are not supported.
## Table of Contents
1. [Introduction](#introduction)
2. [Architecture](#architecture)
3. [Version Information](#version-information)
4. [Prerequisites](#prerequisites)
5. [Repository Structure](#repository-structure)
6. [Installation Guide](#installation-guide)
7. [Understanding `dual-role.yaml`](#understanding-dual-roleyaml)
8. [Understanding `values.yaml`](#understanding-valuesyaml)
9. [Helm Chart Components](#helm-chart-components)
10. [Resources Created After Deployment](#resources-created-after-deployment)
11. [Verification](#verification)
12. [Client Connectivity](#client-connectivity)
13. [Optional: TLS User Management](#optional-tls-user-management)
14. [Operations](#operations)
15. [Configuration Reference](#configuration-reference)
16. [Troubleshooting](#troubleshooting)
17. [Security Considerations](#security-considerations)
18. [References](#references)
---
## Prerequisites
## Introduction
- Kubernetes **1.25+**
- `kubectl` configured for the target cluster
- `helm` **3.x**
- A default **StorageClass** for persistent volumes
Strimzi simplifies running Apache Kafka on Kubernetes by providing:
---
- A **Cluster Operator** that watches Custom Resources and reconciles the desired state
- **KRaft mode** support (no ZooKeeper dependency)
- **KafkaNodePool** for flexible broker/controller topology
- Built-in **Topic Operator** and **User Operator** for declarative management
- Automatic TLS certificate generation and rotation
## Installation Overview
This deployment uses a **dual-role KRaft** pattern where a single node acts as both **controller** and **broker**, suitable for development and small-scale environments.
Installation happens in two phases from the **`strimzi-kafka-operator`** directory:
---
| Phase | Command | What it does |
|-------|---------|--------------|
| **1** | `kubectl apply -f dual-role.yaml -n kafka` | Creates **2 Custom Resources**: `KafkaNodePool` + `Kafka` |
| **2** | `helm install strimzi-kafka-operator . -n kafka -f values.yaml` | Installs the **Cluster Operator**, which reads those resources and creates the actual Kafka pods |
## Architecture
```
dual-role.yaml  ──apply──►  KafkaNodePool + Kafka (CRs in kafka ns)
                                    │
helm install    ──deploy──►  Strimzi Operator (watches kafka ns)
                                    │
                                    ▼
                          Kafka Pods, Services, PVCs, Secrets
┌─────────────────────────────────────────────────────────────────────────┐
│                        Kubernetes Cluster                               │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                     Namespace: kafka                              │  │
│  │                                                                   │  │
│  │  ┌─────────────────────┐      watches      ┌──────────────────┐  │  │
│  │  │  Strimzi Cluster    │◄─────────────────│  Custom Resources │  │  │
│  │  │  Operator (Helm)    │                   │                  │  │  │
│  │  └─────────┬───────────┘                   │  KafkaNodePool   │  │  │
│  │            │ creates                        │  (dual-role)     │  │  │
│  │            ▼                                │                  │  │  │
│  │  ┌─────────────────────┐                   │  Kafka            │  │  │
│  │  │  Kafka Pod          │◄──────────────────│  (kafka-cluster)  │  │  │
│  │  │  (broker+controller)│                   └──────────────────┘  │  │
│  │  └─────────┬───────────┘                                          │  │
│  │            │                                                      │  │
│  │  ┌─────────┴───────────┐  ┌──────────────┐  ┌─────────────────┐  │  │
│  │  │  PVC (5 Gi)           │  │  Bootstrap   │  │  Entity Operator│  │  │
│  │  │  Persistent Storage   │  │  Service     │  │  (Topic + User) │  │  │
│  │  └───────────────────────┘  │  :9092/:9093 │  └─────────────────┘  │  │
│  │                            └──────────────┘                         │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```
### Deployment Flow
| Step | Action | Result |
|------|--------|--------|
| 1 | `kubectl create namespace kafka` | Isolated namespace for all Kafka resources |
| 2 | `kubectl apply -f dual-role.yaml -n kafka` | Creates `KafkaNodePool` + `Kafka` Custom Resources |
| 3 | `helm install ... -n kafka` | Deploys Cluster Operator; operator provisions Kafka cluster |
---
## Quick Start
## Version Information
```bash
cd strimzi-kafka-operator
| Component | Version | Notes |
|-----------|---------|-------|
| Strimzi Operator | 0.46.1 | Deployed via Helm chart |
| Apache Kafka | 4.0.0 | KRaft mode, metadata version `4.0-IV3` |
| Kafka Bridge (optional) | 0.32.0 | Available in `values.yaml` if needed |
| Helm Chart | 0.46.1 | Defined in `Chart.yaml` |
| API Version | `kafka.strimzi.io/v1beta2` | Used by all Strimzi CRs |
kubectl create namespace kafka
> **Important:** Strimzi 0.46.x requires **KRaft mode**. ZooKeeper-based Kafka clusters are no longer supported.
kubectl apply -f dual-role.yaml -n kafka
---
helm install strimzi-kafka-operator . \
  --namespace kafka \
  -f values.yaml
```
## Prerequisites
**First-time install only** — if CRDs are not on the cluster yet:
| Requirement | Minimum | Verification Command |
|-------------|---------|----------------------|
| Kubernetes | 1.25+ | `kubectl version` |
| Helm | 3.x | `helm version` |
| kubectl access | Cluster admin or sufficient RBAC | `kubectl get nodes` |
| StorageClass | Default or named provisioner | `kubectl get storageclass` |
| CPU / Memory | 2 CPU, 4 Gi RAM (recommended per node) | `kubectl describe nodes` |
```bash
kubectl apply -f crds/
### Namespace
All resources in this guide are deployed to the **`kafka`** namespace.
---
## Repository Structure
```
strimzi-kafka-operator/
│
├── dual-role.yaml                 # Kafka cluster manifest (apply first)
├── values.yaml                    # Helm operator configuration
├── Chart.yaml                     # Helm chart metadata
│
├── crds/                          # Custom Resource Definitions
│   ├── 040-Crd-kafka.yaml
│   ├── 041-Crd-kafkaconnect.yaml
│   ├── 042-Crd-strimzipodset.yaml
│   ├── 043-Crd-kafkatopic.yaml
│   ├── 044-Crd-kafkauser.yaml
│   ├── 045-Crd-kafkanodepool.yaml
│   ├── 046-Crd-kafkabridge.yaml
│   ├── 047-Crd-kafkaconnector.yaml
│   ├── 048-Crd-kafkamirrormaker2.yaml
│   └── 049-Crd-kafkarebalance.yaml
│
├── templates/                     # Helm templates (operator, RBAC)
│   ├── 010-ServiceAccount-*.yaml
│   ├── 020–023-* (RBAC Roles and Bindings)
│   ├── 030–033-* (Kafka broker/client delegation)
│   ├── 050-ConfigMap-*.yaml
│   ├── 051-PodDisruptionBudget-*.yaml
│   ├── 060-Deployment-*.yaml
│   ├── 070/080-ClusterRole-*.yaml
│   └── 090-ConfigMap-grafana-dashboards.yaml
│
├── files/grafana-dashboards/        # Pre-built Grafana dashboards (optional)
│
├── tls-user.yaml                  # Optional: TLS-authenticated Kafka user
├── ent-kafka-bootstrap.yaml         # Optional: reference bootstrap Service
├── client.properties              # Optional: Kafka client SSL configuration
├── ca.crt                         # Optional: cluster CA certificate (TLS)
│
├── .helmignore                    # Files excluded from Helm package
└── README.md                      # This document
```
---
## Step 1 — Create Namespace
## Installation Guide
All commands are executed from the **`strimzi-kafka-operator`** directory unless stated otherwise.
### Step 1 — Create the Namespace
```bash
kubectl create namespace kafka
```
All resources — the two Custom Resources from `dual-role.yaml` and the operator from Helm — are deployed in the `kafka` namespace.
Creates an isolated namespace for the operator, Kafka cluster, and all related resources.
---
## Step 2 — Apply `dual-role.yaml`
### Step 2 — Apply the Kafka Cluster Manifest
```bash
kubectl apply -f dual-role.yaml -n kafka
```
This single file contains **two YAML documents** separated by `---`. Applying it creates **two Custom Resources** in the `kafka` namespace:
This creates **two Custom Resources** in the `kafka` namespace:
| Resource | Name | Purpose |
|----------|------|---------|
| `KafkaNodePool` | `dual-role` | Defines node count, roles, and storage |
| `Kafka` | `kafka-cluster` | Defines Kafka version, listeners, and configuration |
Confirm the resources exist:
```bash
kubectl get kafkanodepool,kafka -n kafka
```
Expected output:
> **Note:** At this stage, only the resource definitions exist. No Kafka pods are running yet because the Cluster Operator has not been installed.
**First-time cluster only:** If Strimzi CRDs are not registered on the cluster, apply them before Step 2:
```bash
kubectl apply -f crds/
```
NAME                                 DESIRED REPLICAS   ROLES   ...
kafkanodepool.kafka.strimzi.io/dual-role   1          ...
NAME                                READY   ...
kafka.kafka.strimzi.io/kafka-cluster
---
### Step 3 — Install the Strimzi Cluster Operator
From the same directory, install the operator using Helm:
```bash
helm install strimzi-kafka-operator . \
  --namespace kafka \
  -f values.yaml
```
At this point only the **definitions** exist. No Kafka pods run yet — the operator is not installed.
| Parameter | Value | Description |
|-----------|-------|-------------|
| Release name | `strimzi-kafka-operator` | Helm release identifier |
| Chart path | `.` | Current directory (local Helm chart) |
| Namespace | `kafka` | Must match the namespace used in Step 2 |
| Values file | `values.yaml` | Operator configuration overrides |
The Cluster Operator pod starts, discovers the `KafkaNodePool` and `Kafka` resources, and begins provisioning the cluster.
---
### Resource 1: `KafkaNodePool` (name: `dual-role`)
### Step 4 — Verify the Installation
Defines **how many Kafka nodes** to run and **what role** each node plays.
```bash
# Operator pod
kubectl get pods -n kafka -l name=strimzi-cluster-operator
# Custom Resources
kubectl get kafkanodepool,kafka -n kafka
# Kafka broker pods
kubectl get pods -n kafka -l strimzi.io/cluster=kafka-cluster
# Wait for Kafka to become Ready
kubectl wait kafka/kafka-cluster -n kafka --for=condition=Ready --timeout=600s
```
---
## Understanding `dual-role.yaml`
The file `dual-role.yaml` contains **two YAML documents** separated by `---`. Each document defines one Kubernetes Custom Resource.
### Document 1 — `KafkaNodePool`
```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaNodePool
metadata:
  name: dual-role
  labels:
    strimzi.io/cluster: kafka-cluster    # links this pool to the Kafka cluster
    strimzi.io/cluster: kafka-cluster
spec:
  replicas: 1
  roles:
    - controller    # KRaft controller (cluster metadata)
    - broker        # message broker (topics/partitions)
    - controller
    - broker
  storage:
    type: jbod
    volumes:
        kraftMetadata: shared
```
| Field | Purpose |
|-------|---------|
| `name: dual-role` | Name of this node pool |
| `strimzi.io/cluster: kafka-cluster` | **Critical link** — connects this pool to the `Kafka` resource below |
| `roles: controller + broker` | Single node acts as both KRaft controller and broker (dual-role) |
| `replicas: 1` | One Kafka node |
| `storage 5Gi` | Persistent volume for Kafka data and KRaft metadata |
| Field | Value | Description |
|-------|-------|-------------|
| `metadata.name` | `dual-role` | Unique name of this node pool |
| `labels.strimzi.io/cluster` | `kafka-cluster` | Links this pool to the `Kafka` resource |
| `spec.replicas` | `1` | Number of Kafka nodes in this pool |
| `spec.roles` | `controller`, `broker` | Dual-role: one node handles both KRaft control and message brokering |
| `storage.type` | `jbod` | Just a Bunch of Disks — supports multiple volumes |
| `storage.volumes[].size` | `5Gi` | Persistent volume size per node |
| `storage.volumes[].deleteClaim` | `false` | PVC is retained when the node pool is deleted |
| `storage.volumes[].kraftMetadata` | `shared` | KRaft metadata stored on the same volume as broker data |
---
### Resource 2: `Kafka` (name: `kafka-cluster`)
### Document 2 — `Kafka`
Defines **how the Kafka cluster behaves** — version, listeners, config, and operators.
```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: kafka-cluster
  annotations:
    strimzi.io/node-pools: enabled    # use KafkaNodePool resources
    strimzi.io/kraft: enabled          # KRaft mode (no ZooKeeper)
    strimzi.io/node-pools: enabled
    strimzi.io/kraft: enabled
spec:
  kafka:
    version: 4.0.0
    userOperator: {}
```
| Field | Purpose |
|-------|---------|
| `name: kafka-cluster` | Cluster name — used in service names (e.g. `kafka-cluster-kafka-bootstrap`) |
| `strimzi.io/node-pools: enabled` | Tells Strimzi to use `KafkaNodePool` CRs instead of inline storage/replicas |
| `strimzi.io/kraft: enabled` | Runs Kafka in KRaft mode |
| `version: 4.0.0` | Apache Kafka version |
| `listeners` | Port 9092 (plain), port 9093 (TLS) — internal cluster access |
| `config` | Replication factor 1 — suited for single-node deployment |
| `entityOperator` | Deploys Topic Operator and User Operator for managing topics and users |
| Field | Value | Description |
|-------|-------|-------------|
| `metadata.name` | `kafka-cluster` | Cluster name; used in generated service names |
| `annotations.strimzi.io/node-pools` | `enabled` | Use separate `KafkaNodePool` resources for node configuration |
| `annotations.strimzi.io/kraft` | `enabled` | Run in KRaft mode (no ZooKeeper) |
| `kafka.version` | `4.0.0` | Apache Kafka broker version |
| `kafka.metadataVersion` | `4.0-IV3` | KRaft metadata format version |
| `listeners[plain]` | Port `9092`, no TLS | Internal plaintext listener for in-cluster clients |
| `listeners[tls]` | Port `9093`, TLS enabled | Internal encrypted listener |
| `config.*.replication.factor` | `1` | Single-replica settings for one-node deployment |
| `entityOperator` | Topic + User operators | Enables declarative topic and user management |
---
### How the two resources work together
### Relationship Between the Two Resources
```
KafkaNodePool (dual-role)              Kafka (kafka-cluster)
├── replicas: 1                        ├── version: 4.0.0
├── roles: controller, broker          ├── listeners: 9092, 9093
├── storage: 5Gi PVC                   ├── kafka config
└── label: strimzi.io/cluster ─────────┤── entityOperator
         kafka-cluster                 └── annotations: kraft + node-pools
                    │
                    └── Operator merges both → creates 1 pod with broker + controller
KafkaNodePool (dual-role)                    Kafka (kafka-cluster)
─────────────────────────                    ──────────────────────
HOW MANY nodes?          ◄──linked by──►    HOW should Kafka behave?
HOW are they stored?         label              Version, listeners, config
WHAT roles do they have?   kafka-cluster      Entity operators
```
The label `strimzi.io/cluster: kafka-cluster` on the `KafkaNodePool` must match the `metadata.name` of the `Kafka` resource.
---
## Step 3 — Install Operator with Helm (same path, `kafka` namespace)
## Understanding `values.yaml`
From the **same `strimzi-kafka-operator` directory**, run:
The `values.yaml` file configures the **Cluster Operator** deployed by Helm. It does not configure the Kafka broker directly — broker settings are in `dual-role.yaml`.
```bash
helm install strimzi-kafka-operator . \
  --namespace kafka \
  -f values.yaml
```
### Key Settings
| What Helm installs | Purpose |
|--------------------|---------|
| Cluster Operator Deployment | Watches `Kafka` and `KafkaNodePool` in `kafka` namespace |
| ServiceAccount + RBAC | Permissions to create pods, services, PVCs, secrets |
| CRDs (`crds/`) | Registers `Kafka`, `KafkaNodePool`, etc. as valid Kubernetes types |
| ConfigMaps | Operator configuration from `values.yaml` |
| Parameter | Default | Description |
|-----------|---------|-------------|
| `replicas` | `1` | Number of operator pod replicas |
| `watchNamespaces` | `[]` | Additional namespaces to watch; empty = operator namespace only |
| `watchAnyNamespace` | `false` | Watch all namespaces cluster-wide |
| `defaultImageRegistry` | `quay.io` | Container image registry |
| `defaultImageRepository` | `strimzi` | Container image repository |
| `defaultImageTag` | `0.46.1` | Strimzi version tag for all component images |
| `rbac.create` | `yes` | Create RBAC roles and bindings |
| `serviceAccount` | `strimzi-cluster-operator` | Service account name for the operator |
| `leaderElection.enable` | `true` | Enable leader election for HA operator deployments |
| `generateNetworkPolicy` | `true` | Auto-generate NetworkPolicy resources for Kafka |
| `generatePodDisruptionBudget` | `true` | Auto-generate PDB for Kafka pods |
Once the operator pod starts, it:
### Resource Limits (Operator Pod)
1. Finds `KafkaNodePool/dual-role` and `Kafka/kafka-cluster` in the `kafka` namespace
2. Creates a Kafka pod (StatefulSet) with controller + broker roles
3. Provisions a 5 Gi PersistentVolumeClaim
4. Creates bootstrap service: `kafka-cluster-kafka-bootstrap`
5. Generates TLS certificates for the TLS listener (9093)
6. Deploys Entity Operator (topic + user management)
| Resource | Request | Limit |
|----------|---------|-------|
| CPU | `200m` | `1000m` |
| Memory | `384Mi` | `384Mi` |
**Verify:**
### Component Image Overrides
```bash
kubectl get pods -n kafka -l name=strimzi-cluster-operator
kubectl get kafkanodepool,kafka -n kafka
kubectl get pods -n kafka -l strimzi.io/cluster=kafka-cluster
kubectl wait kafka/kafka-cluster -n kafka --for=condition=Ready --timeout=600s
The operator provisions Kafka and supporting components using images defined under:
- `kafka.image` — Kafka broker
- `topicOperator.image` — Topic Operator
- `userOperator.image` — User Operator
- `kafkaConnect.image` — Kafka Connect
- `cruiseControl.image` — Cruise Control
- `kafkaExporter.image` — Kafka Exporter
- `kafkaBridge.image` — Kafka Bridge
Leave registry/repository empty to use `defaultImageRegistry` / `defaultImageRepository` / `defaultImageTag`.
### Optional: Node Selector
Uncomment in `values.yaml` to pin the operator to specific nodes:
```yaml
nodeSelector:
  node-type: platform
```
---
## Directory Structure
## Helm Chart Components
### `Chart.yaml`
Defines chart metadata: name (`strimzi-kafka-operator`), version (`0.46.1`), and app version.
### `crds/` — Custom Resource Definitions
CRDs extend the Kubernetes API so Strimzi resources can be stored and managed.
| File | Resource Type | Purpose |
|------|---------------|---------|
| `040-Crd-kafka.yaml` | `Kafka` | Kafka cluster definition |
| `041-Crd-kafkaconnect.yaml` | `KafkaConnect` | Kafka Connect clusters |
| `042-Crd-strimzipodset.yaml` | `StrimziPodSet` | Internal pod set management |
| `043-Crd-kafkatopic.yaml` | `KafkaTopic` | Declarative topic management |
| `044-Crd-kafkauser.yaml` | `KafkaUser` | User credentials and ACLs |
| `045-Crd-kafkanodepool.yaml` | `KafkaNodePool` | KRaft node pool configuration |
| `046-Crd-kafkabridge.yaml` | `KafkaBridge` | HTTP protocol bridge |
| `047-Crd-kafkaconnector.yaml` | `KafkaConnector` | Connect connector definitions |
| `048-Crd-kafkamirrormaker2.yaml` | `KafkaMirrorMaker2` | Cross-cluster replication |
| `049-Crd-kafkarebalance.yaml` | `KafkaRebalance` | Cruise Control rebalancing |
### `templates/` — Rendered Kubernetes Manifests
| Template | Purpose |
|----------|---------|
| `010-ServiceAccount-strimzi-cluster-operator.yaml` | Service account for the operator |
| `020-ClusterRole-strimzi-cluster-operator-role.yaml` | Namespace-scoped operator permissions |
| `020-RoleBinding-strimzi-cluster-operator.yaml` | Binds namespace role to service account |
| `021-ClusterRole-strimzi-cluster-operator-role.yaml` | Cluster-wide operator permissions |
| `021-ClusterRoleBinding-strimzi-cluster-operator.yaml` | Binds cluster role to service account |
| `022-023-*` | Additional RBAC for multi-namespace watch |
| `030-033-*` | Kafka broker and client delegation roles |
| `050-ConfigMap-strimzi-cluster-operator.yaml` | Operator environment and logging config |
| `051-PodDisruptionBudget-strimzi-cluster-operator.yaml` | Operator availability during disruptions |
| `060-Deployment-strimzi-cluster-operator.yaml` | **Cluster Operator Deployment** |
| `070-ClusterRole-strimzi-admin.yaml` | Admin aggregate role for Strimzi CRDs |
| `080-ClusterRole-strimzi-view.yaml` | Read-only aggregate role for Strimzi CRDs |
| `090-ConfigMap-strimzi-grafana-dashboards.yaml` | Grafana dashboards (when `dashboards.enabled: true`) |
---
## Resources Created After Deployment
Once installation is complete, the following resources exist in the `kafka` namespace:
### Operator Resources (from Helm)
| Resource | Name |
|----------|------|
| Deployment | `strimzi-cluster-operator` |
| ServiceAccount | `strimzi-cluster-operator` |
| ConfigMap | `strimzi-cluster-operator` |
| ClusterRole / RoleBinding | Multiple (RBAC) |
### Kafka Cluster Resources (from Operator)
| Resource | Name | Description |
|----------|------|-------------|
| Pod | `kafka-cluster-dual-role-0` | Kafka broker + controller node |
| StatefulSet | `kafka-cluster-dual-role` | Manages Kafka pod lifecycle |
| Service | `kafka-cluster-kafka-bootstrap` | Client bootstrap endpoint |
| Service | `kafka-cluster-kafka-brokers` | Inter-broker communication |
| PVC | `data-0-kafka-cluster-dual-role-0` | 5 Gi persistent storage |
| Secret | `kafka-cluster-cluster-ca-cert` | Cluster CA for TLS |
| Secret | `kafka-cluster-clients-ca-cert` | Clients CA for user certificates |
| Deployment | `kafka-cluster-entity-operator` | Topic and User operators |
### Listeners and Ports
| Listener | Port | Protocol | Access |
|----------|------|----------|--------|
| `plain` | 9092 | PLAINTEXT | In-cluster |
| `tls` | 9093 | SSL/TLS | In-cluster |
### Bootstrap Address
```
strimzi-kafka-operator/
├── dual-role.yaml          # Step 2 — creates KafkaNodePool + Kafka
├── values.yaml             # Step 3 — operator config for helm install
├── Chart.yaml              # Helm chart metadata
├── crds/                   # Strimzi CRDs (installed by Helm)
├── templates/              # Operator Deployment, RBAC, ConfigMaps
├── tls-user.yaml           # Optional — TLS user with ACLs
├── client.properties       # Optional — client SSL config
└── ca.crt                  # Optional — cluster CA for TLS clients
kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092   (plain)
kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9093   (TLS)
```
---
## Verification
### Health Checks
```bash
kubectl get kafkanodepool,kafka -n kafka
kubectl get pods -n kafka
kubectl get svc -n kafka | grep bootstrap
kubectl get pvc -n kafka
```
# 1. Operator is running
kubectl get deployment strimzi-cluster-operator -n kafka
| Check | Expected |
|-------|----------|
| `KafkaNodePool/dual-role` | 1 replica, roles controller+broker |
| `Kafka/kafka-cluster` | Status `Ready` |
| Kafka pod | `Running` |
| Service | `kafka-cluster-kafka-bootstrap` on port 9092 |
| PVC | `Bound`, 5 Gi |
# 2. Custom Resources exist
kubectl get kafkanodepool dual-role -n kafka
kubectl get kafka kafka-cluster -n kafka
---
# 3. Kafka cluster is Ready
kubectl get kafka kafka-cluster -n kafka \
  -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}{"\n"}'
## Client Connectivity
# 4. Kafka pod is Running
kubectl get pods -n kafka -l strimzi.io/cluster=kafka-cluster
**Bootstrap address (in-cluster):**
# 5. Bootstrap service exists
kubectl get svc kafka-cluster-kafka-bootstrap -n kafka
# 6. PVC is bound
kubectl get pvc -n kafka
```
kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
```
**Test:**
### Expected Results
| Check | Expected Value |
|-------|----------------|
| Operator Deployment | `1/1` Ready |
| `KafkaNodePool/dual-role` | `1` replica, roles `controller+broker` |
| `Kafka/kafka-cluster` | Condition `Ready: True` |
| Kafka Pod | Status `Running`, `1/1` Ready |
| Bootstrap Service | ClusterIP, ports `9092`, `9093` |
| PVC | Status `Bound`, capacity `5Gi` |
### Functional Test
```bash
kubectl -n kafka run kafka-client --restart=Never --rm -it \
  --image=quay.io/strimzi/kafka:0.46.1-kafka-4.0.0 \
---
## Upgrade
## Client Connectivity
### In-Cluster — Plaintext
```properties
bootstrap.servers=kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
security.protocol=PLAINTEXT
```
### In-Cluster — TLS
Use `ca.crt` from this directory or export from the cluster:
```bash
helm upgrade strimzi-kafka-operator . --namespace kafka -f values.yaml
kubectl get secret kafka-cluster-cluster-ca-cert -n kafka \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt
```
Client configuration (`client.properties`):
```properties
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/path/to/ca.crt
```
### Mutual TLS (with KafkaUser)
When using `tls-user.yaml`, add keystore settings:
```properties
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/path/to/ca.crt
ssl.keystore.type=PEM
ssl.keystore.location=/path/to/user.crt
ssl.key.location=/path/to/user.key
```
---
## Optional: TLS User Management
Apply `tls-user.yaml` to create a TLS-authenticated user with topic-level ACLs:
```bash
kubectl apply -f tls-user.yaml -n kafka
```
| Field | Value |
|-------|-------|
| User name | `my-user` |
| Authentication | TLS (certificate-based) |
| Authorization | Simple ACLs |
| Permissions | Read + Write on topic `my-topic` |
Export user credentials:
```bash
kubectl get secret my-user -n kafka -o jsonpath='{.data.user\.crt}' | base64 -d > user.crt
kubectl get secret my-user -n kafka -o jsonpath='{.data.user\.key}' | base64 -d > user.key
kubectl get secret my-user -n kafka -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt
```
> The User Operator must be enabled in `dual-role.yaml` (`entityOperator.userOperator: {}`) for this to work.
---
## Operations
### Upgrade the Operator
```bash
helm upgrade strimzi-kafka-operator . \
  --namespace kafka \
  -f values.yaml
kubectl apply -f crds/
```
### Upgrade Kafka Version
1. Update `version` and `metadataVersion` in `dual-role.yaml`
2. Apply the manifest:
```bash
kubectl apply -f dual-role.yaml -n kafka
```
---
Consult the [Strimzi upgrade guide](https://strimzi.io/docs/operators/latest/deploying.html#assembly-upgrading-kafka-versions-str) for supported version paths.
## Uninstall
### Scale the Cluster
Edit `replicas` in the `KafkaNodePool` section of `dual-role.yaml`, then apply:
```bash
kubectl apply -f dual-role.yaml -n kafka
```
> When scaling beyond 1 replica, update replication factors in the `Kafka` config section accordingly.
### Uninstall
```bash
# 1. Remove Kafka cluster
kubectl delete -f dual-role.yaml -n kafka
# 2. Remove operator
helm uninstall strimzi-kafka-operator --namespace kafka
# 3. Remove CRDs (only if no other Strimzi installations exist)
kubectl delete -f crds/
# 4. Remove namespace (optional)
kubectl delete namespace kafka
```
---
## Configuration Reference
### File Summary
| File | Required | Role |
|------|----------|------|
| `dual-role.yaml` | **Yes** | Defines `KafkaNodePool` + `Kafka` — applied before Helm |
| `values.yaml` | **Yes** | Operator configuration — passed to `helm install` |
| `Chart.yaml` | **Yes** | Helm chart metadata (used automatically) |
| `crds/` | **Yes** | CRDs installed by Helm or applied manually on first install |
| `templates/` | **Yes** | Helm templates rendered during `helm install` |
| `tls-user.yaml` | Optional | TLS user with ACLs |
| `ent-kafka-bootstrap.yaml` | Optional | Reference Service for external access patterns |
| `client.properties` | Optional | Sample SSL client configuration |
| `ca.crt` | Optional | Cluster CA for TLS client trust |
| `files/grafana-dashboards/` | Optional | Enable via `dashboards.enabled: true` in values |
### `ent-kafka-bootstrap.yaml`
A reference Service manifest for external client discovery. Strimzi automatically creates `kafka-cluster-kafka-bootstrap` — this file is only needed for custom external access configurations. Update `strimzi.io/cluster` labels to match your cluster name before use.
---
## Troubleshooting
| Symptom | Fix |
|---------|-----|
| `no matches for kind "Kafka"` | `kubectl apply -f crds/` |
| CRs exist but no pods | Check operator: `kubectl get pods -n kafka -l name=strimzi-cluster-operator` |
| Kafka `Not Ready` | `kubectl describe kafka kafka-cluster -n kafka` |
| PVC `Pending` | `kubectl get sc` — ensure StorageClass exists |
| Symptom | Possible Cause | Resolution |
|---------|----------------|------------|
| `no matches for kind "Kafka"` | CRDs not installed | `kubectl apply -f crds/` |
| `no matches for kind "KafkaNodePool"` | CRDs not installed | `kubectl apply -f crds/` |
| CRs exist but no Kafka pods | Operator not running | `kubectl get pods -n kafka -l name=strimzi-cluster-operator` |
| Operator pod `CrashLoopBackOff` | RBAC or image pull issue | `kubectl logs -n kafka -l name=strimzi-cluster-operator` |
| Kafka status `Not Ready` | Storage or config issue | `kubectl describe kafka kafka-cluster -n kafka` |
| PVC status `Pending` | No StorageClass available | `kubectl get storageclass` |
| Pod status `Pending` | Insufficient cluster resources | `kubectl describe pod -n kafka <pod-name>` |
| TLS connection refused | Wrong CA or port | Verify port `9093` and export CA from cluster secret |
| User secret not created | User Operator disabled | Ensure `entityOperator.userOperator: {}` in `dual-role.yaml` |
### Diagnostic Commands
```bash
# Kafka resource events and status
kubectl describe kafka kafka-cluster -n kafka
kubectl logs -n kafka -l name=strimzi-cluster-operator --tail=100
# Operator logs
kubectl logs -n kafka -l name=strimzi-cluster-operator --tail=200
# Kafka broker logs
kubectl logs -n kafka kafka-cluster-dual-role-0
# Recent namespace events
kubectl get events -n kafka --sort-by='.lastTimestamp'
# All resources in namespace
kubectl get all,pvc,secret -n kafka
```
---
## Security Considerations
| Topic | Current Configuration | Recommendation |
|-------|----------------------|----------------|
| Plaintext listener | Enabled on port 9092 | Use TLS listener (9093) for production workloads |
| Replication factor | Set to 1 | Increase to 3 for production HA |
| Node count | 1 replica | Scale to 3+ nodes for fault tolerance |
| ACLs | Available via `KafkaUser` | Define users with least-privilege ACLs |
| Network policies | Auto-generated by operator | Review and restrict ingress/egress as needed |
| Certificate rotation | Managed by Strimzi | Monitor certificate expiry via operator logs |
---
## References
- [Strimzi Documentation](https://strimzi.io/documentation/)
- [Kafka Node Pools (KRaft)](https://strimzi.io/docs/operators/latest/deploying.html#proc-configuring-kafka-node-pools-str)
- [Strimzi Official Documentation](https://strimzi.io/documentation/)
- [Deploying the Cluster Operator with Helm](https://strimzi.io/docs/operators/latest/deploying.html#deploying-cluster-operator-helm-chart-str)
- [Configuring Kafka Node Pools (KRaft)](https://strimzi.io/docs/operators/latest/deploying.html#proc-configuring-kafka-node-pools-str)
- [Kafka Listener Configuration](https://strimzi.io/docs/operators/latest/deploying.html#con-kafka-listeners-str)
- [Managing Kafka Users](https://strimzi.io/docs/operators/latest/deploying.html#con-kafka-authentication-str)
- [Upgrading Kafka Versions](https://strimzi.io/docs/operators/latest/deploying.html#assembly-upgrading-kafka-versions-str)
- [Strimzi GitHub Repository](https://github.com/strimzi/strimzi-kafka-operator)
---
## License
Strimzi is licensed under the [Apache License, Version 2.0](https://github.com/strimzi/strimzi-kafka-operator/blob/main/LICENSE).

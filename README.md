# Kafka Installation
This repository contains Helm charts and manifests for deploying Apache Kafka on Kubernetes.
Deploy **Apache Kafka** on Kubernetes using Strimzi, and manage it through a web UI using **Kafka UI** (Provectus).
## Components
| Directory | Purpose |
|-----------|---------|
| [`strimzi-kafka-operator/`](strimzi-kafka-operator/) | Strimzi operator Helm chart, CRDs, and Kafka cluster manifest |
| [`kafka-ui/`](kafka-ui/) | Kafka UI Helm chart for browser-based cluster management |
| Directory | Description | Documentation |
|-----------|-------------|---------------|
| [`strimzi-kafka-operator/`](strimzi-kafka-operator/) | Strimzi Cluster Operator, CRDs, and Kafka cluster manifests | [Installation Guide](strimzi-kafka-operator/README.md) |
| [`kafka-ui/`](kafka-ui/) | Kafka UI Helm chart for cluster management and monitoring | See chart `values.yaml` |
Both components are installed in the **`kafka`** namespace.
## Quick Start (Strimzi)
---
## Table of Contents
1. [Architecture](#architecture)
2. [Prerequisites](#prerequisites)
3. [Part 1 — Strimzi Kafka Cluster](#part-1--strimzi-kafka-cluster)
4. [Part 2 — Kafka UI](#part-2--kafka-ui)
5. [Access Kafka UI via Port Forward](#access-kafka-ui-via-port-forward)
6. [Verification](#verification)
7. [Uninstall](#uninstall)
8. [References](#references)
---
## Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                     Namespace: kafka                            │
│                                                                 │
│  ┌──────────────────────┐         ┌──────────────────────────┐   │
│  │  Strimzi Operator    │──────►│  Kafka Cluster           │   │
│  │  (Helm)                │         │  kafka-cluster           │   │
│  └──────────────────────┘         │  bootstrap :9092 / :9093 │   │
│                                      └────────────┬─────────────┘   │
│                                                   │                 │
│  ┌──────────────────────┐                        │                 │
│  │  Kafka UI (Helm)      │────────────────────────┘                 │
│  │  Release: kafka-ui    │   connects to bootstrap service          │
│  └──────────────────────┘                                          │
│           │                                                         │
│           └── port-forward ──► http://localhost:8080/kafka-ui        │
└─────────────────────────────────────────────────────────────────┘
```
---
## Prerequisites
- Kubernetes **1.25+**
- `kubectl` configured for the target cluster
- `helm` **3.x**
- A **StorageClass** for Kafka persistent volumes
---
## Part 1 — Strimzi Kafka Cluster
Detailed documentation: **[strimzi-kafka-operator/README.md](strimzi-kafka-operator/README.md)**
### Step 1 — Create namespace
```bash
kubectl create namespace kafka
```
### Step 2 — Apply Kafka cluster manifest
From the `strimzi-kafka-operator` directory, apply `dual-role.yaml`. This creates **two Custom Resources**:
| Resource | Name | Purpose |
|----------|------|---------|
| `KafkaNodePool` | `dual-role` | Node count, roles (broker + controller), storage |
| `Kafka` | `kafka-cluster` | Kafka version, listeners, configuration |
```bash
cd strimzi-kafka-operator
kubectl apply -f dual-role.yaml -n kafka
```
**First-time install only** — if Strimzi CRDs are not on the cluster:
```bash
kubectl apply -f crds/
```
### Step 3 — Install Strimzi operator
From the same `strimzi-kafka-operator` directory:
```bash
helm install strimzi-kafka-operator . \
  --namespace kafka \
  -f values.yaml
```
The operator detects the `KafkaNodePool` and `Kafka` resources and provisions the Kafka cluster.
### Verify Kafka cluster
```bash
kubectl get pods -n kafka -l name=strimzi-cluster-operator
kubectl get kafkanodepool,kafka -n kafka
kubectl wait kafka/kafka-cluster -n kafka --for=condition=Ready --timeout=600s
```
**Bootstrap address (used by Kafka UI):**
```
kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
```
---
## Part 2 — Kafka UI
Kafka UI provides a web interface to browse topics, consumer groups, brokers, and messages.
| Setting | Value |
|---------|-------|
| Chart | `kafka-ui` (Provectus) |
| Image | `provectuslabs/kafka-ui:v0.7.2` |
| Namespace | `kafka` |
| Helm release name | `kafka-ui` |
| Context path | `/kafka-ui` |
| Default login | `admin` / `admin` |
### Step 1 — Review configuration
The file `kafka-ui/values.yaml` connects Kafka UI to the Strimzi cluster:
```yaml
yamlApplicationConfig:
  kafka:
    clusters:
      - name: kafka-cluster
        bootstrapServers: kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
  auth:
    type: LOGIN_FORM
  spring:
    security:
      user:
        name: "admin"
        password: "admin"
  server:
    servlet:
      context-path: "/kafka-ui"
```
| Field | Description |
|-------|-------------|
| `bootstrapServers` | Strimzi bootstrap service inside the `kafka` namespace |
| `context-path` | URL path prefix — UI is served at `/kafka-ui` |
| `auth.type` | Login form enabled |
| `spring.security.user` | UI login credentials |
### Step 2 — Install Kafka UI
Move into the `kafka-ui` directory and install with release name **`kafka-ui`** in the **`kafka`** namespace:
```bash
cd kafka-ui
helm install kafka-ui . \
  --namespace kafka \
  -f values.yaml
```
### Step 3 — Verify Kafka UI pod
```bash
kubectl get pods -n kafka -l app.kubernetes.io/instance=kafka-ui
kubectl get svc kafka-ui -n kafka
```
Expected output:
| Resource | Expected |
|----------|----------|
| Pod | `Running`, `1/1` Ready |
| Service `kafka-ui` | ClusterIP, port `80` → target `8080` |
---
## Access Kafka UI via Port Forward
Kafka UI runs as a `ClusterIP` service. Use `kubectl port-forward` to access it from your local machine.
### Option A — Port forward via Service (recommended)
```bash
kubectl port-forward svc/kafka-ui 8080:80 -n kafka
```
### Option B — Port forward via Pod
```bash
kubectl port-forward \
  $(kubectl get pod -n kafka -l app.kubernetes.io/instance=kafka-ui -o jsonpath='{.items[0].metadata.name}') \
  8080:8080 -n kafka
```
### Open in browser
```
http://localhost:8080/kafka-ui
```
| Field | Value |
|-------|-------|
| Username | `admin` |
| Password | `admin` |
After login, select the **kafka-cluster** cluster from the UI to browse topics, brokers, and consumer groups.
---
## Verification
### Kafka cluster
```bash
kubectl get kafka kafka-cluster -n kafka
kubectl get pods -n kafka -l strimzi.io/cluster=kafka-cluster
kubectl get svc kafka-cluster-kafka-bootstrap -n kafka
```
### Kafka UI
```bash
kubectl get deployment kafka-ui -n kafka
kubectl get pods -n kafka -l app.kubernetes.io/instance=kafka-ui
kubectl logs -n kafka -l app.kubernetes.io/instance=kafka-ui --tail=50
```
### End-to-end test
1. Port-forward Kafka UI (see above)
2. Open `http://localhost:8080/kafka-ui`
3. Log in with `admin` / `admin`
4. Confirm the **kafka-cluster** cluster shows as connected
5. Browse **Brokers** — should list the Kafka broker from Strimzi
---
## Uninstall
```bash
# Remove Kafka UI
helm uninstall kafka-ui --namespace kafka
# Remove Kafka cluster
kubectl delete -f strimzi-kafka-operator/dual-role.yaml -n kafka
# Remove Strimzi operator
helm uninstall strimzi-kafka-operator --namespace kafka
# Remove namespace (optional)
kubectl delete namespace kafka
```
---
## Quick Reference
### Full install (both components)
```bash
# --- Strimzi Kafka ---
kubectl create namespace kafka
cd strimzi-kafka-operator
kubectl apply -f dual-role.yaml -n kafka
helm install strimzi-kafka-operator . --namespace kafka -f values.yaml
kubectl wait kafka/kafka-cluster -n kafka --for=condition=Ready --timeout=600s
# --- Kafka UI ---
cd ../kafka-ui
helm install kafka-ui . --namespace kafka -f values.yaml
# --- Access UI ---
kubectl port-forward svc/kafka-ui 8080:80 -n kafka
# Open: http://localhost:8080/kafka-ui  (admin / admin)
```
See the **[Strimzi Installation Guide](strimzi-kafka-operator/README.md)** for full details, file descriptions, and verification steps.
---
## References
- [Strimzi Installation Guide](strimzi-kafka-operator/README.md)
- [Strimzi Documentation](https://strimzi.io/documentation/)
- [Kafka UI (Provectus) GitHub](https://github.com/provectus/kafka-ui)

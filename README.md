# Kubernetes GPU Operator (Control Plane)

Custom Kubernetes operator in Go (controller-runtime) that manages distributed GPU workloads via CRDs (`GPUJob`, `TrainingJob`). Implements reconciliation loops, GPU-aware scheduling (bin-packing + node affinity), retry/backoff, and idempotent job execution.

## Features

| Capability | Implementation |
|---|---|
| CRDs | `GPUJob` (single GPU workload), `TrainingJob` (master + workers) |
| Reconciliation | Desired vs actual state via controller-runtime + Kubernetes API / etcd |
| Scheduling | BinPack (reduce fragmentation) or Spread; node affinity on selected node |
| Fault tolerance | Exponential backoff, max retries, preemption/failure recovery |
| Idempotency | Stable pod names + `idempotencyKey` labels avoid duplicate execution |
| Lifecycle | Pending → Scheduled → Running → Succeeded/Failed + finalizers |

## Project layout

```
api/v1alpha1/          # GPUJob & TrainingJob API types
controllers/           # Reconcile loops
pkg/scheduler/         # GPU bin-pack / spread scoring
pkg/retry/             # Backoff helpers
cmd/main.go            # Operator entrypoint
config/crd/            # CRD manifests
config/rbac/           # RBAC
config/manager/        # Deployment
config/samples/        # Example CRs
Dockerfile             # Multi-stage CUDA-workload control-plane image
```

## Prerequisites

- Go 1.22+
- Docker (to build the operator image)
- kubectl + a cluster (kind/minikube/GKE etc.)
- Nodes advertising `nvidia.com/gpu` (or labels `gpu.operator.io/count`)

## Quick start

```bash
# Fetch deps & build
make tidy
make build

# Unit tests (retry + scheduler helpers)
make test

# Install CRDs and deploy operator
make docker-build IMG=gpu-operator:latest
# kind load docker-image gpu-operator:latest   # if using kind
make deploy

# Submit sample workloads
make samples

# Watch status
kubectl get gpujobs,trainingjobs -A
kubectl describe gpujob cuda-vector-add
```

### Run locally against a cluster

```bash
make install          # apply CRDs
make run              # run manager with your KUBECONFIG
```

## CRD examples

### GPUJob — single CUDA job with bin-packing

```yaml
apiVersion: gpu.operator.io/v1alpha1
kind: GPUJob
metadata:
  name: cuda-vector-add
spec:
  image: nvcr.io/nvidia/k8s/cuda-sample:vectoradd-cuda11.7.1-ubuntu20.04
  gpuCount: 1
  schedulingStrategy: BinPack   # or Spread
  idempotencyKey: cuda-vector-add-v1
  retryPolicy:
    maxRetries: 3
    backoffBaseSeconds: 10
    backoffMaxSeconds: 120
```

### TrainingJob — distributed PyTorch-style roles

```yaml
apiVersion: gpu.operator.io/v1alpha1
kind: TrainingJob
metadata:
  name: pytorch-dist-demo
spec:
  framework: pytorch
  schedulingStrategy: BinPack
  master:
    replicas: 1
    gpusPerReplica: 1
    image: pytorch/pytorch:2.1.0-cuda12.1-cudnn8-runtime
  workers:
    replicas: 2
    gpusPerReplica: 1
    image: pytorch/pytorch:2.1.0-cuda12.1-cudnn8-runtime
```

Pods receive `RANK`, `WORLD_SIZE`, `ROLE`, and `FRAMEWORK` env vars for distributed frameworks.

## Scheduling model

1. List Ready nodes with allocatable `nvidia.com/gpu` (or `gpu.operator.io/count` label).
2. Subtract GPUs already requested by non-terminal pods.
3. Filter by `nodeSelector` / GPU memory labels.
4. Score candidates:
   - **BinPack**: prefer higher utilization nodes that still fit (less fragmentation).
   - **Spread**: prefer emptier nodes.
5. Pin the pod with required node affinity on `kubernetes.io/hostname` plus preferred affinity on `gpu.operator.io/present=true`.

## Retry & idempotency

On pod failure or preemption:

1. Delete the failed pod(s).
2. Increment `status.retryCount`.
3. Set `status.lastBackoffUntil` using `base * 2^attempt` (capped).
4. Requeue after backoff; recreate pods with a new attempt key so execution stays idempotent per attempt.

## Labeling GPU nodes (dev / simulation)

```bash
kubectl label node <node> gpu.operator.io/present=true gpu.operator.io/count=4 gpu.operator.io/memory-mib=16384
# Or patch allocatable nvidia.com/gpu via device plugin / NVIDIA GPU Operator in real clusters
```

## License

Apache-2.0

# Simulate GPU capacity on a kind/minikube node (no real NVIDIA device plugin).
# Usage: ./scripts/label-gpu-node.sh <node-name> [gpu-count]
set -euo pipefail
NODE="${1:?node name required}"
COUNT="${2:-4}"
kubectl label node "$NODE" \
  gpu.operator.io/present=true \
  gpu.operator.io/count="$COUNT" \
  gpu.operator.io/memory-mib=16384 \
  --overwrite
echo "Labeled $NODE with $COUNT simulated GPUs (label-based allocatable)."
echo "For real GPUs, install the NVIDIA device plugin so nodes expose nvidia.com/gpu."

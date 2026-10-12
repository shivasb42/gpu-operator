package scheduler

import (
	"testing"

	gpuv1alpha1 "github.com/shivasb42/gpu-operator/api/v1alpha1"
)

func TestBinPackPrefersFullerNode(t *testing.T) {
	// Node A: 4 GPUs, 3 used → high util, room for 1
	scoreA, _ := scoreNode(4, 3, 1, gpuv1alpha1.BinPack)
	// Node B: 4 GPUs, 0 used → empty
	scoreB, _ := scoreNode(4, 0, 1, gpuv1alpha1.BinPack)
	if scoreA <= scoreB {
		t.Fatalf("bin-pack should prefer fuller node: A=%.2f B=%.2f", scoreA, scoreB)
	}
}

func TestSpreadPrefersEmptierNode(t *testing.T) {
	scoreA, _ := scoreNode(4, 3, 1, gpuv1alpha1.Spread)
	scoreB, _ := scoreNode(4, 0, 1, gpuv1alpha1.Spread)
	if scoreB <= scoreA {
		t.Fatalf("spread should prefer emptier node: A=%.2f B=%.2f", scoreA, scoreB)
	}
}

func TestBuildNodeAffinity(t *testing.T) {
	aff := BuildNodeAffinity("gpu-node-1", map[string]string{"disk": "ssd"})
	if aff.NodeAffinity == nil || aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		t.Fatal("expected required node affinity")
	}
	terms := aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchExpressions) < 2 {
		t.Fatalf("unexpected match expressions: %+v", terms)
	}
}

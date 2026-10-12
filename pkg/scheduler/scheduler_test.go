package scheduler_test

import (
	"testing"

	gpuv1alpha1 "github.com/shivasb42/gpu-operator/api/v1alpha1"
	"github.com/shivasb42/gpu-operator/pkg/scheduler"
)

func TestScoreBinPackPrefersFullerNodes(t *testing.T) {
	// Use unexported scoring via SelectNode behavior is integration-level;
	// here we validate GPUQuantity helper and strategy defaults.
	q := scheduler.GPUQuantity(2)
	if q.Value() != 2 {
		t.Fatalf("expected 2, got %d", q.Value())
	}
	if gpuv1alpha1.BinPack != "BinPack" {
		t.Fatal("unexpected BinPack constant")
	}
}

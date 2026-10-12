package retry_test

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	gpuv1alpha1 "github.com/shivasb42/gpu-operator/api/v1alpha1"
	"github.com/shivasb42/gpu-operator/pkg/retry"
)

func TestExponentialBackoff(t *testing.T) {
	p := gpuv1alpha1.RetryPolicy{BackoffBaseSeconds: 10, BackoffMaxSeconds: 300}
	if d := retry.NextBackoff(0, p); d != 10*time.Second {
		t.Fatalf("retry 0: got %v", d)
	}
	if d := retry.NextBackoff(1, p); d != 20*time.Second {
		t.Fatalf("retry 1: got %v", d)
	}
	if d := retry.NextBackoff(2, p); d != 40*time.Second {
		t.Fatalf("retry 2: got %v", d)
	}
	if d := retry.NextBackoff(10, p); d != 300*time.Second {
		t.Fatalf("capped: got %v", d)
	}
}

func TestCanRetry(t *testing.T) {
	p := gpuv1alpha1.RetryPolicy{MaxRetries: 3}
	if !retry.CanRetry(0, p) || !retry.CanRetry(2, p) {
		t.Fatal("expected can retry")
	}
	if retry.CanRetry(3, p) {
		t.Fatal("expected exhausted")
	}
}

func TestInBackoff(t *testing.T) {
	now := time.Now()
	until := metav1.NewTime(now.Add(30 * time.Second))
	if !retry.InBackoff(&until, now) {
		t.Fatal("expected in backoff")
	}
	if retry.InBackoff(&until, now.Add(31*time.Second)) {
		t.Fatal("expected expired")
	}
}

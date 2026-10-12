/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
*/

// Package retry implements exponential backoff and retry policy helpers
// for fault-tolerant GPU job execution under node failures and preemptions.
package retry

import (
	"math"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	gpuv1alpha1 "github.com/shivasb42/gpu-operator/api/v1alpha1"
)

const (
	defaultMaxRetries     int32 = 3
	defaultBackoffBaseSec int32 = 10
	defaultBackoffMaxSec  int32 = 300
)

// Normalized returns a RetryPolicy with defaults applied.
func Normalized(p gpuv1alpha1.RetryPolicy) gpuv1alpha1.RetryPolicy {
	if p.MaxRetries == 0 {
		p.MaxRetries = defaultMaxRetries
	}
	if p.BackoffBaseSeconds == 0 {
		p.BackoffBaseSeconds = defaultBackoffBaseSec
	}
	if p.BackoffMaxSeconds == 0 {
		p.BackoffMaxSeconds = defaultBackoffMaxSec
	}
	return p
}

// CanRetry reports whether another attempt is allowed.
func CanRetry(retryCount int32, p gpuv1alpha1.RetryPolicy) bool {
	p = Normalized(p)
	return retryCount < p.MaxRetries
}

// NextBackoff computes exponential backoff: base * 2^retryCount, capped at max.
func NextBackoff(retryCount int32, p gpuv1alpha1.RetryPolicy) time.Duration {
	p = Normalized(p)
	secs := float64(p.BackoffBaseSeconds) * math.Pow(2, float64(retryCount))
	max := float64(p.BackoffMaxSeconds)
	if secs > max {
		secs = max
	}
	return time.Duration(secs) * time.Second
}

// Until returns metav1.Time for when the next retry is allowed.
func Until(retryCount int32, p gpuv1alpha1.RetryPolicy, now time.Time) metav1.Time {
	return metav1.NewTime(now.Add(NextBackoff(retryCount, p)))
}

// InBackoff returns true if now is still before the backoff deadline.
func InBackoff(until *metav1.Time, now time.Time) bool {
	if until == nil {
		return false
	}
	return now.Before(until.Time)
}

// Remaining returns duration until backoff expires (0 if expired/nil).
func Remaining(until *metav1.Time, now time.Time) time.Duration {
	if until == nil || !now.Before(until.Time) {
		return 0
	}
	return until.Time.Sub(now)
}

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
*/

// Package scheduler implements GPU-aware placement policies.
// It scores nodes using bin-packing (prefer fuller nodes) or spread
// (prefer emptier nodes) and applies node affinity constraints to
// reduce GPU resource fragmentation on multi-GPU nodes.
package scheduler

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	gpuv1alpha1 "github.com/shivasb42/gpu-operator/api/v1alpha1"
)

const (
	// NVIDIAResourceName is the extended resource name for NVIDIA GPUs.
	NVIDIAResourceName = "nvidia.com/gpu"

	// LabelGPUPresent marks nodes that have GPUs.
	LabelGPUPresent = "gpu.operator.io/present"

	// LabelGPUCount is an optional label with total GPU count on the node.
	LabelGPUCount = "gpu.operator.io/count"

	// LabelGPUMemory is optional GPU memory in MiB.
	LabelGPUMemory = "gpu.operator.io/memory-mib"
)

// Request describes a GPU placement request.
type Request struct {
	GPUCount     int64
	GPUMemoryMiB int64
	Strategy     gpuv1alpha1.SchedulingStrategy
	NodeSelector map[string]string
}

// NodeScore is a scored candidate node.
type NodeScore struct {
	Node          corev1.Node
	Allocatable   int64
	Requested     int64
	Available     int64
	Score         float64
	Fragmentation float64
}

// Scheduler selects nodes for GPU workloads.
type Scheduler struct {
	Client client.Client
}

// SelectNode picks the best node for the request, or returns an error if none fit.
func (s *Scheduler) SelectNode(ctx context.Context, req Request) (*NodeScore, error) {
	logger := log.FromContext(ctx)

	nodes := &corev1.NodeList{}
	if err := s.Client.List(ctx, nodes); err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	pods := &corev1.PodList{}
	if err := s.Client.List(ctx, pods); err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	requestedByNode := aggregateGPURequests(pods.Items)
	strategy := req.Strategy
	if strategy == "" {
		strategy = gpuv1alpha1.BinPack
	}

	var candidates []NodeScore
	for i := range nodes.Items {
		node := nodes.Items[i]
		if !nodeMatchesSelector(&node, req.NodeSelector) {
			continue
		}
		if !isSchedulable(&node) {
			continue
		}

		allocatable := gpuAllocatable(&node)
		if allocatable <= 0 {
			continue
		}
		if req.GPUMemoryMiB > 0 {
			mem := gpuMemoryMiB(&node)
			if mem > 0 && mem < req.GPUMemoryMiB {
				continue
			}
		}

		requested := requestedByNode[node.Name]
		available := allocatable - requested
		if available < req.GPUCount {
			continue
		}

		score, frag := scoreNode(allocatable, requested, req.GPUCount, strategy)
		candidates = append(candidates, NodeScore{
			Node:          node,
			Allocatable:   allocatable,
			Requested:     requested,
			Available:     available,
			Score:         score,
			Fragmentation: frag,
		})
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no node with %d available GPU(s)", req.GPUCount)
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score == candidates[j].Score {
			return candidates[i].Node.Name < candidates[j].Node.Name
		}
		return candidates[i].Score > candidates[j].Score
	})

	best := candidates[0]
	logger.Info("selected GPU node",
		"node", best.Node.Name,
		"strategy", strategy,
		"score", best.Score,
		"available", best.Available,
		"allocatable", best.Allocatable,
		"fragmentation", best.Fragmentation,
	)
	return &best, nil
}

// BuildNodeAffinity returns affinity that pins the pod to the selected node
// and prefers nodes already labeled as GPU-present (reduces fragmentation).
func BuildNodeAffinity(nodeName string, selector map[string]string) *corev1.Affinity {
	matchExprs := []corev1.NodeSelectorRequirement{
		{
			Key:      "kubernetes.io/hostname",
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{nodeName},
		},
	}
	for k, v := range selector {
		matchExprs = append(matchExprs, corev1.NodeSelectorRequirement{
			Key:      k,
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{v},
		})
	}

	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: matchExprs,
				}},
			},
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
				Weight: 100,
				Preference: corev1.NodeSelectorTerm{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      LabelGPUPresent,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"true"},
					}},
				},
			}},
		},
	}
}

func scoreNode(allocatable, requested, need int64, strategy gpuv1alpha1.SchedulingStrategy) (score, fragmentation float64) {
	if allocatable <= 0 {
		return 0, 1
	}
	utilization := float64(requested) / float64(allocatable)
	remainingAfter := float64(allocatable-requested-need) / float64(allocatable)
	// Fragmentation: leftover GPUs that are hard to pack later (prefer 0 remainder).
	fragmentation = remainingAfter

	switch strategy {
	case gpuv1alpha1.Spread:
		// Prefer emptier nodes.
		score = (1.0 - utilization) * 100
	default: // BinPack
		// Prefer fuller nodes that still fit — classic bin packing.
		score = utilization*100 + (1.0-fragmentation)*10
	}
	return score, fragmentation
}

func aggregateGPURequests(pods []corev1.Pod) map[string]int64 {
	out := make(map[string]int64)
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName == "" {
			continue
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		var total int64
		for _, c := range pod.Spec.Containers {
			if q, ok := c.Resources.Requests[NVIDIAResourceName]; ok {
				total += q.Value()
			} else if q, ok := c.Resources.Limits[NVIDIAResourceName]; ok {
				total += q.Value()
			}
		}
		out[pod.Spec.NodeName] += total
	}
	return out
}

func gpuAllocatable(node *corev1.Node) int64 {
	if q, ok := node.Status.Allocatable[NVIDIAResourceName]; ok {
		return q.Value()
	}
	if v, ok := node.Labels[LabelGPUCount]; ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return n
		}
	}
	return 0
}

func gpuMemoryMiB(node *corev1.Node) int64 {
	if v, ok := node.Labels[LabelGPUMemory]; ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return n
		}
	}
	return 0
}

func nodeMatchesSelector(node *corev1.Node, selector map[string]string) bool {
	for k, v := range selector {
		if node.Labels[k] != v {
			return false
		}
	}
	return true
}

func isSchedulable(node *corev1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady && c.Status != corev1.ConditionTrue {
			return false
		}
	}
	return true
}

// GPUQuantity returns a resource.Quantity for nvidia.com/gpu.
func GPUQuantity(count int64) resource.Quantity {
	return *resource.NewQuantity(count, resource.DecimalSI)
}

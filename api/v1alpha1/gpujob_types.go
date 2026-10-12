/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GPUJobPhase represents the lifecycle phase of a GPUJob.
type GPUJobPhase string

const (
	GPUJobPending   GPUJobPhase = "Pending"
	GPUJobScheduled GPUJobPhase = "Scheduled"
	GPUJobRunning   GPUJobPhase = "Running"
	GPUJobSucceeded GPUJobPhase = "Succeeded"
	GPUJobFailed    GPUJobPhase = "Failed"
)

// SchedulingStrategy controls how GPU resources are packed onto nodes.
type SchedulingStrategy string

const (
	// BinPack prefers filling nodes that already have GPUs in use (reduce fragmentation).
	BinPack SchedulingStrategy = "BinPack"
	// Spread prefers spreading workloads across nodes.
	Spread SchedulingStrategy = "Spread"
)

// RetryPolicy defines fault-tolerance behavior for GPU jobs.
type RetryPolicy struct {
	// MaxRetries is the maximum number of retries after failure or preemption.
	// +kubebuilder:default=3
	// +optional
	MaxRetries int32 `json:"maxRetries,omitempty"`

	// BackoffBaseSeconds is the base delay for exponential backoff.
	// +kubebuilder:default=10
	// +optional
	BackoffBaseSeconds int32 `json:"backoffBaseSeconds,omitempty"`

	// BackoffMaxSeconds caps the backoff delay.
	// +kubebuilder:default=300
	// +optional
	BackoffMaxSeconds int32 `json:"backoffMaxSeconds,omitempty"`
}

// GPUJobSpec defines the desired state of GPUJob.
type GPUJobSpec struct {
	// Image is the container image to run (typically CUDA-enabled).
	Image string `json:"image"`

	// Command overrides the image entrypoint.
	// +optional
	Command []string `json:"command,omitempty"`

	// Args are arguments passed to the command.
	// +optional
	Args []string `json:"args,omitempty"`

	// GPUCount is the number of GPUs required by this job.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	GPUCount int32 `json:"gpuCount"`

	// GPUMemoryMiB optionally requires a minimum GPU memory (MiB).
	// +optional
	GPUMemoryMiB int32 `json:"gpuMemoryMiB,omitempty"`

	// SchedulingStrategy selects bin-packing or spread placement.
	// +kubebuilder:default=BinPack
	// +kubebuilder:validation:Enum=BinPack;Spread
	// +optional
	SchedulingStrategy SchedulingStrategy `json:"schedulingStrategy,omitempty"`

	// NodeSelector constrains candidate nodes (merged with GPU affinity).
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Affinity is additional pod affinity/anti-affinity.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// Tolerations for GPU / preemptible node taints.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Resources for non-GPU requests/limits (CPU/memory).
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Env environment variables injected into the job pod.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// RetryPolicy controls retries after node failure or preemption.
	// +optional
	RetryPolicy RetryPolicy `json:"retryPolicy,omitempty"`

	// PriorityClassName optionally sets scheduling priority.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// IdempotencyKey uniquely identifies this logical job execution.
	// Reconciles reuse the same key to avoid duplicate work.
	// +optional
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
}

// GPUJobStatus defines the observed state of GPUJob.
type GPUJobStatus struct {
	// Phase is the current lifecycle phase.
	// +optional
	Phase GPUJobPhase `json:"phase,omitempty"`

	// AssignedNode is the node selected by the GPU scheduler.
	// +optional
	AssignedNode string `json:"assignedNode,omitempty"`

	// PodName is the name of the managed pod.
	// +optional
	PodName string `json:"podName,omitempty"`

	// RetryCount is how many times the job has been retried.
	// +optional
	RetryCount int32 `json:"retryCount,omitempty"`

	// LastBackoffUntil is when the next retry is allowed.
	// +optional
	LastBackoffUntil *metav1.Time `json:"lastBackoffUntil,omitempty"`

	// Message is a human-readable status message.
	// +optional
	Message string `json:"message,omitempty"`

	// StartTime is when the job first became Running.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the job reached a terminal state.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=gj
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.status.assignedNode`
// +kubebuilder:printcolumn:name="GPUs",type=integer,JSONPath=`.spec.gpuCount`
// +kubebuilder:printcolumn:name="Retries",type=integer,JSONPath=`.status.retryCount`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// GPUJob is the Schema for the gpujobs API — a single distributed GPU workload unit.
type GPUJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GPUJobSpec   `json:"spec,omitempty"`
	Status GPUJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// GPUJobList contains a list of GPUJob.
type GPUJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GPUJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&GPUJob{}, &GPUJobList{})
}

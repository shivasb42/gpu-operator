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

// TrainingJobPhase represents the lifecycle phase of a TrainingJob.
type TrainingJobPhase string

const (
	TrainingJobPending   TrainingJobPhase = "Pending"
	TrainingJobRunning   TrainingJobPhase = "Running"
	TrainingJobSucceeded TrainingJobPhase = "Succeeded"
	TrainingJobFailed    TrainingJobPhase = "Failed"
)

// WorkerSpec describes a role (master / worker) in a distributed training job.
type WorkerSpec struct {
	// Replicas is the number of pods for this role.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	Replicas int32 `json:"replicas"`

	// GPUsPerReplica is GPUs required per pod.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	GPUsPerReplica int32 `json:"gpusPerReplica"`

	// Image is the CUDA training image.
	Image string `json:"image"`

	// Command overrides the image entrypoint.
	// +optional
	Command []string `json:"command,omitempty"`

	// Args are arguments passed to the command.
	// +optional
	Args []string `json:"args,omitempty"`

	// Resources for non-GPU requests/limits.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Env environment variables.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`
}

// TrainingJobSpec defines the desired state of TrainingJob.
type TrainingJobSpec struct {
	// Master is the chief / rank-0 worker specification.
	Master WorkerSpec `json:"master"`

	// Workers are additional distributed workers (optional for single-node).
	// +optional
	Workers *WorkerSpec `json:"workers,omitempty"`

	// SchedulingStrategy selects bin-packing or spread placement.
	// +kubebuilder:default=BinPack
	// +kubebuilder:validation:Enum=BinPack;Spread
	// +optional
	SchedulingStrategy SchedulingStrategy `json:"schedulingStrategy,omitempty"`

	// NodeSelector constrains candidate nodes.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Framework hints (pytorch, tensorflow, etc.) for env injection.
	// +kubebuilder:default=pytorch
	// +optional
	Framework string `json:"framework,omitempty"`

	// RetryPolicy controls retries after node failure or preemption.
	// +optional
	RetryPolicy RetryPolicy `json:"retryPolicy,omitempty"`

	// IdempotencyKey uniquely identifies this logical training run.
	// +optional
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
}

// RoleStatus tracks pods for one training role.
type RoleStatus struct {
	// Desired replicas.
	Desired int32 `json:"desired"`
	// Ready replicas.
	Ready int32 `json:"ready"`
	// Failed replicas.
	Failed int32 `json:"failed"`
	// Active pod names.
	// +optional
	Pods []string `json:"pods,omitempty"`
}

// TrainingJobStatus defines the observed state of TrainingJob.
type TrainingJobStatus struct {
	// Phase is the current lifecycle phase.
	// +optional
	Phase TrainingJobPhase `json:"phase,omitempty"`

	// Master status.
	// +optional
	Master RoleStatus `json:"master,omitempty"`

	// Workers status.
	// +optional
	Workers RoleStatus `json:"workers,omitempty"`

	// RetryCount is how many times the job has been retried.
	// +optional
	RetryCount int32 `json:"retryCount,omitempty"`

	// LastBackoffUntil is when the next retry is allowed.
	// +optional
	LastBackoffUntil *metav1.Time `json:"lastBackoffUntil,omitempty"`

	// Message is a human-readable status message.
	// +optional
	Message string `json:"message,omitempty"`

	// StartTime is when training first became Running.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when training reached a terminal state.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=tj
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Master",type=integer,JSONPath=`.status.master.ready`
// +kubebuilder:printcolumn:name="Workers",type=integer,JSONPath=`.status.workers.ready`
// +kubebuilder:printcolumn:name="Retries",type=integer,JSONPath=`.status.retryCount`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TrainingJob is the Schema for distributed multi-node GPU training workloads.
type TrainingJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TrainingJobSpec   `json:"spec,omitempty"`
	Status TrainingJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TrainingJobList contains a list of TrainingJob.
type TrainingJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TrainingJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TrainingJob{}, &TrainingJobList{})
}

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
*/

package controllers

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	gpuv1alpha1 "github.com/shivasb42/gpu-operator/api/v1alpha1"
	"github.com/shivasb42/gpu-operator/pkg/retry"
	"github.com/shivasb42/gpu-operator/pkg/scheduler"
)

const (
	gpuJobFinalizer   = "gpu.operator.io/gpujob-finalizer"
	labelGPUJobName   = "gpu.operator.io/gpujob"
	labelIdempotency  = "gpu.operator.io/idempotency-key"
	annotationManaged = "gpu.operator.io/managed-by"
)

// GPUJobReconciler reconciles a GPUJob object.
type GPUJobReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Scheduler *scheduler.Scheduler
}

// +kubebuilder:rbac:groups=gpu.operator.io,resources=gpujobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gpu.operator.io,resources=gpujobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gpu.operator.io,resources=gpujobs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile maintains desired vs actual state for GPUJob (control-plane loop).
func (r *GPUJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	job := &gpuv1alpha1.GPUJob{}
	if err := r.Get(ctx, req.NamespacedName, job); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Deletion / finalizer cleanup.
	if !job.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, job)
	}
	if !controllerutil.ContainsFinalizer(job, gpuJobFinalizer) {
		controllerutil.AddFinalizer(job, gpuJobFinalizer)
		if err := r.Update(ctx, job); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Terminal states — no further work.
	if job.Status.Phase == gpuv1alpha1.GPUJobSucceeded ||
		(job.Status.Phase == gpuv1alpha1.GPUJobFailed && !retry.CanRetry(job.Status.RetryCount, job.Spec.RetryPolicy)) {
		return ctrl.Result{}, nil
	}

	// Honor exponential backoff after failure/preemption.
	now := time.Now()
	if retry.InBackoff(job.Status.LastBackoffUntil, now) {
		remaining := retry.Remaining(job.Status.LastBackoffUntil, now)
		logger.Info("backoff in progress", "remaining", remaining)
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	// Idempotent lookup: find existing managed pod by name or idempotency key.
	pod, err := r.findOrBuildPod(ctx, job)
	if err != nil {
		return ctrl.Result{}, err
	}

	if pod == nil {
		return r.scheduleAndCreate(ctx, job)
	}

	return r.syncPodStatus(ctx, job, pod)
}

func (r *GPUJobReconciler) handleDelete(ctx context.Context, job *gpuv1alpha1.GPUJob) (ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(job, gpuJobFinalizer) {
		if job.Status.PodName != "" {
			pod := &corev1.Pod{}
			err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Status.PodName}, pod)
			if err == nil {
				_ = r.Delete(ctx, pod)
			} else if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		controllerutil.RemoveFinalizer(job, gpuJobFinalizer)
		if err := r.Update(ctx, job); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *GPUJobReconciler) findOrBuildPod(ctx context.Context, job *gpuv1alpha1.GPUJob) (*corev1.Pod, error) {
	if job.Status.PodName != "" {
		pod := &corev1.Pod{}
		err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Status.PodName}, pod)
		if err == nil {
			return pod, nil
		}
		if !apierrors.IsNotFound(err) {
			return nil, err
		}
	}

	// Idempotent search by label to avoid duplicate pods after reconcile races.
	key := idempotencyKey(job)
	list := &corev1.PodList{}
	if err := r.List(ctx, list,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{
			labelGPUJobName:  job.Name,
			labelIdempotency: key,
		},
	); err != nil {
		return nil, err
	}
	if len(list.Items) > 0 {
		return &list.Items[0], nil
	}
	return nil, nil
}

func (r *GPUJobReconciler) scheduleAndCreate(ctx context.Context, job *gpuv1alpha1.GPUJob) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	strategy := job.Spec.SchedulingStrategy
	if strategy == "" {
		strategy = gpuv1alpha1.BinPack
	}

	score, err := r.Scheduler.SelectNode(ctx, scheduler.Request{
		GPUCount:     int64(job.Spec.GPUCount),
		GPUMemoryMiB: int64(job.Spec.GPUMemoryMiB),
		Strategy:     strategy,
		NodeSelector: job.Spec.NodeSelector,
	})
	if err != nil {
		logger.Info("waiting for GPU capacity", "error", err.Error())
		job.Status.Phase = gpuv1alpha1.GPUJobPending
		job.Status.Message = err.Error()
		_ = r.Status().Update(ctx, job)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	pod := r.desiredPod(job, score.Node.Name)
	if err := controllerutil.SetControllerReference(job, pod, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.Create(ctx, pod); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Idempotent: another reconcile won the race.
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}

	job.Status.Phase = gpuv1alpha1.GPUJobScheduled
	job.Status.AssignedNode = score.Node.Name
	job.Status.PodName = pod.Name
	job.Status.Message = fmt.Sprintf("scheduled on %s via %s (score=%.1f)", score.Node.Name, strategy, score.Score)
	job.Status.StartTime = nil
	job.Status.CompletionTime = nil
	if err := r.Status().Update(ctx, job); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("created GPU pod", "pod", pod.Name, "node", score.Node.Name)
	return ctrl.Result{}, nil
}

func (r *GPUJobReconciler) syncPodStatus(ctx context.Context, job *gpuv1alpha1.GPUJob, pod *corev1.Pod) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	orig := job.Status.DeepCopy()

	switch pod.Status.Phase {
	case corev1.PodPending:
		job.Status.Phase = gpuv1alpha1.GPUJobScheduled
		job.Status.Message = "pod pending"
		if pod.Spec.NodeName != "" {
			job.Status.AssignedNode = pod.Spec.NodeName
		}
	case corev1.PodRunning:
		job.Status.Phase = gpuv1alpha1.GPUJobRunning
		job.Status.Message = "pod running"
		job.Status.AssignedNode = pod.Spec.NodeName
		if job.Status.StartTime == nil {
			t := metav1.Now()
			job.Status.StartTime = &t
		}
	case corev1.PodSucceeded:
		job.Status.Phase = gpuv1alpha1.GPUJobSucceeded
		job.Status.Message = "completed successfully"
		t := metav1.Now()
		job.Status.CompletionTime = &t
	case corev1.PodFailed:
		return r.handleFailure(ctx, job, pod, "pod failed")
	}

	// Detect preemption / unexpected deletion of running work.
	if !pod.DeletionTimestamp.IsZero() && job.Status.Phase == gpuv1alpha1.GPUJobRunning {
		return r.handleFailure(ctx, job, pod, "pod preempted or deleted")
	}

	if statusChanged(orig, &job.Status) {
		if err := r.Status().Update(ctx, job); err != nil {
			return ctrl.Result{}, err
		}
	}

	logger.V(1).Info("synced", "phase", job.Status.Phase)
	return ctrl.Result{}, nil
}

func (r *GPUJobReconciler) handleFailure(ctx context.Context, job *gpuv1alpha1.GPUJob, pod *corev1.Pod, reason string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	policy := job.Spec.RetryPolicy

	if !retry.CanRetry(job.Status.RetryCount, policy) {
		job.Status.Phase = gpuv1alpha1.GPUJobFailed
		job.Status.Message = fmt.Sprintf("%s; retries exhausted (%d)", reason, job.Status.RetryCount)
		t := metav1.Now()
		job.Status.CompletionTime = &t
		if err := r.Status().Update(ctx, job); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Clean up failed pod, bump retry, set backoff — then requeue.
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	until := retry.Until(job.Status.RetryCount, policy, time.Now())
	job.Status.RetryCount++
	job.Status.LastBackoffUntil = &until
	job.Status.Phase = gpuv1alpha1.GPUJobPending
	job.Status.PodName = ""
	job.Status.AssignedNode = ""
	job.Status.Message = fmt.Sprintf("%s; retry %d after backoff", reason, job.Status.RetryCount)
	if err := r.Status().Update(ctx, job); err != nil {
		return ctrl.Result{}, err
	}

	backoff := retry.NextBackoff(job.Status.RetryCount-1, policy)
	logger.Info("scheduling retry", "retry", job.Status.RetryCount, "backoff", backoff)
	return ctrl.Result{RequeueAfter: backoff}, nil
}

func (r *GPUJobReconciler) desiredPod(job *gpuv1alpha1.GPUJob, nodeName string) *corev1.Pod {
	key := idempotencyKey(job)
	name := fmt.Sprintf("%s-%s", job.Name, shortKey(key))

	gpuQty := scheduler.GPUQuantity(int64(job.Spec.GPUCount))
	resources := job.Spec.Resources.DeepCopy()
	if resources.Requests == nil {
		resources.Requests = corev1.ResourceList{}
	}
	if resources.Limits == nil {
		resources.Limits = corev1.ResourceList{}
	}
	resources.Requests[scheduler.NVIDIAResourceName] = gpuQty
	resources.Limits[scheduler.NVIDIAResourceName] = gpuQty

	affinity := scheduler.BuildNodeAffinity(nodeName, job.Spec.NodeSelector)
	if job.Spec.Affinity != nil && job.Spec.Affinity.PodAntiAffinity != nil {
		affinity.PodAntiAffinity = job.Spec.Affinity.PodAntiAffinity
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: job.Namespace,
			Labels: map[string]string{
				labelGPUJobName:  job.Name,
				labelIdempotency: key,
				"app.kubernetes.io/name":       "gpujob",
				"app.kubernetes.io/managed-by": "gpu-operator",
			},
			Annotations: map[string]string{
				annotationManaged: "gpu-operator",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:     corev1.RestartPolicyNever,
			PriorityClassName: job.Spec.PriorityClassName,
			NodeSelector:      job.Spec.NodeSelector,
			Affinity:          affinity,
			Tolerations:       job.Spec.Tolerations,
			Containers: []corev1.Container{{
				Name:            "gpu",
				Image:           job.Spec.Image,
				Command:         job.Spec.Command,
				Args:            job.Spec.Args,
				Env:             job.Spec.Env,
				Resources:       *resources,
				ImagePullPolicy: corev1.PullIfNotPresent,
			}},
		},
	}
	return pod
}

func idempotencyKey(job *gpuv1alpha1.GPUJob) string {
	if job.Spec.IdempotencyKey != "" {
		return job.Spec.IdempotencyKey
	}
	// Stable per attempt: name + retry generation.
	return fmt.Sprintf("%s-r%d", job.UID, job.Status.RetryCount)
}

func shortKey(key string) string {
	if len(key) <= 8 {
		return key
	}
	return key[len(key)-8:]
}

func statusChanged(a, b *gpuv1alpha1.GPUJobStatus) bool {
	return a.Phase != b.Phase ||
		a.AssignedNode != b.AssignedNode ||
		a.PodName != b.PodName ||
		a.Message != b.Message ||
		a.RetryCount != b.RetryCount
}

// SetupWithManager registers the controller with a workqueue rate limiter (backoff).
func (r *GPUJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gpuv1alpha1.GPUJob{}).
		Owns(&corev1.Pod{}).
		WithOptions(controllerOptions()).
		Complete(r)
}

func controllerOptions() controller.Options {
	return controller.Options{
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			5*time.Millisecond, 1000*time.Second,
		),
	}
}

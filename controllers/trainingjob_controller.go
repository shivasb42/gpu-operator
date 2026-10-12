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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	gpuv1alpha1 "github.com/shivasb42/gpu-operator/api/v1alpha1"
	"github.com/shivasb42/gpu-operator/pkg/retry"
	"github.com/shivasb42/gpu-operator/pkg/scheduler"
)

const (
	trainingJobFinalizer = "gpu.operator.io/trainingjob-finalizer"
	labelTrainingJob     = "gpu.operator.io/trainingjob"
	labelRole            = "gpu.operator.io/role"
)

// TrainingJobReconciler reconciles a TrainingJob object.
type TrainingJobReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Scheduler *scheduler.Scheduler
}

// +kubebuilder:rbac:groups=gpu.operator.io,resources=trainingjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gpu.operator.io,resources=trainingjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gpu.operator.io,resources=trainingjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile drives distributed training lifecycle (master + workers).
func (r *TrainingJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	tj := &gpuv1alpha1.TrainingJob{}
	if err := r.Get(ctx, req.NamespacedName, tj); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !tj.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, tj)
	}
	if !controllerutil.ContainsFinalizer(tj, trainingJobFinalizer) {
		controllerutil.AddFinalizer(tj, trainingJobFinalizer)
		if err := r.Update(ctx, tj); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if tj.Status.Phase == gpuv1alpha1.TrainingJobSucceeded ||
		(tj.Status.Phase == gpuv1alpha1.TrainingJobFailed && !retry.CanRetry(tj.Status.RetryCount, tj.Spec.RetryPolicy)) {
		return ctrl.Result{}, nil
	}

	now := time.Now()
	if retry.InBackoff(tj.Status.LastBackoffUntil, now) {
		return ctrl.Result{RequeueAfter: retry.Remaining(tj.Status.LastBackoffUntil, now)}, nil
	}

	strategy := tj.Spec.SchedulingStrategy
	if strategy == "" {
		strategy = gpuv1alpha1.BinPack
	}

	masterStatus, err := r.ensureRole(ctx, tj, "master", &tj.Spec.Master, strategy, 0)
	if err != nil {
		return ctrl.Result{}, err
	}
	tj.Status.Master = masterStatus

	workerStatus := gpuv1alpha1.RoleStatus{}
	if tj.Spec.Workers != nil {
		workerStatus, err = r.ensureRole(ctx, tj, "worker", tj.Spec.Workers, strategy, int(tj.Spec.Master.Replicas))
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	tj.Status.Workers = workerStatus

	return r.updateAggregatePhase(ctx, tj, logger)
}

func (r *TrainingJobReconciler) handleDelete(ctx context.Context, tj *gpuv1alpha1.TrainingJob) (ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(tj, trainingJobFinalizer) {
		list := &corev1.PodList{}
		if err := r.List(ctx, list, client.InNamespace(tj.Namespace), client.MatchingLabels{labelTrainingJob: tj.Name}); err != nil {
			return ctrl.Result{}, err
		}
		for i := range list.Items {
			if err := r.Delete(ctx, &list.Items[i]); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		controllerutil.RemoveFinalizer(tj, trainingJobFinalizer)
		if err := r.Update(ctx, tj); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *TrainingJobReconciler) ensureRole(
	ctx context.Context,
	tj *gpuv1alpha1.TrainingJob,
	role string,
	spec *gpuv1alpha1.WorkerSpec,
	strategy gpuv1alpha1.SchedulingStrategy,
	rankOffset int,
) (gpuv1alpha1.RoleStatus, error) {
	status := gpuv1alpha1.RoleStatus{Desired: spec.Replicas}

	list := &corev1.PodList{}
	if err := r.List(ctx, list,
		client.InNamespace(tj.Namespace),
		client.MatchingLabels{labelTrainingJob: tj.Name, labelRole: role},
	); err != nil {
		return status, err
	}

	existing := map[string]corev1.Pod{}
	for i := range list.Items {
		existing[list.Items[i].Name] = list.Items[i]
	}

	key := trainingIdempotencyKey(tj)
	var pods []string
	var ready, failed int32

	for i := int32(0); i < spec.Replicas; i++ {
		podName := fmt.Sprintf("%s-%s-%d-%s", tj.Name, role, i, shortKey(key))
		pods = append(pods, podName)

		if pod, ok := existing[podName]; ok {
			switch pod.Status.Phase {
			case corev1.PodRunning:
				if isPodReady(&pod) {
					ready++
				}
			case corev1.PodSucceeded:
				ready++
			case corev1.PodFailed:
				failed++
			}
			continue
		}

		score, err := r.Scheduler.SelectNode(ctx, scheduler.Request{
			GPUCount:     int64(spec.GPUsPerReplica),
			Strategy:     strategy,
			NodeSelector: tj.Spec.NodeSelector,
		})
		if err != nil {
			tj.Status.Message = fmt.Sprintf("waiting for GPUs for %s/%d: %v", role, i, err)
			continue
		}

		pod := r.desiredTrainingPod(tj, role, int(i)+rankOffset, podName, spec, score.Node.Name, key)
		if err := controllerutil.SetControllerReference(tj, pod, r.Scheme); err != nil {
			return status, err
		}
		if err := r.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
			return status, err
		}
	}

	status.Pods = pods
	status.Ready = ready
	status.Failed = failed
	return status, nil
}

func (r *TrainingJobReconciler) desiredTrainingPod(
	tj *gpuv1alpha1.TrainingJob,
	role string,
	rank int,
	podName string,
	spec *gpuv1alpha1.WorkerSpec,
	nodeName string,
	key string,
) *corev1.Pod {
	gpuQty := scheduler.GPUQuantity(int64(spec.GPUsPerReplica))
	resources := spec.Resources.DeepCopy()
	if resources.Requests == nil {
		resources.Requests = corev1.ResourceList{}
	}
	if resources.Limits == nil {
		resources.Limits = corev1.ResourceList{}
	}
	resources.Requests[scheduler.NVIDIAResourceName] = gpuQty
	resources.Limits[scheduler.NVIDIAResourceName] = gpuQty

	worldSize := tj.Spec.Master.Replicas
	if tj.Spec.Workers != nil {
		worldSize += tj.Spec.Workers.Replicas
	}

	env := append([]corev1.EnvVar{}, spec.Env...)
	env = append(env,
		corev1.EnvVar{Name: "ROLE", Value: role},
		corev1.EnvVar{Name: "RANK", Value: fmt.Sprintf("%d", rank)},
		corev1.EnvVar{Name: "WORLD_SIZE", Value: fmt.Sprintf("%d", worldSize)},
		corev1.EnvVar{Name: "FRAMEWORK", Value: defaultFramework(tj.Spec.Framework)},
		corev1.EnvVar{Name: "NVIDIA_VISIBLE_DEVICES", Value: "all"},
	)

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: tj.Namespace,
			Labels: map[string]string{
				labelTrainingJob:               tj.Name,
				labelRole:                      role,
				labelIdempotency:               key,
				"app.kubernetes.io/name":       "trainingjob",
				"app.kubernetes.io/managed-by": "gpu-operator",
			},
			Annotations: map[string]string{annotationManaged: "gpu-operator"},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			NodeSelector:  tj.Spec.NodeSelector,
			Affinity:      scheduler.BuildNodeAffinity(nodeName, tj.Spec.NodeSelector),
			Containers: []corev1.Container{{
				Name:            "trainer",
				Image:           spec.Image,
				Command:         spec.Command,
				Args:            spec.Args,
				Env:             env,
				Resources:       *resources,
				ImagePullPolicy: corev1.PullIfNotPresent,
			}},
		},
	}
}

func (r *TrainingJobReconciler) updateAggregatePhase(ctx context.Context, tj *gpuv1alpha1.TrainingJob, logger interface {
	Info(msg string, keysAndValues ...interface{})
}) (ctrl.Result, error) {
	desired := tj.Status.Master.Desired + tj.Status.Workers.Desired
	ready := tj.Status.Master.Ready + tj.Status.Workers.Ready
	failed := tj.Status.Master.Failed + tj.Status.Workers.Failed

	switch {
	case failed > 0:
		return r.handleTrainingFailure(ctx, tj, fmt.Sprintf("%d pod(s) failed", failed))
	case ready == desired && desired > 0:
		// Check if all succeeded.
		list := &corev1.PodList{}
		if err := r.List(ctx, list, client.InNamespace(tj.Namespace), client.MatchingLabels{labelTrainingJob: tj.Name}); err != nil {
			return ctrl.Result{}, err
		}
		allSucceeded := len(list.Items) > 0
		for i := range list.Items {
			if list.Items[i].Status.Phase != corev1.PodSucceeded {
				allSucceeded = false
				break
			}
		}
		if allSucceeded {
			tj.Status.Phase = gpuv1alpha1.TrainingJobSucceeded
			tj.Status.Message = "training completed"
			t := metav1.Now()
			tj.Status.CompletionTime = &t
		} else {
			tj.Status.Phase = gpuv1alpha1.TrainingJobRunning
			tj.Status.Message = fmt.Sprintf("%d/%d ready", ready, desired)
			if tj.Status.StartTime == nil {
				t := metav1.Now()
				tj.Status.StartTime = &t
			}
		}
	case ready > 0:
		tj.Status.Phase = gpuv1alpha1.TrainingJobRunning
		tj.Status.Message = fmt.Sprintf("%d/%d ready", ready, desired)
		if tj.Status.StartTime == nil {
			t := metav1.Now()
			tj.Status.StartTime = &t
		}
	default:
		tj.Status.Phase = gpuv1alpha1.TrainingJobPending
		if tj.Status.Message == "" {
			tj.Status.Message = "scheduling workers"
		}
	}

	if err := r.Status().Update(ctx, tj); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("reconciled training job", "phase", tj.Status.Phase, "ready", ready, "desired", desired)
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *TrainingJobReconciler) handleTrainingFailure(ctx context.Context, tj *gpuv1alpha1.TrainingJob, reason string) (ctrl.Result, error) {
	policy := tj.Spec.RetryPolicy
	if !retry.CanRetry(tj.Status.RetryCount, policy) {
		tj.Status.Phase = gpuv1alpha1.TrainingJobFailed
		tj.Status.Message = fmt.Sprintf("%s; retries exhausted", reason)
		t := metav1.Now()
		tj.Status.CompletionTime = &t
		if err := r.Status().Update(ctx, tj); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	list := &corev1.PodList{}
	if err := r.List(ctx, list, client.InNamespace(tj.Namespace), client.MatchingLabels{labelTrainingJob: tj.Name}); err != nil {
		return ctrl.Result{}, err
	}
	for i := range list.Items {
		_ = r.Delete(ctx, &list.Items[i])
	}

	until := retry.Until(tj.Status.RetryCount, policy, time.Now())
	tj.Status.RetryCount++
	tj.Status.LastBackoffUntil = &until
	tj.Status.Phase = gpuv1alpha1.TrainingJobPending
	tj.Status.Message = fmt.Sprintf("%s; retry %d", reason, tj.Status.RetryCount)
	if err := r.Status().Update(ctx, tj); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: retry.NextBackoff(tj.Status.RetryCount-1, policy)}, nil
}

func trainingIdempotencyKey(tj *gpuv1alpha1.TrainingJob) string {
	if tj.Spec.IdempotencyKey != "" {
		return tj.Spec.IdempotencyKey
	}
	return fmt.Sprintf("%s-r%d", tj.UID, tj.Status.RetryCount)
}

func defaultFramework(f string) string {
	if f == "" {
		return "pytorch"
	}
	return f
}

func isPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// SetupWithManager registers the TrainingJob controller.
func (r *TrainingJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gpuv1alpha1.TrainingJob{}).
		Owns(&corev1.Pod{}).
		WithOptions(controllerOptions()).
		Complete(r)
}

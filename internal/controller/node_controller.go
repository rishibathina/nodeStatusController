/*
Copyright 2025.

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

package controller

import (
	"context"
	// "encoding/json"
	// "fmt"
	// "strconv"
	// "strings"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	// "sigs.k8s.io/controller-runtime/pkg/log"

	// error package
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	// "sigs.k8s.io/controller-runtime/pkg/reflect"
)

const (
	terminatingLabelKey     = "cloud.google.com/terminating"
	terminatingLabelValue   = "true"
	outOfServiceTaintKey    = "node.kubernetes.io/out-of-service"
	outOfServiceTaintValue  = "nodeshutdown"
	outOfServiceTaintEffect = corev1.TaintEffectNoExecute
)

var outOfServiceTaint = corev1.Taint{
	Key:    outOfServiceTaintKey,
	Value:  outOfServiceTaintValue,
	Effect: outOfServiceTaintEffect,
}

var log = ctrl.Log.WithName("terminationController")

// NodeReconciler reconciles a Node object
type NodeReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=core,resources=nodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=nodes/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Node object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// _ = log.FromContext(ctx)

	log.Info("Termination Reconciling start")
	// get node
	var node corev1.Node
	if err := r.Get(ctx, req.NamespacedName, &node); err != nil {
		if apierrors.IsNotFound(err) {
			// we'll ignore not-found errors, since we can get them on deleted requests.
			return ctrl.Result{}, nil
		}

		log.Error(err, "unable to fetch Node")
		return ctrl.Result{}, err
	}

	nodeName := node.Name
	log.Info("Termination Reconciling on node:", nodeName, req.NamespacedName)

	if hasTaint(&node, &outOfServiceTaint) { // if node has out-of-service
		// remove the taint
		node.Spec.Taints = removeTaint(&outOfServiceTaint, node.Spec.Taints)

		//remove the label if it is there
		delete(node.Labels, terminatingLabelKey)

		// check if it was able remove the taint and label
		if err := r.Update(ctx, &node); err != nil {
			log.Error(err, "unable to remove taint and label")
			return ctrl.Result{}, err
		}
		log.Info("Removed out-of-service taint and terminating label from node", "node", node.Name)
	}

	// check if node not ready
	nodeReady, success := checkNodeReady(&node)
	if !success {
		log.Info("unable to find node status condition")
		return ctrl.Result{}, nil

	}

	// If node not ready then do majority of logic
	if !nodeReady {

		if _, exists := node.Labels[terminatingLabelKey]; exists {
			// has terminating label and not ready
			delete(node.Labels, terminatingLabelKey)

			// check if it was able remove the taint and label
			if err := r.Update(ctx, &node); err != nil {
				log.Error(err, "unable to remove taint and label")
				return ctrl.Result{}, err
			}

			// node not ready and doesnt have the out of service taint
			if !hasTaint(&node, &outOfServiceTaint) {
				node.Spec.Taints = append(node.Spec.Taints, outOfServiceTaint)
				if err := r.Update(ctx, &node); err != nil {
					log.Error(err, "unable to add out-of-service taint")
					return ctrl.Result{}, err
				}
				log.Info("Added out-of-service taint to node", "node", node.Name)
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}

		} else {

			terminatingPods, err := r.arePodsTerminating(ctx, &node)
			if err != nil {
				log.Error(err, "unable to check for terminating pods")
				return ctrl.Result{}, err
			}

			// node not ready and no terminating label but there are pods terminating
			if terminatingPods {
				node.Labels[terminatingLabelKey] = "true"
				if err := r.Update(ctx, &node); err != nil {
					log.Error(err, "unable to add terminating label")
					return ctrl.Result{}, err
				}
				log.Info("Added terminating label to node", "node", node.Name)

				minGracePeriod := r.getTerminationGracePeriod(ctx, &node)

				if minGracePeriod > 0 {
					return ctrl.Result{RequeueAfter: time.Duration(minGracePeriod) * time.Second}, nil
				}

				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}

			// node not ready but there are no pods terminating and there is no terminating label
			return ctrl.Result{}, nil
		}

	}

	return ctrl.Result{}, nil

}

// SetupWithManager sets up the controller with the Manager.
func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		WithEventFilter(predicate.Funcs{
			UpdateFunc: func(e event.UpdateEvent) bool {
				oldNode := e.ObjectOld.(*corev1.Node)
				newNode := e.ObjectNew.(*corev1.Node)
				return !reflect.DeepEqual(oldNode.Status, newNode.Status)
			},
		}).
		Complete(r)
}

func checkNodeReady(node *corev1.Node) (bool, bool) {
	found := false
	ready := false
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			found = true
			ready = condition.Status == corev1.ConditionTrue
			break
		}
	}

	return ready, found

}

// func (r *NodeReconciler) checkOutOfService(ctx context.Context, node *corev1.Node) (bool, error) {
// 	if hasTaint(&node.Spec.Taints, &outOfServiceTaint) { // if node has out-of-service
// 		// remove the taint
// 		node.Spec.Taints = removeTaint(&outOfServiceTaint, &node.Spec.Taints)

// 		//remove the label if it is there
// 		delete(node.Labels, terminatingLabelKey)

// 		// check if it was able remove the taint and label
// 		if err := r.Update(ctx, &node); err != nil {
// 			log.Error(err, "unable to remove taint and label")
// 			return true, err
// 		}
// 		log.Info("Removed out-of-service taint and terminating label from node", "node", node.Name)
// 		return true, nil
// 	}
// 	return false, nil
// }

func (r *NodeReconciler) getTerminationGracePeriod(ctx context.Context, node *corev1.Node) int64 {
	podList := &corev1.PodList{}

	// opts := &client.ListOption{
	// client.MatchingFields{"spec.nodeName": node.Name},
	// }
	opts := client.MatchingFields{"spec.nodeName": node.Name}

	err := r.Client.List(ctx, podList, opts)
	if err != nil {
		return 0
	}

	for _, pod := range podList.Items {
		if pod.DeletionTimestamp != nil {
			if pod.Spec.TerminationGracePeriodSeconds != nil {
				return *pod.Spec.TerminationGracePeriodSeconds
			}
			return 30
		}
	}
	return 0
}

func hasTaint(node *corev1.Node, taintToCheck *corev1.Taint) bool {
	taints := node.Spec.Taints
	for _, taint := range taints {
		if taint.Key == taintToCheck.Key && taint.Effect == taintToCheck.Effect {
			if taintToCheck.Value != "" && taint.Value != taintToCheck.Value {
				continue
			}
			return true
		}
	}
	return false
}

func (r *NodeReconciler) arePodsTerminating(ctx context.Context, node *corev1.Node) (bool, error) {

	podList := &corev1.PodList{}
	err := r.Client.List(ctx, podList, client.MatchingFields{"spec.nodeName": node.Name})
	if err != nil {
		log.Error(err, "unable to list pods on node", "node", node.Name, "error", err)
		return false, err
	}

	for _, pod := range podList.Items {
		if pod.DeletionTimestamp != nil {
			log.Info("Pod is terminating", "pod", pod.Name, "node", node.Name)
			return true, nil
		}
	}

	log.Info("No terminating pods found on node", "node", node.Name)
	return false, nil
}

func removeTaint(searchTaint *corev1.Taint, taints []corev1.Taint) []corev1.Taint {
	index := -1
	for i, t := range taints {
		if t.MatchTaint(searchTaint) {
			index = i
			break
		}
	}
	if index > -1 {
		return append(taints[:index], taints[index+1:]...)
	}
	return taints
}

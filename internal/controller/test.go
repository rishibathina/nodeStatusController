package controller

import (
        "context"
        "reflect"
        "time"

        corev1 "k8s.io/api/core/v1"
        apierrors "k8s.io/apimachinery/pkg/api/errors"
        metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
        "k8s.io/apimachinery/pkg/runtime"
        ctrl "sigs.k8s.io/controller-runtime"
        "sigs.k8s.io/controller-runtime/pkg/client"
        "sigs.k8s.io/controller-runtime/pkg/event"
        "sigs.k8s.io/controller-runtime/pkg/log"
        "sigs.k8s.io/controller-runtime/pkg/predicate"
)

// NodeReconciler reconciles a Node object
type NodeReconciler struct {
        client.Client
        Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=core,resources=nodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=list
// +kubebuilder:rbac:groups=core,resources=nodes/finalizers,verbs=update

func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
        log := log.FromContext(ctx)

        var node corev1.Node
        if err := r.Get(ctx, req.NamespacedName, &node); err != nil {
                if apierrors.IsNotFound(err) {
                        return ctrl.Result{}, nil // Ignore not-found errors
                }
                log.Error(err, "unable to fetch Node")
                return ctrl.Result{}, err
        }

        nodeReady := getNodeReadyStatus(&node)
        terminatingLabel := "node.example.com/terminating"
        outOfServiceTaint := corev1.Taint{
                Key:    "node.example.com/out-of-service",
                Effect: corev1.TaintEffectNoSchedule,
        }

        if !nodeReady {
                // Node is not ready
                if _, exists := node.Labels[terminatingLabel]; exists {
                        // Node is not ready and has the terminating label
                        if !hasTaint(&node.Spec.Taints, &outOfServiceTaint) {
                                node.Spec.Taints = append(node.Spec.Taints, outOfServiceTaint)
                                if err := r.Update(ctx, &node); err != nil {
                                        log.Error(err, "unable to add out-of-service taint")
                                        return ctrl.Result{}, err
                                }
                                log.Info("Added out-of-service taint to node", "node", node.Name)
                                return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
                        }
                } else {
                        // Check if pods are terminating
                        terminatingPods, err := r.arePodsTerminating(ctx, &node)
                        if err != nil {
                                return ctrl.Result{}, err
                        }
                        if terminatingPods {
                                node.Labels[terminatingLabel] = "true"
                                if err := r.Update(ctx, &node); err != nil {
                                        log.Error(err, "unable to add terminating label")
                                        return ctrl.Result{}, err
                                }
                                log.Info("Added terminating label to node", "node", node.Name)
                                return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
                        }
                }
        } else {
                // Node is ready
                if hasTaint(&node.Spec.Taints, &outOfServiceTaint) {
                        node.Spec.Taints = removeTaint(&node.Spec.Taints, &outOfServiceTaint)
                        delete(node.Labels, terminatingLabel)
                        if err := r.Update(ctx, &node); err != nil {
                                log.Error(err, "unable to remove taint and label")
                                return ctrl.Result{}, err
                        }
                        log.Info("Removed out-of-service taint and terminating label from node", "node", node.Name)
                }
        }

        return ctrl.Result{}, nil
}

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

func getNodeReadyStatus(node *corev1.Node) bool {
        for _, condition := range node.Status.Conditions {
                if condition.Type == corev1.NodeReady {
                        return condition.Status == corev1.ConditionTrue
                }
        }
        return false
}

func (r *NodeReconciler) arePodsTerminating(ctx context.Context, node *corev1.Node) (bool, error) {
        podList := &corev1.PodList{}
        err := r.List(ctx, podList, client.MatchingFields{"spec.nodeName": node.Name})
        if err != nil {
                return false, err
        }

        for _, pod := range podList.Items {
                if pod.DeletionTimestamp != nil {
                        return true, nil
                }
        }
        return false, nil
}

func hasTaint(taints *[]corev1.Taint, taintToFind *corev1.Taint) bool {
        for _, taint := range *taints {
                if taint.Key == taintToFind.Key && taint.Effect == taintToFind.Effect {
                        return true
                }
        }
        return false
}

func removeTaint(taints *[]corev1.Taint, taintToRemove *corev1.Taint) []corev1.Taint {
        result := []corev1.Taint{}
        for _, taint := range *taints {
                if taint.Key == taintToRemove.Key && taint.Effect == taintToRemove.Effect {
                        continue
                }
                result = append(result, taint)
        }
        return result
}
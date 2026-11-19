package agentcoverage

import (
	"fmt"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	corehelpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
)

type nodeCoverage struct {
	Name        string
	State       string
	Unavailable bool
}

type snapshot struct {
	Nodes        []nodeCoverage
	OrphanedPods int
	UnmappedPods int
}

func evaluate(ds *appsv1.DaemonSet, nodes []corev1.Node, agents []corev1.Pod, serving map[string]bool, now time.Time, grace time.Duration) (snapshot, error) {
	result := snapshot{}
	known := make(map[string]bool, len(nodes))
	for i := range nodes {
		known[nodes[i].Name] = true
	}
	ready := make(map[string]bool)
	for i := range agents {
		pod := &agents[i]
		if !ownedBy(pod, ds.UID) || pod.Namespace != ds.Namespace {
			continue
		}
		target := targetNode(pod)
		if target == "" {
			result.UnmappedPods++
			continue
		}
		if !known[target] {
			result.OrphanedPods++
			continue
		}
		if pod.Status.Phase == corev1.PodRunning {
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
					ready[target] = true
				}
			}
		}
	}
	selector := nodeaffinity.GetRequiredNodeAffinity(&corev1.Pod{Spec: ds.Spec.Template.Spec})
	tolerations := append([]corev1.Toleration{}, ds.Spec.Template.Spec.Tolerations...)
	// Kubernetes adds DaemonSet tolerations: https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/#taints-and-tolerations
	for _, key := range []string{"node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"} {
		tolerations = append(tolerations, corev1.Toleration{Key: key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute})
	}
	for _, key := range []string{"node.kubernetes.io/disk-pressure", "node.kubernetes.io/memory-pressure", "node.kubernetes.io/pid-pressure", "node.kubernetes.io/unschedulable"} {
		tolerations = append(tolerations, corev1.Toleration{Key: key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule})
	}
	if ds.Spec.Template.Spec.HostNetwork {
		tolerations = append(tolerations, corev1.Toleration{Key: "node.kubernetes.io/network-unavailable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule})
	}
	for i := range nodes {
		node := &nodes[i]
		matches, err := selector.Match(node)
		if err != nil {
			return snapshot{}, fmt.Errorf("match DaemonSet node eligibility: %w", err)
		}
		coverage := nodeCoverage{Name: node.Name, State: "excluded"}
		_, untolerated := corehelpers.FindMatchingUntoleratedTaint(logr.Discard(), node.Spec.Taints, tolerations, func(t *corev1.Taint) bool {
			return t.Effect == corev1.TaintEffectNoSchedule || t.Effect == corev1.TaintEffectNoExecute
		}, true)
		retiring := isRetiring(node)
		matchesNodeName := ds.Spec.Template.Spec.NodeName == "" || ds.Spec.Template.Spec.NodeName == node.Name
		if !matches || !matchesNodeName || untolerated {
			result.Nodes = append(result.Nodes, coverage)
			continue
		}
		if retiring && !serving[node.Name] {
			coverage.State = "retiring"
		} else {
			condition := nodeReady(node)
			switch {
			case !serving[node.Name] && (node.CreationTimestamp.IsZero() || now.Sub(node.CreationTimestamp.Time) < grace):
				coverage.State = "starting"
			case !serving[node.Name] && condition != nil && condition.Status == corev1.ConditionTrue && !condition.LastTransitionTime.IsZero() && now.Sub(condition.LastTransitionTime.Time) < grace:
				coverage.State = "starting"
			case !serving[node.Name] && (condition == nil || condition.Status != corev1.ConditionTrue):
				coverage.State = "node_unavailable"
			case ready[node.Name]:
				coverage.State = "ready"
			default:
				coverage.State = "unavailable"
				coverage.Unavailable = true
			}
		}
		result.Nodes = append(result.Nodes, coverage)
	}
	return result, nil
}

func ownedBy(pod *corev1.Pod, uid types.UID) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.APIVersion == "apps/v1" && owner.Kind == "DaemonSet" && owner.UID == uid && owner.Controller != nil && *owner.Controller {
			return true
		}
	}
	return false
}

func targetNode(pod *corev1.Pod) string {
	if pod.Spec.NodeName != "" {
		return pod.Spec.NodeName
	}
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil || pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return ""
	}
	var target string
	for _, term := range pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		var name string
		for _, field := range term.MatchFields {
			if field.Key == "metadata.name" && field.Operator == corev1.NodeSelectorOpIn && len(field.Values) == 1 {
				name = field.Values[0]
			}
		}
		if name == "" || (target != "" && name != target) {
			return ""
		}
		target = name
	}
	return target
}

func nodeReady(node *corev1.Node) *corev1.NodeCondition {
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == corev1.NodeReady {
			return &node.Status.Conditions[i]
		}
	}
	return nil
}

func isRetiring(node *corev1.Node) bool {
	return node.DeletionTimestamp != nil || node.Spec.Unschedulable
}

func hasWorkload(pod corev1.Pod) bool {
	return pod.Spec.NodeName != "" && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed
}

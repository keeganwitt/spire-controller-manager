package agentcoverage

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestEvaluateNodeLifecycle(t *testing.T) {
	now := time.Date(2026, 11, 18, 15, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name        string
		change      func(*corev1.Node)
		pods        []corev1.Pod
		serving     bool
		state       string
		unavailable bool
	}{
		{name: "missing agent on established node", state: "unavailable", unavailable: true},
		{name: "ready agent", pods: []corev1.Pod{testAgent("node-a", true)}, state: "ready"},
		{name: "pending agent", pods: []corev1.Pod{testAgent("node-a", false)}, state: "unavailable", unavailable: true},
		{name: "node startup grace", change: func(n *corev1.Node) { n.CreationTimestamp = metav1.NewTime(now.Add(-time.Minute)) }, state: "starting"},
		{name: "ready transition grace", change: func(n *corev1.Node) {
			n.Status.Conditions[0].LastTransitionTime = metav1.NewTime(now.Add(-time.Minute))
		}, state: "starting"},
		{name: "deleting empty node", change: func(n *corev1.Node) { n.DeletionTimestamp = &metav1.Time{Time: now} }, state: "retiring"},
		{name: "deleting node with workloads", change: func(n *corev1.Node) { n.DeletionTimestamp = &metav1.Time{Time: now} }, serving: true, state: "unavailable", unavailable: true},
		{name: "cordoned empty node", change: func(n *corev1.Node) {
			n.Spec.Unschedulable = true
		}, state: "retiring"},
		{name: "unreachable empty node", change: func(n *corev1.Node) { n.Status.Conditions[0].Status = corev1.ConditionUnknown }, state: "node_unavailable"},
		{name: "unreachable node serving workloads", change: func(n *corev1.Node) { n.Status.Conditions[0].Status = corev1.ConditionUnknown }, serving: true, state: "unavailable", unavailable: true},
		{name: "node excluded by selector", change: func(n *corev1.Node) { n.Labels["role"] = "other" }, state: "excluded"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			node := testNode(now)
			if tt.change != nil {
				tt.change(&node)
			}
			snapshot, err := evaluate(testDaemonSet(), []corev1.Node{node}, tt.pods, map[string]bool{"node-a": tt.serving}, now, 10*time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Nodes) != 1 || snapshot.Nodes[0].State != tt.state || snapshot.Nodes[0].Unavailable != tt.unavailable {
				t.Fatalf("unexpected coverage: %+v", snapshot)
			}
		})
	}
}

func TestEvaluateReplacementAndAffinity(t *testing.T) {
	now := time.Now()
	failed := testAgent("node-a", false)
	failed.Status.Phase = corev1.PodFailed
	ready := testAgent("", true)
	ready.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-a"}}}}}}}}
	orphan := testAgent("deleted-node", false)
	snapshot, err := evaluate(testDaemonSet(), []corev1.Node{testNode(now)}, []corev1.Pod{failed, ready, orphan}, nil, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Nodes[0].Unavailable || snapshot.OrphanedPods != 1 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	ready.OwnerReferences[0].UID = types.UID("previous-daemonset")
	snapshot, err = evaluate(testDaemonSet(), []corev1.Node{testNode(now)}, []corev1.Pod{ready}, nil, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Nodes[0].Unavailable {
		t.Fatal("a pod from another DaemonSet must not satisfy coverage")
	}
}

func TestCordonedNodeCannotHideActiveConsumers(t *testing.T) {
	now := time.Now()
	ds := testDaemonSet()
	ds.Spec.Template.Spec.Tolerations = nil
	node := testNode(now)
	node.Spec.Unschedulable = true
	node.Spec.Taints = []corev1.Taint{{Key: "node.kubernetes.io/unschedulable", Effect: corev1.TaintEffectNoSchedule}}
	snapshot, err := evaluate(ds, []corev1.Node{node}, nil, map[string]bool{"node-a": true}, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Nodes[0].Unavailable {
		t.Fatal("a cordon cannot hide active consumers")
	}
	node.Spec.Taints[0].Key = "unrelated-node-class"
	snapshot, err = evaluate(ds, []corev1.Node{node}, nil, map[string]bool{"node-a": true}, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Nodes[0].State != "excluded" {
		t.Fatal("unrelated scheduling exclusions must remain excluded")
	}
}

func TestExplicitDaemonSetNodeNameRestrictsInventory(t *testing.T) {
	now := time.Now()
	ds := testDaemonSet()
	ds.Spec.Template.Spec.NodeName = "node-a"
	first, second := testNode(now), testNode(now)
	second.Name = "node-b"
	result, err := evaluate(ds, []corev1.Node{first, second}, nil, nil, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Nodes[0].Unavailable || result.Nodes[1].State != "excluded" {
		t.Fatalf("explicit nodeName was not respected: %+v", result)
	}
}

func TestWorkloadDemandIncludesTerminatingConsumers(t *testing.T) {
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "application", DeletionTimestamp: &metav1.Time{Time: time.Now()}}, Spec: corev1.PodSpec{NodeName: "node-a"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	if !hasWorkload(pod) {
		t.Fatal("terminating workloads may still need identities")
	}
	pod.Status.Phase = corev1.PodSucceeded
	if hasWorkload(pod) {
		t.Fatal("completed work must not retain a retired node")
	}
	pod.Status.Phase = corev1.PodRunning
	pod.OwnerReferences = []metav1.OwnerReference{{Kind: "DaemonSet"}}
	if !hasWorkload(pod) {
		t.Fatal("application DaemonSets can also require identities")
	}
}

func testNode(now time.Time) corev1.Node {
	return corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"role": "worker"}, CreationTimestamp: metav1.NewTime(now.Add(-time.Hour))}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(now.Add(-time.Hour))}}}}
}

func testDaemonSet() *appsv1.DaemonSet {
	return &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: "agents", Name: "spire-agent", UID: types.UID("agent-ds")}, Spec: appsv1.DaemonSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "agent"}}, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{NodeSelector: map[string]string{"role": "worker"}, Tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists}}}}}}
}

func testAgent(node string, ready bool) corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "agents", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", UID: types.UID("agent-ds"), Controller: boolPointer(true)}}}, Spec: corev1.PodSpec{NodeName: node}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}}}
}

func boolPointer(value bool) *bool { return &value }

func TestNonControllerOwnerDoesNotSatisfyCoverage(t *testing.T) {
	now := time.Now()
	for _, change := range []func(*metav1.OwnerReference){
		func(owner *metav1.OwnerReference) { owner.Controller = boolPointer(false) },
		func(owner *metav1.OwnerReference) { owner.APIVersion = "unrelated.example/v1" },
	} {
		agent := testAgent("node-a", true)
		change(&agent.OwnerReferences[0])
		result, err := evaluate(testDaemonSet(), []corev1.Node{testNode(now)}, []corev1.Pod{agent}, nil, now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if !result.Nodes[0].Unavailable {
			t.Fatal("only the current apps/v1 DaemonSet controller can satisfy coverage")
		}
	}
}

func TestNumericTolerationAndUnknownDisruptionTaint(t *testing.T) {
	now := time.Now()
	node := testNode(now)
	node.Spec.Taints = []corev1.Taint{{Key: "example.org/capacity", Value: "20", Effect: corev1.TaintEffectNoSchedule}}
	ds := testDaemonSet()
	ds.Spec.Template.Spec.Tolerations = []corev1.Toleration{{Key: "example.org/capacity", Value: "10", Operator: corev1.TolerationOpGt, Effect: corev1.TaintEffectNoSchedule}}
	result, err := evaluate(ds, []corev1.Node{node}, nil, nil, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Nodes[0].Unavailable {
		t.Fatal("numeric tolerations in the accepted DaemonSet spec must be respected")
	}
	node.Spec.Taints = []corev1.Taint{{Key: "autoscaler.example/disrupted", Effect: corev1.TaintEffectNoSchedule}}
	ds.Spec.Template.Spec.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	result, err = evaluate(ds, []corev1.Node{node}, nil, nil, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Nodes[0].Unavailable {
		t.Fatal("vendor-specific taints must not implicitly retire a node")
	}
}

func TestAmbiguousPendingTargetIsUnmapped(t *testing.T) {
	now := time.Now()
	agent := testAgent("", false)
	agent.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
		{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-a"}}}},
		{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-b"}}}},
	}}}}
	result, err := evaluate(testDaemonSet(), []corev1.Node{testNode(now)}, []corev1.Pod{agent}, nil, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if result.UnmappedPods != 1 || !result.Nodes[0].Unavailable {
		t.Fatalf("ambiguous affinity must not assign coverage: %+v", result)
	}
}

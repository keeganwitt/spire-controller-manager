package agentcoverage

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	spirev1alpha1 "github.com/spiffe/spire-controller-manager/api/v1alpha1"
)

func TestDiscoveryAndPrometheusLifecycle(t *testing.T) {
	now := time.Now()
	ds := testDaemonSet()
	ds.Name = "custom-release-agent"
	ds.Labels = map[string]string{"app.kubernetes.io/name": "spire-agent"}
	node := testNode(now)
	reader := testReader(t, ds, &node)
	o := newTestObserver(t, reader, &spirev1alpha1.AgentCoverageConfig{Namespace: "agents"})
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(o))

	require.NoError(t, o.collect(context.Background(), now))
	body := scrape(t, registry)
	require.Contains(t, body, `spire_controller_manager_agent_coverage_unavailable{daemonset="custom-release-agent",namespace="agents",node="node-a"} 1`)
	require.Contains(t, body, "spire_controller_manager_agent_coverage_collection_success 1")
	require.Contains(t, body, "spire_controller_manager_agent_coverage_last_success_timestamp_seconds ")
	require.NotContains(t, body, "kube_cluster_name")

	agent := testAgent("node-a", true)
	agent.Name = "replacement-agent"
	require.NoError(t, reader.Create(context.Background(), &agent))
	require.NoError(t, o.collect(context.Background(), now.Add(time.Second)))
	require.Contains(t, scrape(t, registry), `node="node-a"} 0`)

	require.NoError(t, reader.Delete(context.Background(), &node))
	require.NoError(t, o.collect(context.Background(), now.Add(2*time.Second)))
	require.NotContains(t, scrape(t, registry), `node="node-a"`)

	require.NoError(t, reader.Delete(context.Background(), ds))
	require.ErrorContains(t, o.collect(context.Background(), now.Add(3*time.Second)), "no agent DaemonSets")
	body = scrape(t, registry)
	require.Contains(t, body, "spire_controller_manager_agent_coverage_collection_success 0")
	require.NotContains(t, body, "spire_controller_manager_agent_coverage_unavailable{")
}

func TestMultipleDiscoveredDaemonSetsStaySeparate(t *testing.T) {
	now := time.Now()
	first := testDaemonSet()
	first.Labels = map[string]string{"app.kubernetes.io/name": "spire-agent"}
	second := first.DeepCopy()
	second.Name = "other-agent"
	second.UID = types.UID("other-agent-uid")
	second.Spec.Template.Spec.NodeSelector = map[string]string{"role": "other"}
	unrelated := first.DeepCopy()
	unrelated.Name = "unrelated"
	unrelated.Labels = map[string]string{"app.kubernetes.io/name": "other"}
	node := testNode(now)
	otherNode := testNode(now)
	otherNode.Name = "node-b"
	otherNode.Labels["role"] = "other"
	reader := testReader(t, first, second, unrelated, &node, &otherNode)
	o := newTestObserver(t, reader, &spirev1alpha1.AgentCoverageConfig{Namespace: "agents"})
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(o))
	require.NoError(t, o.collect(context.Background(), now))
	body := scrape(t, registry)
	require.Contains(t, body, `unavailable{daemonset="spire-agent",namespace="agents",node="node-a"} 1`)
	require.Contains(t, body, `unavailable{daemonset="other-agent",namespace="agents",node="node-b"} 1`)
	require.NotContains(t, body, `daemonset="unrelated"`)
}

func TestDiscoveryOverrides(t *testing.T) {
	now := time.Now()
	ds := testDaemonSet()
	ds.Name = "no-conventional-name"
	ds.Labels = map[string]string{"identity": "agent"}
	node := testNode(now)
	reader := testReader(t, ds, &node)
	for _, config := range []*spirev1alpha1.AgentCoverageConfig{
		{Namespace: "agents", DaemonSetName: ds.Name},
		{Namespace: "agents", DaemonSetSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"identity": "agent"}}},
	} {
		o := newTestObserver(t, reader, config)
		require.NoError(t, o.collect(context.Background(), now))
		registry := prometheus.NewRegistry()
		require.NoError(t, registry.Register(o))
		require.Contains(t, scrape(t, registry), `daemonset="no-conventional-name"`)
	}
}

func TestCollectionRetainsConsumersDuringStartupAndDrain(t *testing.T) {
	now := time.Now()
	for _, state := range []string{"starting", "cordoned", "deleting"} {
		t.Run(state, func(t *testing.T) {
			ds := testDaemonSet()
			node := testNode(now)
			switch state {
			case "starting":
				node.CreationTimestamp = metav1.NewTime(now.Add(-time.Second))
			case "cordoned":
				node.Spec.Unschedulable = true
			case "deleting":
				node.DeletionTimestamp = &metav1.Time{Time: now}
				node.Finalizers = []string{"example.org/hold"}
			}
			reader := testReader(t, ds, &node)
			o := newTestObserver(t, reader, &spirev1alpha1.AgentCoverageConfig{Namespace: "agents", DaemonSetName: ds.Name})
			registry := prometheus.NewRegistry()
			require.NoError(t, registry.Register(o))
			require.NoError(t, o.collect(context.Background(), now))
			require.Contains(t, scrape(t, registry), `node="node-a"} 0`)
			consumer := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "applications", Name: "consumer"}, Spec: corev1.PodSpec{NodeName: "node-a"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
			require.NoError(t, reader.Create(context.Background(), consumer))
			require.NoError(t, o.collect(context.Background(), now))
			require.Contains(t, scrape(t, registry), `node="node-a"} 1`)
		})
	}
}

func TestReadFailureInvalidatesPreviousSnapshot(t *testing.T) {
	now := time.Now()
	ds := testDaemonSet()
	node := testNode(now)
	reader := testReader(t, ds, &node)
	o := newTestObserver(t, reader, &spirev1alpha1.AgentCoverageConfig{Namespace: "agents", DaemonSetName: ds.Name})
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(o))
	require.NoError(t, o.collect(context.Background(), now))
	for _, kind := range []string{"nodes", "pods", "daemonsets"} {
		t.Run(kind, func(t *testing.T) {
			o.reader = &failingReader{Reader: reader, kind: kind}
			o.apiReader = o.reader
			require.Error(t, o.collect(context.Background(), now))
			body := scrape(t, registry)
			require.Contains(t, body, "spire_controller_manager_agent_coverage_collection_success 0")
			require.NotContains(t, body, "spire_controller_manager_agent_coverage_unavailable{")
		})
	}
}

func TestInvalidConfiguration(t *testing.T) {
	for _, config := range []*spirev1alpha1.AgentCoverageConfig{
		{Namespace: ""},
		{Namespace: "bad namespace"},
		{Namespace: "agents", DaemonSetName: "bad name"},
		{Namespace: "agents", DaemonSetName: "agent", DaemonSetSelector: &metav1.LabelSelector{}},
		{Namespace: "agents", DaemonSetSelector: &metav1.LabelSelector{}},
		{Namespace: "agents", DaemonSetSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "invalid key", Operator: metav1.LabelSelectorOpExists}}}},
		{Namespace: "agents", Interval: &metav1.Duration{Duration: -time.Second}},
		{Namespace: "agents", StartupGrace: &metav1.Duration{Duration: -time.Second}},
	} {
		require.Error(t, ValidateConfig(config))
	}
	require.NoError(t, ValidateConfig(nil))
	require.NoError(t, ValidateConfig(&spirev1alpha1.AgentCoverageConfig{Namespace: "agents", StartupGrace: &metav1.Duration{}}))
}

func TestObserverStopsAndClearsMetrics(t *testing.T) {
	ds := testDaemonSet()
	node := testNode(time.Now())
	o := newTestObserver(t, testReader(t, ds, &node), &spirev1alpha1.AgentCoverageConfig{Namespace: "agents", DaemonSetName: ds.Name})
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(o))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Start(ctx) }()
	require.Eventually(t, func() bool { return strings.Contains(scrape(t, registry), "collection_success 1") }, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	require.Contains(t, scrape(t, registry), "collection_success 0")
	require.NotContains(t, scrape(t, registry), "unavailable{")
	require.True(t, o.NeedLeaderElection())
}

func newTestObserver(t *testing.T, reader client.Reader, config *spirev1alpha1.AgentCoverageConfig) *observer {
	t.Helper()
	options, err := normalize(config)
	require.NoError(t, err)
	return newObserver(reader, reader, options)
}

func testReader(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func scrape(t *testing.T, registry *prometheus.Registry) string {
	t.Helper()
	response := httptest.NewRecorder()
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	require.Equal(t, 200, response.Code)
	return response.Body.String()
}

type failingReader struct {
	client.Reader
	kind string
}

func (r *failingReader) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	switch list.(type) {
	case *corev1.NodeList:
		if r.kind == "nodes" {
			return errors.New("nodes unavailable")
		}
	case *corev1.PodList:
		if r.kind == "pods" {
			return errors.New("pods unavailable")
		}
	case *appsv1.DaemonSetList:
		if r.kind == "daemonsets" {
			return errors.New("daemonsets unavailable")
		}
	}
	return r.Reader.List(ctx, list, options...)
}

func (r *failingReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if r.kind == "daemonsets" {
		return errors.New("daemonsets unavailable")
	}
	return r.Reader.Get(ctx, key, object, options...)
}

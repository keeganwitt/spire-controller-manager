package agentcoverage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	k8smetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	spirev1alpha1 "github.com/spiffe/spire-controller-manager/api/v1alpha1"
)

const metricPrefix = "spire_controller_manager_agent_coverage_"

type options struct {
	namespace     string
	daemonSetName string
	selector      labels.Selector
	startupGrace  time.Duration
	interval      time.Duration
}

type observer struct {
	options              options
	reader               client.Reader
	apiReader            client.Reader
	log                  logr.Logger
	waitForSync          func(context.Context) bool
	unavailable          *prometheus.Desc
	nodes                *prometheus.Desc
	orphanedPods         *prometheus.Desc
	unmappedPods         *prometheus.Desc
	collectionSuccess    *prometheus.Desc
	lastSuccessTimestamp *prometheus.Desc
	mu                   sync.RWMutex
	samples              []prometheus.Metric
	success              float64
	lastSuccess          float64
}

func ValidateConfig(config *spirev1alpha1.AgentCoverageConfig) error {
	if config == nil {
		return nil
	}
	_, err := normalize(config)
	return err
}

func normalize(config *spirev1alpha1.AgentCoverageConfig) (options, error) {
	if config == nil {
		return options{}, errors.New("agent coverage configuration is required")
	}
	result := options{namespace: config.Namespace, daemonSetName: config.DaemonSetName, startupGrace: 10 * time.Minute, interval: 30 * time.Second}
	if config.Namespace == "" || len(validation.IsDNS1123Label(config.Namespace)) != 0 {
		return options{}, errors.New("agent coverage namespace must be a valid Kubernetes namespace")
	}
	if config.DaemonSetName != "" && len(validation.IsDNS1123Subdomain(config.DaemonSetName)) != 0 {
		return options{}, errors.New("agent coverage DaemonSet name must be a valid Kubernetes name")
	}
	if config.DaemonSetName != "" && config.DaemonSetSelector != nil {
		return options{}, errors.New("agent coverage daemonSetName and daemonSetSelector are mutually exclusive")
	}
	selector := config.DaemonSetSelector
	if selector == nil {
		selector = &metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": "spire-agent"}}
	}
	var err error
	result.selector, err = metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return options{}, fmt.Errorf("parse agent DaemonSet selector: %w", err)
	}
	if result.selector.Empty() {
		return options{}, errors.New("agent coverage DaemonSet selector must not be empty")
	}
	if config.StartupGrace != nil {
		result.startupGrace = config.StartupGrace.Duration
	}
	if config.Interval != nil {
		result.interval = config.Interval.Duration
	}
	if result.startupGrace < 0 || result.interval <= 0 {
		return options{}, errors.New("agent coverage startupGrace must be nonnegative and interval must be positive")
	}
	return result, nil
}

// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list

func Register(mgr manager.Manager, config *spirev1alpha1.AgentCoverageConfig) error {
	settings, err := normalize(config)
	if err != nil {
		return err
	}
	o := newObserver(mgr.GetClient(), mgr.GetAPIReader(), settings)
	o.log = mgr.GetLogger().WithName("agent-coverage")
	o.waitForSync = mgr.GetCache().WaitForCacheSync
	if err := k8smetrics.Registry.Register(o); err != nil {
		return fmt.Errorf("register agent coverage metrics: %w", err)
	}
	if err := mgr.Add(o); err != nil {
		k8smetrics.Registry.Unregister(o)
		return fmt.Errorf("register agent coverage observer: %w", err)
	}
	return nil
}

func newObserver(reader, apiReader client.Reader, settings options) *observer {
	return &observer{
		options: settings, reader: reader, apiReader: apiReader, log: logr.Discard(),
		unavailable:          prometheus.NewDesc(metricPrefix+"unavailable", "Whether a required node lacks a Ready agent pod (1 unavailable, 0 otherwise).", []string{"namespace", "daemonset", "node"}, nil),
		nodes:                prometheus.NewDesc(metricPrefix+"nodes", "Number of nodes by agent coverage state.", []string{"namespace", "daemonset", "state"}, nil),
		orphanedPods:         prometheus.NewDesc(metricPrefix+"orphaned_pods", "Agent pods targeting nodes absent from the observed inventory.", []string{"namespace", "daemonset"}, nil),
		unmappedPods:         prometheus.NewDesc(metricPrefix+"unmapped_pods", "Agent pods without an unambiguous target node.", []string{"namespace", "daemonset"}, nil),
		collectionSuccess:    prometheus.NewDesc(metricPrefix+"collection_success", "Whether the last agent coverage collection succeeded.", nil, nil),
		lastSuccessTimestamp: prometheus.NewDesc(metricPrefix+"last_success_timestamp_seconds", "Unix timestamp of the last successful agent coverage collection.", nil, nil),
	}
}

func (o *observer) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{o.unavailable, o.nodes, o.orphanedPods, o.unmappedPods, o.collectionSuccess, o.lastSuccessTimestamp} {
		ch <- desc
	}
}

func (o *observer) Collect(ch chan<- prometheus.Metric) {
	o.mu.RLock()
	samples, success, lastSuccess := o.samples, o.success, o.lastSuccess
	o.mu.RUnlock()
	for _, sample := range samples {
		ch <- sample
	}
	ch <- prometheus.MustNewConstMetric(o.collectionSuccess, prometheus.GaugeValue, success)
	ch <- prometheus.MustNewConstMetric(o.lastSuccessTimestamp, prometheus.GaugeValue, lastSuccess)
}

func (o *observer) NeedLeaderElection() bool { return true }

func (o *observer) Start(ctx context.Context) error {
	defer o.invalidate()
	if o.waitForSync != nil && !o.waitForSync(ctx) {
		return nil
	}
	ticker := time.NewTicker(o.options.interval)
	defer ticker.Stop()
	for {
		collectionCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		err := o.collect(collectionCtx, time.Now())
		cancel()
		if err != nil && ctx.Err() == nil {
			o.log.Error(err, "could not collect agent coverage")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (o *observer) collect(ctx context.Context, now time.Time) error {
	samples, err := o.read(ctx, now)
	if err != nil {
		o.invalidate()
		return err
	}
	o.mu.Lock()
	o.samples, o.success, o.lastSuccess = samples, 1, float64(now.Unix())
	o.mu.Unlock()
	return nil
}

func (o *observer) invalidate() {
	o.mu.Lock()
	o.samples, o.success = nil, 0
	o.mu.Unlock()
}

func (o *observer) read(ctx context.Context, now time.Time) ([]prometheus.Metric, error) {
	daemonSets, err := o.discover(ctx)
	if err != nil {
		return nil, err
	}
	nodes := &corev1.NodeList{}
	if err := o.reader.List(ctx, nodes); err != nil {
		return nil, fmt.Errorf("read nodes: %w", err)
	}
	pods := &corev1.PodList{}
	if err := o.reader.List(ctx, pods); err != nil {
		return nil, fmt.Errorf("read pods: %w", err)
	}
	var samples []prometheus.Metric
	for i := range daemonSets {
		ds := &daemonSets[i]
		if len(nodes.Items) == 0 && ds.Status.DesiredNumberScheduled > 0 {
			return nil, errors.New("node inventory is empty while an agent DaemonSet expects nodes")
		}
		selector, err := metav1.LabelSelectorAsSelector(ds.Spec.Selector)
		if err != nil || selector.Empty() {
			return nil, fmt.Errorf("agent DaemonSet %s/%s must have a valid nonempty pod selector", ds.Namespace, ds.Name)
		}
		var agents []corev1.Pod
		serving := map[string]bool{}
		for j := range pods.Items {
			pod := &pods.Items[j]
			if pod.Namespace == ds.Namespace && ownedBy(pod, ds.UID) {
				agents = append(agents, *pod)
			} else if hasWorkload(*pod) {
				serving[pod.Spec.NodeName] = true
			}
		}
		result, err := evaluate(ds, nodes.Items, agents, serving, now, o.options.startupGrace)
		if err != nil {
			return nil, err
		}
		samples = append(samples, o.metrics(ds, result)...)
	}
	return samples, nil
}

func (o *observer) discover(ctx context.Context) ([]appsv1.DaemonSet, error) {
	if o.options.daemonSetName != "" {
		ds := &appsv1.DaemonSet{}
		if err := o.apiReader.Get(ctx, client.ObjectKey{Namespace: o.options.namespace, Name: o.options.daemonSetName}, ds); err != nil {
			return nil, fmt.Errorf("read agent DaemonSet: %w", err)
		}
		return []appsv1.DaemonSet{*ds}, nil
	}
	daemonSets := &appsv1.DaemonSetList{}
	if err := o.apiReader.List(ctx, daemonSets, client.InNamespace(o.options.namespace), client.MatchingLabelsSelector{Selector: o.options.selector}); err != nil {
		return nil, fmt.Errorf("discover agent DaemonSets: %w", err)
	}
	if len(daemonSets.Items) == 0 {
		return nil, errors.New("no agent DaemonSets match the configured namespace and selector")
	}
	return daemonSets.Items, nil
}

func (o *observer) metrics(ds *appsv1.DaemonSet, result snapshot) []prometheus.Metric {
	samples := make([]prometheus.Metric, 0, len(result.Nodes)+8)
	counts := map[string]int{"ready": 0, "unavailable": 0, "starting": 0, "retiring": 0, "node_unavailable": 0, "excluded": 0}
	for _, node := range result.Nodes {
		counts[node.State]++
		value := float64(0)
		if node.Unavailable {
			value = 1
		}
		samples = append(samples, prometheus.MustNewConstMetric(o.unavailable, prometheus.GaugeValue, value, ds.Namespace, ds.Name, node.Name))
	}
	for state, count := range counts {
		samples = append(samples, prometheus.MustNewConstMetric(o.nodes, prometheus.GaugeValue, float64(count), ds.Namespace, ds.Name, state))
	}
	samples = append(samples,
		prometheus.MustNewConstMetric(o.orphanedPods, prometheus.GaugeValue, float64(result.OrphanedPods), ds.Namespace, ds.Name),
		prometheus.MustNewConstMetric(o.unmappedPods, prometheus.GaugeValue, float64(result.UnmappedPods), ds.Namespace, ds.Name),
	)
	return samples
}

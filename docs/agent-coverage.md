# Kubernetes Agent Coverage Metrics

Optional agent coverage metrics report whether nodes eligible for a selected SPIRE Agent DaemonSet have a Ready agent pod. They distinguish sustained node-local coverage gaps from node startup and drain. Pod readiness does not prove that the agent can issue identities or that workloads can reach the Workload API.

## Configuration and discovery

The feature is disabled unless `agentCoverage` is configured. Metrics use the controller manager's existing Prometheus endpoint; no additional metrics transport is required.

```yaml
agentCoverage:
  namespace: identity
```

This discovers DaemonSets in `identity` labelled `app.kubernetes.io/name=spire-agent`, including the default labels used by the SPIFFE hardened Helm chart. Each match is evaluated and labelled independently, supporting separate agent DaemonSets for different node pools. Discovery does not infer trust domains or associate a Helm release with the configured SPIRE Server. Scope it to the intended installation when a namespace contains multiple installations:

```yaml
agentCoverage:
  namespace: identity
  daemonSetSelector:
    matchLabels:
      app.kubernetes.io/name: spire-agent
      app.kubernetes.io/instance: example-installation
  startupGrace: 2m
  interval: 30s
```

A custom selector replaces the default selector. For an unlabelled installation, specify `daemonSetName` instead of `daemonSetSelector`. There is no assumed DaemonSet name, namespace, environment structure or cloud provider. An empty explicit selector is rejected; zero discovery matches fail collection instead of reporting healthy coverage.

| Field | Required | Default | Meaning |
| --- | --- | --- | --- |
| `namespace` | yes | none | Namespace containing the agent DaemonSets. |
| `daemonSetName` | no | none | Select one DaemonSet by name; mutually exclusive with `daemonSetSelector`. |
| `daemonSetSelector` | no | `app.kubernetes.io/name=spire-agent` | Kubernetes label selector on DaemonSet metadata. All matches are monitored. |
| `startupGrace` | no | `10m` | Grace after node creation or a Ready transition when no active workloads are present; zero disables it. |
| `interval` | no | `30s` | Positive collection interval. |

This feature intentionally covers Kubernetes DaemonSets, the usual per-node Kubernetes agent deployment. SPIRE Agents can also run as host processes, static pods or other workload types. Those deployments are not discovered or evaluated, and absence of matching DaemonSets must not be interpreted as their health. Their expected placement cannot be inferred from a DaemonSet template.

The controller needs `get` and `list` on `apps/daemonsets` in the selected namespace, plus its existing cluster-wide node/pod access. The generated manager ClusterRole includes these read permissions; custom RBAC may grant them with a namespace Role. The observer reuses the node/pod cache and reads DaemonSets directly every interval. Namespace-restricted pod caches and static-manifest mode are rejected when the feature is enabled because they cannot establish complete workload placement. The metrics endpoint must remain enabled.

## Node coverage

Node eligibility follows the selected DaemonSet's node selector, required affinity, explicit node name and tolerations, including the automatic Kubernetes DaemonSet tolerations. Pods must have a controller owner reference to the current DaemonSet UID. A Ready replacement satisfies coverage for the same node; recreating an agent pod does not restart the node startup grace. Pending target nodes are resolved from `spec.nodeName` or unambiguous exact `metadata.name` affinity. Agent pods targeting absent nodes and pods without an attributable target are counted separately.

Nodes are classified as `ready`, `unavailable`, `starting`, `retiring`, `node_unavailable` or `excluded`. Eligible cordoned or deleting nodes are `retiring` only when they have no active workloads. Eligible nodes with active workloads remain covered during startup and retirement. Kubernetes' automatic DaemonSet tolerations keep ordinary cordoned nodes eligible; unrelated untolerated taints remain scheduling exclusions. All assigned nonterminal pods except those belonging to the selected agent DaemonSet count conservatively as potential workloads, including terminating pods and other DaemonSets. This can retain infrastructure-only pods; it does not identify which pods actually use SPIRE.

Only standard Kubernetes cordon and deletion signals are used. Autoscaler-specific disruption annotations and taints have no special meaning. A node remains in scope until standard lifecycle signals allow its exclusion. Cache/watch delays remain a limitation; successful collection means the cached objects were read successfully, not that the API watch is known to be current.

## Prometheus metrics

All names below have the prefix `spire_controller_manager_agent_coverage_`.

| Suffix | Labels | Meaning |
| --- | --- | --- |
| `unavailable` | `namespace`, `daemonset`, `node` | One when a required node lacks a Ready agent, zero otherwise. |
| `nodes` | `namespace`, `daemonset`, `state` | Node count in each coverage state. |
| `orphaned_pods` | `namespace`, `daemonset` | Agent pods targeting nodes absent from the inventory. |
| `unmapped_pods` | `namespace`, `daemonset` | Agent pods without an unambiguous target node. |
| `collection_success` | none | One after a successful complete collection; zero before the first collection or after a failed collection. |
| `last_success_timestamp_seconds` | none | Unix timestamp of the last successful collection, or zero before the first success. |

The observer runs on the elected controller leader. Followers expose collection health as zero and no node samples. Monitor successful collection across the controller replicas and use Prometheus target health to detect a stopped controller. Per-node series grow with the number of selected DaemonSets and nodes; assess cardinality before enabling in large clusters.

A failed read, invalid DaemonSet selector, missing discovery result or inconsistent empty node inventory clears the coverage samples and marks collection unsuccessful, while preserving the last-success timestamp. Removed node and DaemonSet series disappear from the next successful snapshot and follow normal Prometheus staleness handling. Scrapes never perform Kubernetes API requests. Use collection health and last-success age alongside coverage; missing observations are not evidence of healthy agents.

Alert thresholds, persistence, routing and environment labels belong in the monitoring system. Validate representative startup, drain, missing-agent and failed-collection cases before using these metrics operationally.

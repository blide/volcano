# Capacity  Plugin User Guide

## Introduction

Capacity plugin is a replacement of proportion plugin, but instead of dividing the queue's deserved resources by weight, it realizes elastic queue capacity management i.e., queue's resource borrowing and lending mechanism by specifying the amount of deserved resources for each dimension resource of the queue. 

A queue can use the idle resources of other queues, and when other queues submit jobs, they can reclaim the resources that have been lent, and the amount of reclaimed resources is the amount of queue's deserved resources. For more detail,  please see [Capacity scheduling design](../design/capacity-scheduling.md)

## Environment setup

### Install volcano

Refer to [Install Guide](https://github.com/volcano-sh/volcano/blob/master/installer/README.md) to install volcano.

After installed, update the scheduler configuration:

```shell
kubectl edit cm -n volcano-system volcano-scheduler-configmap
```

Please make sure

- reclaim action is enabled.
- capacity plugin is enabled and proportion plugin is removed.

Note:  capacity and proportion plugin are in conflict, the two plugins cannot be used together.

```yaml
kind: ConfigMap
apiVersion: v1
metadata:
  name: volcano-scheduler-configmap
  namespace: volcano-system
data:
  volcano-scheduler.conf: |
    actions: "enqueue, allocate, backfill, reclaim" # add reclaim action.
    tiers:
    - plugins:
      - name: priority
      - name: gang
        enablePreemptable: false
      - name: conformance
    - plugins:
      - name: drf
        enablePreemptable: false
      - name: predicates
      - name: capacity # add this field and remove proportion plugin.
      - name: nodeorder
      - name: binpack
```

## Configure ancestor reclaim level

When hierarchical queue mode is enabled in the capacity plugin, reclaim scope can be controlled with plugin argument `ancestorReclaimLevel`:

- `0`: no ancestor restriction. This is the default behavior.
- `1`: add parent-level deserved checks for cross-parent reclaim.
- `2`: also add grandparent-level deserved checks when queues diverge at that level.
- `N`: check deeper ancestor levels in the same way.

Example configuration:

```yaml
kind: ConfigMap
apiVersion: v1
metadata:
  name: volcano-scheduler-configmap
  namespace: volcano-system
data:
  volcano-scheduler.conf: |
    actions: "enqueue, allocate, backfill, reclaim" # add reclaim action.
    tiers:
    - plugins:
      - name: priority
      - name: gang
        enablePreemptable: false
      - name: conformance
    - plugins:
      - name: drf
        enablePreemptable: false
      - name: predicates
      - name: capacity # add this field and remove proportion plugin.
        enableHierarchy: true
        arguments:
          # Controls how far reclaim can cross queue hierarchy boundaries.
          # 0: no ancestor restriction (default behavior)
          # 1: adds parent-level deserved checks for cross-parent reclaim
          # 2: adds grandparent-level deserved checks, and so on for larger levels
          ancestorReclaimLevel: 1
          # Admit a job at enqueue when its only blocker is an ancestor queue's capability that
          # reclaim can free. Default false. See "Configure enqueue admission on reclaimable
          # ancestor capacity" below.
          enqueueAncestorCapReclaim: true
      - name: nodeorder
      - name: binpack
```

## Configure enqueue admission on reclaimable ancestor capacity

By default a PodGroup with `minResources` is only enqueued when `minResources + allocated + inqueue - elastic`
fits the `capability` of its queue and of every ancestor. The `elastic` term is the usage of running jobs
above their own `minResources`, so holders whose `minResources` equal their usage (for example PodGroups
auto-created per pod) contribute no elastic credit. In a hierarchy where a parent carries a `capability`,
an under-deserved child can then be rejected at enqueue even though the `reclaim` action would evict an
over-deserved sibling for it. The job stays Pending and reclaim never sees it (see
[volcano-sh/volcano#4817](https://github.com/volcano-sh/volcano/issues/4817)).

Setting the plugin argument `enqueueAncestorCapReclaim: true` (hierarchy mode only, default `false`) admits
such a job when all of the following hold on every resource dimension it requests:

- the leaf queue passes its own enqueue check unchanged (the leaf `capability` is never relaxed);
- after admission the leaf stays at or under its `deserved`: `allocated + inqueue + minResources <= deserved`,
  with no elastic credit, and a requested dimension missing from `deserved` counts as zero;
- every ancestor that fails its check fails only on `capability`, and the reclaimable slack in its subtree
  covers the shortfall. Slack is summed over leaf queues that are open and reclaimable, excluding the asker,
  and per leaf is the minimum of usage above `deserved`, usage above `guarantee`, and the requests of
  preemptable running pods. When `ancestorReclaimLevel` is greater than 0, intermediate queues outside the
  asker's own branch are also capped by their usage above `deserved`.

Jobs admitted this way are then served by the `reclaim` action in the same or a following session, so
`reclaim` must be in the `actions` list (the default configuration does not include it); without it an
admitted job stays `Inqueue` and keeps its `inqueue` reservation. Other plugins with `enableJobEnqueued`
still vote: a `Reject` from any of them (for example `overcommit` on a fully allocated cluster) wins over
this admission. The gang plugin's `minAvailable` veto on victims is not modeled by this check, so with
gang `reclaimable` enabled a job may be admitted whose last victim gang refuses to evict. The slack is
also summed cluster-wide while reclaim evicts on one node at a time, so an ask whose reclaimable
usage is spread over several nodes can be admitted and never served. Pair this admission with the
[dequeue action](../design/dequeue-action.md), which moves a PodGroup that made no progress within
a timeout back to `Pending` and releases its reservation.

## Choose the cheapest victims across nodes

The `reclaim` action evicts on one node per task. By default it commits on the first candidate node
where the task fits after evicting that node's victims, and the candidate order is not stable, so
which pods get evicted depends on which node happens to come first. With several pods of the same
job spread over nodes, for example a Spark driver on one node and its executors on others, the
driver can be evicted while an executor would have freed the same amount.

Setting the reclaim action argument `victimSelection: bestFit` makes reclaim plan every candidate
node and commit the one whose victims cost least, ranked the way victims are already ranked within a
node: the victim queue first (for the capacity plugin the queue nearest the asker in the hierarchy,
then the one with the higher share), then the lowest highest-victim priority, then the fewest
victims; a node that fits without eviction wins outright. Priority thereby becomes a cost that
reclaim pays as late as possible among a queue's own pods, instead of the
`volcano.sh/preemptable: "false"` label, which exempts a pod altogether and lets a tenant that
labels everything keep borrowed capacity for good. The class value never ranks one tenant's pods
against another's.

```yaml
kind: ConfigMap
apiVersion: v1
metadata:
  name: volcano-scheduler-configmap
  namespace: volcano-system
data:
  volcano-scheduler.conf: |
    actions: "enqueue, allocate, backfill, reclaim"
    tiers:
    - plugins:
      - name: priority
      - name: gang
      - name: conformance
    - plugins:
      - name: drf
      - name: predicates
      - name: capacity
      - name: nodeorder
      - name: binpack
    configurations:
    - name: reclaim
      arguments:
        victimSelection: bestFit   # firstFit (default) or bestFit
        maxCandidateNodes: 0       # bestFit only: stop after this many nodes with a plan; 0 = all
```

Pair it with a PriorityClass ladder so the cost reflects what you want kept: drivers above
executors, long-lived services above batch. PriorityClasses are cluster-scoped; cap which ones a
namespace may use with a `ResourceQuota` using the `PriorityClass` scope, and keep the
`volcano.sh/preemptable: "false"` label for the few workloads that truly must not move. Leave
`preemptionPolicy` at its default: a pod whose class says `Never` is skipped by the `reclaim` action
as an asker.

```yaml
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: spark-driver
value: 1000
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: spark-executor
value: 100
```

`bestFit` chooses between nodes, not across them: it never combines victims from several nodes to
free a queue's quota, and it does not hold the freed room for the task beyond the current session.

## Config queue's deserved resources

Assume there are two nodes and two queues named queue1 and queue2 in your kubernetes cluster, and each node has 4 CPU and 16Gi memory, then there will be total 8 CPU and 32Gi memory in your cluster.

```yaml
allocatable:
  cpu: "4"
  memory: 16Gi
  pods: "110"
```

config queue1's deserved field with 2 cpu and 8Gi memory.

```yaml
apiVersion: scheduling.volcano.sh/v1beta1
kind: Queue
metadata:
  name: queue1
spec:
  reclaimable: true
  deserved: # set the deserved field.
    cpu: 2
    memory: 8Gi
```

config queue2's deserved field with 6 cpu and 24Gi memory.

```yaml
apiVersion: scheduling.volcano.sh/v1beta1
kind: Queue
metadata:
  name: queue2
spec:
  reclaimable: true
  deserved: # set the deserved field.
    cpu: 6
    memory: 24Gi
```

## Submit pods to each queue

First, submit a deployment named demo-1 to queue1 with replicas=8 and each pod requests 1 cpu and 4Gi memory, because queue2 is idle, so queue1 can use the whole clusters' resources, and you can see that 8 pods are in Running state.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: demo-1
spec:
  selector:
    matchLabels:
      app: demo-1
  replicas: 8
  template:
    metadata:
      labels:
        app: demo-1
      annotations:
        scheduling.volcano.sh/queue-name: "queue1" # set the queue
    spec:
      schedulerName: volcano
      containers:
      - name: nginx
        image: nginx:1.14.2
        resources:
          requests:
            cpu: 1
            memory: 4Gi
        ports:
        - containerPort: 80
```

Expected result:

```shell
$ kubectl get po                                                                                             
NAME                      READY   STATUS    RESTARTS   AGE
demo-1-7bc649f544-2wjg7   1/1     Running   0          5s
demo-1-7bc649f544-cvsmr   1/1     Running   0          5s
demo-1-7bc649f544-j5lzp   1/1     Running   0          5s
demo-1-7bc649f544-jvlbx   1/1     Running   0          5s
demo-1-7bc649f544-mzgg2   1/1     Running   0          5s
demo-1-7bc649f544-ntrs2   1/1     Running   0          5s
demo-1-7bc649f544-nv424   1/1     Running   0          5s
demo-1-7bc649f544-zd6d9   1/1     Running   0          5s
```

Then submit a deployment named demo-2 to queue2 with replicas=8 and each pod requests 1 cpu and 4Gi memory.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: demo-2
spec:
  selector:
    matchLabels:
      app: demo-2
  replicas: 8
  template:
    metadata:
      labels:
        app: demo-2
      annotations:
        scheduling.volcano.sh/queue-name: "queue2" # set the queue
    spec:
      schedulerName: volcano
      containers:
      - name: nginx
        image: nginx:1.14.2
        resources:
          requests:
            cpu: 1
            memory: 4Gi
        ports:
        - containerPort: 80
```

Because queue1 occupied queue2's resources, so queue2 will reclaim its deserved resources with 6 cpu and 24Gi memory. And each pod of demo-2 request 1 cpu and 4Gi memory, so there will be 6 Pods in Running state of demo-2,  and demo-1's pods will be evicted. 

Finally, you can see that there are 2 Running pods in demo-1(belongs to queue1), and 6 Running pods in demo-2(belongs to queue2), which meets queue's deserved resources respectively.

```shell
$ kubectl get po                                                                                             
NAME                      READY   STATUS    RESTARTS   AGE
demo-1-7bc649f544-4vvdv   0/1     Pending   0          37s
demo-1-7bc649f544-c6mds   0/1     Pending   0          37s
demo-1-7bc649f544-j5lzp   1/1     Running   0          14m
demo-1-7bc649f544-mzgg2   1/1     Running   0          14m
demo-1-7bc649f544-pqdgk   0/1     Pending   0          37s
demo-1-7bc649f544-tx6wp   0/1     Pending   0          37s
demo-1-7bc649f544-wmshq   0/1     Pending   0          37s
demo-1-7bc649f544-wrhrr   0/1     Pending   0          37s
demo-2-6dfb86c49b-2jvgm   0/1     Pending   0          37s
demo-2-6dfb86c49b-dnjzv   1/1     Running   0          37s
demo-2-6dfb86c49b-fzvmp   1/1     Running   0          37s
demo-2-6dfb86c49b-jlf69   1/1     Running   0          37s
demo-2-6dfb86c49b-k62f7   1/1     Running   0          37s
demo-2-6dfb86c49b-k9b9v   1/1     Running   0          37s
demo-2-6dfb86c49b-rpzvg   0/1     Pending   0          37s
demo-2-6dfb86c49b-zch7w   1/1     Running   0          37s
```

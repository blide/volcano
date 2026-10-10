# Quota Plugin User Guide

## Introduction

The quota plugin enforces queue capability: whether a task may be allocated to its queue and whether a
PodGroup may be admitted (`Inqueue`), checked at the queue and at every ancestor in a hierarchy. It is the
quota half of what the [capacity plugin](how_to_use_capacity_plugin.md) does; capacity keeps the share half
(queue order, which queue's pods are evicted first, whether an ask may reclaim and whether a victim may be
taken, all against `deserved` and `guarantee`). Running the two side by side, with capacity's capability
checks switched off, gives the quota plugin room for what a plain gate lacks:

- admitting a job that is entitled to its leaf's `deserved` but blocked by an ancestor's `capability`, for
  a trial by the `reclaim` action (`enqueueAncestorCapReclaim`);
- holding the `deserved` share that admitted jobs are still owed against every other subtree
  (`reserveDeserved`);
- telling the `reclaim` action which ancestor blocks an ask, for the quota-aware victim round.

The plugin keeps its own per-queue usage from the session's allocate and deallocate events, like every
queue plugin; it does not read capacity's state. See the [design](../design/quota-plugin.md).

## Configuration

Enable the plugin in the same tier as capacity, switch capacity's `enabledAllocatable` and
`enableJobEnqueued` off so that capability is enforced once, and keep `enableHierarchy` on both:

```yaml
kind: ConfigMap
apiVersion: v1
metadata:
  name: volcano-scheduler-configmap
  namespace: volcano-system
data:
  volcano-scheduler.conf: |
    actions: "enqueue, allocate, backfill, reclaim, dequeue"
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
      - name: capacity
        enableHierarchy: true
        enabledAllocatable: false   # the quota plugin owns the capability checks
        enableJobEnqueued: false
        arguments:
          ancestorReclaimLevel: 1
      - name: quota
        enableHierarchy: true
        arguments:
          # Admit a job at enqueue when its only blocker is an ancestor queue's capability that
          # reclaim can free. Default false. See "Configure enqueue admission on reclaimable
          # ancestor capacity" below.
          enqueueAncestorCapReclaim: true
          # Charge every ancestor's capability with the deserved share its other subtrees are
          # still owed by admitted jobs. Default false. See "Reserve the deserved share of
          # admitted jobs" below.
          reserveDeserved: true
      - name: nodeorder
      - name: binpack
```

If capacity or proportion still has one of those checks on, the quota plugin logs a warning once per
session: two gates give the stricter of two answers, but the admission trial and the reserve only work
when the quota plugin's gate is the one that counts.

The plugin does not enforce DRA (dynamic resource allocation) quota. A deployment that needs capacity's
DRA checks keeps capacity's `enabledAllocatable` on and does not use this plugin for now.

With `enableHierarchy: false` the plugin works on flat queues: each queue's real capability is the
cluster minus the other queues' guarantees plus its own, capped by its `capability` when one is set.

## Configure enqueue admission on reclaimable ancestor capacity

By default a PodGroup with `minResources` is only enqueued when `minResources + allocated + inqueue - elastic`
fits the `capability` of its queue and of every ancestor. The `elastic` term is the usage of running jobs
above their own `minResources`, so holders whose `minResources` equal their usage (for example PodGroups
auto-created per pod) contribute no elastic credit. In a hierarchy where a parent carries a `capability`,
an under-deserved child can then be rejected at enqueue even though the `reclaim` action would evict an
over-deserved sibling for it. The job stays Pending and reclaim never sees it (see
[volcano-sh/volcano#4817](https://github.com/volcano-sh/volcano/issues/4817)).

Setting the quota plugin argument `enqueueAncestorCapReclaim: true` (hierarchy mode only, default `false`) admits
such a job for a trial by the `reclaim` action when all of the following hold on every resource dimension it
requests:

- the leaf queue passes its own enqueue check unchanged (the leaf `capability` is never relaxed);
- after admission the leaf stays at or under its `deserved`: `allocated + inqueue + minResources <= deserved`,
  with no elastic credit, and a requested dimension missing from `deserved` counts as zero;
- every ancestor that fails its check fails only on `capability`.

The gate does not predict what reclaim will free. The admitted PodGroup is tagged (an `Inqueue` condition with
reason `AncestorCapReclaim`), the `reclaim` action tries to serve it in the same session, and the
[dequeue action](../design/dequeue-action.md) returns it to `Pending` when reclaim reports that it cannot: either
because victims existed but no node could be made to fit, or because there was nothing to reclaim at all. A
failed trial commits no eviction and leaves only those conditions behind; the job is reconsidered after the
dequeue action's `enqueueBackoff`. The `dequeue` action must therefore be in the `actions` list after `reclaim`;
if it is not, the relaxed admission is refused with a warning and the strict gate's verdict stands.

Other plugins with `enableJobEnqueued` still vote: a `Reject` from any of them (for example `overcommit` on a
fully allocated cluster) wins over this admission.

### Volcano Jobs

A Volcano Job's pods are created by the job controller only after its PodGroup is `Inqueue`, so in the
admission session reclaim has no task to evaluate, the verdict is "not attempted", and the trial continues
in the sessions after the pods appear. Two settings make this usable:

- Enable the `SchedulingGatesQueueAdmission` feature gate on the scheduler and the admission webhook and put
  `scheduling.volcano.sh/queue-allocation-gate: "true"` in the Job's pod template annotations. The webhook then
  creates the pods with Volcano's scheduling gate: the api-server keeps them out of cluster autoscalers'
  view, and the `reclaim` action treats a pod gated only by that gate as a real asker, so the trial runs on
  the real pods (with their affinity, tolerations and requests) while they are still gated. `allocate`
  removes the gate once the queue check passes, after the victims have been evicted. A dequeued Job keeps
  its gated pods; they stay invisible and are picked up again on re-admission.
- Set `inqueueTimeout` on the dequeue action as a fallback for Jobs whose pods never appear (for example
  while the job controller is unavailable); without pods there is never a reclaim verdict.

Without the feature gate the pods are created ungated, which works but exposes them as `Unschedulable`
to autoscalers during the trial.

## Reserve the deserved share of admitted jobs

A queue's `deserved` is its guaranteed share, but by default it is enforced only after the fact, by the
`reclaim` action evicting an over-deserved queue. The allocatable check that every task passes looks at
`capability` alone, so an over-deserved queue can keep growing into a parent's capability while an
under-deserved sibling has an admitted (`Inqueue`) job waiting for that very space. The most common
case is the session after a successful reclaim: the evicted pod's replacement is admitted at the shared
parent before the job the eviction was made for, the job evicts again or finds nothing left to evict,
and the eviction was spent for nothing.

Setting the quota plugin argument `reserveDeserved: true` (hierarchy mode only, default `false`) makes the
allocatable check honor the share the hierarchy still owes:

- a leaf is owed `min(deserved, allocated + unplaced) - allocated` per resource dimension, where
  `unplaced` is the `minResources` of its `Inqueue` PodGroups that no allocated or pipelined task holds
  yet. A PodGroup without `minResources`, or one that is `Pending` (for example dequeued), is owed nothing;
- a parent is owed the sum over its children, capped by its own `deserved - allocated`;
- at every ancestor of a candidate task's queue, the check becomes
  `allocated + request + (owed by the ancestor - owed by the child on the path to the candidate) <= capability`.
  The candidate's own subtree never pays for its own reservation. The leaf check is unchanged.

With the example above the replacement pod is refused at the parent for as long as the admitted job is
owed its share, on every node and in every session, and the job takes the freed space. The reservation
ends when the job is placed, or when the [dequeue action](../design/dequeue-action.md) returns it to
`Pending`. Two queues that are both within their `deserved` are not protected from each other: which of
them gets a freed node slot first is decided by queue order.

The reservation never exceeds a parent's own `deserved`, so a child whose `deserved` exceeds what its
parent guarantees is reserved only the parent's remainder. Flat queues are not affected: the root has no
`capability` unless one is set.


# Quota Plugin User Guide

## Introduction

The quota plugin enforces queue capability: whether a task may be allocated to its queue and whether a
PodGroup may be admitted, at the queue and every ancestor. It runs next to the
[capacity plugin](how_to_use_capacity_plugin.md), which keeps the share side (queue order, victim
order, whether an ask may reclaim and whether a victim may be taken), and adds, on an unmodified
scheduler:

- admission of a job entitled to its leaf's `deserved` but blocked by an ancestor's `capability`, for
  a reclaim trial that is reverted when nothing serves the job (`enqueueAncestorCapReclaim`);
- a reservation of the `deserved` share admitted jobs are still owed, so the room a reclaim frees is
  not taken back by the evicted pod's replacement (`reserveDeserved`);
- a pass at session open that keeps an ask whose victims are still terminating on its nominated node
  ahead of every other ask (`reservedAsks`);
- quota-aware reclaim at session close: an ask the `reclaim` action could not serve because its queue
  hierarchy, not a node, refuses it gets victims under the blocking ancestor from any node
  (`quotaReclaim`).

See the [design](../design/quota-plugin.md).

## Configuration

Enable the plugin in the last tier, switch capacity's capability checks off so that capability is
enforced once, and keep `enableHierarchy` on both:

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
      - name: nodeorder
      - name: binpack
      - name: quota                 # last in the tier
        enableHierarchy: true
        arguments:
          enqueueAncestorCapReclaim: true
          reserveDeserved: true
          quotaReclaim: true
          enqueueBackoff: 1m
          inqueueTimeout: 10m
```

| Argument | Default | Meaning |
|---|---|---|
| `enqueueAncestorCapReclaim` | `false` | admit entitled jobs past an ancestor's capability for a reclaim trial |
| `reserveDeserved` | `false` | charge every ancestor with the deserved share its other subtrees are owed |
| `reservedAsks` | `true` | place asks whose victims are still terminating on their nominated node at session open |
| `quotaReclaim` | `false` | quota-aware reclaim at session close for asks the reclaim action left unserved |
| `maxCrossNodeVictims` | `0` | bound on the victims one ask may take in the quota round; `0` is unbounded |
| `enqueueBackoff` | `1m` | how long a dequeued PodGroup stays Pending before it may be admitted again |
| `inqueueTimeout` | `0` | return an admitted PodGroup with nothing placed and no verdict to Pending after this; `0` is off |

The plugin must come after `predicates` and the order plugins in its tier: its session-open pass uses
what they registered. If capacity or proportion still has `enabledAllocatable` or `enableJobEnqueued`
on, the plugin logs a warning once per session; two gates give the stricter answer, but the trial and
the reserve only work when this plugin's gate is the one that counts. The plugin does not enforce DRA
quota; a deployment that needs capacity's DRA checks keeps capacity's `enabledAllocatable` on and does
not use this plugin.

With `enableHierarchy: false` the plugin works on flat queues.

## What happens in a session

1. **Open.** The plugin builds its per-queue usage. Every pending pod whose `nominatedNodeName` names
   a node that still holds a pod the scheduler evicted is placed there, bound when the node has idle
   room and pipelined onto the releasing room otherwise, before any action runs.
2. **enqueue.** A PodGroup with `minResources` is admitted when `minResources + allocated + inqueue -
   elastic` fits the capability of its queue and every ancestor. If only an ancestor's capability
   blocks it and `enqueueAncestorCapReclaim` is on, it is admitted when its leaf passes on its own and
   stays within its `deserved` after admission; the PodGroup gets an `Inqueue` condition with reason
   `AncestorCapReclaim`. A PodGroup dequeued less than `enqueueBackoff` ago is rejected.
3. **allocate.** Every task passes the plugin's allocatable check at its queue and every ancestor,
   with the owed share of other subtrees charged when `reserveDeserved` is on.
4. **reclaim.** The upstream action evicts on one node per task, choosing victims by capacity's share
   rules.
5. **Close.** With `quotaReclaim`, the plugin serves what reclaim could not: a task reclaim pipelined
   but the hierarchy refuses gets the quota round; a trial admission's pending tasks get the node
   round and the quota round on the first node that fits. Then every trial admission nothing served
   goes back to Pending with a `Dequeued` condition (reason `ReclaimFailed`), and every admitted
   PodGroup with nothing placed and no verdict after `inqueueTimeout` likewise (reason
   `InqueueTimeout`).

### Volcano Jobs

A Volcano Job's pods are created by the job controller only after its PodGroup is `Inqueue`, so in the
admission session there is no task to evaluate and the trial continues in the sessions after the
pods appear. Set `inqueueTimeout` as the fallback for Jobs whose pods never appear. With the
`SchedulingGatesQueueAdmission` feature gate and the `scheduling.volcano.sh/queue-allocation-gate:
"true"` pod template annotation, the pods are created gated and invisible to autoscalers; the plugin
counts a gated pod that passed its check against the queue until it is allocated.

## Limits

- The reclaim action runs before the plugin's pass and stops at node fit. It can evict a pod for the
  node and pipeline an ask the hierarchy then refuses; the plugin recognizes the shape, serves it with
  the quota round when it can and reverts the trial when it cannot, but the action's own eviction
  stands.
- The reserved-ask pass holds a node for the session only; nothing beyond the nomination is
  persisted.
- Quota-aware reclaim is first fit across nodes; it does not score candidate nodes.

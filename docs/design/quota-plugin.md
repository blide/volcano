# Quota plugin

Status: implemented in `pkg/scheduler/plugins/quota` on an unmodified scheduler. The only in-tree
change is the registration line in the plugin factory, which the `--plugins-dir` loader makes
unnecessary.

## Motivation

The capacity plugin answers two questions from one set of per-queue attributes. The first is share:
which queue goes next, whose pods are evicted first, whether an ask may reclaim, whether a victim may
be taken. Those compare allocated against `deserved` and `guarantee`. The second is quota: whether a
task may be allocated and whether a PodGroup may be admitted. Those compare allocated against
`capability`, up the hierarchy, as a boolean gate at allocate and at enqueue.

A boolean gate leaves jobs behind. A job entitled to its leaf's `deserved` stays Pending behind an
ancestor's `capability` although an over-deserved sibling holds exactly what it needs
([volcano-sh/volcano#4817](https://github.com/volcano-sh/volcano/issues/4817)). The reclaim action
frees quota only with the victims of the node it tries, so an ask whose relief is spread over nodes
is never served; and when the node has room it pipelines the ask with nothing evicted and allocate
refuses it in every following session. After a reclaim, the room the victims free goes to whichever
ask allocate reaches first, which in a failover is often the evicted pod's own replacement.

This plugin owns the quota concern and carries what the gate lacks, with the plugin hooks that exist:

| Concern | Where it runs |
|---|---|
| capability check with the reserve and the gated-task set | `AllocatableFn` |
| admission, with the ancestor-cap reclaim trial and the enqueue backoff | `JobEnqueueableFn`, `JobEnqueuedFn` |
| reserved-ask pass: asks whose victims are still terminating hold their node | `OnSessionOpen`, before any action |
| quota-aware reclaim: asks the reclaim action left to the hierarchy | `OnSessionClose`, after every action, before statuses persist |
| dequeue of trial admissions nothing served, and the inqueue timeout | `OnSessionClose` |

Capacity keeps queue order, victim order, the preemptive and reclaimable checks, and runs with
`enabledAllocatable: false` and `enableJobEnqueued: false`. The session aggregates allocatable and
enqueueable across enabled plugins only, so capability is enforced once. The plugin keeps its own
per-queue counters from the session's allocate and deallocate events, as every queue plugin does.

## Attributes and events

Per queue, rebuilt at session open: the tree; `capability`, inheriting unset dimensions from the
parent; `realCapability`, the parent's real capability minus the guarantees of the parent's other
children plus the queue's own, capped by its capability (the same hold-back as capacity); `deserved`
raised to `guarantee`; `allocated` from every task in an allocated status, summed to every ancestor,
terminating pods excluded; `inqueue`, the unplaced `minResources` of admitted PodGroups; `elastic`,
the usage above `minResources` of running jobs; `reserve`, the deserved share the subtree is still
owed (with `reserveDeserved`). Allocate and deallocate events move `allocated` at the queue and every
ancestor and refresh the reserve.

## Allocatable

For a task of a leaf queue: the queue is open, and at the leaf and every ancestor
`allocated + gated tasks + request + owed to other subtrees <= realCapability` on the task's
dimensions, where "owed to other subtrees" is the ancestor's reserve minus the reserve of the child
on the path. A task already placed in the session (pipelined or allocated) is not counted twice when
asked about.

For an ancestor of the task's own queue the same check restricted to that queue and above. The
plugin uses this itself to find the deepest blocking ancestor; nothing else allocates to a non-leaf.

## Admission

`minResources + allocated + inqueue - elastic <= realCapability` at the leaf and every ancestor. With
`enqueueAncestorCapReclaim`, a job that fails only at an ancestor's capability is admitted on
entitlement: its leaf passes on its own, stays within its deserved after admission with no elastic
credit, and every failing ancestor fails on capability. The admitted PodGroup is tagged with an
`Inqueue` condition, reason `AncestorCapReclaim`. A PodGroup dequeued less than `enqueueBackoff` ago
is rejected.

The strict gate credits the elastic usage of running jobs, usage above their `minResources`, as
reclaimable. Holders whose PodGroups carry no `minResources` are entirely elastic and the strict gate
admits asks on their account without a trial; the trial matters for holders with `minResources`.

## Reserved-ask pass (session open)

The cache writes the node reclaim pipelined an ask on into the pod's nominated node, only when a
pipeline followed an eviction, and the evictor marks its victims with the DisruptionTarget condition.
At session open the plugin takes every pending task whose nomination is live: the node exists, still
carries a scheduler-evicted pod that is terminating, and the task fits the node's future idle, is
allocatable and passes the predicates. In queue, job and task order it binds each on that node when
idle room exists and pipelines it onto the releasing room otherwise, under the allocate action's
commit rule. The ask then holds its room in the session's accounting before any action runs; the
room its victims free never goes to an ask that sorts earlier. A stale nomination falls through to
the actions.

The pass uses the predicates and order functions registered before the plugin: place it in the last
tier.

## Quota-aware reclaim (session close)

After the actions, for every admitted PodGroup with `minResources`:

- a task the reclaim action pipelined but the hierarchy refuses (the node had room, so reclaim
  stopped at node fit with nothing evicted): the quota round alone;
- for a trial admission, every pending task: node by node, the node round evicts that node's
  admissible victims cheapest first until the task fits it physically, the quota round relieves the
  hierarchy, the task is pipelined there.

The quota round: while the hierarchy refuses the task, find the deepest queue on the task's path
whose own check refuses it, take the cheapest admissible candidate whose queue lies under it, from
any node, evict it into the statement, re-read. Candidates are the reclaimees of every node, handed
to the plugins as one list after the node round so capacity's over-deserved checks and gang's
arrival-order veto apply with cumulative counters, in the per-node victim order: victim queue, job,
lowest task priority. The round stops at the first pass, never takes a victim outside the blocked
subtree, and `maxCrossNodeVictims` fails the ask rather than paying for it partly. One statement per
job, committed only when the job is pipelined; otherwise every eviction is rolled back.

This is YuniKorn's shape: victims on the node for the fit, then additional victims under the queue
that lacks headroom. The reclaim action keeps doing capacity's share-aware per-node reclaim; the
plugin does the quota-aware round after it.

## Dequeue (session close)

A PodGroup admitted for a trial whose evaluated tasks neither the reclaim action nor the quota round
served goes back to Pending, with the `Inqueue` condition set false and a `Dequeued` condition
(reason `ReclaimFailed`) whose time starts the backoff. A pipelined task the hierarchy refuses is not
progress. Any other admitted PodGroup with nothing placed and no verdict (a Volcano Job before its
pods appear) is stamped on first sight and returned to Pending after `inqueueTimeout`.

## What upstream's reclaim still does on its own

The reclaim action runs before the plugin's pass and stops at node fit. It can evict a pod for the
node and pipeline an ask the hierarchy then refuses (T4 in the tests): that eviction is upstream's,
the plugin rolls back only its own. It also hands reclaimees to the plugins in the node's map order,
so on a node holding a job's driver and one executor gang's veto can fall on the executor. Both are
small upstream fixes; the plugin's own passes order their candidates cheapest first.

## Configuration

```yaml
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
    enableHierarchy: true
    enabledAllocatable: false
    enableJobEnqueued: false
  - name: nodeorder
  - name: binpack
  - name: quota                     # last: its open pass uses what is registered before it
    enableHierarchy: true
    arguments:
      enqueueAncestorCapReclaim: true
      reserveDeserved: true
      reservedAsks: true            # default true
      quotaReclaim: true            # default false
      maxCrossNodeVictims: 0        # 0 = unbounded
      enqueueBackoff: 1m
      inqueueTimeout: 10m           # 0 = off
```

DRA quota stays with capacity; a deployment that enforces it keeps capacity's allocatable on and
does not use this plugin. The plugin logs once per session when capacity or proportion still gates
capability.

## Tests

`quota_session_test.go` runs one session per case through the upstream actions with the plugin's
close pass invoked explicitly: the trial served by the reclaim action (T1), the 4817 pipeline served
by the quota round (T2) and reverted without it (T2b), victims spread over nodes (T3), nothing
evicted outside the blocked subtree and the trial reverted (T4), the shortfall's worth only (T5),
gang's veto across nodes (T6), the entitlement rejection (T7), the backoff (B1, B2), the timeout (B3,
B4), the reserved-ask pass (R1 to R3), and the reserve against the replacement (S1, S1b).
`quota_test.go` unit-tests the reserve arithmetic and the arguments.

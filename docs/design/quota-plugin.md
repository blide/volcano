# Quota plugin

## Motivation

The capacity plugin answers two different questions from one set of per-queue attributes. The
first is share: which queue goes next, which queue's pods are evicted first, whether an ask may
reclaim at all, whether a victim may be taken. Those compare allocated against `deserved` and
`guarantee`. The second is quota: whether a task may be allocated and whether a PodGroup may be
admitted. Those compare allocated against `capability`, up the hierarchy. Upstream enforces the
second as a boolean gate at allocate and at enqueue and nothing more.

Every quota feature this branch needed went into capacity because that is where the gate lives:
the admission past an ancestor's cap for a reclaim trial, the reserve of the share owed to admitted
jobs, and now the query the quota-aware reclaim round needs to know which ancestor blocks an ask.
Each one widens the fork's patch on a file upstream changes often.

The split puts the quota concern in its own plugin, `quota`, next to capacity. The session already
switches every extension point per plugin, so capacity can keep its share functions and give up its
quota ones by configuration alone. No framework change: the plugin keeps its own per-queue counters
from the same events every other plugin uses, which is what capacity, proportion and DRF each do
today, and the one new query is expressed through the existing allocatable hook.

## Ownership after the split

| Extension point | capacity | quota |
|---|---|---|
| queue order, victim queue order | keeps | |
| preemptive (ask within deserved) | keeps | |
| reclaimable (victim over deserved, ancestor level, guarantee floor) | keeps | |
| allocatable (capability up the hierarchy, reserve, scheduling-gate reserved tasks) | off | owns |
| job enqueueable (capability admission, ancestor-cap reclaim trial) | off | owns |
| job enqueued (inqueue accounting, reserve, trial tag) | off | owns |
| blocking ancestor query for the quota-aware reclaim round | | owns |
| DRA quota (`dynamicResourceAllocation`, consumable capacity) | keeps, see below | |
| hint provider for the unschedulable-job cache | keeps | own, same shape |

Capacity runs with `enabledAllocatable: false` and `enableJobEnqueued: false`. Its hierarchy stays
on, since its share functions need the tree. The session's allocatable and enqueueable aggregators
skip plugins with those flags off, so there is exactly one enforcer of capability.

## The plugin

### Attributes

Per queue, rebuilt at session open from the session's queues and jobs:

- the tree: ancestors from the root, children; the root's capability is infinite when unset;
- `capability` from the spec, inheriting unset cpu, memory and scalar dimensions from the parent;
- `realCapability`: the parent's real capability minus the guarantees of the parent's other
  children, plus the queue's own guarantee, capped by its own capability. The same hold-back
  formula capacity uses; it is the capacity a queue can actually reach when every sibling takes its
  guarantee;
- `deserved`, raised to `guarantee`, for the reserve and for the entitlement test of the trial;
- `allocated`: the requests of every task in an allocated status (bound, binding, running,
  allocated), summed to every ancestor; a terminating pod is not counted;
- `inqueue`: the unplaced `minResources` of Inqueue PodGroups and of running PodGroups that have
  reached `minMember`, deducting scheduling-gated requests, summed to every ancestor;
- `elastic`: usage above `minResources` of running jobs, the credit the enqueue check gives back;
- `reserve`: for a leaf, the unplaced `minResources` of its Inqueue jobs up to its deserved; for a
  parent, the sum over children capped by its own deserved. Only with `reserveDeserved`.

The formulas are copied from capacity on purpose, not shared: a shared package would move
upstream code, and the two plugins must be free to diverge on what they count. Both derive from
the same session, so their `allocated` agrees by construction.

### Events

Allocate and deallocate events add or subtract the task's request at its queue and every ancestor
and refresh the leaf's reserve. A task that passed the allocatable check while scheduling-gated is
held in a reserved set that counts against its queue until it is allocated, as capacity does under
the `SchedulingGatesQueueAdmission` gate.

### Allocatable

For a task of a leaf queue: the queue is open, and at the leaf and every ancestor
`allocated + reserved gated tasks + request + owed to other subtrees <= realCapability` on the
task's dimensions, where "owed to other subtrees" is the ancestor's reserve minus the reserve of the
child on the path to the task.

For an ancestor queue of the task's own queue, the same check restricted to that queue and above,
with the path and its reserve charges still taken from the task's job. This is the blocking-ancestor
query: an action that walks the task's queue ancestry calling `ssn.Allocatable` with each queue
finds the deepest queue that fails. Any other queue is refused. The leaf-only rule capacity applies
to its allocatable check is kept for leaves; the ancestor form exists for this query alone and never
allocates anything, since the actions only ever allocate to a job's own queue.

### Job enqueueable and enqueued

Admission is capacity's check, moved: `minResources + allocated + inqueue - elastic <=
realCapability` at the leaf and every ancestor. When only an ancestor's capability fails and
`enqueueAncestorCapReclaim` is on, the job is admitted on entitlement, as designed for the trial:
the leaf passes on its own, stays within its deserved after admission, and every failing ancestor
fails on capability only; the admission is refused when the dequeue action is not configured. The
enqueued hook adds `minResources` to inqueue at the leaf and every ancestor, registers the job as
owed its share, and tags a trial admission with the Inqueue condition the dequeue action reads.

### Arguments

```yaml
- name: quota
  enabledAllocatable: true
  enableJobEnqueued: true
  enableHierarchy: true
  arguments:
    enqueueAncestorCapReclaim: false   # admit past an ancestor cap for a reclaim trial
    reserveDeserved: false             # hold the owed deserved share at every ancestor
```

Flat queues work with `enableHierarchy: false`: the leaf is the only queue on every path, the
reserve has no parents to cap it and the trial has no ancestor to pass.

### Guard

At session open the plugin reads the tiers. If another plugin has `enabledAllocatable` or
`enableJobEnqueued` on while implementing those hooks on queue capability (capacity, proportion), it
logs once that capability is enforced twice and continues; two gates give the stricter answer, not a
wrong one, but the reserve and the trial only work when this plugin's gate is the one that counts.

## What stays in capacity, and why

DRA quota stays with capacity for now: its simulate hooks and consumable-capacity checks are tied
to capacity's attributes and to the allocatable check it runs during simulation. A deployment that
enforces DRA quota keeps capacity's allocatable on and does not use the quota plugin, until the DRA
side is moved as a second step. The guard above makes the overlap visible.

The ancestor reclaim level stays with capacity: it filters victims by deserved, which is share.

## Configuration

```yaml
actions: "enqueue, allocate, backfill, reclaim, dequeue"
tiers:
- plugins:
  - name: priority
  - name: gang
  - name: conformance
- plugins:
  - name: drf
  - name: predicates
  - name: capacity
    enabledAllocatable: false
    enableJobEnqueued: false
    enableHierarchy: true
    arguments:
      ancestorReclaimLevel: 2
  - name: quota
    enableHierarchy: true
    arguments:
      enqueueAncestorCapReclaim: true
      reserveDeserved: true
  - name: nodeorder
  - name: binpack
configurations:
  - name: reclaim
    arguments:
      victimSelection: bestFit
      crossNodeVictims: true
```

## Migration of this branch

1. New package `pkg/scheduler/plugins/quota`, registered in the plugin factory. Attributes,
   events, allocatable, enqueueable, enqueued, the reserved set for gated tasks, metrics under its
   own names, a hint provider of the same shape as capacity's.
2. Move the ancestor-cap admission and the reserve out of capacity: the commits that added
   `enqueueAncestorCapReclaim`, `reserveDeserved`, the owed-share arithmetic and the trial tag are
   re-targeted at the quota plugin, and capacity returns to upstream plus the ancestor reclaim
   level. The constants the dequeue action reads (the Inqueue condition reason) already live in the
   api package and do not move.
3. The gap table and the reserve tests configure both plugins as above; the capacity-only cases
   that exercised allocatable or enqueue move to the quota package.
4. The quota-aware reclaim round is built against the ancestor form of `ssn.Allocatable` from the
   quota plugin, as described in [quota-aware-reclaim.md](quota-aware-reclaim.md).
5. Docs: the capacity user guide loses the two moved sections, which become the quota plugin's
   user guide; the dequeue design points at the quota plugin for the trial admission.

## Upstream angle

Nothing in capacity changes. The plugin is additive and off unless configured, and the only thing
other code needs from it is already an extension point. That is a smaller ask than the capacity
patches it replaces, but it still asks the maintainers to accept a second owner of the capability
concept, so the plugin should be proposed as what it is: a quota enforcer with reservations and a
reclaim trial, for hierarchies where capacity's gate alone leaves entitled jobs Pending.

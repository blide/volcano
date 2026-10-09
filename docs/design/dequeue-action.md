# Dequeue action

## Motivation

A PodGroup that reaches the `Inqueue` phase is treated by the scheduler as admitted:

- its `minResources` are reserved against its queue and every ancestor (the `inqueue` term of the
  capacity and proportion plugins' enqueue checks), which reduces what other jobs in those queues
  can enqueue;
- for Volcano Jobs, the job controller creates the pods once the PodGroup is `Inqueue`;
- the allocate, preempt and reclaim actions evaluate it every session.

Nothing moves a PodGroup back from `Inqueue`. `getPodGroupPhase` keeps the phase until `minMember`
tasks are scheduled, and no action releases the reservation. A PodGroup that cannot be served
therefore stays `Inqueue` forever: its reservation starves its queue's siblings and the actions
spend work on it every session. Known ways to reach that state include node affinity or node
selectors that the enqueue gate cannot see, a victim that the gang plugin refuses to evict, the
ancestor-capability admission of the capacity plugin when the reclaimable usage is spread over
nodes, and any change of cluster state between admission and service.

The dequeue action returns such a PodGroup to `Pending`. It does not make a job servable; it
releases the reservation of a job the scheduler has established it cannot serve, and lets the
enqueue gate decide again later.

## The reclaim verdict

The primary trigger is explicit, not time based. The reclaim action now records a per-session
verdict on every job it evaluates (`api.JobInfo.ReclaimResult`, never persisted):

| Verdict | Meaning |
|---|---|
| `ReclaimSucceeded` | reclaim pipelined a task of the job in this session |
| `ReclaimFailed` | eligible victims existed on at least one node, but no node could be made to fit the job physically and in its queue hierarchy, so every tentative eviction was rolled back |
| `ReclaimNoVictims` | reclaim evaluated the job and found no eligible victim on any candidate node |
| `ReclaimNotAttempted` | reclaim did not evaluate the job (not starving, queue overused, no task passed the preemptive or pre-predicate checks, or the action is not configured) |

`ReclaimFailed` is a safe immediate trigger. The one transient that looks like a failure is the
termination window after a successful reclaim, while the victims drain; but a terminating task is
not counted in the victim queue's allocation and the node's future idle already includes it, so in
those sessions reclaim finds the job allocatable, fits, and pipelines it without evicting. That is a
success, not a failure. A verdict is therefore only `ReclaimFailed` when the current cluster state
cannot serve the job, and the enqueue backoff covers the case where that state changes later.

`ReclaimNoVictims` is ordinary waiting for capacity and leaves the job `Inqueue`, with one
exception: a PodGroup the capacity plugin admitted past an ancestor's capability on entitlement
(`enqueueAncestorCapReclaim`) carries an `Inqueue` condition with reason `AncestorCapReclaim`. Its admission depended on reclaim, so `ReclaimNoVictims` means the premise is
false and the job is dequeued as well.

## Behavior

Each session, for every job whose PodGroup is `Inqueue` and declares `minResources` (a PodGroup
without `minResources` reserves nothing and is admitted unconditionally, so dequeuing it would only
add churn) and which has no task `Pipelined`, `Allocated`, `Binding`, `Bound`, `Running` or
`Succeeded`:

1. `ReclaimFailed`: dequeue now, reason `ReclaimFailed`.
2. `ReclaimNoVictims` on a PodGroup tagged `AncestorCapReclaim`: dequeue now, reason
   `ReclaimFailed`.
3. Otherwise, if `inqueueTimeout` is configured: record an `Inqueue` condition on first sight and
   dequeue once it is older than the timeout, reason `InqueueTimeout`. This is the fallback for jobs
   reclaim never evaluates, for example Volcano Jobs whose pods do not exist yet or configurations
   without the reclaim action. It is off by default.

Dequeuing sets the phase to `Pending`, sets the `Inqueue` condition to `False`, records a
`Dequeued` condition with the reason, and emits a `Dequeued` event on the PodGroup. The enqueue
action keeps a `Pending` PodGroup out of the enqueue candidates while its `Dequeued` condition is
younger than `enqueueBackoff`, which bounds the cycle of a job that is never served to one attempt
per `enqueueBackoff` plus one session. The conditions live in the PodGroup status and are
persisted by the job updater like the `Unschedulable` and `Scheduled` conditions.

For Volcano Jobs, moving the PodGroup back to `Pending` does not delete pods already created; the
job controller stops syncing tasks for a `Pending` PodGroup and resumes when it is `Inqueue` again.

## Configuration

The action must run after `reclaim`; last is simplest:

```yaml
actions: "enqueue, allocate, backfill, reclaim, dequeue"
configurations:
- name: dequeue
  arguments:
    enqueueBackoff: 1m    # how long a dequeued PodGroup stays Pending before enqueue reconsiders it; default 1m
    inqueueTimeout: 10m   # optional fallback for jobs without a reclaim verdict; default 0 (disabled)
```

Both arguments are Go duration strings. Invalid or non-positive values fall back to the default.
With the defaults the action is driven purely by reclaim's verdict.

## Interactions

- The unschedulable-job cache (`job.Skip.Enqueue`) is independent: it suppresses enqueue attempts
  based on rejection hints, while the backoff here is time based. Both are honored.
- The capacity plugin's `enqueueAncestorCapReclaim` admission is a trial that relies on this
  action: it admits on entitlement alone, reclaim tries in the same session, and this action
  reverts the admission on a failed verdict. The plugin refuses the relaxed admission when this
  action is not enabled. See the capacity plugin user guide.
- The gang plugin's `Unschedulable` condition is unaffected; the `Inqueue` and `Dequeued` conditions
  are additional entries in the same list. The verdict is derived from what reclaim did, not from
  the session's job-pipelined query, which defaults to permit when no plugin implements it.

## Tests

`pkg/scheduler/actions/dequeue/dequeue_test.go` covers every verdict, the tag exception, the
timeout lane, progress detection, the `minResources` exemption, the enqueue backoff in both
directions and re-stamping after re-admission. In `Test_capacityPlugin_ReclaimOnAncestorCapacityStarvation`,
G14 admits a job on cluster-wide slack that reclaim cannot serve on any single node, asserts the
`ReclaimFailed` verdict, and shows the job returned to `Pending` in the same session with nothing
evicted; G15 shows the opt-in timeout doing the same for a job reclaim never evaluated.

## Known gaps and follow-ups

Where the admission-trial design (capacity `enqueueAncestorCapReclaim` + reclaim verdict + dequeue)
can still leave a job in the wrong state, with the intended follow-up for each. None of these is
implemented yet.

### Volcano Jobs: the trial spans sessions and pods get created

- A vcjob has no pods until the PodGroup is `Inqueue`. In the admission session reclaim finds no
  tasks, the verdict is `ReclaimNotAttempted`, dequeue does nothing, the phase persists as
  `Inqueue`, the job controller creates the pods, and only the next session yields a real verdict.
  The trial therefore spans sessions for vcjobs and the reservation lasts that long.
- Mitigation in place: with the `SchedulingGatesQueueAdmission` feature gate and the opt-in pod
  annotation, the pods are created gated, autoscalers never see them, and the reclaim action now
  treats a pod gated only by Volcano's queue-allocation gate as an asker (`queueGatedAsker`), so the
  trial runs on the real gated pods and allocate removes the gate once the queue check passes.
  See the capacity plugin user guide, "Volcano Jobs".
- If the pods never appear, or are gated by something other than Volcano's gate, reclaim skips the
  job every session and no verdict is ever produced. Only the opt-in `inqueueTimeout` covers this.
  Follow-up: treat a tagged group with `ReclaimNotAttempted` for N consecutive sessions as a failed
  trial.
- On a dequeue the created pods are not deleted; they stay Pending (gated, if the feature is on)
  until re-admission, when the next trial runs in the same session.
- The verdict is keyed on the commit: a statement discarded by the job-pipelined check (gang's
  minAvailable) after a partial pipeline is `ReclaimFailed`, because victims existed and the job
  was not served. The legacy reclaim action is task-level and applies gang semantics only at
  commit; gang-level victim reasoning belongs to the gangreclaim action, which has no verdict yet
  and is out of scope here.

### Asks reclaim refuses to evaluate

- A pod with `preemptionPolicy: Never`, or a pre-predicate failure, is skipped before the attempt is
  counted. The job is admitted, tagged, never gets a verdict, and stays `Inqueue`. Follow-up: the
  gate refuses the relaxed admission when the job's pending tasks carry `PreemptNever`.
- A leaf with no `deserved` never passes the entitlement check and never uses the relaxed path.
  By design; document.

### Order and configuration

- The guard checks that dequeue is enabled, not that it runs after reclaim. With dequeue before
  reclaim every verdict it sees is `ReclaimNotAttempted` and nothing is reverted. Follow-up:
  validate the action order in the scheduler configuration loader (`pkg/scheduler/scheduler.go`,
  where the action list is parsed). Not a priority.
- Other enqueue voters still reject before the trial starts (`overcommit` on a full cluster, `sla`,
  `resourcequota`, `extender`). Expected; document.

### Lost work and churn

- Pipelining is not persisted across sessions and Volcano has no node reservation. After a
  successful trial the victims drain; if another pod takes the freed space first, the next session
  finds no victims left and dequeues the asker. The evictions were spent for nothing. The capacity
  plugin's `reserveDeserved` argument closes the common case: while the asker is owed its deserved
  share, every ancestor refuses candidates from other subtrees (the victim's replacement above
  all) that would consume it. What remains is the node-level race between two queues both within
  their deserved, decided by queue order. Follow-up: a node hold derived from the nomination the
  cache already writes after an eviction-backed pipeline (`pod.Status.NominatedNodeName`), the way
  YuniKorn reserves the node for the ask.
- Two jobs admitted in one session can compete for the same victims; the second finds them already
  terminating, gets `ReclaimNoVictims`, and is dequeued, then retried after the backoff.
- A hopeless but entitled job (G13's shape) is re-admitted every `enqueueBackoff`: one full reclaim
  trial, two status writes and a `Dequeued` event per cycle. Follow-up: exponential backoff, which
  needs an attempt counter carried in the `Dequeued` condition.
- The gang plugin writes an `Unschedulable` condition and bumps the job-retries metric on every
  failed session of an admitted job. Noise, not harm.

### Preempt has no verdict

- A job admitted on the elastic credit of its own queue is served by same-queue preemption, which
  records no verdict. Dequeue leaves it alone; only the timeout applies. Follow-up: the same verdict
  in the preempt action, with dequeue acting when neither action could serve the job.

### The remaining structural limit

- Reclaim frees quota only with victims on the node it is trying (G13). The trial recovers such a
  job instead of leaving it stuck, but does not serve it. Follow-up, designed but not implemented:
  a second victim round in `reclaimForTask`, behind a reclaim action argument (`crossNodeVictims`,
  default off), modeled on YuniKorn's `calculateAdditionalVictims`:
  - runs once per task, on the first node the task fits physically after the local round while
    `ssn.Allocatable` still fails, so the queue hierarchy is the only blocker;
  - candidates are the reclaimees of every other node, filtered by `ssn.Reclaimable` (the capacity
    plugin already keeps only over-deserved victim queues on the task's dimensions, applies
    `ancestorReclaimLevel` and the gang veto, and sees the local evictions through its
    `DeallocateFunc` counters);
  - victims are ordered by the depth of their nearest common ancestor with the asker's queue,
    deepest first, then by the usual victim order: an eviction lowers `allocated` at every
    ancestor of the victim's queue, so it relieves the blocking ancestor only if it sits in that
    ancestor's subtree, and `Allocatable` is boolean, so this ordering stands in for knowing the
    blocker;
  - evicted one at a time into the same per-node statement until `Allocatable` passes, so the
    isolation of PR #5067 holds: nothing is committed unless the pipeline on that node succeeds;
  - tests: G13 served with the flag (both victims, pipelined on n1), G14 served instead of
    dequeued, G16 unchanged (no node fits physically, so no second round), and a two-tenant case
    where the sibling's pod on another node is chosen over an over-deserved pod of another tenant.

### Victim choice across nodes: cost, not exemption

- Reclaim is first-fit per node: it walks the candidate nodes in list order and commits on the
  first node where the asker fits after evicting that node's admissible victims, sorted lowest
  priority first. Priority therefore orders victims only within a node. A pod that is the only
  reclaimable pod on an early node is evicted even when cheaper victims exist on the next node,
  which is how a Spark driver gets reclaimed although its executors were available.
- The hard filter (`volcano.sh/preemptable: "false"`) is the only protection today, and it is
  user-settable: a tenant that labels everything never returns borrowed capacity, and no queue
  setting overrides it. Guarantees of other queues become unenforceable.
- Follow-up, designed but not implemented: evaluate every candidate node, keep each per-node
  statement open, score the victim sets and commit the cheapest, modeled on YuniKorn's solution
  score and kube-scheduler's preemption rule: the priority of the highest-priority victim first,
  then the victim count; a node that fits without eviction scores zero. Priority then is a cost:
  a high-priority driver is taken only when no cheaper node exists, and abusing a PriorityClass
  buys "evicted last", never "never evicted". PriorityClasses are cluster-scoped and can be capped
  per namespace with a ResourceQuota scope. The label stays a filter for the few pods that truly
  need it and should not be grantable to users. The hierarchy round above would run on the chosen
  node.

### Reclaim retry throttle

- A pipelined asker whose nominated node still carries terminating victims is Pending again next
  session; if it does not fit yet, reclaim evicts again. Preempt already waits in that case
  (`taskEligibleToPreempt`). Follow-up: skip an asker in reclaim while its nominated node has a
  terminating pod evicted by the scheduler, counted as not attempted so the dequeue action leaves
  it alone. A few lines; removes most double evictions before any node hold is considered.

### External gangs: Spark

- Spark's own Volcano integration (the `VolcanoFeatureStep`) and the PodGroup controller both put
  the driver and all executors in one PodGroup with `minMember` 1: the controller names an
  auto-created group after the controller owner reference, and executors are owned by the driver
  pod, so plain spark-submit yields `podgroup-<driver UID>` for every pod with `minResources` equal
  to the driver's request. The Spark operator gives the driver its own group (owned by the
  SparkApplication) and the executors a shared one.
- With gang's starving rule the job stops being starving once the driver runs, so executors never
  trigger reclaim. Upstream workaround: take `jobStarving` from the priority plugin and disable
  gang's. A `minMember` above one deadlocks the driver, which must run to create the executors;
  Volcano has no placeholder mechanism for externally created pods.

### Gang actions and the reserve

- gangpreempt and gangreclaim check queues on the simulated path: PrePredicate clones every
  queue attribute into a per-task cycle state, `SimulateAddTask`/`SimulateRemoveTask` adjust
  `allocated` on the clones, and `SimulateAllocatableFn` checks the clones without the
  `reserveDeserved` reserve. Follow-up, designed but not implemented: make the reserve helpers
  take the attribute map (session map on the real path, state map on the simulated one), refresh
  the touched leaf and re-fold its ancestors in the simulate hooks (removing a victim raises what
  its leaf is owed), and pass the owed share in the simulated check. gangreclaim also has no
  reclaim verdict for the dequeue action.

### What the trial fixes that the estimate could not

- Affinity, taints and node selectors the gate cannot see: the preempt-time node filter yields no
  nodes, the verdict is `ReclaimNoVictims`, and the tag makes dequeue revert the admission
  (volcano-sh/volcano#3373 acknowledged this as unsolved upstream).

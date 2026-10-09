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
exception: a PodGroup the capacity plugin admitted past an ancestor's capability on reclaimable
slack (`enqueueAncestorCapReclaim`) carries an `Inqueue` condition with reason
`AncestorCapReclaim`. Its admission depended on reclaim, so `ReclaimNoVictims` means the premise is
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
- The capacity plugin's `enqueueAncestorCapReclaim` admission is the main producer of jobs this
  action cleans up; see the capacity plugin user guide. Enabling that admission without this action
  leaves no bound on a mis-admitted job's reservation.
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

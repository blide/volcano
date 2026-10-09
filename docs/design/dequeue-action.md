# Dequeue action

## Motivation

A PodGroup that reaches the `Inqueue` phase is treated by the scheduler as admitted:

- its `minResources` are reserved against its queue and every ancestor (the `inqueue` term of the
  capacity and proportion plugins' enqueue checks), which reduces what other jobs in those queues
  can enqueue;
- for Volcano Jobs, the job controller creates the pods once the PodGroup is `Inqueue`;
- the allocate, preempt and reclaim actions evaluate it every session.

Nothing moves a PodGroup back from `Inqueue`. `getPodGroupPhase` keeps the phase until `minMember`
tasks are scheduled, and no action releases the reservation. A PodGroup that was admitted on an
assumption that does not hold therefore stays `Inqueue` forever: its reservation starves its queue's
siblings and the actions spend work on it every session. Known ways to reach that state include
node affinity or node selectors that the enqueue gate cannot see, a victim that the gang plugin
refuses to evict, the ancestor-capability admission of the capacity plugin when the reclaimable
usage is spread over nodes, and any change of cluster state between admission and service.

The dequeue action is a bounded safety net for all of them. It does not make a job servable; it
returns a job that is not being served to `Pending` so that the reservation is released and the
enqueue gate decides again later.

## Behavior

Each session, for every job whose PodGroup is `Inqueue` and declares `minResources` (a PodGroup
without `minResources` reserves nothing and is admitted unconditionally, so dequeuing it would only
add churn):

1. If any task is `Pipelined`, `Allocated`, `Binding`, `Bound`, `Running` or `Succeeded`, the job is
   making progress and is left alone.
2. Otherwise, if the PodGroup has no `Inqueue` condition with status `True`, one is added with the
   current time. This starts the clock; it also covers PodGroups that became `Inqueue` before the
   action was enabled or through another component.
3. Otherwise, if the `Inqueue` condition is older than `inqueueTimeout`, the PodGroup phase is set
   to `Pending`, the `Inqueue` condition is set to `False`, a `Dequeued` condition with status `True`
   and reason `InqueueTimeout` is recorded, and a `Dequeued` event is emitted on the PodGroup.

The enqueue action keeps a `Pending` PodGroup out of the enqueue candidates while its `Dequeued`
condition is younger than `enqueueBackoff`. Once re-admitted, the next dequeue pass stamps a fresh
`Inqueue` condition, so each admission gets a full `inqueueTimeout` again. A job that can never be
served therefore cycles with a period of `inqueueTimeout + enqueueBackoff`, holding its reservation
for at most `inqueueTimeout` out of every period.

The conditions live in the PodGroup status and are persisted by the job updater like the
`Unschedulable` and `Scheduled` conditions, so the clock survives scheduler restarts.

For Volcano Jobs, moving the PodGroup back to `Pending` does not delete pods already created; the
job controller stops syncing tasks for a `Pending` PodGroup and resumes when it is `Inqueue` again.

## Configuration

Place the action last so that it observes what the other actions achieved in the same session:

```yaml
actions: "enqueue, allocate, backfill, reclaim, dequeue"
configurations:
- name: dequeue
  arguments:
    inqueueTimeout: 10m   # how long a PodGroup may stay Inqueue with no scheduled task; default 10m
    enqueueBackoff: 10m   # how long a dequeued PodGroup stays Pending before enqueue reconsiders it; default: inqueueTimeout
```

Both arguments are Go duration strings. Invalid or non-positive values fall back to the default.

`inqueueTimeout` should be longer than the time reclaim or preempt need to serve a job that can be
served, including the pod creation delay for Volcano Jobs; several scheduling periods is a safe
lower bound, and the default of ten minutes is deliberately conservative.

## Interactions

- The unschedulable-job cache (`job.Skip.Enqueue`) is independent: it suppresses enqueue attempts
  based on rejection hints, while the backoff here is time based. Both are honored.
- The capacity plugin's `enqueueAncestorCapReclaim` admission is the main producer of jobs this
  action is meant to clean up; see the capacity plugin user guide. Enabling that admission without
  this action is possible but leaves no bound on a mis-admitted job's reservation.
- The gang plugin's `Unschedulable` condition is unaffected; the `Inqueue` and `Dequeued` conditions
  are additional entries in the same list.

## Tests

`pkg/scheduler/actions/dequeue/dequeue_test.go` covers the stamp, the timeout, progress detection,
the `minResources` exemption, the enqueue backoff in both directions, and re-stamping after
re-admission. Case G14 of `Test_capacityPlugin_ReclaimOnAncestorCapacityStarvation` runs the action
behind enqueue, reclaim and allocate on a job reclaim cannot serve and shows it returned to
`Pending` with nothing evicted.

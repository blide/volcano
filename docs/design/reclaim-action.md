# Reclaim

## Introduction

In kube-batch there are 4 actions such as allocate, preempt, reclaim, backfill and with the help of plugins like conformance, drf, gang, nodeorder and more plugins. All these plugins provides behavioural characteristics how scheduler make scheduling decisions.

## Reclaim Action

Reclaim is one of the actions in kube-batch scheduler.  Reclaim action comes into play when
a new queue is created, and new job comes under that queue but there is no resource / less resource
in cluster because of change of deserved share for previous present queues.

When a new queue is created, resource is divided among queues depending on its respective weight ratio.
Consider two queues is already present and entire cluster resource is used by both the queues.  When third queue
is created, deserved share of previous two queues is reduced since resource should be given to third queue as well.
So jobs/tasks which is under old queues will not be evicted until, new jobs/tasks comes to new queue(Third Queue).  At that point of time,
resource for third queue(i.e. New Queue) should be reclaimed(i.e. few tasks/jobs should be evicted) from previous two queues, so that new job in third queue can
be created.

Reclaim is basically evicting tasks from other queues so that present queue can make use of it's entire deserved share for
creating tasks.

In Reclaim Action, there are multiple plugin functions that are getting used like,

1.  TaskOrderFn(Plugin: Priority),
2.  JobOrderFn(Plugin: Priority, DRF, Gang),
3.  NodeOrderFn(Plugin: NodeOrder),
4.  PredicateFn(Plugin: Predicates),
5.  ReclaimableFn(Plugin: Conformance, Gang, Proportion).

### 1. TaskOrderFn:
#### Priority:
Compares taskPriority set in PodSpec and returns the decision of comparison between two priorities.

### 2. JobOrderFn:
#### Priority:
Compares jobPriority set in Spec(using PriorityClass) and returns the decision of comparison between two priorities.

#### DRF:
The job having the lowest share will have higher priority.

#### Gang:
The job which is not yet ready(i.e. minAvailable number of task is not yet in Bound, Binding, Running, Allocated, Succeeded, Pipelined state) will have high priority.

### 3. NodeOrderFn:
#### NodeOrder:
NodeOrderFn returns the score of a particular node for a specific task by running through sets of priorities.

### 4. PredicateFn:
#### Predicates:
PredicateFn returns whether a task can be bounded to a node or not by running through set of predicates.

### 5. ReclaimableFn:
Checks whether a task can be evicted or not, which returns set of tasks that can be evicted so that new task can be created in new queue.
#### Conformance:
In conformance plugin, it checks whether a task is critical or running in kube-system namespace, so that it can be avoided while computing set of tasks that can be preempted.
#### Gang:
It checks whether by evicting a task, it affects gang scheduling in kube-batch.  It checks whether by evicting particular task,
total number of tasks running for a job is going to be less than the minAvailable requirement for gang scheduling requirement.
#### Proportion:
It checks whether by evicting a task, that task's queue has allocated resource less than the deserved share.  If so, that task
is added as a victim task that can be evicted so that resource can be reclaimed.

## Victim selection

Reclaim evicts on one node per asker. The node loop asks the plugins for the admissible victims
among the node's Running, preemptable tasks of other reclaimable queues, evicts them into a per-node
statement until the asker fits the node and its queue hierarchy, and pipelines the asker there. Two
decisions shape which pods get evicted: the order the victims are tried in on a node, and which node
is committed.

### Order within a node

Victims on a node are tried through `BuildVictimsPriorityQueue`: victim queue order, then job order,
then the lowest task priority first. The reclaim action hands the reclaimees to the plugins in that
same order. This matters for plugins whose veto depends on arrival order: gang's `ReclaimableFn`
admits victims until the job would drop below `minAvailable`, so on a node holding a job's driver
and one of its executors it admits whichever arrives first. Cheapest first means the executor is
admitted and the driver vetoed, never the reverse.

### Which node: `victimSelection`

The action argument `victimSelection` picks the node:

- `firstFit` (default): the first candidate node whose per-node plan succeeds is committed. The
  candidate list comes from the predicate helper, which fills it in parallel, so the order is not
  stable: which node is first, and how expensive its victims are, is incidental. A pod that is the
  only reclaimable pod on an early node is evicted although cheaper victims exist on the next node,
  which is how a Spark driver gets reclaimed while its executors were available.
- `bestFit`: every candidate node is planned the same way, each plan is rolled back, and the cheapest
  one is replayed and committed. Plans are ranked with the keys the per-node victim order already
  uses, so the ranking is fenced the same way: a plan without victims first; then the plan whose
  most protected victim queue (the one the victim queue order ranks last among the queues its
  victims belong to) the victim queue order evicts earlier, which for the capacity plugin is the
  queue nearest the asker in the hierarchy, then the one with the higher share; only between plans
  hurting the same queue the lowest highest-victim pod priority; then the number of victims; then
  the node name so the result does not depend on the candidate order. A node that fits without
  eviction costs less than any eviction and ends the search. The argument `maxCandidateNodes`
  bounds how many nodes with a viable plan are explored before committing; `0` (default) explores
  all candidates.

The rollback-and-replay shape keeps the per-node isolation introduced for first-fit: evictions on
nodes that end up unused are never committed, and the replay runs in the same session with nothing
in between, so the recorded victims are still Running when they are evicted for real.

Priority is thereby a cost rather than an exemption, and a cost local to the queue that uses it. A
high-priority driver is taken only when no node can be served by its queue's lower-priority pods,
abusing a PriorityClass buys "evicted last among my own pods", never "never evicted" and never
"after the other tenants": between queues the victim queue order decides, as it does within a
node. The hard filter `volcano.sh/preemptable: "false"` remains for the few pods that
truly must not move; because it is user-settable and no queue setting overrides it, a tenant that
labels everything never returns borrowed capacity, so it should not be grantable to users.
PriorityClasses are cluster-scoped and can be capped per namespace with a ResourceQuota scope.

Out of scope for this selection: the cost is per node, so it does not combine victims from several
Out of scope for this selection: the cost is per node, so it does not combine victims from several
nodes to free a queue's quota. Across sessions the freed room is held by the allocate action's
reserved-ask pass (below), which is why `allocate` must stay ahead of `reclaim` in the action list.

Configuration:

```yaml
actions: "enqueue, allocate, backfill, reclaim"
configurations:
  - name: reclaim
    arguments:
      victimSelection: bestFit   # firstFit (default) or bestFit
      maxCandidateNodes: 0       # bestFit only; 0 explores every candidate node
```

## Waiting on the nominated node

A pipelined asker is Pending again in the next session while the victims evicted for it are still
terminating. Two things keep reclaim from evicting again for it, every session, for the whole
termination grace period. The cache writes the node reclaim pipelined the asker on into the pod's
`nominatedNodeName` whenever a pipeline followed an eviction, and the evictor marks its victims with
the DisruptionTarget condition, reason "preemption by scheduler".

The allocate action's reserved-ask pass uses both. Before its nominated-hypernode and regular phases
it collects the pending tasks whose nomination is live: the nominated node exists, still carries a
pod the scheduler evicted that is terminating, and the task fits the node's future idle, is
allocatable in its queue hierarchy and passes the predicates. It visits them in queue, job and task
order, re-checks the nomination at service time, and places each on its nominated node: bound when
the node already has idle room, pipelined otherwise, under the regular phase's commit rule. The
pipelined ask takes that room in the session's accounting, so no other ask, not even one that sorts
earlier in queue order, is pipelined onto the room this ask paid for; without the pass the room went
to the first ask in order, which in a failover is often another tenant's over-deserved replacement,
and the originator reclaimed a second time. A stale nomination (victims gone, room taken) falls
through to the regular phases and then to reclaim.

This is YuniKorn's reserved allocation in Volcano's terms. It holds only with `allocate` ahead of
`reclaim`: with reclaim first, a node whose terminating pods were its only reclaimable ones yields no
plan, the asker is planned afresh and evicts elsewhere, again in every session until the victims are
gone. The hold is per session and per ask; nothing beyond the nomination is persisted.

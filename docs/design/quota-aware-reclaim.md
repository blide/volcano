# Quota-aware reclaim

## Motivation

The reclaim action frees room one node at a time. For an ask it walks the candidate nodes and, on
each, evicts that node's admissible victims until the ask fits the node and is allocatable in its
queue hierarchy, then pipelines it there. The queue term of that stop condition (issue 4817) makes
the node round quota-aware for victims that happen to sit on the chosen node, and that is all the
quota awareness there is: an ask blocked by an ancestor's capability is served only if one node
both hosts it and holds enough over-deserved victims to clear the shortfall. Victims spread over
other nodes do not count, however cheap. Gap table G13 is the plain case: the tenant is at its cap,
its over-deserved child holds 2c on each of two nodes, the standby ask needs 4c, fits either node
physically, and is never served.

The admission trial (`enqueueAncestorCapReclaim`, the reclaim verdict and the dequeue action) admits
such an ask on the strength of the ancestor's reclaimable slack, which is a queue-level number, and
hands it to reclaim. While reclaim can only realize that slack per node, the trial fails exactly
where it was meant to help, and the dequeue action mostly undoes an admission that was correct on
paper. Quota-aware reclaim closes that gap: the amount the hierarchy owes the ask is freed from
wherever the over-deserved holders run, and the node is chosen for the ask's physical fit alone.

This is the shape of YuniKorn's preemption: victims on the node to make the ask fit, then
additional victims anywhere under the blocking queue until the queue has headroom.

## Design

### Two rounds per ask

Behind a reclaim action argument, `crossNodeVictims` (default `false`), `reclaimForTask` serves an
ask in two rounds on each candidate node:

1. **Node round.** As today, but the stop condition is the physical one: the ask fits the node's
   future idle after the tentative evictions and the predicates pass. The queue term moves to the
   second round, so the node round evicts the minimum the node needs and never pays for quota with
   victims that merely happen to be local. With the argument off, the stop condition keeps the
   queue term and nothing below runs: current behavior.
2. **Quota round.** If `ssn.Allocatable(queue, task)` still fails, the hierarchy is the only
   blocker. The round evicts, one at a time and cheapest first, victims that relieve the blocking
   ancestor, from any node, into the same per-task statement, until `Allocatable` passes or the
   candidates run out. Every tentative eviction runs the plugins' `DeallocateFunc`, so the capacity
   plugin's `allocated` counters, and with them `Allocatable`, see each victim as it is chosen; the
   node's own victims from round one already count.

Then the pipeline on that node as today. If either round fails the statement is discarded and the
next node is tried. Isolation is unchanged: nothing is committed unless the pipeline on the chosen
node succeeds, and a discarded statement restores every victim, local or remote.

### Knowing what blocks

`Allocatable` is boolean. Evicting a victim outside the blocking ancestor's subtree lowers
`allocated` somewhere else and changes nothing for the ask, so the quota round must know which
ancestor blocks. The [quota plugin](quota-plugin.md) answers this through the existing allocatable
hook: called with an ancestor of the task's own queue, it runs the capability check restricted to
that queue and above, with the path and its reserve charges still taken from the task's job. The
round walks the task's queue ancestry from the leaf to the root calling `ssn.Allocatable` with each
queue; the deepest queue that fails is the blocker. No new extension point, and no framework
change. (The alternative, a dedicated `BlockingQueuesFn` returning the failing ancestors deepest
first, costs one aggregator in the session and is the cleaner API if the maintainers prefer it.)

The round re-reads the blocker after every eviction. The deepest blocker decides the candidate
set, because a victim under it relieves every blocker above it as well; when the deepest blocker
clears and a shallower one remains, the candidate set widens to that ancestor's subtree.

### Candidates and order

Candidates for the quota round are the reclaimees of every node in the session: Running,
preemptable tasks of other, reclaimable queues than the ask's, exactly the per-node definition
widened to all nodes, minus the victims round one already took. They are handed to
`ssn.Reclaimable` as one list, cheapest first, so the capacity plugin applies its over-deserved
and `ancestorReclaimLevel` checks with cumulative counters across the whole list and the gang
plugin's arrival-order veto spends its admissions on the cheap tasks.

Of the admitted victims, the round takes at each step the cheapest one whose queue is in the
deepest blocker's subtree, in the victims-queue order the per-node round already uses: victim queue
first (for the capacity plugin the queue nearest the asker in the hierarchy, then the one with the
higher share), then job order, then lowest task priority. Nearest first is the right default under
a failover tree: the sibling that is over its deserved is drained before anything in another
tenant, and another tenant is touched only when it is itself over its deserved and inside the
blocking ancestor's subtree, which at the root level is everyone.

Ancestry is the queue's `spec.parent` chain, read from `ssn.Queues`; the action needs no plugin
internals for the subtree test.

### Bounds

- The round stops the moment `Allocatable` passes. A victim's request can exceed the remaining
  shortfall, so the overshoot is bounded by one victim per blocked dimension.
- `maxCrossNodeVictims` (default `0`, unbounded) caps the victims one ask may take in the quota
  round; reaching it fails the ask on that node.
- The candidate list is built once per ask and reused across candidate nodes.

### bestFit

With `victimSelection: bestFit` a node's plan is the union of both rounds. The plan cost is
unchanged in its keys: most protected victim queue, highest victim priority, victim count, node
name. Because the quota round picks the same cheapest victims whatever the node, plans differ
mostly in their node round, and bestFit keeps choosing the node whose physical victims cost least.
Rollback and replay cover both rounds: the recorded victims are evicted again in order and the
pipeline follows.

### What holds the freed quota

The quota round frees quota, not room on the chosen node, and the freed quota must still be there
in the session after the victims are gone:

- the slot on the node is held by the allocate action's reserved-ask pass, as for any reclaimed
  ask (the pod carries the node as its nominated node);
- the quota is held by `reserveDeserved` for as long as the ask's leaf is owed its deserved: the
  owed share is charged at every ancestor against everyone else, so the over-deserved sibling's
  replacement pods cannot take it back. An ask above its leaf's deserved, elastic growth, is not
  owed anything and keeps the freed quota only within the session it was pipelined in; if it is
  not placed then, the quota can go to whoever allocate reaches first. Quota-aware reclaim is
  therefore reliable for asks under their deserved, which is what the admission trial admits, and
  best effort beyond that.

### Verdict and the dequeue action

The verdict keeps its meaning and gains the case it was designed for. `ReclaimFailed` now says
that admissible victims existed and neither a node nor the hierarchy could be made to fit, after
both rounds; `ReclaimNoVictims` that no plugin admitted any victim anywhere. The dequeue action
returns an ask to `Pending` only when the whole cluster's over-deserved holders cannot clear the
shortfall, which is the one case the admission estimate cannot see: holders the gang veto keeps,
holders on dimensions the ask does not request, and physical fragmentation (G16: no node can host
the ask even with every victim gone). The admission estimate and the trial then measure the same
thing, queue-level slack, and the trial is the exact check.

### Out of scope

- Victims for physical fit stay on the chosen node. Making room on a node with victims from
  elsewhere is meaningless.
- No reservation of nodes or queues across sessions beyond the reserved-ask pass and
  `reserveDeserved`; nothing new is persisted.
- The gang actions (gangreclaim, gangpreempt) plan whole bundles with their own domain logic and
  are untouched.
- Flat queues are covered trivially: the only blocker is the leaf, and candidates are the
  reclaimees of other queues on every node.

## Configuration

```yaml
actions: "enqueue, allocate, backfill, reclaim, dequeue"
configurations:
  - name: reclaim
    arguments:
      victimSelection: bestFit     # optional, composes
      crossNodeVictims: true       # quota round on; default false
      maxCrossNodeVictims: 0       # 0 = unbounded
```

Requires the quota plugin for the ancestor form of the allocatable check; with flat queues the leaf
is the only blocker and the round still applies.

## Implementation plan

1. Quota plugin: the ancestor form of its allocatable check (see quota-plugin.md), sharing the
   per-ancestor loop with the leaf form.
2. Reclaim action: argument parsing; `reclaimerFitsOnNode` loses the queue term when the round is
   on; `quotaRound(ssn, plan, queue, task, candidates)` evicting into the plan's statement;
   `crossNodeReclaimees` building the candidate list once per ask; integration in `planOnNode`
   before the pipeline, and in `replayPlan` through the recorded victims.
3. Tests, gap table section L:
   - L1: G13 served, both holders evicted, pipelined on the node with room.
   - L2: G14 served instead of dequeued.
   - L3: G16 unchanged, physical fragmentation still fails and dequeues.
   - L4: two tenants, the sibling's holder on another node is taken before another tenant's
     over-deserved pod that is outside the blocked subtree.
   - L5: shortfall 2c with two 2c holders on another node, exactly one is evicted.
   - L6: gang veto across nodes takes the executor, not the driver.
   - L7: bestFit with the quota round, node choice by the physical victims' cost.
   - L8: `ancestorReclaimLevel: 2` applied to cross-node candidates.
   - L9: two sessions with `reserveDeserved`, the freed quota is taken by the ask, not by the
     sibling's replacement.
   - L10: no admissible victim in the blocked subtree, verdict `ReclaimFailed`, dequeued.
4. Docs: this document, the reclaim design's "Victim selection" section, the dequeue design's
   "remaining structural limit" marked implemented, the user guide's reclaim configuration.

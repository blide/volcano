/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Added job validation and preemption policy support
- Enhanced victim selection with priority queue ordering
- Added PrePredicate validation and node filtering

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reclaim

import (
	"math"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/features"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/util"
)

const (
	// VictimSelectionKey is the reclaim action argument choosing how the node to reclaim on is
	// picked for an asker: firstFit (default) or bestFit.
	VictimSelectionKey = "victimSelection"
	// VictimSelectionFirstFit commits on the first candidate node where the asker fits after
	// evicting that node's victims. Candidate order is not stable, so which node that is, and
	// how expensive its victims are, is incidental.
	VictimSelectionFirstFit = "firstFit"
	// VictimSelectionBestFit explores every candidate node and commits the cheapest victim set,
	// ranked with the keys of the per-node victim order: victim queue first, then lowest
	// highest-victim priority, then fewest victims. Priority is thereby a cost rather than an
	// exemption, and one local to the queue that uses it: an expensive pod is taken only when no
	// node can be served without it, and never ahead of another queue's pods on that account.
	VictimSelectionBestFit = "bestFit"
	// MaxCandidateNodesKey bounds how many nodes with a viable plan bestFit explores for one
	// asker before committing; 0 (default) means all candidate nodes.
	MaxCandidateNodesKey = "maxCandidateNodes"
	// CrossNodeVictimsKey enables the quota round: when the asker fits a node physically but its
	// queue hierarchy still refuses it, victims under the blocking ancestor are evicted from any
	// node, cheapest first, until the hierarchy admits it. The node round then evicts only what
	// the node needs. Default false.
	CrossNodeVictimsKey = "crossNodeVictims"
	// MaxCrossNodeVictimsKey bounds the victims one asker may take in the quota round; reaching it
	// fails the asker on that node. 0 (default) means unbounded.
	MaxCrossNodeVictimsKey = "maxCrossNodeVictims"
)

type Action struct {
	enablePredicateErrorCache bool
	victimSelection           string
	maxCandidateNodes         int
	crossNodeVictims          bool
	maxCrossNodeVictims       int
}

func New() *Action {
	return &Action{
		enablePredicateErrorCache: true,
		victimSelection:           VictimSelectionFirstFit,
	}
}

func (ra *Action) Name() string {
	return "reclaim"
}

func (ra *Action) Initialize() {}

func (ra *Action) parseArguments(ssn *framework.Session) {
	arguments := framework.GetArgOfActionFromConf(ssn.Configurations, ra.Name())
	arguments.GetBool(&ra.enablePredicateErrorCache, conf.EnablePredicateErrCacheKey)

	ra.victimSelection = VictimSelectionFirstFit
	var selection string
	arguments.GetString(&selection, VictimSelectionKey)
	switch selection {
	case "", VictimSelectionFirstFit:
	case VictimSelectionBestFit:
		ra.victimSelection = VictimSelectionBestFit
	default:
		klog.Warningf("[reclaim] unknown %s %q; using %s", VictimSelectionKey, selection, VictimSelectionFirstFit)
	}
	ra.maxCandidateNodes = 0
	arguments.GetInt(&ra.maxCandidateNodes, MaxCandidateNodesKey)
	if ra.maxCandidateNodes < 0 {
		ra.maxCandidateNodes = 0
	}
	ra.crossNodeVictims = false
	arguments.GetBool(&ra.crossNodeVictims, CrossNodeVictimsKey)
	ra.maxCrossNodeVictims = 0
	arguments.GetInt(&ra.maxCrossNodeVictims, MaxCrossNodeVictimsKey)
	if ra.maxCrossNodeVictims < 0 {
		ra.maxCrossNodeVictims = 0
	}
}

// queueGatedAsker reports whether a scheduling-gated task may still be a reclaim asker: the pod
// carries only Volcano's queue-allocation gate, opted in by annotation, and the
// SchedulingGatesQueueAdmission feature is on. Such a pod is a real, fully specified pod that the
// api-server keeps out of autoscalers' view until Volcano admits it; the allocate action already
// treats it as a candidate for the queue check and removes the gate once that check passes.
// Letting reclaim evaluate it too gives a Volcano Job a reclaim verdict (and the dequeue action a
// decision) while its pods are still gated. Pods gated by anything else stay out, as before.
func queueGatedAsker(task *api.TaskInfo) bool {
	return utilfeature.DefaultFeatureGate.Enabled(features.SchedulingGatesQueueAdmission) &&
		task.Pod != nil &&
		api.HasOnlyVolcanoSchedulingGate(task.Pod) &&
		api.HasQueueAllocationGateAnnotation(task.Pod)
}

func (ra *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Reclaim ...")
	defer klog.V(5).Infof("Leaving Reclaim ...")

	ra.parseArguments(ssn)

	queues := util.NewPriorityQueue(ssn.QueueOrderFn)
	queueMap := map[api.QueueID]*api.QueueInfo{}

	preemptorsMap := map[api.QueueID]*util.PriorityQueue{}
	preemptorTasks := map[api.JobID]*util.PriorityQueue{}

	klog.V(3).Infof("There are <%d> Jobs and <%d> Queues in total for scheduling.",
		len(ssn.Jobs), len(ssn.Queues))

	for _, job := range ssn.Jobs {
		if job.IsPending() {
			continue
		}

		if vr := ssn.JobValid(job); vr != nil && !vr.Pass {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip reclaim, reason: %v, message %v", job.Namespace, job.Name, job.Queue, vr.Reason, vr.Message)
			continue
		}

		if queue, found := ssn.Queues[job.Queue]; !found {
			klog.Errorf("Failed to find Queue <%s> for Job <%s/%s>", job.Queue, job.Namespace, job.Name)
			continue
		} else if _, existed := queueMap[queue.UID]; !existed {
			klog.V(4).Infof("Added Queue <%s> for Job <%s/%s>", queue.Name, job.Namespace, job.Name)
			queueMap[queue.UID] = queue
			queues.Push(queue)
		}

		if ssn.JobStarving(job) {
			if _, found := preemptorsMap[job.Queue]; !found {
				preemptorsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
			}
			preemptorsMap[job.Queue].Push(job)
			preemptorTasks[job.UID] = util.NewPriorityQueue(ssn.TaskOrderFn)
			for _, task := range job.TaskStatusIndex[api.Pending] {
				if task.SchGated && !queueGatedAsker(task) {
					continue
				}
				preemptorTasks[job.UID].Push(task)
			}
		}
	}

	for {
		if queues.Empty() {
			break
		}

		queue := queues.Pop().(*api.QueueInfo)
		if ssn.Overused(queue) {
			klog.V(3).Infof("Queue <%s> is overused, ignore it.", queue.Name)
			continue
		}

		for {
			// Pick the starving jobs in this queue.
			jobsQ, found := preemptorsMap[queue.UID]
			if !found || jobsQ.Empty() {
				klog.V(4).Infof("No preemptors in Queue <%s>, break.", queue.Name)
				break
			}
			job := jobsQ.Pop().(*api.JobInfo)
			stmt := framework.NewStatement(ssn)
			attempted, victimsSeen, pipelinedAny := false, false, false

			for {
				// If job is not request more resource, then stop reclaiming.
				if !ssn.JobStarving(job) {
					break
				}

				// Pick up all its candidate tasks.
				tasksQ, ok := preemptorTasks[job.UID]
				if !ok || tasksQ.Empty() {
					klog.V(3).Infof("No preemptor task in job <%s/%s>.",
						job.Namespace, job.Name)
					break
				}

				klog.V(3).Infof("Considering reclaim for %d tasks of job <%s/%s>.", tasksQ.Len(), job.Namespace, job.Name)

				task := tasksQ.Pop().(*api.TaskInfo)

				if task.Pod.Spec.PreemptionPolicy != nil && *task.Pod.Spec.PreemptionPolicy == v1.PreemptNever {
					klog.V(3).Infof("Task %s/%s cannot preempt (policy Never)", task.Namespace, task.Name)
					continue
				}

				if !ssn.Preemptive(queue, []*api.TaskInfo{task}) {
					klog.V(3).Infof("Queue <%s> cannot reclaim for task <%s>, skip", queue.Name, task.Name)
					continue
				}

				if err := ssn.PrePredicateFn(task); err != nil {
					klog.V(3).Infof("PrePredicate failed for task %s/%s: %v", task.Namespace, task.Name, err)
					continue
				}

				attempted = true
				seen, pipelined := ra.reclaimForTask(ssn, stmt, queue, task, job)
				victimsSeen = victimsSeen || seen
				pipelinedAny = pipelinedAny || pipelined
			}

			committed := ssn.JobPipelined(job)
			if committed {
				hasEvictions := stmt.HasEvictions()
				stmt.Commit()
				if hasEvictions {
					metrics.RegisterEvictionTransaction(ra.Name())
				}
			} else {
				stmt.Discard()
			}

			// Record the verdict for actions that run after reclaim (dequeue). The job counts as
			// served only if this action pipelined a task and the statement was committed; a
			// statement discarded by the job-pipelined check (gang's minAvailable) means victims
			// existed but the job was not served. Gang-level reasoning itself stays with the
			// gangreclaim action. JobPipelined defaults to permit when no plugin implements it, so
			// "committed" alone is not evidence of success.
			switch {
			case committed && pipelinedAny:
				job.ReclaimResult = api.ReclaimSucceeded
			case !attempted:
				job.ReclaimResult = api.ReclaimNotAttempted
			case victimsSeen || pipelinedAny:
				job.ReclaimResult = api.ReclaimFailed
			default:
				job.ReclaimResult = api.ReclaimNoVictims
			}

			if !jobsQ.Empty() {
				queues.Push(queue)
			}
		}
	}
}

// reclaimForTask tries to place task by evicting on one node. It returns whether eligible victims
// were found on any candidate node, which lets the caller distinguish "nothing to reclaim" from
// "victims existed but could not make the task fit", and whether the task was pipelined.
//
// With firstFit the first node with a viable plan is committed. With bestFit every candidate node
// is planned and rolled back, the cheapest plan is replayed and committed.
func (ra *Action) reclaimForTask(ssn *framework.Session, stmt *framework.Statement, queue *api.QueueInfo, task *api.TaskInfo, job *api.JobInfo) (victimsSeen, pipelined bool) {
	totalNodes := ssn.FilterOutUnschedulableAndUnresolvableNodesForTask(task)
	predicateHelper := util.NewPredicateHelper()
	predicateNodes, _ := predicateHelper.PredicateNodes(task, totalNodes, ssn.PredicateForPreemptAction, ra.enablePredicateErrorCache, ssn.NodesInShard)
	predicateNodesByShard := util.GetPredicatedNodeByShard(predicateNodes, ssn.NodesInShard)
	var predicateNodesByShardFlattened []*api.NodeInfo
	for _, nodes := range predicateNodesByShard {
		predicateNodesByShardFlattened = append(predicateNodesByShardFlattened, nodes...)
	}

	var candidates []*api.TaskInfo
	if ra.crossNodeVictims {
		candidates = reclaimeesEverywhere(ssn, job, task)
	}

	var best *nodePlan
	planned := 0
	for _, n := range predicateNodesByShardFlattened {
		klog.V(3).Infof("Considering Task <%s/%s> on Node <%s>.", task.Namespace, task.Name, n.Name)

		plan, seen := ra.planOnNode(ssn, queue, task, job, n, candidates)
		victimsSeen = victimsSeen || seen
		if plan == nil {
			continue
		}
		if ra.victimSelection != VictimSelectionBestFit {
			stmt.Merge(plan.stmt)
			return victimsSeen, true
		}

		// bestFit: roll the exploration back and keep the plan; the winner is replayed below.
		plan.stmt.Discard()
		plan.stmt = nil
		if plan.cheaperThan(best, ssn, queue) {
			best = plan
		}
		planned++
		if len(best.victims) == 0 || (ra.maxCandidateNodes > 0 && planned >= ra.maxCandidateNodes) {
			break
		}
	}
	if best == nil {
		return victimsSeen, false
	}
	return victimsSeen, replayPlan(ssn, stmt, task, best)
}

// nodePlan is one viable way to serve the asker: the node, the victims evicted on it in order, and,
// while the exploration is still open, the per-node statement holding those tentative evictions
// and the pipeline.
type nodePlan struct {
	node             *api.NodeInfo
	victims          []*api.TaskInfo
	evictionOccurred bool
	stmt             *framework.Statement
}

// maxPriority is the highest pod priority among the plan's victims; a plan without victims ranks
// below any priority.
func (p *nodePlan) maxPriority() int32 {
	m := int32(math.MinInt32)
	for _, v := range p.victims {
		if v.Priority > m {
			m = v.Priority
		}
	}
	return m
}

// lastVictimQueue is the plan's most protected victim queue: among the queues its victims belong
// to, the one the victim queue order ranks last for this asker. A plan is represented by the queue
// it hurts most, as it is by its highest victim priority. Nil when the plan has no victims or none
// of them has a job in the session.
func (p *nodePlan) lastVictimQueue(ssn *framework.Session, asker *api.QueueInfo) *api.QueueInfo {
	var last *api.QueueInfo
	for _, v := range p.victims {
		job, found := ssn.Jobs[v.Job]
		if !found {
			continue
		}
		q := ssn.Queues[job.Queue]
		if q == nil {
			continue
		}
		if last == nil || (last.UID != q.UID && ssn.VictimQueueOrderFn(last, q, asker)) {
			last = q
		}
	}
	return last
}

// cheaperThan orders plans with the keys the per-node victim order already uses, so that a
// PriorityClass stays local to the queue that uses it: a plan without victims first; then the
// plan whose most protected victim queue the victim queue order evicts earlier (for the capacity
// plugin the queue nearest the asker in the hierarchy, then the one with the higher share); only
// between plans hurting the same queue the lowest highest-victim priority; then fewest victims;
// then node name so the choice does not depend on the candidate order, which is not stable.
func (p *nodePlan) cheaperThan(o *nodePlan, ssn *framework.Session, asker *api.QueueInfo) bool {
	if o == nil {
		return true
	}
	if (len(p.victims) == 0) != (len(o.victims) == 0) {
		return len(p.victims) == 0
	}
	if pq, oq := p.lastVictimQueue(ssn, asker), o.lastVictimQueue(ssn, asker); pq != nil && oq != nil && pq.UID != oq.UID {
		return ssn.VictimQueueOrderFn(pq, oq, asker)
	}
	if a, b := p.maxPriority(), o.maxPriority(); a != b {
		return a < b
	}
	if len(p.victims) != len(o.victims) {
		return len(p.victims) < len(o.victims)
	}
	return p.node.Name < o.node.Name
}

// planOnNode explores serving the task on node: it evicts that node's admissible victims, cheapest
// first, into a per-node statement until the task fits the node and its queue hierarchy, then
// pipelines the task there. With crossNodeVictims the node round stops at the physical fit and the
// quota round (quotaRound) relieves the hierarchy with victims from any node. It returns the open
// plan, or nil after rolling the statement back when the node cannot serve the task; and whether
// the plugins admitted any victim for the task.
func (ra *Action) planOnNode(ssn *framework.Session, queue *api.QueueInfo, task *api.TaskInfo, job *api.JobInfo, n *api.NodeInfo, candidates []*api.TaskInfo) (plan *nodePlan, victimsSeen bool) {
	reclaimees := reclaimeesOnNode(ssn, job, task, n)
	if len(reclaimees) == 0 && !ra.crossNodeVictims {
		klog.V(4).Infof("No reclaimees on Node <%s>.", n.Name)
		return nil, false
	}

	victims := ssn.Reclaimable(task, reclaimees)
	victimsSeen = len(victims) > 0
	if err := util.ValidateVictims(task, n, victims); err != nil {
		klog.V(3).Infof("No validated victims on Node <%s>: %v", n.Name, err)
		return nil, victimsSeen
	}

	victimsQueue := ssn.BuildVictimsPriorityQueue(victims, task)
	resreq := task.InitResreq.Clone()
	reclaimed := api.EmptyResource()

	// With the quota round the node round only has to make the task fit the node; the hierarchy
	// is relieved afterwards with the cheapest victims wherever they run.
	queueTerm := !ra.crossNodeVictims
	availableResources := n.FutureIdle()
	reclaimerFits := reclaimerFitsOnNode(ssn, queue, task, n, resreq, availableResources, queueTerm)

	// Use a per-node statement so that evictions are isolated to this node. Only a plan whose
	// Pipeline succeeds is ever merged into the caller's statement; everything else is discarded
	// so victims on nodes that end up unused are never committed to Kubernetes.
	plan = &nodePlan{node: n, stmt: framework.NewStatement(ssn)}
	for !victimsQueue.Empty() && !reclaimerFits {
		reclaimee := victimsQueue.Pop().(*api.TaskInfo)
		klog.V(3).Infof("Try to reclaim Task <%s/%s> for Tasks <%s/%s>",
			reclaimee.Namespace, reclaimee.Name, task.Namespace, task.Name)
		plan.stmt.Evict(reclaimee, "reclaim")
		plan.victims = append(plan.victims, reclaimee)
		plan.evictionOccurred = true
		reclaimed.Add(reclaimee.Resreq)
		availableResources.Add(reclaimee.Resreq)
		reclaimerFits = reclaimerFitsOnNode(ssn, queue, task, n, resreq, availableResources, queueTerm)
	}

	klog.V(3).Infof("Reclaimed <%v> for task <%s/%s> requested <%v>, and Node <%s> availableResources <%v>.", reclaimed, task.Namespace, task.Name, task.InitResreq, n.Name, availableResources)

	if !reclaimerFits {
		plan.stmt.Discard()
		return nil, victimsSeen
	}
	if ra.crossNodeVictims && !ssn.Allocatable(queue, task) {
		seen, ok := ra.quotaRound(ssn, plan, queue, task, candidates)
		victimsSeen = victimsSeen || seen
		if !ok {
			plan.stmt.Discard()
			return nil, victimsSeen
		}
	}
	if err := plan.stmt.Pipeline(task, n.Name, plan.evictionOccurred); err != nil {
		klog.Errorf("Failed to pipeline Task <%s/%s> on Node <%s>: %v", task.Namespace, task.Name, n.Name, err)
		plan.stmt.Discard()
		return nil, victimsSeen
	}
	return plan, victimsSeen
}

// replayPlan re-applies a plan whose exploration was rolled back and merges it into stmt. Nothing
// runs between the exploration and the replay within a session, so the victims are still Running
// and the node is as it was; a failure here is a state bug, not a scheduling outcome.
func replayPlan(ssn *framework.Session, stmt *framework.Statement, task *api.TaskInfo, plan *nodePlan) bool {
	nodeStmt := framework.NewStatement(ssn)
	for _, victim := range plan.victims {
		nodeStmt.Evict(victim, "reclaim")
	}
	if err := nodeStmt.Pipeline(task, plan.node.Name, plan.evictionOccurred); err != nil {
		klog.Errorf("Failed to replay the best-fit reclaim plan for Task <%s/%s> on Node <%s>: %v",
			task.Namespace, task.Name, plan.node.Name, err)
		nodeStmt.Discard()
		return false
	}
	klog.V(3).Infof("Best-fit reclaim for Task <%s/%s>: Node <%s>, %d victims, highest victim priority %d",
		task.Namespace, task.Name, plan.node.Name, len(plan.victims), plan.maxPriority())
	stmt.Merge(nodeStmt)
	return true
}

// reclaimeesOnNode lists the Running, preemptable tasks on node that belong to another, reclaimable
// queue than job's, ordered cheapest victim first (the victims-queue order: queue order, then job
// order, then lowest task priority). Plugins that admit victims in arrival order, such as the gang
// plugin's minAvailable veto, then spend their admissions on the cheap tasks and veto the expensive
// ones, instead of whatever the node's task map yields first.
func reclaimeesOnNode(ssn *framework.Session, job *api.JobInfo, task *api.TaskInfo, node *api.NodeInfo) []*api.TaskInfo {
	var reclaimees []*api.TaskInfo
	for _, taskOnNode := range node.Tasks {
		if taskOnNode.Status != api.Running || !taskOnNode.Preemptable {
			continue
		}
		j, found := ssn.Jobs[taskOnNode.Job]
		if !found || j.Queue == job.Queue {
			continue
		}
		if q := ssn.Queues[j.Queue]; q == nil || !q.Reclaimable() {
			continue
		}
		reclaimees = append(reclaimees, taskOnNode.Clone())
	}
	if len(reclaimees) < 2 {
		return reclaimees
	}
	ordered := make([]*api.TaskInfo, 0, len(reclaimees))
	queue := ssn.BuildVictimsPriorityQueue(reclaimees, task)
	for !queue.Empty() {
		ordered = append(ordered, queue.Pop().(*api.TaskInfo))
	}
	return ordered
}

// reclaimerFitsOnNode verifies that, after the tentative evictions recorded so far, the reclaimer
// both fits the node physically and is allocatable in its queue hierarchy. The queue term is what
// lets reclaim serve an ask starved by an ancestor's capability rather than by node capacity
// (volcano-sh/volcano#4817): every tentative Evict runs the plugins' DeallocateFunc handlers, so
// ssn.Allocatable already sees the victims chosen so far. Mirrors preemptorFitsOnNode in preempt.
func reclaimerFitsOnNode(ssn *framework.Session, queue *api.QueueInfo, task *api.TaskInfo, node *api.NodeInfo, resreq, availableResources *api.Resource, queueTerm bool) bool {
	return (!queueTerm || ssn.Allocatable(queue, task)) &&
		resreq.LessEqual(availableResources, api.Zero) &&
		ssn.PredicateFn(task, node) == nil
}

// quotaRound relieves the task's queue hierarchy after the node round: while ssn.Allocatable still
// refuses the task, it evicts into the plan the cheapest admissible candidate whose queue lies under
// the deepest blocking ancestor, from whatever node it runs on, re-reading the blocker after each
// eviction. Every tentative eviction runs the plugins' deallocate handlers, so the quota plugin's
// counters, and with them ssn.Allocatable, see each victim as it is chosen. It returns whether the
// plugins admitted any candidate and whether the hierarchy admits the task at the end.
func (ra *Action) quotaRound(ssn *framework.Session, plan *nodePlan, queue *api.QueueInfo, task *api.TaskInfo, candidates []*api.TaskInfo) (victimsSeen, ok bool) {
	taken := map[api.TaskID]struct{}{}
	for _, v := range plan.victims {
		taken[v.UID] = struct{}{}
	}
	remaining := make([]*api.TaskInfo, 0, len(candidates))
	for _, c := range candidates {
		if _, found := taken[c.UID]; !found {
			remaining = append(remaining, c)
		}
	}
	// Admission is evaluated now, after the node round, so the plugins' cumulative counters
	// include the node's victims.
	admitted := ssn.Reclaimable(task, remaining)
	victimsSeen = len(admitted) > 0
	if len(admitted) == 0 {
		klog.V(3).Infof("Quota round for Task <%s/%s>: no admissible victims anywhere", task.Namespace, task.Name)
		return false, false
	}
	ordered := make([]*api.TaskInfo, 0, len(admitted))
	for q := ssn.BuildVictimsPriorityQueue(admitted, task); !q.Empty(); {
		ordered = append(ordered, q.Pop().(*api.TaskInfo))
	}

	evicted := 0
	for !ssn.Allocatable(queue, task) {
		if ra.maxCrossNodeVictims > 0 && evicted >= ra.maxCrossNodeVictims {
			klog.V(3).Infof("Quota round for Task <%s/%s>: %d victims reached %s", task.Namespace, task.Name, evicted, MaxCrossNodeVictimsKey)
			return victimsSeen, false
		}
		blocker := deepestBlocker(ssn, queue, task)
		if blocker == nil {
			klog.V(3).Infof("Quota round for Task <%s/%s>: the hierarchy refuses the task but names no blocking queue", task.Namespace, task.Name)
			return victimsSeen, false
		}
		var victim *api.TaskInfo
		for i, c := range ordered {
			if c == nil {
				continue
			}
			if j := ssn.Jobs[c.Job]; j != nil && isUnder(ssn, j.Queue, blocker.UID) {
				victim, ordered[i] = c, nil
				break
			}
		}
		if victim == nil {
			klog.V(3).Infof("Quota round for Task <%s/%s>: no admissible victim under blocking queue <%s>", task.Namespace, task.Name, blocker.Name)
			return victimsSeen, false
		}
		klog.V(3).Infof("Quota round: reclaim Task <%s/%s> on Node <%s> for Task <%s/%s>, relieving queue <%s>",
			victim.Namespace, victim.Name, victim.NodeName, task.Namespace, task.Name, blocker.Name)
		plan.stmt.Evict(victim, "reclaim")
		plan.victims = append(plan.victims, victim)
		plan.evictionOccurred = true
		evicted++
	}
	return victimsSeen, true
}

// deepestBlocker walks the task's queue ancestry from the leaf to the root and returns the deepest
// queue at which ssn.Allocatable refuses the task: the quota plugin, called with an ancestor of the
// task's queue, checks that queue and above. Nil when no queue on the path refuses.
func deepestBlocker(ssn *framework.Session, queue *api.QueueInfo, task *api.TaskInfo) *api.QueueInfo {
	var blocker *api.QueueInfo
	for q := queue; q != nil; {
		if !ssn.Allocatable(q, task) {
			blocker = q
		}
		parent := q.Queue.Spec.Parent
		if parent == "" || parent == q.Name {
			break
		}
		q = ssn.Queues[api.QueueID(parent)]
	}
	return blocker
}

// isUnder reports whether queueID is ancestorID or lies in its subtree, by the queues' parent
// fields.
func isUnder(ssn *framework.Session, queueID, ancestorID api.QueueID) bool {
	for q := ssn.Queues[queueID]; q != nil; {
		if q.UID == ancestorID {
			return true
		}
		parent := q.Queue.Spec.Parent
		if parent == "" || parent == q.Name {
			return false
		}
		q = ssn.Queues[api.QueueID(parent)]
	}
	return false
}

// reclaimeesEverywhere lists the Running, preemptable tasks on every node that belong to another,
// reclaimable queue than job's: the candidates of the quota round, ordered cheapest first.
func reclaimeesEverywhere(ssn *framework.Session, job *api.JobInfo, task *api.TaskInfo) []*api.TaskInfo {
	var all []*api.TaskInfo
	for _, node := range ssn.Nodes {
		all = append(all, reclaimeesOnNode(ssn, job, task, node)...)
	}
	if len(all) < 2 {
		return all
	}
	ordered := make([]*api.TaskInfo, 0, len(all))
	for q := ssn.BuildVictimsPriorityQueue(all, task); !q.Empty(); {
		ordered = append(ordered, q.Pop().(*api.TaskInfo))
	}
	return ordered
}

func (ra *Action) UnInitialize() {
}

/*
Copyright 2026 The Volcano Authors.

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

package quota

import (
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	schedutil "volcano.sh/volcano/pkg/scheduler/util"
)

// The two passes the plugin runs around the actions, and the dequeue decision.
//
// reservedPass, at session open: an ask whose previous reclaim is still in flight (its nominated
// node still carries the scheduler-evicted pod it paid for) is placed on that node before any
// action runs, so the room its victims free counts as taken and no earlier-sorting ask takes it.
//
// closeReclaim, at session close: an ask that the reclaim action left unserved because its queue
// hierarchy, not a node, refuses it is served with victims under the blocking ancestor from any
// node. The reclaim action frees quota only with the victims of the node it tries; this pass is the
// quota-aware round that runs after capacity's share-aware one.
//
// closeDequeue, at session close: a PodGroup admitted for a reclaim trial that nothing served goes
// back to Pending, with a backoff before it may be admitted again.

// ---------------------------------------------------------------- reserved asks

type reservedAsk struct {
	queue *api.QueueInfo
	job   *api.JobInfo
	task  *api.TaskInfo
}

// reservedPass places every pending task whose nomination is live on its nominated node, in queue,
// job and task order: bound when the node already has idle room, pipelined onto the releasing room
// otherwise, under the allocate action's commit rule. The nominated node is written by the cache
// only when a pipeline followed an eviction, so only asks that paid for room qualify. The pass uses
// the predicates and order functions of the plugins registered before this one; place the plugin
// in the last tier.
func (qp *quotaPlugin) reservedPass(ssn *framework.Session) {
	asks := schedutil.NewPriorityQueue(func(l, r interface{}) bool {
		lv, rv := l.(*reservedAsk), r.(*reservedAsk)
		if lv.queue.UID != rv.queue.UID {
			return ssn.QueueOrderFn(lv.queue, rv.queue)
		}
		if lv.job.UID != rv.job.UID {
			return ssn.JobOrderFn(lv.job, rv.job)
		}
		return ssn.TaskOrderFn(lv.task, rv.task)
	})
	for _, job := range ssn.Jobs {
		if job.PodGroup == nil || job.PodGroup.Status.Phase == scheduling.PodGroupPending {
			continue
		}
		queue := ssn.Queues[job.Queue]
		if queue == nil {
			continue
		}
		for _, task := range job.TaskStatusIndex[api.Pending] {
			if task.Pod == nil || task.Pod.Status.NominatedNodeName == "" || task.Resreq.IsEmpty() {
				continue
			}
			asks.Push(&reservedAsk{queue: queue, job: job, task: task})
		}
	}
	for !asks.Empty() {
		ask := asks.Pop().(*reservedAsk)
		node := qp.reservedNode(ssn, ask.queue, ask.task)
		if node == nil {
			continue
		}
		stmt := framework.NewStatement(ssn)
		var err error
		if ask.task.InitResreq.LessEqual(node.Idle, api.Zero) {
			err = stmt.Allocate(ask.task, node)
		} else {
			err = stmt.Pipeline(ask.task, node.Name, false)
		}
		if err != nil {
			klog.Errorf("[quota] failed to place reserved ask <%s/%s> on Node <%s>: %v", ask.task.Namespace, ask.task.Name, node.Name, err)
			stmt.Discard()
			continue
		}
		switch {
		case ssn.JobReady(ask.job):
			stmt.Commit()
			ssn.MarkJobDirty(ask.job.UID)
		case ssn.JobPipelined(ask.job):
			// Stays applied in the session, as the allocate action keeps a pipelined job.
		default:
			stmt.Discard()
			continue
		}
		klog.V(3).Infof("[quota] reserved ask <%s/%s> placed on its nominated Node <%s>", ask.task.Namespace, ask.task.Name, node.Name)
	}
}

// reservedNode returns the node a task's nomination still holds for it: the node exists, still
// carries a pod the scheduler evicted that is terminating, and the task fits its future idle, is
// allocatable in its hierarchy and passes the predicates. Otherwise nil.
func (qp *quotaPlugin) reservedNode(ssn *framework.Session, queue *api.QueueInfo, task *api.TaskInfo) *api.NodeInfo {
	if task.SchGated {
		return nil
	}
	node, found := ssn.Nodes[task.Pod.Status.NominatedNodeName]
	if !found || !hasSchedulerEvictedPod(node) {
		return nil
	}
	if !ssn.Allocatable(queue, task) || !task.InitResreq.LessEqual(node.FutureIdle(), api.Zero) {
		return nil
	}
	if err := ssn.PrePredicateFn(task); err != nil {
		return nil
	}
	if err := ssn.PredicateForAllocateAction(task, node); err != nil {
		klog.V(4).Infof("[quota] reserved ask <%s/%s> no longer fits its nominated Node <%s>: %v", task.Namespace, task.Name, node.Name, err)
		return nil
	}
	return node
}

// hasSchedulerEvictedPod reports whether a pod the scheduler evicted (reclaim or preempt) is still
// terminating on the node. Volcano's evictor marks its victims with the DisruptionTarget condition.
func hasSchedulerEvictedPod(node *api.NodeInfo) bool {
	for _, t := range node.Tasks {
		if t.Status != api.Releasing || t.Pod == nil || t.Pod.DeletionTimestamp == nil {
			continue
		}
		for _, c := range t.Pod.Status.Conditions {
			if c.Type == v1.DisruptionTarget && c.Status == v1.ConditionTrue && c.Reason == v1.PodReasonPreemptionByScheduler {
				return true
			}
		}
	}
	return false
}

// ---------------------------------------------------------------- quota-aware reclaim

// closeReclaim serves, after the actions ran, the asks of admitted PodGroups that the reclaim
// action could not: a pending task of a trial-admitted job, and a task pipelined by reclaim with
// nothing evicted because the node had room while the hierarchy still refuses it (the shape of
// volcano-sh/volcano#4817). Each job gets one statement, committed under the reclaim action's rule
// (the job must be pipelined), so nothing is evicted for a job that is not served.
func (qp *quotaPlugin) closeReclaim(ssn *framework.Session) {
	for _, job := range ssn.Jobs {
		if job.PodGroup == nil || job.PodGroup.Status.Phase != scheduling.PodGroupInqueue || job.PodGroup.Spec.MinResources == nil {
			continue
		}
		queue := ssn.Queues[job.Queue]
		if queue == nil {
			continue
		}
		trial := findCondition(job, PodGroupInqueueType)
		isTrial := trial != nil && trial.Status == v1.ConditionTrue && trial.Reason == InqueueReasonAncestorCapReclaim

		stmt := framework.NewStatement(ssn)
		served := false
		for _, task := range job.TaskStatusIndex[api.Pipelined] {
			if ssn.Allocatable(queue, task) {
				continue
			}
			// Pipelined on a node with room, refused by the hierarchy: the quota round alone.
			victims, ok := qp.quotaRound(ssn, stmt, queue, task, qp.reclaimeesEverywhere(ssn, job, task), nil)
			klog.V(3).Infof("[quota] pipelined Task <%s/%s> refused by its hierarchy: %d victims, served=%t", task.Namespace, task.Name, victims, ok)
			served = served || ok
		}
		if isTrial {
			for _, task := range job.TaskStatusIndex[api.Pending] {
				if task.SchGated || task.Resreq.IsEmpty() {
					continue
				}
				if !ssn.Preemptive(queue, []*api.TaskInfo{task}) {
					klog.V(3).Infof("[quota] Task <%s/%s> may not reclaim in queue <%s>", task.Namespace, task.Name, queue.Name)
					continue
				}
				if err := ssn.PrePredicateFn(task); err != nil {
					continue
				}
				if qp.serveTask(ssn, stmt, queue, job, task) {
					served = true
				}
			}
		}
		if served && ssn.JobPipelined(job) {
			hasEvictions := stmt.HasEvictions()
			stmt.Commit()
			if hasEvictions {
				metrics.RegisterEvictionTransaction(PluginName)
			}
			ssn.MarkJobDirty(job.UID)
		} else {
			stmt.Discard()
		}
	}
}

// serveTask tries every candidate node in turn: the node round evicts that node's admissible
// victims until the task fits it physically, the quota round relieves the hierarchy, then the
// task is pipelined there. Evictions go into stmt; a node that cannot serve the task has its
// evictions rolled back before the next is tried.
func (qp *quotaPlugin) serveTask(ssn *framework.Session, stmt *framework.Statement, queue *api.QueueInfo, job *api.JobInfo, task *api.TaskInfo) bool {
	nodes := ssn.FilterOutUnschedulableAndUnresolvableNodesForTask(task)
	candidates, _ := schedutil.NewPredicateHelper().PredicateNodes(task, nodes, ssn.PredicateForPreemptAction, true, ssn.NodesInShard)
	everywhere := qp.reclaimeesEverywhere(ssn, job, task)
	for _, node := range candidates {
		nodeStmt := framework.NewStatement(ssn)
		evicted := false

		victims := ssn.Reclaimable(task, qp.reclaimeesOnNode(ssn, job, task, node))
		queue_ := ssn.BuildVictimsPriorityQueue(victims, task)
		available := node.FutureIdle()
		taken := map[api.TaskID]struct{}{}
		fits := task.InitResreq.LessEqual(available, api.Zero) && ssn.PredicateFn(task, node) == nil
		for !queue_.Empty() && !fits {
			victim := queue_.Pop().(*api.TaskInfo)
			nodeStmt.Evict(victim, "reclaim")
			taken[victim.UID] = struct{}{}
			evicted = true
			available.Add(victim.Resreq)
			fits = task.InitResreq.LessEqual(available, api.Zero) && ssn.PredicateFn(task, node) == nil
		}
		if !fits {
			nodeStmt.Discard()
			continue
		}
		if !ssn.Allocatable(queue, task) {
			n, ok := qp.quotaRound(ssn, nodeStmt, queue, task, everywhere, taken)
			if !ok {
				nodeStmt.Discard()
				continue
			}
			evicted = evicted || n > 0
		}
		if err := nodeStmt.Pipeline(task, node.Name, evicted); err != nil {
			klog.Errorf("[quota] failed to pipeline Task <%s/%s> on Node <%s>: %v", task.Namespace, task.Name, node.Name, err)
			nodeStmt.Discard()
			continue
		}
		klog.V(3).Infof("[quota] Task <%s/%s> served on Node <%s> (evictions=%t)", task.Namespace, task.Name, node.Name, evicted)
		stmt.Merge(nodeStmt)
		return true
	}
	return false
}

// quotaRound relieves the task's queue hierarchy: while ssn.Allocatable refuses the task, it
// evicts into stmt the cheapest admissible candidate whose queue lies under the deepest blocking
// ancestor, from whatever node it runs on, re-reading the blocker after each eviction. Every
// tentative eviction runs the deallocate handlers, so this plugin's counters, and with them
// Allocatable, see each victim as it is chosen. It returns the number of victims taken and
// whether the hierarchy admits the task at the end; on failure the caller discards stmt. Victims
// in taken were already evicted into stmt by the node round and are skipped.
func (qp *quotaPlugin) quotaRound(ssn *framework.Session, stmt *framework.Statement, queue *api.QueueInfo, task *api.TaskInfo, candidates []*api.TaskInfo, taken map[api.TaskID]struct{}) (int, bool) {
	remaining := make([]*api.TaskInfo, 0, len(candidates))
	for _, c := range candidates {
		if _, found := taken[c.UID]; !found {
			remaining = append(remaining, c)
		}
	}
	// Admission is evaluated now, after the node round, so the plugins' cumulative counters
	// include the node's victims.
	admitted := ssn.Reclaimable(task, remaining)
	if len(admitted) == 0 {
		klog.V(3).Infof("[quota] quota round for Task <%s/%s>: no admissible victims anywhere", task.Namespace, task.Name)
		return 0, false
	}
	ordered := make([]*api.TaskInfo, 0, len(admitted))
	for q := ssn.BuildVictimsPriorityQueue(admitted, task); !q.Empty(); {
		ordered = append(ordered, q.Pop().(*api.TaskInfo))
	}
	evicted := 0
	for !ssn.Allocatable(queue, task) {
		if qp.maxCrossNodeVictims > 0 && evicted >= qp.maxCrossNodeVictims {
			klog.V(3).Infof("[quota] quota round for Task <%s/%s>: %d victims reached %s", task.Namespace, task.Name, evicted, MaxCrossNodeVictimsKey)
			return evicted, false
		}
		blocker := qp.deepestBlocker(ssn, queue, task)
		if blocker == nil {
			return evicted, false
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
			klog.V(3).Infof("[quota] quota round for Task <%s/%s>: no admissible victim under blocking queue <%s>", task.Namespace, task.Name, blocker.Name)
			return evicted, false
		}
		klog.V(3).Infof("[quota] quota round: reclaim Task <%s/%s> on Node <%s> for Task <%s/%s>, relieving queue <%s>",
			victim.Namespace, victim.Name, victim.NodeName, task.Namespace, task.Name, blocker.Name)
		stmt.Evict(victim, "reclaim")
		evicted++
	}
	return evicted, true
}

// deepestBlocker returns the deepest queue on the task's path, leaf first, whose own capability
// check refuses the task, with the reserve charge the path implies. Nil when none does. A victim
// under the deepest blocker relieves every blocker above it as well.
func (qp *quotaPlugin) deepestBlocker(ssn *framework.Session, queue *api.QueueInfo, task *api.TaskInfo) *api.QueueInfo {
	leaf := qp.queueOpts[queue.UID]
	if leaf == nil {
		return nil
	}
	path := append(append([]api.QueueID{}, leaf.ancestors...), leaf.queueID)
	for i := len(path) - 1; i >= 0; i-- {
		attr := qp.queueOpts[path[i]]
		var owedToOthers *api.Resource
		if qp.reserveDeserved && i < len(path)-1 {
			owedToOthers = api.ExceededPart(attr.reserve, qp.queueOpts[path[i+1]].reserve)
		}
		if !qp.queueAllocatable(attr, task, owedToOthers) {
			return ssn.Queues[path[i]]
		}
	}
	return nil
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

// reclaimeesOnNode lists the Running, preemptable tasks on node that belong to another, reclaimable
// queue than job's, ordered cheapest first (victim queue order, job order, lowest task priority),
// so plugins that admit victims in arrival order, such as the gang plugin's minAvailable veto,
// spend their admissions on the cheap tasks.
func (qp *quotaPlugin) reclaimeesOnNode(ssn *framework.Session, job *api.JobInfo, task *api.TaskInfo, node *api.NodeInfo) []*api.TaskInfo {
	var reclaimees []*api.TaskInfo
	for _, t := range node.Tasks {
		if t.Status != api.Running || !t.Preemptable {
			continue
		}
		j, found := ssn.Jobs[t.Job]
		if !found || j.Queue == job.Queue {
			continue
		}
		if q := ssn.Queues[j.Queue]; q == nil || !q.Reclaimable() {
			continue
		}
		reclaimees = append(reclaimees, t.Clone())
	}
	return orderVictims(ssn, reclaimees, task)
}

// reclaimeesEverywhere is reclaimeesOnNode over every node: the candidates of the quota round.
func (qp *quotaPlugin) reclaimeesEverywhere(ssn *framework.Session, job *api.JobInfo, task *api.TaskInfo) []*api.TaskInfo {
	var all []*api.TaskInfo
	for _, node := range ssn.Nodes {
		all = append(all, qp.reclaimeesOnNode(ssn, job, task, node)...)
	}
	return orderVictims(ssn, all, task)
}

func orderVictims(ssn *framework.Session, victims []*api.TaskInfo, task *api.TaskInfo) []*api.TaskInfo {
	if len(victims) < 2 {
		return victims
	}
	ordered := make([]*api.TaskInfo, 0, len(victims))
	for q := ssn.BuildVictimsPriorityQueue(victims, task); !q.Empty(); {
		ordered = append(ordered, q.Pop().(*api.TaskInfo))
	}
	return ordered
}

// ---------------------------------------------------------------- dequeue

// closeDequeue returns to Pending every admitted PodGroup with minResources that nothing served:
// a trial admission whose eligible tasks were evaluated and refused by the reclaim action and by
// this plugin's quota-aware reclaim, right away; any other admitted PodGroup with nothing placed,
// after inqueueTimeout. Dequeuing releases the inqueue reservation; the enqueue backoff keeps the
// PodGroup out for a while.
func (qp *quotaPlugin) closeDequeue(ssn *framework.Session) {
	now := time.Now()
	for _, job := range ssn.Jobs {
		if job.PodGroup == nil || job.PodGroup.Status.Phase != scheduling.PodGroupInqueue || job.PodGroup.Spec.MinResources == nil {
			continue
		}
		if qp.hasProgress(ssn, job) {
			continue
		}
		inqueueCond := findCondition(job, PodGroupInqueueType)
		isTrial := inqueueCond != nil && inqueueCond.Status == v1.ConditionTrue && inqueueCond.Reason == InqueueReasonAncestorCapReclaim
		if isTrial && hasEligibleTask(job) {
			qp.dequeue(ssn, job, DequeuedReasonReclaimFailed,
				"admitted past an ancestor capability for a reclaim trial, but no victims could serve the job; moved back to Pending and released the queue reservation")
			continue
		}
		if qp.inqueueTimeout <= 0 {
			continue
		}
		if inqueueCond == nil || inqueueCond.Status != v1.ConditionTrue {
			qp.setCondition(ssn, job, PodGroupInqueueType, v1.ConditionTrue, InqueueReasonWaiting,
				"Inqueue with no task placed; moved back to Pending after "+qp.inqueueTimeout.String()+" unless a task is placed")
			continue
		}
		if waited := now.Sub(inqueueCond.LastTransitionTime.Time); waited >= qp.inqueueTimeout {
			qp.dequeue(ssn, job, DequeuedReasonInqueueTimeout,
				fmt.Sprintf("no task placed within %s of Inqueue (waited %s); moved back to Pending and released the queue reservation",
					qp.inqueueTimeout, waited.Truncate(time.Second)))
		}
	}
}

// hasEligibleTask reports whether the job has a task the reclaim action and the quota round could
// have evaluated: pipelined (and, since hasProgress said no, refused by the hierarchy), or pending
// and ungated (or gated by Volcano's queue gate alone) and not BestEffort.
func hasEligibleTask(job *api.JobInfo) bool {
	if len(job.TaskStatusIndex[api.Pipelined]) > 0 {
		return true
	}
	for _, task := range job.TaskStatusIndex[api.Pending] {
		if task.Resreq.IsEmpty() {
			continue
		}
		if !task.SchGated || api.HasOnlyVolcanoSchedulingGate(task.Pod) {
			return true
		}
	}
	return false
}

// hasProgress reports whether the job has a task placed: allocated or beyond, or pipelined onto a
// node by a reclaim its hierarchy admits. A task the reclaim action pipelined for node fit while
// the hierarchy still refuses it (volcano-sh/volcano#4817) is not progress: allocate will refuse
// it in every following session.
func (qp *quotaPlugin) hasProgress(ssn *framework.Session, job *api.JobInfo) bool {
	for _, status := range []api.TaskStatus{api.Allocated, api.Binding, api.Bound, api.Running, api.Succeeded} {
		if len(job.TaskStatusIndex[status]) > 0 {
			return true
		}
	}
	queue := ssn.Queues[job.Queue]
	for _, task := range job.TaskStatusIndex[api.Pipelined] {
		if queue == nil || ssn.Allocatable(queue, task) {
			return true
		}
	}
	return false
}

func findCondition(job *api.JobInfo, condType scheduling.PodGroupConditionType) *scheduling.PodGroupCondition {
	if job.PodGroup == nil {
		return nil
	}
	for i := range job.PodGroup.Status.Conditions {
		if job.PodGroup.Status.Conditions[i].Type == condType {
			return &job.PodGroup.Status.Conditions[i]
		}
	}
	return nil
}

// inBackoff reports whether the PodGroup was dequeued less than backoff ago.
func inBackoff(job *api.JobInfo, backoff time.Duration, now time.Time) bool {
	if backoff <= 0 {
		return false
	}
	cond := findCondition(job, PodGroupDequeuedType)
	if cond == nil || cond.Status != v1.ConditionTrue {
		return false
	}
	return now.Before(cond.LastTransitionTime.Add(backoff))
}

func (qp *quotaPlugin) dequeue(ssn *framework.Session, job *api.JobInfo, reason, msg string) {
	msg = fmt.Sprintf("%s; next enqueue after %s", msg, qp.enqueueBackoff)
	job.PodGroup.Status.Phase = scheduling.PodGroupPending
	qp.setCondition(ssn, job, PodGroupInqueueType, v1.ConditionFalse, reason, msg)
	qp.setCondition(ssn, job, PodGroupDequeuedType, v1.ConditionTrue, reason, msg)
	ssn.RecordPodGroupEvent(job.PodGroup, v1.EventTypeNormal, "Dequeued", msg)
	ssn.MarkJobDirty(job.UID)
	klog.V(3).Infof("[quota] dequeued Job <%s/%s> from Queue <%s> (%s): %s", job.Namespace, job.Name, job.Queue, reason, msg)
}

func (qp *quotaPlugin) setCondition(ssn *framework.Session, job *api.JobInfo, condType scheduling.PodGroupConditionType, status v1.ConditionStatus, reason, msg string) {
	cond := &scheduling.PodGroupCondition{
		Type:               condType,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		TransitionID:       string(ssn.UID),
		Reason:             reason,
		Message:            msg,
	}
	if err := ssn.UpdatePodGroupCondition(job, cond); err != nil {
		klog.Errorf("[quota] failed to update condition %s of job <%s/%s>: %v", condType, job.Namespace, job.Name, err)
	}
}

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

// Package quota enforces queue capability: whether a task may be allocated to its queue and whether
// a PodGroup may be admitted, checked at the queue and every ancestor. It is the quota half of what
// the capacity plugin does, split out so that the share half (queue order, victim order, the
// preemptive and reclaimable checks against deserved) stays with capacity and this plugin can
// carry what a quota gate alone lacks: the admission of entitled jobs past an ancestor's cap for a
// reclaim trial, the reservation of the deserved share owed to admitted jobs, and the query that
// tells the reclaim action which ancestor blocks an ask.
//
// Run it next to capacity with capacity's enabledAllocatable and enableJobEnqueued switched off.
// The plugin keeps its own per-queue counters from the session's allocate and deallocate events,
// as every queue plugin does; nothing in the framework changes.
package quota

import (
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/features"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/helpers"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/capacity/hintprovider"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

const (
	// PluginName is the name under which the plugin is registered and configured.
	PluginName = "quota"
	// EnqueueAncestorCapReclaimKey enables admitting a job at enqueue when it is entitled to the
	// capacity (its leaf stays under deserved) and only an ancestor queue's capability blocks it, so
	// that the reclaim action can try to serve it; the dequeue action reverts the admission when
	// reclaim cannot (see volcano-sh/volcano#4817). Default false.
	EnqueueAncestorCapReclaimKey = "enqueueAncestorCapReclaim"
	// ReserveDeservedKey makes the allocatable check honor the deserved share that admitted
	// (Inqueue) jobs are still owed: at every ancestor, a candidate may not consume capability that
	// another subtree is owed within its deserved. Hierarchy mode only. Default false.
	ReserveDeservedKey = "reserveDeserved"

	rootQueueID = "root"
)

type quotaPlugin struct {
	pluginArguments framework.Arguments

	enqueueAncestorCapReclaim bool
	reserveDeserved           bool

	hierarchyEnabled bool
	readyToSchedule  bool
	totalResource    *api.Resource
	totalGuarantee   *api.Resource
	queueOpts        map[api.QueueID]*queueAttr

	// inqueueJobsByQueue lists, per leaf, the Inqueue jobs with minResources whose unplaced demand
	// makes up the leaf's reserve.
	inqueueJobsByQueue map[api.QueueID][]*api.JobInfo
	// admittedPastAncestorCap lists the jobs admitted on entitlement in this session, so the
	// enqueued hook can tag their PodGroup for the dequeue action.
	admittedPastAncestorCap map[api.JobID]struct{}
	// gatedReserved holds the tasks that passed the allocatable check while scheduling-gated and
	// are not allocated yet; they count against their queue until they are.
	gatedReserved map[api.QueueID]map[api.TaskID]*api.TaskInfo

	warnedNoDequeue bool
}

// queueAttr is the quota view of one queue for one session.
type queueAttr struct {
	queueID   api.QueueID
	name      string
	ancestors []api.QueueID // from the root down to the parent
	children  map[api.QueueID]*queueAttr

	deserved       *api.Resource
	guarantee      *api.Resource
	capability     *api.Resource
	realCapability *api.Resource
	allocated      *api.Resource
	// inqueue is the unplaced minResources of admitted jobs.
	inqueue *api.Resource
	// elastic is the usage of running jobs above their minResources.
	elastic *api.Resource
	// reserve is the deserved share this queue's subtree is still owed: for a leaf, the unplaced
	// minResources of its Inqueue jobs up to its deserved; for a parent, the sum over children
	// capped by its own deserved. Only maintained with reserveDeserved.
	reserve *api.Resource
}

// New builds the plugin.
func New(arguments framework.Arguments) framework.Plugin {
	return &quotaPlugin{pluginArguments: arguments}
}

func (qp *quotaPlugin) Name() string {
	return PluginName
}

func (qp *quotaPlugin) parseArguments() {
	qp.enqueueAncestorCapReclaim = false
	qp.reserveDeserved = false
	qp.pluginArguments.GetBool(&qp.enqueueAncestorCapReclaim, EnqueueAncestorCapReclaimKey)
	qp.pluginArguments.GetBool(&qp.reserveDeserved, ReserveDeservedKey)
	klog.V(4).Infof("[quota] %s configured as %t, %s configured as %t",
		EnqueueAncestorCapReclaimKey, qp.enqueueAncestorCapReclaim, ReserveDeservedKey, qp.reserveDeserved)
}

func (qp *quotaPlugin) OnSessionOpen(ssn *framework.Session) {
	qp.parseArguments()
	qp.totalResource = api.EmptyResource().Add(ssn.TotalResource)
	qp.totalGuarantee = api.EmptyResource()
	qp.queueOpts = map[api.QueueID]*queueAttr{}
	qp.inqueueJobsByQueue = map[api.QueueID][]*api.JobInfo{}
	qp.admittedPastAncestorCap = map[api.JobID]struct{}{}
	qp.gatedReserved = map[api.QueueID]map[api.TaskID]*api.TaskInfo{}
	qp.hierarchyEnabled = ssn.HierarchyEnabled(qp.Name())
	qp.warnOnDoubleGate(ssn)

	if utilfeature.DefaultFeatureGate.Enabled(features.SchedulingGatesQueueAdmission) {
		qp.buildGatedReserved(ssn)
	}

	ssn.AddHintProvider(qp.Name(), hintprovider.NewCapacityHintProvider(qp.hierarchyEnabled))

	if qp.hierarchyEnabled {
		qp.readyToSchedule = qp.buildHierarchicalQueueAttrs(ssn)
	} else {
		qp.buildQueueAttrs(ssn)
		qp.readyToSchedule = true
	}

	ssn.AddAllocatableFn(qp.Name(), func(queue *api.QueueInfo, candidate *api.TaskInfo) bool {
		return qp.allocatable(ssn, queue, candidate)
	})

	ssn.AddJobEnqueueableFn(qp.Name(), func(obj interface{}) int {
		return qp.jobEnqueueable(ssn, obj.(*api.JobInfo))
	})

	ssn.AddJobEnqueuedFn(qp.Name(), func(obj interface{}) {
		if job, ok := obj.(*api.JobInfo); ok && job != nil && job.PodGroup != nil {
			qp.jobEnqueued(ssn, job)
		}
	})

	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(event *framework.Event) {
			attr := qp.attrOfTask(ssn, event.Task)
			if attr == nil {
				return
			}
			attr.allocated.Add(event.Task.Resreq)
			for _, ancestorID := range attr.ancestors {
				qp.queueOpts[ancestorID].allocated.Add(event.Task.Resreq)
			}
			if qp.reserveDeserved {
				qp.refreshReserve(attr)
			}
			if utilfeature.DefaultFeatureGate.Enabled(features.SchedulingGatesQueueAdmission) {
				qp.removeGatedReserved(event.Task.UID)
			}
		},
		DeallocateFunc: func(event *framework.Event) {
			attr := qp.attrOfTask(ssn, event.Task)
			if attr == nil {
				return
			}
			attr.allocated.Sub(event.Task.Resreq)
			for _, ancestorID := range attr.ancestors {
				qp.queueOpts[ancestorID].allocated.Sub(event.Task.Resreq)
			}
			if qp.reserveDeserved {
				qp.refreshReserve(attr)
			}
			if utilfeature.DefaultFeatureGate.Enabled(features.SchedulingGatesQueueAdmission) &&
				api.HasQueueAllocationGateAnnotation(event.Task.Pod) {
				qp.addGatedReserved(attr.queueID, event.Task)
			}
		},
	})
}

func (qp *quotaPlugin) OnSessionClose(ssn *framework.Session) {
	qp.totalResource = nil
	qp.totalGuarantee = nil
	qp.queueOpts = nil
	qp.inqueueJobsByQueue = nil
	qp.admittedPastAncestorCap = nil
	qp.gatedReserved = nil
	qp.warnedNoDequeue = false
}

// warnOnDoubleGate logs once per session when another plugin also enforces queue capability, so a
// configuration that gates twice is visible: two gates give the stricter answer, but the reserve
// and the reclaim trial only work when this plugin's gate is the one that counts.
func (qp *quotaPlugin) warnOnDoubleGate(ssn *framework.Session) {
	on := func(b *bool) bool { return b != nil && *b }
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if plugin.Name != "capacity" && plugin.Name != "proportion" {
				continue
			}
			if on(plugin.EnabledAllocatable) || on(plugin.EnabledJobEnqueued) {
				klog.Warningf("[quota] plugin %s also enforces queue capability (enabledAllocatable=%t, enableJobEnqueued=%t); switch those off on %s so that %s is the only gate",
					plugin.Name, on(plugin.EnabledAllocatable), on(plugin.EnabledJobEnqueued), plugin.Name, PluginName)
			}
		}
	}
}

func (qp *quotaPlugin) attrOfTask(ssn *framework.Session, task *api.TaskInfo) *queueAttr {
	job := ssn.Jobs[task.Job]
	if job == nil {
		klog.V(4).Infof("[quota] skip event for task <%s/%s>: job <%s> not in session", task.Namespace, task.Name, task.Job)
		return nil
	}
	attr := qp.queueOpts[job.Queue]
	if attr == nil {
		klog.V(4).Infof("[quota] skip event for task <%s/%s>: queue <%s> unknown", task.Namespace, task.Name, job.Queue)
	}
	return attr
}

func (qp *quotaPlugin) isLeaf(queueID api.QueueID) bool {
	attr := qp.queueOpts[queueID]
	return attr != nil && len(attr.children) == 0
}

// ---------------------------------------------------------------- allocatable

// allocatable is the AllocatableFn. For the task's own leaf it is the capability check at the leaf
// and every ancestor. For an ancestor of the task's leaf it is the same check restricted to that
// queue and above, with the path and its reserve charges still taken from the task's job: the form
// the reclaim action uses to find the deepest blocking ancestor by walking the ancestry. Any other
// queue is refused.
func (qp *quotaPlugin) allocatable(ssn *framework.Session, queue *api.QueueInfo, candidate *api.TaskInfo) bool {
	if !qp.readyToSchedule {
		klog.V(3).Infof("[quota] queue hierarchy is not valid; refusing task <%s/%s>", candidate.Namespace, candidate.Name)
		return false
	}
	if queue.Queue.Status.State != scheduling.QueueStateOpen {
		klog.V(3).Infof("[quota] queue <%s> is %s, cannot allocate task <%s/%s>", queue.Name, queue.Queue.Status.State, candidate.Namespace, candidate.Name)
		return false
	}
	attr := qp.queueOpts[queue.UID]
	if attr == nil {
		return false
	}
	if len(attr.children) == 0 {
		ok := qp.allocatableFrom(ssn, attr, attr.queueID, candidate)
		if ok && utilfeature.DefaultFeatureGate.Enabled(features.SchedulingGatesQueueAdmission) &&
			api.HasQueueAllocationGateAnnotation(candidate.Pod) {
			qp.addGatedReserved(queue.UID, candidate)
		}
		return ok
	}
	// Ancestor form.
	job := ssn.Jobs[candidate.Job]
	if job == nil {
		return false
	}
	leafAttr := qp.queueOpts[job.Queue]
	if leafAttr == nil || !isAncestor(leafAttr, queue.UID) {
		klog.V(3).Infof("[quota] queue <%s> is not the queue of task <%s/%s> nor one of its ancestors", queue.Name, candidate.Namespace, candidate.Name)
		return false
	}
	return qp.allocatableFrom(ssn, leafAttr, queue.UID, candidate)
}

func isAncestor(leaf *queueAttr, queueID api.QueueID) bool {
	for _, id := range leaf.ancestors {
		if id == queueID {
			return true
		}
	}
	return false
}

// allocatableFrom checks the candidate at every queue on the leaf's path from startID up to the
// root. With reserveDeserved each ancestor is also charged with the deserved share owed to subtrees
// other than the one on the path to the candidate.
func (qp *quotaPlugin) allocatableFrom(ssn *framework.Session, leaf *queueAttr, startID api.QueueID, candidate *api.TaskInfo) bool {
	path := append(append([]api.QueueID{}, leaf.ancestors...), leaf.queueID)
	start := len(path) - 1
	for i, id := range path {
		if id == startID {
			start = i
		}
	}
	for i := start; i >= 0; i-- {
		attr := qp.queueOpts[path[i]]
		var owedToOthers *api.Resource
		if qp.reserveDeserved && i < len(path)-1 {
			owedToOthers = api.ExceededPart(attr.reserve, qp.queueOpts[path[i+1]].reserve)
		}
		if !qp.queueAllocatable(attr, candidate, owedToOthers) {
			return false
		}
	}
	return true
}

func (qp *quotaPlugin) queueAllocatable(attr *queueAttr, candidate *api.TaskInfo, owedToOthers *api.Resource) bool {
	gated := api.EmptyResource()
	for _, task := range qp.gatedReserved[attr.queueID] {
		if task.UID != candidate.UID {
			gated.Add(task.Resreq)
		}
	}
	futureUsed := attr.allocated.Clone().Add(gated).Add(candidate.Resreq)
	if owedToOthers != nil {
		futureUsed.Add(owedToOthers)
	}
	ok, _ := futureUsed.LessEqualWithDimensionAndResourcesName(attr.realCapability, candidate.Resreq)
	if !ok {
		klog.V(3).Infof("[quota] queue <%s>: realCapability <%v>, allocated <%v>, gated <%v>, owed to other subtrees <%v>; candidate <%s/%s> requests <%v>",
			attr.name, attr.realCapability, attr.allocated, gated, owedToOthers, candidate.Namespace, candidate.Name, candidate.Resreq)
	}
	return ok
}

// ---------------------------------------------------------------- enqueue

func (qp *quotaPlugin) jobEnqueueable(ssn *framework.Session, job *api.JobInfo) int {
	if !qp.readyToSchedule {
		return util.Reject
	}
	queue := ssn.Queues[job.Queue]
	attr := qp.queueOpts[job.Queue]
	if queue == nil || attr == nil {
		return util.Reject
	}
	if qp.hierarchyEnabled && len(attr.children) > 0 {
		return util.Reject
	}
	if queue.Queue.Status.State != scheduling.QueueStateOpen {
		klog.V(3).Infof("[quota] queue <%s> is %s, rejecting job <%s/%s>", queue.Name, queue.Queue.Status.State, job.Namespace, job.Name)
		return util.Reject
	}
	if job.PodGroup.Spec.MinResources == nil {
		return util.Permit
	}
	ok, blocker, resourceNames := qp.checkJobEnqueueableHierarchically(attr, job)
	if ok {
		return util.Permit
	}
	// Blocked by an ancestor's capability only. If configured, admit the job on entitlement (its
	// leaf stays within deserved) and let reclaim try to serve it; otherwise it would stay Pending
	// for as long as an over-deserved sibling keeps the capacity.
	if qp.enqueueAncestorCapReclaim && qp.hierarchyEnabled && blocker != attr.queueID &&
		qp.enqueueableViaAncestorReclaim(attr, job) {
		return util.Permit
	}
	ssn.RecordPodGroupEvent(job.PodGroup, v1.EventTypeNormal, string(scheduling.PodGroupUnschedulableType),
		util.FormatResourceNames("queue resource quota insufficient", "insufficient", resourceNames))
	return util.Reject
}

// queueEnqueueable is the admission check at one queue: minResources + allocated + inqueue -
// elastic <= realCapability on the job's dimensions.
func (qp *quotaPlugin) queueEnqueueable(attr *queueAttr, job *api.JobInfo) (bool, []string) {
	minReq := job.GetMinResources()
	future := minReq.Clone().Add(attr.allocated).Add(attr.inqueue).Sub(attr.elastic)
	ok, reasons := future.LessEqualWithDimensionAndResourcesName(attr.realCapability, minReq)
	if !ok {
		klog.V(5).Infof("[quota] job <%s/%s> min <%v> does not fit queue <%s>: realCapability <%v>, allocated <%v>, inqueue <%v>, elastic <%v>",
			job.Namespace, job.Name, minReq, attr.name, attr.realCapability, attr.allocated, attr.inqueue, attr.elastic)
	}
	return ok, reasons
}

// checkJobEnqueueableHierarchically checks the job at its leaf and every ancestor, leaf first, and
// on failure returns the first blocking queue and the insufficient resource names.
func (qp *quotaPlugin) checkJobEnqueueableHierarchically(leaf *queueAttr, job *api.JobInfo) (bool, api.QueueID, []string) {
	path := append(append([]api.QueueID{}, leaf.ancestors...), leaf.queueID)
	for i := len(path) - 1; i >= 0; i-- {
		if ok, reasons := qp.queueEnqueueable(qp.queueOpts[path[i]], job); !ok {
			return false, path[i], reasons
		}
	}
	return true, "", nil
}

// enqueueableViaAncestorReclaim decides whether a job that failed the hierarchical check is
// entitled to be admitted anyway for a reclaim trial (volcano-sh/volcano#4817). It does not predict
// reclaim; it checks entitlement on every requested dimension: the leaf passes its own check, the
// leaf stays within its deserved after admission (allocated + inqueue + minReq <= deserved, no
// elastic credit, a dimension missing from deserved counts as zero), and every failing ancestor
// fails on realCapability. The enqueued hook tags the admitted job and the dequeue action reverts
// the admission when reclaim reports it cannot serve the job, so the relaxed path is refused when
// that action is not enabled.
func (qp *quotaPlugin) enqueueableViaAncestorReclaim(leaf *queueAttr, job *api.JobInfo) bool {
	if !conf.EnabledActionMap[conf.DequeueActionName] {
		if !qp.warnedNoDequeue {
			klog.Warningf("[quota] %s is enabled but the %s action is not; refusing the relaxed admission because nothing would revert a job reclaim cannot serve",
				EnqueueAncestorCapReclaimKey, conf.DequeueActionName)
			qp.warnedNoDequeue = true
		}
		return false
	}
	if ok, _ := qp.queueEnqueueable(leaf, job); !ok {
		klog.V(4).Infof("[quota] %s: job <%s/%s> is blocked by its own queue <%s>, not by an ancestor", EnqueueAncestorCapReclaimKey, job.Namespace, job.Name, leaf.name)
		return false
	}
	minReq := job.GetMinResources()
	leafFuture := leaf.allocated.Clone().Add(leaf.inqueue).Add(minReq)
	if ok, dims := leafFuture.LessEqualWithDimensionAndResourcesName(leaf.deserved, minReq); !ok {
		klog.V(4).Infof("[quota] %s: job <%s/%s> would take queue <%s> over its deserved <%v> on %v (future <%v>)",
			EnqueueAncestorCapReclaimKey, job.Namespace, job.Name, leaf.name, leaf.deserved, dims, leafFuture)
		return false
	}
	for i := len(leaf.ancestors) - 1; i >= 0; i-- {
		anc := qp.queueOpts[leaf.ancestors[i]]
		if anc == nil {
			return false
		}
		if ok, _ := qp.queueEnqueueable(anc, job); ok {
			continue
		}
		klog.V(4).Infof("[quota] %s: job <%s/%s> admitted past ancestor <%s> (realCapability <%v>) for a reclaim trial",
			EnqueueAncestorCapReclaimKey, job.Namespace, job.Name, anc.name, anc.realCapability)
	}
	qp.admittedPastAncestorCap[job.UID] = struct{}{}
	return true
}

func (qp *quotaPlugin) jobEnqueued(ssn *framework.Session, job *api.JobInfo) {
	attr := qp.queueOpts[job.Queue]
	if attr == nil || job.PodGroup.Spec.MinResources == nil {
		return
	}
	deducted := job.DeductSchGatedResources(job.GetMinResources())
	attr.inqueue.Add(deducted)
	for _, ancestorID := range attr.ancestors {
		qp.queueOpts[ancestorID].inqueue.Add(deducted)
	}
	// A job admitted in this session is owed its share from now on, like one that was already
	// Inqueue at session open.
	if qp.reserveDeserved && qp.hierarchyEnabled {
		qp.inqueueJobsByQueue[job.Queue] = append(qp.inqueueJobsByQueue[job.Queue], job)
		qp.refreshReserve(attr)
	}
	if _, admitted := qp.admittedPastAncestorCap[job.UID]; admitted {
		// Tag the PodGroup: its admission depends on reclaim. The dequeue action returns it to
		// Pending as soon as reclaim reports it cannot serve it.
		cond := &scheduling.PodGroupCondition{
			Type:               api.PodGroupInqueueType,
			Status:             v1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
			TransitionID:       string(ssn.UID),
			Reason:             api.PodGroupInqueueReasonAncestorCapReclaim,
			Message:            "admitted past an ancestor capability on entitlement; the reclaim action tries to serve it",
		}
		if err := ssn.UpdatePodGroupCondition(job, cond); err != nil {
			klog.Errorf("[quota] failed to tag job <%s/%s> as admitted for a reclaim trial: %v", job.Namespace, job.Name, err)
		}
	}
}

// ---------------------------------------------------------------- gated tasks

func (qp *quotaPlugin) addGatedReserved(queueID api.QueueID, task *api.TaskInfo) {
	if qp.gatedReserved[queueID] == nil {
		qp.gatedReserved[queueID] = map[api.TaskID]*api.TaskInfo{}
	}
	qp.gatedReserved[queueID][task.UID] = task
}

func (qp *quotaPlugin) removeGatedReserved(taskID api.TaskID) {
	for queueID, tasks := range qp.gatedReserved {
		if _, found := tasks[taskID]; found {
			delete(tasks, taskID)
			if len(tasks) == 0 {
				delete(qp.gatedReserved, queueID)
			}
			return
		}
	}
}

// buildGatedReserved lists the pending tasks that passed the check in an earlier session and had
// their gate removed but are not allocated yet; they hold their queue's capacity meanwhile.
func (qp *quotaPlugin) buildGatedReserved(ssn *framework.Session) {
	for _, job := range ssn.Jobs {
		for _, task := range job.TaskStatusIndex[api.Pending] {
			if !task.SchGated && api.HasQueueAllocationGateAnnotation(task.Pod) {
				qp.addGatedReserved(job.Queue, task)
			}
		}
	}
}

// ---------------------------------------------------------------- attributes

func (qp *quotaPlugin) newQueueAttr(queue *api.QueueInfo) *queueAttr {
	attr := &queueAttr{
		queueID:        queue.UID,
		name:           queue.Name,
		ancestors:      []api.QueueID{},
		children:       map[api.QueueID]*queueAttr{},
		deserved:       api.NewResource(queue.Queue.Spec.Deserved),
		guarantee:      api.EmptyResource(),
		capability:     api.EmptyResource(),
		realCapability: api.EmptyResource(),
		allocated:      api.EmptyResource(),
		inqueue:        api.EmptyResource(),
		elastic:        api.EmptyResource(),
		reserve:        api.EmptyResource(),
	}
	if len(queue.Queue.Spec.Capability) != 0 {
		attr.capability = api.NewResource(queue.Queue.Spec.Capability)
	}
	if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
		attr.guarantee = api.NewResource(queue.Queue.Spec.Guarantee.Resource)
	}
	return attr
}

// addJobUsage adds a job's allocated, inqueue and elastic usage to its leaf and returns the deltas
// for the ancestors.
func (qp *quotaPlugin) addJobUsage(attr *queueAttr, job *api.JobInfo) (allocated, inqueue, elastic *api.Resource) {
	allocated, inqueue, elastic = api.EmptyResource(), api.EmptyResource(), api.EmptyResource()
	for status, tasks := range job.TaskStatusIndex {
		if !api.AllocatedStatus(status) {
			continue
		}
		for _, t := range tasks {
			allocated.Add(t.Resreq)
		}
	}
	if job.PodGroup.Spec.MinResources != nil {
		// Deduct what allocated tasks already hold so it is not counted in both allocated and
		// inqueue while the PodGroup stays Inqueue until its tasks run.
		if job.PodGroup.Status.Phase == scheduling.PodGroupInqueue {
			inqueue.Add(job.DeductSchGatedResources(util.GetInqueueResource(job, job.Allocated)))
			qp.inqueueJobsByQueue[job.Queue] = append(qp.inqueueJobsByQueue[job.Queue], job)
		}
		// A running job below minMember (for example a completed Spark driver with a PodGroup that
		// keeps running) must not reserve its minResources again.
		if job.PodGroup.Status.Phase == scheduling.PodGroupRunning &&
			int32(util.CalculateAllocatedTaskNum(job)) >= job.PodGroup.Spec.MinMember {
			inqueue.Add(job.DeductSchGatedResources(util.GetInqueueResource(job, job.Allocated)))
		}
	}
	elastic.Add(job.GetElasticResources())
	attr.allocated.Add(allocated)
	attr.inqueue.Add(inqueue)
	attr.elastic.Add(elastic)
	return allocated, inqueue, elastic
}

// buildQueueAttrs is the flat mode: every queue stands alone, its real capability is the cluster
// minus the other queues' guarantees plus its own, capped by its capability when one is set.
func (qp *quotaPlugin) buildQueueAttrs(ssn *framework.Session) {
	for _, queue := range ssn.Queues {
		if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
			qp.totalGuarantee.Add(api.NewResource(queue.Queue.Spec.Guarantee.Resource))
		}
	}
	for _, queue := range ssn.Queues {
		attr := qp.newQueueAttr(queue)
		realCapability := api.ExceededPart(qp.totalResource, qp.totalGuarantee).Add(attr.guarantee)
		if len(queue.Queue.Spec.Capability) != 0 {
			if attr.capability.MilliCPU <= 0 {
				attr.capability.MilliCPU = math.MaxFloat64
			}
			if attr.capability.Memory <= 0 {
				attr.capability.Memory = math.MaxFloat64
			}
			realCapability.MinDimensionResource(attr.capability, api.Infinity)
		}
		attr.realCapability = realCapability
		qp.queueOpts[queue.UID] = attr
	}
	for _, job := range ssn.Jobs {
		attr := qp.queueOpts[job.Queue]
		if attr == nil {
			continue
		}
		qp.addJobUsage(attr, job)
	}
	for _, attr := range qp.queueOpts {
		attr.deserved.MinDimensionResource(attr.realCapability, api.Infinity)
		attr.deserved = helpers.Max(attr.deserved, attr.guarantee)
		klog.V(4).Infof("[quota] queue <%s>: realCapability <%v>, allocated <%v>, inqueue <%v>, elastic <%v>",
			attr.name, attr.realCapability, attr.allocated, attr.inqueue, attr.elastic)
	}
}

// buildHierarchicalQueueAttrs builds the tree, sums usage up the ancestors, and derives the real
// capability of every queue from its parent's with the guarantee hold-back. It returns false when
// the hierarchy is invalid; the plugin then refuses everything for the session.
func (qp *quotaPlugin) buildHierarchicalQueueAttrs(ssn *framework.Session) bool {
	for _, queue := range ssn.Queues {
		if _, found := qp.queueOpts[queue.UID]; found {
			continue
		}
		qp.queueOpts[queue.UID] = qp.newQueueAttr(queue)
		if err := qp.linkAncestors(queue, ssn, map[api.QueueID]struct{}{}); err != nil {
			klog.Errorf("[quota] invalid queue hierarchy at <%s>: %v", queue.Name, err)
			return false
		}
	}
	for _, job := range ssn.Jobs {
		attr := qp.queueOpts[job.Queue]
		if attr == nil {
			klog.Warningf("[quota] job <%s/%s> references unknown queue <%s>", job.Namespace, job.Name, job.Queue)
			continue
		}
		allocated, inqueue, elastic := qp.addJobUsage(attr, job)
		for _, ancestorID := range attr.ancestors {
			anc := qp.queueOpts[ancestorID]
			anc.allocated.Add(allocated)
			anc.inqueue.Add(inqueue)
			anc.elastic.Add(elastic)
		}
	}
	root := qp.queueOpts[api.QueueID(rootQueueID)]
	if root == nil {
		klog.Warningf("[quota] root queue %q not found in the session", rootQueueID)
		return false
	}
	// The root is not restricted by the cluster's current size; users restrict through queues.
	if root.capability.IsEmpty() {
		infinite := api.InfiniteResource()
		for k := range qp.totalResource.ScalarResources {
			infinite.SetScalar(k, math.MaxInt64)
		}
		root.capability = infinite
	}
	root.realCapability = root.capability
	qp.deriveSubtree(root)
	if qp.reserveDeserved {
		qp.rebuildReserves(root)
	}
	for _, attr := range qp.queueOpts {
		klog.V(4).Infof("[quota] queue <%s>: realCapability <%v>, deserved <%v>, allocated <%v>, inqueue <%v>, elastic <%v>, reserve <%v>",
			attr.name, attr.realCapability, attr.deserved, attr.allocated, attr.inqueue, attr.elastic, attr.reserve)
	}
	return true
}

func (qp *quotaPlugin) linkAncestors(queue *api.QueueInfo, ssn *framework.Session, visited map[api.QueueID]struct{}) error {
	if queue.Name == rootQueueID {
		return nil
	}
	if _, seen := visited[queue.UID]; seen {
		return fmt.Errorf("cycle through queue %s", queue.Name)
	}
	visited[queue.UID] = struct{}{}
	defer delete(visited, queue.UID)

	parentName := rootQueueID
	if queue.Queue.Spec.Parent != "" {
		parentName = queue.Queue.Spec.Parent
	}
	parent, found := ssn.Queues[api.QueueID(parentName)]
	if !found {
		return fmt.Errorf("queue %s has unknown parent %s", queue.Name, parentName)
	}
	if _, found := qp.queueOpts[parent.UID]; !found {
		qp.queueOpts[parent.UID] = qp.newQueueAttr(parent)
		if err := qp.linkAncestors(parent, ssn, visited); err != nil {
			return err
		}
	}
	qp.queueOpts[parent.UID].children[queue.UID] = qp.queueOpts[queue.UID]
	qp.queueOpts[queue.UID].ancestors = append(append([]api.QueueID{}, qp.queueOpts[parent.UID].ancestors...), parent.UID)
	return nil
}

// deriveSubtree sets, for every child of attr, deserved raised to guarantee, capability inherited
// from the parent on unset dimensions, and the real capability: the parent's real capability
// minus the children's guarantees plus the child's own, capped by the child's capability.
func (qp *quotaPlugin) deriveSubtree(attr *queueAttr) {
	totalGuarantee := api.EmptyResource()
	totalDeserved := api.EmptyResource()
	for _, child := range attr.children {
		child.deserved = helpers.Max(child.deserved, child.guarantee)
		totalDeserved.Add(child.deserved)
		totalGuarantee.Add(child.guarantee)
		if child.capability.MilliCPU <= 0 {
			child.capability.MilliCPU = attr.capability.MilliCPU
		}
		if child.capability.Memory <= 0 {
			child.capability.Memory = attr.capability.Memory
		}
		if attr.capability.ScalarResources != nil {
			if child.capability.ScalarResources == nil {
				child.capability.ScalarResources = map[v1.ResourceName]float64{}
			}
			for k, v := range attr.capability.ScalarResources {
				if _, set := child.capability.ScalarResources[k]; !set {
					child.capability.ScalarResources[k] = v
				}
			}
		}
	}
	if attr.name == rootQueueID {
		attr.guarantee = totalGuarantee
		attr.deserved = totalDeserved
	}
	for _, child := range attr.children {
		realCapability := api.ExceededPart(attr.realCapability, totalGuarantee).Add(child.guarantee)
		realCapability.MinDimensionResource(child.capability, api.Infinity)
		child.realCapability = realCapability
	}
	for _, child := range attr.children {
		qp.deriveSubtree(child)
	}
}

// ---------------------------------------------------------------- reserve

// jobUnplaced returns the part of a job's minResources no placed task holds yet. Placed tasks are
// those in an allocated status plus Pipelined ones: a task pipelined by reclaim or allocate is
// already accounted in its queue's allocated and is no longer owed. Gated pods are deducted as the
// inqueue term does.
func jobUnplaced(job *api.JobInfo) *api.Resource {
	placed := job.Allocated.Clone()
	for _, t := range job.TaskStatusIndex[api.Pipelined] {
		placed.Add(t.Resreq)
	}
	return job.DeductSchGatedResources(util.GetInqueueResource(job, placed))
}

// perDimension applies f to the cpu, memory and every scalar dimension named by any of the three
// operands, keeping the positive results.
func perDimension(a, b, c *api.Resource, f func(a, b, c float64) float64) *api.Resource {
	out := api.EmptyResource()
	out.MilliCPU = f(a.MilliCPU, b.MilliCPU, c.MilliCPU)
	out.Memory = f(a.Memory, b.Memory, c.Memory)
	for _, r := range []*api.Resource{a, b, c} {
		for name := range r.ScalarResources {
			if v := f(a.Get(name), b.Get(name), c.Get(name)); v > 0 {
				out.SetScalar(name, v)
			}
		}
	}
	return out
}

// leafOwed is the deserved share a leaf is still owed given its unplaced admitted demand:
// clamp0(min(deserved, allocated + unplaced) - allocated). A dimension missing from deserved is
// owed nothing.
func leafOwed(deserved, allocated, unplaced *api.Resource) *api.Resource {
	return perDimension(deserved, allocated, unplaced, func(d, a, u float64) float64 {
		return math.Max(0, math.Min(d, a+u)-a)
	})
}

// parentReserve folds the children's reserves: min(sum of children, clamp0(deserved - allocated)).
// A dimension on which the parent has no deserved passes the children's sum through, since the
// parent has no guarantee of its own to cap it with.
func parentReserve(attr *queueAttr) *api.Resource {
	sum := api.EmptyResource()
	for _, child := range attr.children {
		sum.Add(child.reserve)
	}
	return perDimension(attr.deserved, attr.allocated, sum, func(d, a, s float64) float64 {
		if d <= 0 {
			return s
		}
		return math.Max(0, math.Min(s, d-a))
	})
}

func (qp *quotaPlugin) leafReserve(attr *queueAttr) *api.Resource {
	unplaced := api.EmptyResource()
	for _, job := range qp.inqueueJobsByQueue[attr.queueID] {
		unplaced.Add(jobUnplaced(job))
	}
	return leafOwed(attr.deserved, attr.allocated, unplaced)
}

// rebuildReserves computes every queue's reserve bottom-up from the given subtree root.
func (qp *quotaPlugin) rebuildReserves(attr *queueAttr) {
	if len(attr.children) == 0 {
		attr.reserve = qp.leafReserve(attr)
		return
	}
	for _, child := range attr.children {
		qp.rebuildReserves(child)
	}
	attr.reserve = parentReserve(attr)
}

// refreshReserve recomputes a leaf's reserve after its allocated changed and re-folds its
// ancestors, nearest first.
func (qp *quotaPlugin) refreshReserve(leaf *queueAttr) {
	if len(leaf.children) > 0 {
		return
	}
	leaf.reserve = qp.leafReserve(leaf)
	for i := len(leaf.ancestors) - 1; i >= 0; i-- {
		if anc := qp.queueOpts[leaf.ancestors[i]]; anc != nil {
			anc.reserve = parentReserve(anc)
		}
	}
}

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

// Package dequeue implements the dequeue action: it moves a PodGroup from Inqueue back to Pending
// when the reclaim action reports that it cannot serve it, releasing the PodGroup's queue
// reservation instead of holding it indefinitely.
//
// A PodGroup that reaches Inqueue reserves its minResources against its queue and every ancestor
// (the capacity and proportion plugins' inqueue accounting), and for Volcano Jobs it triggers pod
// creation. Nothing in the scheduler ever moves a PodGroup back from Inqueue: the phase stays until
// minMember tasks are scheduled. A group that cannot be served therefore holds its reservation
// forever and starves its queue's siblings.
//
// The action runs after reclaim and reads reclaim's per-session verdict (api.JobInfo.ReclaimResult):
//
//   - ReclaimFailed: eligible victims existed, but no node could be made to fit the job, so every
//     tentative eviction was rolled back. The job is dequeued immediately.
//   - ReclaimNoVictims: nothing to reclaim. That is ordinary waiting for capacity and the job stays
//     Inqueue, unless its admission depended on reclaim (the capacity plugin's ancestor-capability
//     admission tags such PodGroups), in which case the admission premise is false and the job is
//     dequeued immediately.
//   - otherwise an optional timeout applies: a PodGroup Inqueue with no scheduled task for longer
//     than inqueueTimeout is dequeued. This covers jobs reclaim never evaluates (no pods yet, no
//     reclaim action). It is off by default.
//
// A dequeued PodGroup gets a "Dequeued" condition; the enqueue action keeps it Pending while that
// condition is younger than enqueueBackoff, which bounds the enqueue/dequeue cycle of a job that
// is never served.
//
// Configure it after reclaim, preferably last:
//
//	actions: "enqueue, allocate, backfill, reclaim, dequeue"
//	configurations:
//	- name: dequeue
//	  arguments:
//	    enqueueBackoff: 1m    # default 1m
//	    inqueueTimeout: 10m   # default 0 (disabled)
package dequeue

import (
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	// Dequeue is the action name.
	Dequeue = "dequeue"
	// EnqueueBackoffKey is the action argument: how long a dequeued PodGroup stays Pending before
	// the enqueue action considers it again. A Go duration string.
	EnqueueBackoffKey = "enqueueBackoff"
	// InqueueTimeoutKey is the action argument: how long a PodGroup may stay Inqueue without any
	// scheduled task before it is moved back to Pending even without a reclaim verdict. A Go
	// duration string; empty or zero disables the timeout.
	InqueueTimeoutKey = "inqueueTimeout"
	// DefaultEnqueueBackoff is used when enqueueBackoff is not configured or cannot be parsed.
	DefaultEnqueueBackoff = time.Minute

	// DequeuedReasonReclaimFailed: reclaim evaluated the job and could not serve it.
	DequeuedReasonReclaimFailed = "ReclaimFailed"
	// DequeuedReasonInqueueTimeout: the job stayed Inqueue past inqueueTimeout with no progress.
	DequeuedReasonInqueueTimeout = "InqueueTimeout"
	inqueueReason                = "WaitingForResources"
)

// Action is the dequeue action.
type Action struct {
	enqueueBackoff time.Duration
	inqueueTimeout time.Duration
}

// New returns the action instance.
func New() *Action {
	return &Action{enqueueBackoff: DefaultEnqueueBackoff}
}

// Name returns the action name.
func (a *Action) Name() string {
	return Dequeue
}

// Initialize inits the action.
func (a *Action) Initialize() {}

// UnInitialize releases resources.
func (a *Action) UnInitialize() {}

// ParseArguments reads enqueueBackoff and inqueueTimeout from the action's configuration. A zero
// inqueueTimeout means the timeout lane is disabled.
func ParseArguments(configurations []conf.Configuration) (enqueueBackoff, inqueueTimeout time.Duration) {
	enqueueBackoff = DefaultEnqueueBackoff
	arguments := framework.GetArgOfActionFromConf(configurations, Dequeue)
	if d, ok := parseDuration(arguments, EnqueueBackoffKey); ok {
		enqueueBackoff = d
	}
	if d, ok := parseDuration(arguments, InqueueTimeoutKey); ok {
		inqueueTimeout = d
	}
	return enqueueBackoff, inqueueTimeout
}

func parseDuration(arguments framework.Arguments, key string) (time.Duration, bool) {
	if arguments == nil {
		return 0, false
	}
	var value string
	arguments.GetString(&value, key)
	if value == "" {
		return 0, false
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		klog.Warningf("[dequeue] invalid %s %q: %v; using default", key, value, err)
		return 0, false
	}
	return d, true
}

// InBackoff reports whether a Pending PodGroup was dequeued less than backoff ago and should not be
// enqueued yet. It is consulted by the enqueue action.
func InBackoff(job *api.JobInfo, backoff time.Duration, now time.Time) bool {
	if job == nil || job.PodGroup == nil || backoff <= 0 {
		return false
	}
	cond := findCondition(job, api.PodGroupDequeuedType)
	if cond == nil || cond.Status != v1.ConditionTrue {
		return false
	}
	return now.Before(cond.LastTransitionTime.Add(backoff))
}

// Execute moves Inqueue PodGroups that reclaim cannot serve back to Pending.
func (a *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Dequeue ...")
	defer klog.V(5).Infof("Leaving Dequeue ...")

	a.enqueueBackoff, a.inqueueTimeout = ParseArguments(ssn.Configurations)
	now := time.Now()

	for _, job := range ssn.Jobs {
		if job.PodGroup == nil || job.PodGroup.Status.Phase != scheduling.PodGroupInqueue {
			continue
		}
		// A PodGroup without minResources reserves nothing and is admitted unconditionally by
		// enqueue, so dequeuing it would only add churn.
		if job.PodGroup.Spec.MinResources == nil {
			continue
		}
		if hasProgress(job) {
			continue
		}

		inqueueCond := findCondition(job, api.PodGroupInqueueType)
		admittedOnReclaim := inqueueCond != nil && inqueueCond.Status == v1.ConditionTrue &&
			inqueueCond.Reason == api.PodGroupInqueueReasonAncestorCapReclaim

		switch {
		case job.ReclaimResult == api.ReclaimFailed:
			a.dequeue(ssn, job, DequeuedReasonReclaimFailed,
				"reclaim found victims but could not make any node fit the job; moved back to Pending and released the queue reservation")
		case job.ReclaimResult == api.ReclaimNoVictims && admittedOnReclaim:
			a.dequeue(ssn, job, DequeuedReasonReclaimFailed,
				"admitted on reclaimable slack but reclaim found no eligible victim; moved back to Pending and released the queue reservation")
		case a.inqueueTimeout > 0:
			a.applyTimeout(ssn, job, inqueueCond, now)
		}
	}
}

// applyTimeout is the fallback lane for jobs without a reclaim verdict: stamp the first sighting,
// dequeue once the stamp is older than inqueueTimeout.
func (a *Action) applyTimeout(ssn *framework.Session, job *api.JobInfo, inqueueCond *scheduling.PodGroupCondition, now time.Time) {
	if inqueueCond == nil || inqueueCond.Status != v1.ConditionTrue {
		a.setCondition(ssn, job, api.PodGroupInqueueType, v1.ConditionTrue, inqueueReason,
			"Inqueue with no scheduled task; the dequeue action will move the PodGroup back to Pending after "+a.inqueueTimeout.String())
		return
	}
	waited := now.Sub(inqueueCond.LastTransitionTime.Time)
	if waited < a.inqueueTimeout {
		return
	}
	a.dequeue(ssn, job, DequeuedReasonInqueueTimeout,
		fmt.Sprintf("no task scheduled within %s of Inqueue (waited %s); moved back to Pending and released the queue reservation",
			a.inqueueTimeout, waited.Truncate(time.Second)))
}

func (a *Action) dequeue(ssn *framework.Session, job *api.JobInfo, reason, msg string) {
	msg = fmt.Sprintf("%s; next enqueue after %s", msg, a.enqueueBackoff)
	job.PodGroup.Status.Phase = scheduling.PodGroupPending
	a.setCondition(ssn, job, api.PodGroupInqueueType, v1.ConditionFalse, reason, msg)
	a.setCondition(ssn, job, api.PodGroupDequeuedType, v1.ConditionTrue, reason, msg)
	ssn.RecordPodGroupEvent(job.PodGroup, v1.EventTypeNormal, "Dequeued", msg)
	klog.V(3).Infof("Dequeued Job <%s/%s> from Queue <%s> (%s): %s", job.Namespace, job.Name, job.Queue, reason, msg)
}

// hasProgress reports whether any task of the job has been placed in this or an earlier session.
func hasProgress(job *api.JobInfo) bool {
	for _, status := range []api.TaskStatus{api.Pipelined, api.Allocated, api.Binding, api.Bound, api.Running, api.Succeeded} {
		if len(job.TaskStatusIndex[status]) > 0 {
			return true
		}
	}
	return false
}

func findCondition(job *api.JobInfo, condType scheduling.PodGroupConditionType) *scheduling.PodGroupCondition {
	for i := range job.PodGroup.Status.Conditions {
		if job.PodGroup.Status.Conditions[i].Type == condType {
			return &job.PodGroup.Status.Conditions[i]
		}
	}
	return nil
}

func (a *Action) setCondition(ssn *framework.Session, job *api.JobInfo, condType scheduling.PodGroupConditionType, status v1.ConditionStatus, reason, msg string) {
	cond := &scheduling.PodGroupCondition{
		Type:               condType,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		TransitionID:       string(ssn.UID),
		Reason:             reason,
		Message:            msg,
	}
	if err := ssn.UpdatePodGroupCondition(job, cond); err != nil {
		klog.Errorf("Failed to update condition %s of job <%s/%s>: %v", condType, job.Namespace, job.Name, err)
	}
}

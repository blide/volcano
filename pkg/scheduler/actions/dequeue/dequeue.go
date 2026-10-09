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

// Package dequeue implements the dequeue action: a safety net that moves a PodGroup from Inqueue
// back to Pending when it has made no scheduling progress for a configured time.
//
// A PodGroup that reaches Inqueue reserves its minResources against its queue and every ancestor
// (the capacity and proportion plugins' inqueue accounting), and for Volcano Jobs it triggers pod
// creation. Nothing in the scheduler ever moves a PodGroup back from Inqueue: the phase stays until
// minMember tasks are scheduled. A group admitted on a wrong assumption (the ancestor-capability
// admission in the capacity plugin, affinity or node selectors that enqueue cannot see, a victim
// gang refuses to evict) therefore holds its reservation forever and starves its queue's siblings.
//
// The action records an "Inqueue" condition the first time it sees a group Inqueue with no task
// pipelined, allocated, bound or running, and once that condition is older than inqueueTimeout it
// sets the phase back to Pending, flips the condition to False and adds a "Dequeued" condition.
// The enqueue action keeps a group Pending while its "Dequeued" condition is younger than
// enqueueBackoff, which bounds the enqueue/dequeue cycle for a group that is never served.
//
// Configure it last in the action list so it observes what the other actions achieved in the same
// session:
//
//	actions: "enqueue, allocate, backfill, reclaim, dequeue"
//	configurations:
//	- name: dequeue
//	  arguments:
//	    inqueueTimeout: 10m   # default 10m
//	    enqueueBackoff: 10m   # default: same as inqueueTimeout
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
	// InqueueTimeoutKey is the action argument: how long a PodGroup may stay Inqueue without any
	// scheduled task before it is moved back to Pending. A Go duration string.
	InqueueTimeoutKey = "inqueueTimeout"
	// EnqueueBackoffKey is the action argument: how long a dequeued PodGroup stays Pending before
	// the enqueue action considers it again. A Go duration string. Defaults to inqueueTimeout.
	EnqueueBackoffKey = "enqueueBackoff"
	// DefaultInqueueTimeout is used when inqueueTimeout is not configured or cannot be parsed.
	DefaultInqueueTimeout = 10 * time.Minute

	// DequeuedReason is the reason on the Dequeued condition and the PodGroup event.
	DequeuedReason = "InqueueTimeout"
	inqueueReason  = "WaitingForResources"
)

// Action is the dequeue action.
type Action struct {
	inqueueTimeout time.Duration
	enqueueBackoff time.Duration
}

// New returns the action instance.
func New() *Action {
	return &Action{inqueueTimeout: DefaultInqueueTimeout, enqueueBackoff: DefaultInqueueTimeout}
}

// Name returns the action name.
func (a *Action) Name() string {
	return Dequeue
}

// Initialize inits the action.
func (a *Action) Initialize() {}

// UnInitialize releases resources.
func (a *Action) UnInitialize() {}

// ParseArguments reads inqueueTimeout and enqueueBackoff from the action's configuration.
func ParseArguments(configurations []conf.Configuration) (inqueueTimeout, enqueueBackoff time.Duration) {
	inqueueTimeout = DefaultInqueueTimeout
	arguments := framework.GetArgOfActionFromConf(configurations, Dequeue)
	if d, ok := parseDuration(arguments, InqueueTimeoutKey); ok {
		inqueueTimeout = d
	}
	enqueueBackoff = inqueueTimeout
	if d, ok := parseDuration(arguments, EnqueueBackoffKey); ok {
		enqueueBackoff = d
	}
	return inqueueTimeout, enqueueBackoff
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

// Execute moves Inqueue PodGroups that made no progress within inqueueTimeout back to Pending.
func (a *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Dequeue ...")
	defer klog.V(5).Infof("Leaving Dequeue ...")

	a.inqueueTimeout, a.enqueueBackoff = ParseArguments(ssn.Configurations)
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

		cond := findCondition(job, api.PodGroupInqueueType)
		if cond == nil || cond.Status != v1.ConditionTrue {
			// First observation without progress: start the clock.
			a.setCondition(ssn, job, api.PodGroupInqueueType, v1.ConditionTrue, inqueueReason,
				"Inqueue with no scheduled task; the dequeue action will move the PodGroup back to Pending after "+a.inqueueTimeout.String())
			continue
		}
		waited := now.Sub(cond.LastTransitionTime.Time)
		if waited < a.inqueueTimeout {
			continue
		}

		msg := fmt.Sprintf("no task scheduled within %s of Inqueue (waited %s); moved back to Pending and released the queue reservation, next enqueue after %s",
			a.inqueueTimeout, waited.Truncate(time.Second), a.enqueueBackoff)
		job.PodGroup.Status.Phase = scheduling.PodGroupPending
		a.setCondition(ssn, job, api.PodGroupInqueueType, v1.ConditionFalse, DequeuedReason, msg)
		a.setCondition(ssn, job, api.PodGroupDequeuedType, v1.ConditionTrue, DequeuedReason, msg)
		ssn.RecordPodGroupEvent(job.PodGroup, v1.EventTypeNormal, DequeuedReason, msg)
		klog.V(3).Infof("Dequeued Job <%s/%s> from Queue <%s>: %s", job.Namespace, job.Name, job.Queue, msg)
	}
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

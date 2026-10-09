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

package dequeue_test

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/actions/dequeue"
	"volcano.sh/volcano/pkg/scheduler/actions/enqueue"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// Every case uses one PodGroup pg1 in namespace ns1 and queue q1.

var (
	q1      = util.BuildQueue("q1", 1, api.BuildResourceList("8", "8Gi"))
	minRes  = api.BuildResourceList("1", "1Gi")
	pending = util.BuildPod("ns1", "p1", "", v1.PodPending, minRes, "pg1", map[string]string{}, map[string]string{})
)

func pgCond(condType scheduling.PodGroupConditionType, status v1.ConditionStatus, reason string, age time.Duration) schedulingv1beta1.PodGroupCondition {
	return schedulingv1beta1.PodGroupCondition{
		Type:               schedulingv1beta1.PodGroupConditionType(condType),
		Status:             status,
		Reason:             reason,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-age)),
	}
}

// inqueueSince is the stamp the timeout lane writes, transitioned age ago.
func inqueueSince(age time.Duration) schedulingv1beta1.PodGroupCondition {
	return pgCond(api.PodGroupInqueueType, v1.ConditionTrue, "", age)
}

// dequeuedAgo is the condition a dequeue leaves behind, transitioned age ago.
func dequeuedAgo(reason string, age time.Duration) schedulingv1beta1.PodGroupCondition {
	return pgCond(api.PodGroupDequeuedType, v1.ConditionTrue, reason, age)
}

func pg1(phase schedulingv1beta1.PodGroupPhase, minMember int32, minResources v1.ResourceList, conds ...schedulingv1beta1.PodGroupCondition) *schedulingv1beta1.PodGroup {
	var pg *schedulingv1beta1.PodGroup
	if minResources == nil {
		pg = util.BuildPodGroup("pg1", "ns1", "q1", minMember, nil, phase)
	} else {
		pg = util.BuildPodGroupWithMinResources("pg1", "ns1", "q1", minMember, nil, minResources, phase)
	}
	pg.Status.Conditions = conds
	return pg
}

// fixture is a session with q1, the given PodGroup and pods.
func fixture(pg *schedulingv1beta1.PodGroup, pods ...*v1.Pod) uthelper.TestCommonStruct {
	return uthelper.TestCommonStruct{
		Pods:      pods,
		PodGroups: []*schedulingv1beta1.PodGroup{pg},
		Queues:    []*schedulingv1beta1.Queue{q1},
	}
}

// inqueueNoProgress is an Inqueue minMember-1 group with minResources and one pending pod.
func inqueueNoProgress(conds ...schedulingv1beta1.PodGroupCondition) uthelper.TestCommonStruct {
	return fixture(pg1(schedulingv1beta1.PodGroupInqueue, 1, minRes, conds...), pending)
}

func dequeueConf(args map[string]interface{}) []conf.Configuration {
	return []conf.Configuration{{Name: dequeue.Dequeue, Arguments: args}}
}

func findCond(job *api.JobInfo, condType scheduling.PodGroupConditionType) *scheduling.PodGroupCondition {
	for i := range job.PodGroup.Status.Conditions {
		if job.PodGroup.Status.Conditions[i].Type == condType {
			return &job.PodGroup.Status.Conditions[i]
		}
	}
	return nil
}

func TestDequeue(t *testing.T) {
	timeoutConfig := dequeueConf(map[string]interface{}{dequeue.InqueueTimeoutKey: "1m"})
	node := util.BuildNode("n1", api.BuildResourceList("8", "8Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{})
	running := util.BuildPod("ns1", "p0", "n1", v1.PodRunning, minRes, "pg1", map[string]string{}, map[string]string{})

	cases := []struct {
		name          string
		test          uthelper.TestCommonStruct
		config        []conf.Configuration
		reclaimResult api.ReclaimOutcome // set on the job before the actions run
		actions       []framework.Action // default: dequeue only
		wantPhase     scheduling.PodGroupPhase
		wantInq       v1.ConditionStatus // expected Inqueue condition status, "" = must be absent
		wantDequeued  string             // expected Dequeued condition reason, "" = must be absent
	}{
		// --- reclaim verdict lane (default configuration)
		{
			name:          "reclaim failed with victims: dequeued at once",
			test:          inqueueNoProgress(),
			reclaimResult: api.ReclaimFailed,
			wantPhase:     scheduling.PodGroupPending,
			wantInq:       v1.ConditionFalse,
			wantDequeued:  dequeue.DequeuedReasonReclaimFailed,
		},
		{
			name:          "reclaim found no victims: ordinary waiting, untouched",
			test:          inqueueNoProgress(),
			reclaimResult: api.ReclaimNoVictims,
			wantPhase:     scheduling.PodGroupInqueue,
		},
		{
			name:          "reclaim found no victims for a group tagged AncestorCapReclaim: dequeued at once",
			test:          inqueueNoProgress(pgCond(api.PodGroupInqueueType, v1.ConditionTrue, api.PodGroupInqueueReasonAncestorCapReclaim, time.Second)),
			reclaimResult: api.ReclaimNoVictims,
			wantPhase:     scheduling.PodGroupPending,
			wantInq:       v1.ConditionFalse,
			wantDequeued:  dequeue.DequeuedReasonReclaimFailed,
		},
		{
			name:          "reclaim not attempted and no timeout configured: untouched even with a stale stamp",
			test:          inqueueNoProgress(inqueueSince(time.Hour)),
			reclaimResult: api.ReclaimNotAttempted,
			wantPhase:     scheduling.PodGroupInqueue,
			wantInq:       v1.ConditionTrue,
		},
		{
			name:          "reclaim succeeded this session: untouched",
			test:          inqueueNoProgress(),
			reclaimResult: api.ReclaimSucceeded,
			wantPhase:     scheduling.PodGroupInqueue,
		},
		// --- timeout lane (opt-in)
		{
			name:         "timeout: stamp older than the timeout and no progress: moved back to Pending",
			test:         inqueueNoProgress(inqueueSince(2 * time.Minute)),
			config:       timeoutConfig,
			wantPhase:    scheduling.PodGroupPending,
			wantInq:      v1.ConditionFalse,
			wantDequeued: dequeue.DequeuedReasonInqueueTimeout,
		},
		{
			name:      "timeout: stamp younger than the timeout: untouched",
			test:      inqueueNoProgress(inqueueSince(10 * time.Second)),
			config:    timeoutConfig,
			wantPhase: scheduling.PodGroupInqueue,
			wantInq:   v1.ConditionTrue,
		},
		{
			name:      "timeout: no stamp yet: stamped and left Inqueue",
			test:      inqueueNoProgress(),
			config:    timeoutConfig,
			wantPhase: scheduling.PodGroupInqueue,
			wantInq:   v1.ConditionTrue,
		},
		{
			name: "a running task counts as progress: untouched even with a stale stamp and a failed verdict",
			test: uthelper.TestCommonStruct{
				Pods:      []*v1.Pod{running, pending},
				Nodes:     []*v1.Node{node},
				PodGroups: []*schedulingv1beta1.PodGroup{pg1(schedulingv1beta1.PodGroupInqueue, 2, minRes, inqueueSince(time.Hour))},
				Queues:    []*schedulingv1beta1.Queue{q1},
			},
			config:        timeoutConfig,
			reclaimResult: api.ReclaimFailed,
			wantPhase:     scheduling.PodGroupInqueue,
			wantInq:       v1.ConditionTrue,
		},
		{
			name:          "no minResources: nothing is reserved, untouched",
			test:          fixture(pg1(schedulingv1beta1.PodGroupInqueue, 1, nil, inqueueSince(time.Hour)), pending),
			config:        timeoutConfig,
			reclaimResult: api.ReclaimFailed,
			wantPhase:     scheduling.PodGroupInqueue,
			wantInq:       v1.ConditionTrue,
		},
		// --- enqueue backoff
		{
			name:         "enqueue honors the backoff: dequeued recently stays Pending",
			test:         fixture(pg1(schedulingv1beta1.PodGroupPending, 1, minRes, dequeuedAgo(dequeue.DequeuedReasonReclaimFailed, 10*time.Second)), pending),
			actions:      []framework.Action{enqueue.New()},
			wantPhase:    scheduling.PodGroupPending,
			wantDequeued: dequeue.DequeuedReasonReclaimFailed,
		},
		{
			name:         "enqueue after the backoff: admitted again",
			test:         fixture(pg1(schedulingv1beta1.PodGroupPending, 1, minRes, dequeuedAgo(dequeue.DequeuedReasonReclaimFailed, 2*time.Minute)), pending),
			actions:      []framework.Action{enqueue.New()},
			wantPhase:    scheduling.PodGroupInqueue,
			wantDequeued: dequeue.DequeuedReasonReclaimFailed,
		},
		{
			name: "re-admitted group is stamped afresh by the next timeout pass",
			test: fixture(pg1(schedulingv1beta1.PodGroupPending, 1, minRes,
				pgCond(api.PodGroupInqueueType, v1.ConditionFalse, dequeue.DequeuedReasonInqueueTimeout, 2*time.Minute),
				dequeuedAgo(dequeue.DequeuedReasonInqueueTimeout, 2*time.Minute)), pending),
			config:       timeoutConfig,
			actions:      []framework.Action{enqueue.New(), dequeue.New()},
			wantPhase:    scheduling.PodGroupInqueue,
			wantInq:      v1.ConditionTrue,
			wantDequeued: dequeue.DequeuedReasonInqueueTimeout,
		},
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			test := c.test
			test.Name = c.name
			test.Plugins = map[string]framework.PluginBuilder{}
			test.ExpectStatus = map[api.JobID]scheduling.PodGroupPhase{"ns1/pg1": c.wantPhase}
			ssn := test.RegisterSession(nil, c.config)
			defer test.Close()
			ssn.Jobs["ns1/pg1"].ReclaimResult = c.reclaimResult
			actions := c.actions
			if actions == nil {
				actions = []framework.Action{dequeue.New()}
			}
			test.Run(actions)
			if err := test.CheckAll(i); err != nil {
				t.Fatal(err)
			}
			job := ssn.Jobs["ns1/pg1"]
			inq := findCond(job, api.PodGroupInqueueType)
			switch {
			case c.wantInq == "" && inq != nil:
				t.Fatalf("unexpected Inqueue condition %+v", *inq)
			case c.wantInq != "" && inq == nil:
				t.Fatalf("missing Inqueue condition, want status %s", c.wantInq)
			case c.wantInq != "" && inq.Status != c.wantInq:
				t.Fatalf("Inqueue condition status: want %s, got %s", c.wantInq, inq.Status)
			}
			deq := findCond(job, api.PodGroupDequeuedType)
			switch {
			case c.wantDequeued == "" && deq != nil:
				t.Fatalf("unexpected Dequeued condition %+v", *deq)
			case c.wantDequeued != "" && deq == nil:
				t.Fatalf("missing Dequeued condition, want reason %s", c.wantDequeued)
			case c.wantDequeued != "" && deq.Reason != c.wantDequeued:
				t.Fatalf("Dequeued condition reason: want %s, got %s", c.wantDequeued, deq.Reason)
			}
		})
	}
}

func TestParseArguments(t *testing.T) {
	cases := []struct {
		name        string
		config      []conf.Configuration
		wantBackoff time.Duration
		wantTimeout time.Duration
	}{
		{name: "defaults: backoff 1m, timeout disabled", config: nil, wantBackoff: dequeue.DefaultEnqueueBackoff, wantTimeout: 0},
		{name: "timeout only", config: dequeueConf(map[string]interface{}{dequeue.InqueueTimeoutKey: "5m"}), wantBackoff: dequeue.DefaultEnqueueBackoff, wantTimeout: 5 * time.Minute},
		{name: "both set", config: dequeueConf(map[string]interface{}{dequeue.InqueueTimeoutKey: "5m", dequeue.EnqueueBackoffKey: "30s"}), wantBackoff: 30 * time.Second, wantTimeout: 5 * time.Minute},
		{name: "invalid falls back", config: dequeueConf(map[string]interface{}{dequeue.InqueueTimeoutKey: "soon", dequeue.EnqueueBackoffKey: "-1m"}), wantBackoff: dequeue.DefaultEnqueueBackoff, wantTimeout: 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			backoff, timeout := dequeue.ParseArguments(c.config)
			if backoff != c.wantBackoff || timeout != c.wantTimeout {
				t.Fatalf("want (backoff %s, timeout %s), got (%s, %s)", c.wantBackoff, c.wantTimeout, backoff, timeout)
			}
		})
	}
}

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

func condition(condType scheduling.PodGroupConditionType, status v1.ConditionStatus, age time.Duration) schedulingv1beta1.PodGroupCondition {
	return schedulingv1beta1.PodGroupCondition{
		Type:               schedulingv1beta1.PodGroupConditionType(condType),
		Status:             status,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-age)),
	}
}

func podGroup(phase schedulingv1beta1.PodGroupPhase, minMember int32, minResources v1.ResourceList, conds ...schedulingv1beta1.PodGroupCondition) *schedulingv1beta1.PodGroup {
	var pg *schedulingv1beta1.PodGroup
	if minResources == nil {
		pg = util.BuildPodGroup("pg1", "ns1", "q1", minMember, nil, phase)
	} else {
		pg = util.BuildPodGroupWithMinResources("pg1", "ns1", "q1", minMember, nil, minResources, phase)
	}
	pg.Status.Conditions = conds
	return pg
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
	config := []conf.Configuration{{Name: dequeue.Dequeue, Arguments: map[string]interface{}{dequeue.InqueueTimeoutKey: "1m"}}}
	queue := util.BuildQueue("q1", 1, api.BuildResourceList("8", "8Gi"))
	node := util.BuildNode("n1", api.BuildResourceList("8", "8Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{})
	pending := util.BuildPod("ns1", "p1", "", v1.PodPending, api.BuildResourceList("1", "1Gi"), "pg1", map[string]string{}, map[string]string{})
	running := util.BuildPod("ns1", "p0", "n1", v1.PodRunning, api.BuildResourceList("1", "1Gi"), "pg1", map[string]string{}, map[string]string{})
	minRes := api.BuildResourceList("1", "1Gi")

	cases := []struct {
		name       string
		test       uthelper.TestCommonStruct
		actions    []framework.Action
		wantPhase  scheduling.PodGroupPhase
		wantInq    *v1.ConditionStatus // expected Inqueue condition status, nil = must be absent
		wantDequed bool
	}{
		{
			name: "Inqueue stamp older than the timeout and no progress: moved back to Pending",
			test: uthelper.TestCommonStruct{
				Pods:      []*v1.Pod{pending},
				PodGroups: []*schedulingv1beta1.PodGroup{podGroup(schedulingv1beta1.PodGroupInqueue, 1, minRes, condition(api.PodGroupInqueueType, v1.ConditionTrue, 2*time.Minute))},
				Queues:    []*schedulingv1beta1.Queue{queue},
			},
			actions:    []framework.Action{dequeue.New()},
			wantPhase:  scheduling.PodGroupPending,
			wantInq:    ptr(v1.ConditionFalse),
			wantDequed: true,
		},
		{
			name: "Inqueue stamp younger than the timeout: untouched",
			test: uthelper.TestCommonStruct{
				Pods:      []*v1.Pod{pending},
				PodGroups: []*schedulingv1beta1.PodGroup{podGroup(schedulingv1beta1.PodGroupInqueue, 1, minRes, condition(api.PodGroupInqueueType, v1.ConditionTrue, 10*time.Second))},
				Queues:    []*schedulingv1beta1.Queue{queue},
			},
			actions:   []framework.Action{dequeue.New()},
			wantPhase: scheduling.PodGroupInqueue,
			wantInq:   ptr(v1.ConditionTrue),
		},
		{
			name: "no stamp yet: stamped and left Inqueue",
			test: uthelper.TestCommonStruct{
				Pods:      []*v1.Pod{pending},
				PodGroups: []*schedulingv1beta1.PodGroup{podGroup(schedulingv1beta1.PodGroupInqueue, 1, minRes)},
				Queues:    []*schedulingv1beta1.Queue{queue},
			},
			actions:   []framework.Action{dequeue.New()},
			wantPhase: scheduling.PodGroupInqueue,
			wantInq:   ptr(v1.ConditionTrue),
		},
		{
			name: "a running task counts as progress: untouched even with a stale stamp",
			test: uthelper.TestCommonStruct{
				Pods:      []*v1.Pod{running, pending},
				Nodes:     []*v1.Node{node},
				PodGroups: []*schedulingv1beta1.PodGroup{podGroup(schedulingv1beta1.PodGroupInqueue, 2, minRes, condition(api.PodGroupInqueueType, v1.ConditionTrue, time.Hour))},
				Queues:    []*schedulingv1beta1.Queue{queue},
			},
			actions:   []framework.Action{dequeue.New()},
			wantPhase: scheduling.PodGroupInqueue,
			wantInq:   ptr(v1.ConditionTrue),
		},
		{
			name: "no minResources: nothing is reserved, untouched",
			test: uthelper.TestCommonStruct{
				Pods:      []*v1.Pod{pending},
				PodGroups: []*schedulingv1beta1.PodGroup{podGroup(schedulingv1beta1.PodGroupInqueue, 1, nil, condition(api.PodGroupInqueueType, v1.ConditionTrue, time.Hour))},
				Queues:    []*schedulingv1beta1.Queue{queue},
			},
			actions:   []framework.Action{dequeue.New()},
			wantPhase: scheduling.PodGroupInqueue,
			wantInq:   ptr(v1.ConditionTrue),
		},
		{
			name: "enqueue honors the backoff: dequeued recently stays Pending",
			test: uthelper.TestCommonStruct{
				Pods:      []*v1.Pod{pending},
				PodGroups: []*schedulingv1beta1.PodGroup{podGroup(schedulingv1beta1.PodGroupPending, 1, minRes, condition(api.PodGroupDequeuedType, v1.ConditionTrue, 10*time.Second))},
				Queues:    []*schedulingv1beta1.Queue{queue},
			},
			actions:    []framework.Action{enqueue.New()},
			wantPhase:  scheduling.PodGroupPending,
			wantDequed: true,
		},
		{
			name: "enqueue after the backoff: admitted again",
			test: uthelper.TestCommonStruct{
				Pods:      []*v1.Pod{pending},
				PodGroups: []*schedulingv1beta1.PodGroup{podGroup(schedulingv1beta1.PodGroupPending, 1, minRes, condition(api.PodGroupDequeuedType, v1.ConditionTrue, 2*time.Minute))},
				Queues:    []*schedulingv1beta1.Queue{queue},
			},
			actions:    []framework.Action{enqueue.New()},
			wantPhase:  scheduling.PodGroupInqueue,
			wantDequed: true,
		},
		{
			name: "re-admitted group is stamped afresh by the next dequeue pass",
			test: uthelper.TestCommonStruct{
				Pods: []*v1.Pod{pending},
				PodGroups: []*schedulingv1beta1.PodGroup{podGroup(schedulingv1beta1.PodGroupPending, 1, minRes,
					condition(api.PodGroupInqueueType, v1.ConditionFalse, 2*time.Minute),
					condition(api.PodGroupDequeuedType, v1.ConditionTrue, 2*time.Minute))},
				Queues: []*schedulingv1beta1.Queue{queue},
			},
			actions:    []framework.Action{enqueue.New(), dequeue.New()},
			wantPhase:  scheduling.PodGroupInqueue,
			wantInq:    ptr(v1.ConditionTrue),
			wantDequed: true,
		},
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			test := c.test
			test.Name = c.name
			test.Plugins = map[string]framework.PluginBuilder{}
			test.ExpectStatus = map[api.JobID]scheduling.PodGroupPhase{"ns1/pg1": c.wantPhase}
			ssn := test.RegisterSession(nil, config)
			defer test.Close()
			test.Run(c.actions)
			if err := test.CheckAll(i); err != nil {
				t.Fatal(err)
			}
			job := ssn.Jobs["ns1/pg1"]
			inq := findCond(job, api.PodGroupInqueueType)
			switch {
			case c.wantInq == nil && inq != nil:
				t.Fatalf("unexpected Inqueue condition %+v", *inq)
			case c.wantInq != nil && inq == nil:
				t.Fatalf("missing Inqueue condition, want status %s", *c.wantInq)
			case c.wantInq != nil && inq.Status != *c.wantInq:
				t.Fatalf("Inqueue condition status: want %s, got %s", *c.wantInq, inq.Status)
			}
			if deq := findCond(job, api.PodGroupDequeuedType); (deq != nil) != c.wantDequed {
				t.Fatalf("Dequeued condition present=%v, want %v", deq != nil, c.wantDequed)
			}
		})
	}
}

func ptr(s v1.ConditionStatus) *v1.ConditionStatus { return &s }

func TestParseArguments(t *testing.T) {
	cases := []struct {
		name        string
		config      []conf.Configuration
		wantTimeout time.Duration
		wantBackoff time.Duration
	}{
		{name: "defaults", config: nil, wantTimeout: dequeue.DefaultInqueueTimeout, wantBackoff: dequeue.DefaultInqueueTimeout},
		{name: "timeout only sets both", config: []conf.Configuration{{Name: dequeue.Dequeue, Arguments: map[string]interface{}{dequeue.InqueueTimeoutKey: "5m"}}}, wantTimeout: 5 * time.Minute, wantBackoff: 5 * time.Minute},
		{name: "both set", config: []conf.Configuration{{Name: dequeue.Dequeue, Arguments: map[string]interface{}{dequeue.InqueueTimeoutKey: "5m", dequeue.EnqueueBackoffKey: "30s"}}}, wantTimeout: 5 * time.Minute, wantBackoff: 30 * time.Second},
		{name: "invalid falls back", config: []conf.Configuration{{Name: dequeue.Dequeue, Arguments: map[string]interface{}{dequeue.InqueueTimeoutKey: "soon", dequeue.EnqueueBackoffKey: "-1m"}}}, wantTimeout: dequeue.DefaultInqueueTimeout, wantBackoff: dequeue.DefaultInqueueTimeout},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			timeout, backoff := dequeue.ParseArguments(c.config)
			if timeout != c.wantTimeout || backoff != c.wantBackoff {
				t.Fatalf("want (%s, %s), got (%s, %s)", c.wantTimeout, c.wantBackoff, timeout, backoff)
			}
		})
	}
}

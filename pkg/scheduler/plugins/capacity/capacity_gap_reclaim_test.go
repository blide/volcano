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

package capacity

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/actions/enqueue"
	"volcano.sh/volcano/pkg/scheduler/actions/reclaim"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// Test_capacityPlugin_ReclaimOnAncestorCapacityStarvation reproduces
// https://github.com/volcano-sh/volcano/issues/4817 in its "free nodes" form:
//
// This is the YARN capacity-scheduler shape translated field-for-field
// (capacity -> deserved, maximum-capacity -> capability):
//
//	root
//	└── tenant        capability 4c            (the parent cap)
//	    ├── t-active  deserved 2c, running 4c  (over deserved, borrowing)
//	    └── t-standby deserved 2c, asks 2c     (under deserved)
//	node n1: 8c, 4c free
//
// t-standby is entitled to 2c and the parent has reclaimable over-usage in
// t-active, but the ask is blocked by the parent cap, not by the node. Reclaim
// must evict one 2c pod from t-active and pipeline the standby pod. On master
// the reclaim loop stops as soon as the ask physically fits on n1 (it does,
// n1 has 4c free), evicts nothing, and the ask starves forever because
// allocate refuses it on the ancestor capability check every session.
func Test_capacityPlugin_ReclaimOnAncestorCapacityStarvation(t *testing.T) {
	plugins := map[string]framework.PluginBuilder{
		PluginName:            New,
		predicates.PluginName: predicates.New,
		gang.PluginName:       gang.New,
	}
	trueValue := true
	actions := []framework.Action{enqueue.New(), reclaim.New(), allocate.New()}

	n1 := util.BuildNode("n1", api.BuildResourceList("8", "8Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{})

	root := buildQueueWithParents("root", "", nil, nil)
	tenant := buildQueueWithParents("tenant", "root", api.BuildResourceList("4", "4Gi"), api.BuildResourceList("4", "4Gi"))
	active := buildQueueWithParents("t-active", "tenant", api.BuildResourceList("2", "2Gi"), api.BuildResourceList("4", "4Gi"))
	standby := buildQueueWithParents("t-standby", "tenant", api.BuildResourceList("2", "2Gi"), api.BuildResourceList("4", "4Gi"))

	pgActive := util.BuildPodGroup("pg-active", "ns1", "t-active", 1, nil, schedulingv1beta1.PodGroupRunning)
	pgStandby := util.BuildPodGroup("pg-standby", "ns1", "t-standby", 1, nil, schedulingv1beta1.PodGroupInqueue)

	// two 2c executors in t-active: 4c allocated against deserved 2c.
	// exec-2 is marked non-preemptable so the only admissible victim is exec-1 (and so the
	// test also checks that the fix does not widen victim selection to protected pods).
	exec1 := util.BuildPod("ns1", "exec-1", "n1", corev1.PodRunning, api.BuildResourceList("2", "2Gi"), "pg-active",
		map[string]string{schedulingv1beta1.PodPreemptable: "true"}, map[string]string{})
	exec2 := util.BuildPod("ns1", "exec-2", "n1", corev1.PodRunning, api.BuildResourceList("2", "2Gi"), "pg-active",
		map[string]string{schedulingv1beta1.PodPreemptable: "false"}, map[string]string{})
	// the standby activation ask
	ask := util.BuildPod("ns1", "standby-driver", "", corev1.PodPending, api.BuildResourceList("2", "2Gi"), "pg-standby",
		map[string]string{}, map[string]string{})

	tiers := []conf.Tier{{
		Plugins: []conf.PluginOption{
			{
				Name:               PluginName,
				EnabledAllocatable: &trueValue,
				EnablePreemptive:   &trueValue,
				EnabledReclaimable: &trueValue,
				EnabledQueueOrder:  &trueValue,
				EnabledHierarchy:   &trueValue,
				EnabledJobEnqueued: &trueValue,
			},
			{Name: predicates.PluginName, EnabledPredicate: &trueValue},
			{Name: gang.PluginName, EnabledJobStarving: &trueValue},
		},
	}}

	test := uthelper.TestCommonStruct{
		Name:      "under-deserved child blocked by ancestor capability reclaims over-deserved sibling despite free node space",
		Plugins:   plugins,
		Pods:      []*corev1.Pod{exec1, exec2, ask},
		Nodes:     []*corev1.Node{n1},
		PodGroups: []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
		Queues:    []*schedulingv1beta1.Queue{root, tenant, active, standby},
		ExpectPipeLined: map[string][]string{
			"ns1/pg-standby": {"n1"},
		},
		ExpectEvictNum: 1,
		ExpectEvicted:  []string{"ns1/exec-1"},
	}

	test.RegisterSession(tiers, nil)
	defer test.Close()
	test.Run(actions)
	if err := test.CheckAll(0); err != nil {
		t.Fatal(err)
	}
}

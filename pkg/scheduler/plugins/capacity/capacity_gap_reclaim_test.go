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
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/actions/enqueue"
	"volcano.sh/volcano/pkg/scheduler/actions/reclaim"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/plugins/priority"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// This file exercises reclaim in hierarchical capacity trees where the ask is blocked by an
// ancestor queue's capability rather than by node capacity (volcano-sh/volcano#4817), plus the
// surrounding victim-selection, gang and preemptionPolicy behavior an active/standby failover
// design relies on.
//
// Conventions shared by all cases:
//
//   - every pod asks cpu == memory (2c means cpu=2, memory=2Gi); gapNode("n1", "8") is an 8c/8Gi node
//   - running pods live on n1; pending pods have no node
//   - a PodGroup built with gapRunningPG is Running, one built with gapAskPG is Inqueue; minMember 1
//   - preemptable=false puts volcano.sh/preemptable=false on the pod, which keeps it out of the
//     reclaimee list entirely; it is also used to pin an otherwise interchangeable victim so the
//     exact-name assertion in uthelper is meaningful
//
// The default tree is
//
//	root
//	└── tenant        deserved 4c, capability 4c
//	    ├── active    deserved 2c, capability 4c
//	    └── standby   deserved 2c, capability 4c
//
// A case either states a *requirement* of the failover design or *documents* current behavior so
// that an upgrade cannot change it silently. A case with a non-empty skip reason describes behavior
// that is wanted but not implemented yet (EXPECT-FAIL); it is skipped rather than made to pass by
// changing the scheduler.

const gapNS = "ns1"

type gapCase struct {
	uthelper.TestCommonStruct
	// ancestorReclaimLevel for the capacity plugin (0 = siblings only).
	level int
	// add the priority plugin with job and task order enabled.
	priority bool
	// enable gang's ReclaimableFn (the minAvailable veto). Off by default, as in the baseline tiers.
	gangReclaim bool
	// take JobStarving from the priority plugin (any unplaced task) instead of gang
	// (ready+pipelined < minAvailable). Implies priority.
	starvingByPending bool
	// non-empty: t.Skip with this reason (EXPECT-FAIL).
	skip string
}

func cpuMem(c string) corev1.ResourceList {
	if c == "" {
		return nil
	}
	return api.BuildResourceList(c, c+"Gi")
}

func gapNode(name, c string) *corev1.Node {
	return util.BuildNode(name, api.BuildResourceList(c, c+"Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{})
}

func gapQueue(name, parent, deserved, capability string) *schedulingv1beta1.Queue {
	return buildQueueWithParents(name, parent, cpuMem(deserved), cpuMem(capability))
}

func gapRunningPG(name, queue string) *schedulingv1beta1.PodGroup {
	return util.BuildPodGroup(name, gapNS, queue, 1, nil, schedulingv1beta1.PodGroupRunning)
}

func gapAskPG(name, queue string) *schedulingv1beta1.PodGroup {
	return util.BuildPodGroup(name, gapNS, queue, 1, nil, schedulingv1beta1.PodGroupInqueue)
}

func preemptableLabel(preemptable bool) map[string]string {
	return map[string]string{schedulingv1beta1.PodPreemptable: strconv.FormatBool(preemptable)}
}

func gapRunningPod(name, pg, c string, preemptable bool) *corev1.Pod {
	return util.BuildPod(gapNS, name, "n1", corev1.PodRunning, cpuMem(c), pg, preemptableLabel(preemptable), map[string]string{})
}

func gapPendingPod(name, pg, c string) *corev1.Pod {
	return util.BuildPod(gapNS, name, "", corev1.PodPending, cpuMem(c), pg, map[string]string{}, map[string]string{})
}

// gapTree describes the default root → tenant → {active, standby} tree; "" leaves a field unset.
type gapTree struct {
	tenantDeserved, tenantCap   string
	activeDeserved, activeCap   string
	standbyDeserved, standbyCap string
}

var defaultGapTree = gapTree{"4", "4", "2", "4", "2", "4"}

func (tr gapTree) queues() []*schedulingv1beta1.Queue {
	return []*schedulingv1beta1.Queue{
		gapQueue("root", "", "", ""),
		gapQueue("tenant", "root", tr.tenantDeserved, tr.tenantCap),
		gapQueue("active", "tenant", tr.activeDeserved, tr.activeCap),
		gapQueue("standby", "tenant", tr.standbyDeserved, tr.standbyCap),
	}
}

func gapTiers(level int, withPriority, gangReclaim, starvingByPending bool) []conf.Tier {
	trueValue := true
	withPriority = withPriority || starvingByPending
	capacityOpt := conf.PluginOption{
		Name:               PluginName,
		EnabledAllocatable: &trueValue,
		EnablePreemptive:   &trueValue,
		EnabledReclaimable: &trueValue,
		EnabledQueueOrder:  &trueValue,
		EnabledHierarchy:   &trueValue,
		EnabledJobEnqueued: &trueValue,
	}
	if level > 0 {
		capacityOpt.Arguments = framework.Arguments{ancestorReclaimLevelKey: level}
	}
	gangOpt := conf.PluginOption{Name: gang.PluginName, EnabledJobStarving: &trueValue}
	if gangReclaim {
		gangOpt.EnabledReclaimable = &trueValue
	}
	if starvingByPending {
		// ssn.JobStarving ANDs every plugin with the flag on, so gang's must be off for the
		// priority plugin's definition to take effect.
		gangOpt.EnabledJobStarving = nil
	}
	plugins := []conf.PluginOption{
		capacityOpt,
		{Name: predicates.PluginName, EnabledPredicate: &trueValue},
		gangOpt,
	}
	if withPriority {
		prioOpt := conf.PluginOption{
			Name:             priority.PluginName,
			EnabledJobOrder:  &trueValue,
			EnabledTaskOrder: &trueValue,
		}
		if starvingByPending {
			prioOpt.EnabledJobStarving = &trueValue
		}
		plugins = append(plugins, prioOpt)
	}
	return []conf.Tier{{Plugins: plugins}}
}

func Test_capacityPlugin_ReclaimOnAncestorCapacityStarvation(t *testing.T) {
	plugins := map[string]framework.PluginBuilder{
		PluginName:            New,
		predicates.PluginName: predicates.New,
		gang.PluginName:       gang.New,
		priority.PluginName:   priority.New,
	}
	actions := []framework.Action{enqueue.New(), reclaim.New(), allocate.New()}

	falseValue := false
	n1 := gapNode("n1", "8")
	pgActive := gapRunningPG("pg-active", "active")
	pgStandby := gapAskPG("pg-standby", "standby")
	// The A1 fixture, reused by several cases: active borrows 4c against deserved 2c with one
	// protected pod, standby asks 2c, the node has 4c free, the tenant cap is full.
	exec1 := gapRunningPod("exec-1", "pg-active", "2", true)
	exec2Protected := gapRunningPod("exec-2", "pg-active", "2", false)
	ask := gapPendingPod("standby-driver", "pg-standby", "2")

	// E-section tree: two tenants under root.
	//
	//	root
	//	├── tenant-a  deserved 4c, cap 4c   ├── a-active  (2c, 4c)   ├── a-standby (2c, 4c)
	//	└── tenant-b  deserved 2c, cap 4c   └── b-leaf    (2c, 4c)
	//
	// tenant-b's deserved is 2c (not 4c) so that b-leaf's 4c is over-deserved at the tenant level
	// too; otherwise the ancestorReclaimLevel 1 check would reject b-leaf victims before the stop
	// condition is ever consulted.
	twoTenantQueues := func(tenantACap string) []*schedulingv1beta1.Queue {
		return []*schedulingv1beta1.Queue{
			gapQueue("root", "", "", ""),
			gapQueue("tenant-a", "root", "4", tenantACap),
			gapQueue("a-active", "tenant-a", "2", "4"),
			gapQueue("a-standby", "tenant-a", "2", "4"),
			gapQueue("tenant-b", "root", "2", "4"),
			gapQueue("b-leaf", "tenant-b", "2", "4"),
		}
	}
	pgAActive := gapRunningPG("pg-a-active", "a-active")
	pgAStandby := gapAskPG("pg-a-standby", "a-standby")
	pgBLeaf := gapRunningPG("pg-b-leaf", "b-leaf")
	aAsk := gapPendingPod("a-standby-driver", "pg-a-standby", "2")

	// B7: the victim queue opts out of reclaim.
	activeNotReclaimable := gapQueue("active", "tenant", "2", "4")
	activeNotReclaimable.Spec.Reclaimable = &falseValue

	// F: infra queue with a guarantee.
	infraWithGuarantee := func(deserved string) *schedulingv1beta1.Queue {
		q := gapQueue("infra", "tenant", deserved, "4")
		q.Spec.Guarantee = schedulingv1beta1.Guarantee{Resource: cpuMem("2")}
		return q
	}

	// C1/C2: preemptionPolicy Never on the asker and on a victim.
	askNever := util.BuildPodWithPreemptionPolicy(gapNS, "standby-driver", "", corev1.PodPending, cpuMem("2"), "pg-standby",
		map[string]string{}, map[string]string{}, corev1.PreemptNever)
	exec1Never := util.BuildPodWithPreemptionPolicy(gapNS, "exec-1", "n1", corev1.PodRunning, cpuMem("2"), "pg-active",
		preemptableLabel(true), map[string]string{}, corev1.PreemptNever)

	// B: PriorityClass ladder.
	prioHigh, prioLow := int32(1000), int32(100)
	pcHigh := util.BuildPriorityClass("high", prioHigh)
	pcLow := util.BuildPriorityClass("low", prioLow)

	// G: PodGroups that carry minResources are gated at enqueue, before reclaim runs.
	pgStandbyMinRes := util.BuildPodGroupWithMinResources("pg-standby", gapNS, "standby", 1, nil, cpuMem("2"), schedulingv1beta1.PodGroupPending)
	pgActiveMinRes := util.BuildPodGroupWithMinResources("pg-active", gapNS, "active", 1, nil, cpuMem("4"), schedulingv1beta1.PodGroupRunning)

	cases := []gapCase{
		// ---------------------------------------------------------------- A. capacity-gap reclaim
		{
			// Requirement. The fix itself: node has room, tenant cap is full, standby is under
			// deserved. Reclaim must evict the one admissible over-deserved pod and pipeline the ask.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:            "A1: under-deserved child blocked by ancestor capability reclaims over-deserved sibling despite free node space",
				Plugins:         plugins,
				Pods:            []*corev1.Pod{exec1, exec2Protected, ask},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          defaultGapTree.queues(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-1"},
			},
		},
		{
			// Requirement. With room in the hierarchy (tenant cap 8c) the added Allocatable
			// conjunct is inert: zero evictions. Reclaim still runs first and pipelines the ask on
			// n1 with no eviction (the victim list is non-empty, so reclaimForTask reaches
			// Pipeline); allocate in the same session only looks at Pending tasks, so the bind
			// happens in the next session. Hence pipelined, not bound, here.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:            "A2: no eviction when the ancestor capability has room",
				Plugins:         plugins,
				Pods:            []*corev1.Pod{exec1, gapRunningPod("exec-2", "pg-active", "2", true), ask},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          gapTree{"4", "8", "2", "4", "2", "4"}.queues(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  0,
			},
		},
		{
			// Requirement (over-eviction guard). The ask is 2c, victims are 1c each: the loop must
			// stop after exactly two evictions, as soon as the tenant has room, and not drain the
			// queue. exec-4 is protected so the admissible set (3) is larger than what is needed (2).
			// Within one job the victims queue is the reverse of the default task order
			// (helpers.CompareTask: pod index, then creation time, then UID), so the highest
			// indexes go first: exec-3, then exec-2. exec-1 must survive.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "A3: ask larger than a single victim evicts exactly the gap, not every admissible pod",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("exec-1", "pg-active", "1", true),
					gapRunningPod("exec-2", "pg-active", "1", true),
					gapRunningPod("exec-3", "pg-active", "1", true),
					gapRunningPod("exec-4", "pg-active", "1", false),
					ask,
				},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          defaultGapTree.queues(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  2,
				ExpectEvicted:   []string{"ns1/exec-2", "ns1/exec-3"},
			},
		},
		{
			// Requirement. Full failover: standby deserved 4c, active 0c, the 4c ask needs both
			// of active's pods to go.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "A4: ask that needs every victim evicts them all",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					exec1,
					gapRunningPod("exec-2", "pg-active", "2", true),
					gapPendingPod("standby-driver", "pg-standby", "4"),
				},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          gapTree{"4", "4", "0", "4", "4", "4"}.queues(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  2,
				ExpectEvicted:   []string{"ns1/exec-1", "ns1/exec-2"},
			},
		},
		{
			// Requirement. Evicting the only admissible pod (exec-1) leaves the tenant at 2c, and a
			// 4c ask still does not fit the 4c cap. The per-node statement must be discarded so
			// that no partial, useless eviction is committed.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:           "A5: no eviction is committed when the admissible victims cannot free enough",
				Plugins:        plugins,
				Pods:           []*corev1.Pod{exec1, exec2Protected, gapPendingPod("standby-driver", "pg-standby", "4")},
				Nodes:          []*corev1.Node{n1},
				PodGroups:      []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:         gapTree{"4", "4", "0", "4", "4", "4"}.queues(),
				ExpectEvictNum: 0,
			},
		},
		{
			// Documenting. Flat tree, physical starvation: classic reclaim, unchanged by the fix.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:      "A6: flat tree with a full node still reclaims one pod",
				Plugins:   plugins,
				Pods:      []*corev1.Pod{exec1, exec2Protected, ask},
				Nodes:     []*corev1.Node{gapNode("n1", "4")},
				PodGroups: []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues: []*schedulingv1beta1.Queue{
					gapQueue("root", "", "", ""),
					gapQueue("active", "root", "2", "8"),
					gapQueue("standby", "root", "2", "8"),
				},
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-1"},
			},
		},
		{
			// Requirement (bronze tier). Leaf caps (6c) exceed the parent cap (4c); the leaf's
			// effective capability is clamped to the parent's and the gap is still reclaimed.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:            "A7: oversubscribed leaf capabilities under a parent cap reclaim like A1",
				Plugins:         plugins,
				Pods:            []*corev1.Pod{exec1, exec2Protected, ask},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          gapTree{"4", "4", "2", "6", "2", "6"}.queues(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-1"},
			},
		},

		// ---------------------------------------------------------------- B. victim selection
		{
			// Requirement. Within one job the victims queue pops the lowest task priority first:
			// the executor goes before the driver.
			priority: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "B1: within one job the lower-priority executor is evicted before the driver",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					util.BuildPodWithPriority(gapNS, "driver-a", "n1", corev1.PodRunning, cpuMem("2"), "pg-active", preemptableLabel(true), map[string]string{}, &prioHigh),
					util.BuildPodWithPriority(gapNS, "exec-a", "n1", corev1.PodRunning, cpuMem("2"), "pg-active", preemptableLabel(true), map[string]string{}, &prioLow),
					ask,
				},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          defaultGapTree.queues(),
				PriClass:        []*schedulingv1.PriorityClass{pcHigh, pcLow},
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-a"},
			},
		},
		{
			// Documenting. Across jobs the victims queue orders by JobOrderFn: the job with the
			// lower PriorityClass is the first victim.
			priority: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "B2: across jobs the lower-priority job is evicted first",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					util.BuildPodWithPriority(gapNS, "driver-a", "n1", corev1.PodRunning, cpuMem("2"), "pg-a1", preemptableLabel(true), map[string]string{}, &prioHigh),
					util.BuildPodWithPriority(gapNS, "exec-a", "n1", corev1.PodRunning, cpuMem("2"), "pg-a2", preemptableLabel(true), map[string]string{}, &prioLow),
					ask,
				},
				Nodes: []*corev1.Node{n1},
				PodGroups: []*schedulingv1beta1.PodGroup{
					util.BuildPodGroupWithPrio("pg-a1", gapNS, "active", 1, nil, schedulingv1beta1.PodGroupRunning, "high"),
					util.BuildPodGroupWithPrio("pg-a2", gapNS, "active", 1, nil, schedulingv1beta1.PodGroupRunning, "low"),
					pgStandby,
				},
				Queues:          defaultGapTree.queues(),
				PriClass:        []*schedulingv1.PriorityClass{pcHigh, pcLow},
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-a"},
			},
		},
		{
			// Documenting. Reclaim does not compare victim and asker priority (the priority
			// plugin registers a PreemptableFn, not a ReclaimableFn), so a high-priority pod in an
			// over-deserved queue is still reclaimed by a low-priority ask. The priority plugin is
			// enabled here on purpose: this is the strongest form of the statement.
			priority: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "B3: victim priority above the asker's is not a fence for cross-queue reclaim",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					util.BuildPodWithPriority(gapNS, "exec-a", "n1", corev1.PodRunning, cpuMem("2"), "pg-active", preemptableLabel(true), map[string]string{}, &prioHigh),
					exec2Protected,
					util.BuildPodWithPriority(gapNS, "standby-driver", "", corev1.PodPending, cpuMem("2"), "pg-standby", map[string]string{}, map[string]string{}, &prioLow),
				},
				Nodes: []*corev1.Node{n1},
				PodGroups: []*schedulingv1beta1.PodGroup{
					util.BuildPodGroupWithPrio("pg-active", gapNS, "active", 1, nil, schedulingv1beta1.PodGroupRunning, "high"),
					util.BuildPodGroupWithPrio("pg-standby", gapNS, "standby", 1, nil, schedulingv1beta1.PodGroupInqueue, "low"),
				},
				Queues:          defaultGapTree.queues(),
				PriClass:        []*schedulingv1.PriorityClass{pcHigh, pcLow},
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-a"},
			},
		},
		{
			// Requirement. Non-preemptable pods are never victims, even when they are the only
			// candidates; the ask stays pending.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "B4: non-preemptable pods are never victims even when they are the only ones",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("exec-1", "pg-active", "2", false),
					exec2Protected,
					ask,
				},
				Nodes:          []*corev1.Node{n1},
				PodGroups:      []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:         defaultGapTree.queues(),
				ExpectEvictNum: 0,
			},
		},
		{
			// Requirement. A pending pod of the over-deserved queue (exec-3, the active side still
			// asking for more) is not a victim: only Running tasks on the node are reclaimees. The
			// fixture keeps the cap gap (active runs 4c) so reclaim is actually exercised.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "B5: pending pods of the over-deserved queue are never victims",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					exec1,
					exec2Protected,
					gapPendingPod("exec-3", "pg-active", "2"),
					ask,
				},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          defaultGapTree.queues(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-1"},
			},
		},
		{
			// Documenting. Reclaim is deserved-driven: an asker with deserved 0c is not
			// preemptive (PreemptiveFn), and a victim queue at its deserved has nothing to give.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "B6: a victim queue at or under its deserved is untouchable",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					exec1,
					gapRunningPod("exec-2", "pg-active", "2", true),
					ask,
				},
				Nodes:          []*corev1.Node{n1},
				PodGroups:      []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:         gapTree{"4", "4", "4", "4", "0", "4"}.queues(),
				ExpectEvictNum: 0,
			},
		},
		{
			// Documenting. spec.reclaimable=false on the victim queue removes its pods from the
			// reclaimee list.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:      "B7: reclaimable=false on the victim queue blocks reclaim",
				Plugins:   plugins,
				Pods:      []*corev1.Pod{exec1, exec2Protected, ask},
				Nodes:     []*corev1.Node{n1},
				PodGroups: []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues: []*schedulingv1beta1.Queue{
					gapQueue("root", "", "", ""),
					gapQueue("tenant", "root", "4", "4"),
					activeNotReclaimable,
					gapQueue("standby", "tenant", "2", "4"),
				},
				ExpectEvictNum: 0,
			},
		},

		// ---------------------------------------------------------------- C. preemptionPolicy and gang
		{
			// Documenting. preemptionPolicy Never on the asker disables reclaim for it.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:           "C1: preemptionPolicy Never on the asker disables reclaim for it",
				Plugins:        plugins,
				Pods:           []*corev1.Pod{exec1, exec2Protected, askNever},
				Nodes:          []*corev1.Node{n1},
				PodGroups:      []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:         defaultGapTree.queues(),
				ExpectEvictNum: 0,
			},
		},
		{
			// Documenting. preemptionPolicy Never on a victim does not protect it; only the
			// preemptable annotation does. This is why driver protection uses the annotation.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:            "C2: preemptionPolicy Never on the victim does not protect it",
				Plugins:         plugins,
				Pods:            []*corev1.Pod{exec1Never, exec2Protected, ask},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          defaultGapTree.queues(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-1"},
			},
		},
		{
			// Documenting. With gang's ReclaimableFn enabled, a job is never taken below its
			// minAvailable: minMember 2 with two running pods yields no victims.
			gangReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "C3: gang veto keeps the victim job at minAvailable",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					exec1,
					gapRunningPod("exec-2", "pg-active", "2", true),
					ask,
				},
				Nodes:          []*corev1.Node{n1},
				PodGroups:      []*schedulingv1beta1.PodGroup{util.BuildPodGroup("pg-active", gapNS, "active", 2, nil, schedulingv1beta1.PodGroupRunning), pgStandby},
				Queues:         defaultGapTree.queues(),
				ExpectEvictNum: 0,
			},
		},
		{
			// Requirement. Same as C3 with minMember 1: one of the two pods may go.
			gangReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:            "C4: gang with minMember 1 and two pods lets one go",
				Plugins:         plugins,
				Pods:            []*corev1.Pod{exec1, exec2Protected, ask},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          defaultGapTree.queues(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-1"},
			},
		},
		{
			// Documenting. The flip side of C4: with gang's veto on, a minMember 1 job always keeps
			// one pod, so a 4c ask that needs both of active's pods (A4) gets only one admissible
			// victim, cannot be satisfied, and nothing is committed. A design that relies on A4
			// must either leave gang's reclaimable off or split executors into their own PodGroups.
			gangReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "C5: gang veto on a minMember 1 job leaves the last pod, so an ask needing all pods evicts nothing",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					exec1,
					gapRunningPod("exec-2", "pg-active", "2", true),
					gapPendingPod("standby-driver", "pg-standby", "4"),
				},
				Nodes:          []*corev1.Node{n1},
				PodGroups:      []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:         gapTree{"4", "4", "0", "4", "4", "4"}.queues(),
				ExpectEvictNum: 0,
			},
		},

		// ---------------------------------------------------------------- D. multi-asker and incremental activation
		{
			// Requirement. Two standby askers in one session each reclaim their own gap.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "D1: two standby askers in one session each reclaim one pod",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					exec1,
					gapRunningPod("exec-2", "pg-active", "2", true),
					gapPendingPod("standby-driver", "pg-standby-1", "2"),
					gapPendingPod("standby-driver-2", "pg-standby-2", "2"),
				},
				Nodes:     []*corev1.Node{n1},
				PodGroups: []*schedulingv1beta1.PodGroup{pgActive, gapAskPG("pg-standby-1", "standby"), gapAskPG("pg-standby-2", "standby")},
				Queues:    gapTree{"4", "4", "0", "4", "4", "4"}.queues(),
				ExpectPipeLined: map[string][]string{
					"ns1/pg-standby-1": {"n1"},
					"ns1/pg-standby-2": {"n1"},
				},
				ExpectEvictNum: 2,
				ExpectEvicted:  []string{"ns1/exec-1", "ns1/exec-2"},
			},
		},
		{
			// EXPECT-FAIL (stated as a requirement). Incremental DA ramp, session 2: the standby
			// driver is running and its first executor arrives in the same minMember-1 PodGroup;
			// active still holds one pod over its (now 0c) deserved. Wanted: reclaim the remaining
			// gap. Actual: reclaim only serves starving jobs (gang's JobStarvingFn, i.e.
			// ready+pipelined < minAvailable), and a minMember-1 PodGroup with a running driver is
			// not starving, so its later executors are never reclaim askers; they wait for free
			// capacity. In a failover on a fully allocated cluster that means the standby never
			// ramps. D2b shows a PodGroup-shape workaround (executor in its own PodGroup) and D2c
			// the configuration one (jobStarving from the priority plugin instead of gang).
			skip: "EXPECT-FAIL with gang's jobStarving: reclaim skips jobs that already satisfy minAvailable, so executors joining a running minMember-1 PodGroup never trigger reclaim (see D2b, D2c)",
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "D2: executor arriving after the driver in the same PodGroup reclaims the remaining gap",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("standby-driver", "pg-standby", "2", true),
					gapPendingPod("exec-s1", "pg-standby", "2"),
					gapRunningPod("exec-2", "pg-active", "2", true),
				},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, gapRunningPG("pg-standby", "standby")},
				Queues:          gapTree{"4", "4", "0", "4", "4", "4"}.queues(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-2"},
			},
		},
		{
			// Documenting. Same ramp as D2 but the executor has its own Inqueue PodGroup, so it
			// is a starving job in its own right and reclaim serves it.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "D2b: executor arriving after the driver in its own PodGroup reclaims the remaining gap",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("standby-driver", "pg-standby", "2", true),
					gapPendingPod("exec-s1", "pg-standby-exec", "2"),
					gapRunningPod("exec-2", "pg-active", "2", true),
				},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, gapRunningPG("pg-standby", "standby"), gapAskPG("pg-standby-exec", "standby")},
				Queues:          gapTree{"4", "4", "0", "4", "4", "4"}.queues(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby-exec": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-2"},
			},
		},
		{
			// Documenting. The D2 fixture again, with JobStarving taken from the priority plugin
			// ("any task not yet ready or pipelined") instead of gang ("ready+pipelined <
			// minAvailable"). The executor joining the running minMember-1 PodGroup now makes the
			// job a reclaim asker and the remaining gap is reclaimed. This is the configuration
			// answer to D2 for driver+executor PodGroups with minAvailable 1.
			starvingByPending: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "D2c: with jobStarving from the priority plugin, an executor joining a running minMember-1 PodGroup reclaims the remaining gap",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("standby-driver", "pg-standby", "2", true),
					gapPendingPod("exec-s1", "pg-standby", "2"),
					gapRunningPod("exec-2", "pg-active", "2", true),
				},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, gapRunningPG("pg-standby", "standby")},
				Queues:          gapTree{"4", "4", "0", "4", "4", "4"}.queues(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-2"},
			},
		},
		{
			// Documenting. An asker already at its own capability (standby cap 2c, holding 2c)
			// fails PreemptiveFn and cannot reclaim even though active is over deserved.
			//
			//	tenant   deserved 6c, cap 6c
			//	├── active   deserved 2c, cap 6c   holds 4c
			//	└── standby  deserved 2c, cap 2c   holds 2c, asks 2c more
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "D3: an asker at its own capability cannot reclaim",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("standby-driver", "pg-standby", "2", true),
					gapPendingPod("exec-s1", "pg-standby-exec", "2"),
					exec1,
					gapRunningPod("exec-2", "pg-active", "2", true),
				},
				Nodes:          []*corev1.Node{n1},
				PodGroups:      []*schedulingv1beta1.PodGroup{pgActive, gapRunningPG("pg-standby", "standby"), gapAskPG("pg-standby-exec", "standby")},
				Queues:         gapTree{"6", "6", "2", "6", "2", "2"}.queues(),
				ExpectEvictNum: 0,
			},
		},

		// ---------------------------------------------------------------- E. cross-tenant and ancestorReclaimLevel
		{
			// Requirement. ancestorReclaimLevel 0: a-standby's ask is blocked by tenant-a's cap;
			// b-leaf is also over deserved but evicting it cannot lower tenant-a's allocation.
			// Only a-active may lose a pod. b-leaf's pods are admissible victims at level 0, but
			// the capacity plugin's VictimQueueOrderFn tries queues with a closer common ancestor
			// first (a-active shares tenant-a, b-leaf only shares root), so a-active is drained
			// first and the loop stops before b-leaf is reached. This does not depend on job
			// names or creation order (verified by renaming pg-b-leaf to sort first).
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "E1: level 0 evicts only within the asker's tenant",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("a-exec-1", "pg-a-active", "2", true),
					gapRunningPod("a-exec-2", "pg-a-active", "2", false),
					gapRunningPod("b-exec-1", "pg-b-leaf", "2", true),
					gapRunningPod("b-exec-2", "pg-b-leaf", "2", true),
					aAsk,
				},
				Nodes:           []*corev1.Node{gapNode("n1", "12")},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgAActive, pgAStandby, pgBLeaf},
				Queues:          twoTenantQueues("4"),
				ExpectPipeLined: map[string][]string{"ns1/pg-a-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/a-exec-1"},
			},
		},
		{
			// Requirement. ancestorReclaimLevel 1 admits b-leaf victims (tenant-b is over its
			// deserved too), but evicting them does not lower tenant-a's allocation, so the ask
			// never becomes allocatable and the per-node statement is discarded: zero evictions.
			level: 1,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "E2: level 1 does not commit cross-tenant victims that cannot help",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("a-exec-1", "pg-a-active", "2", false),
					gapRunningPod("a-exec-2", "pg-a-active", "2", false),
					gapRunningPod("b-exec-1", "pg-b-leaf", "2", true),
					gapRunningPod("b-exec-2", "pg-b-leaf", "2", true),
					aAsk,
				},
				Nodes:          []*corev1.Node{gapNode("n1", "12")},
				PodGroups:      []*schedulingv1beta1.PodGroup{pgAActive, pgAStandby, pgBLeaf},
				Queues:         twoTenantQueues("4"),
				ExpectEvictNum: 0,
			},
		},
		{
			// Documenting. ancestorReclaimLevel 1 where the cross-tenant victim does help: tenant-a
			// has room (cap 8c) and the starvation is physical (8c node full, b-leaf over deserved
			// holds the space). One b-leaf pod goes.
			level: 1,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "E3: level 1 reclaims a cross-tenant pod when the starvation is physical",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("a-exec-1", "pg-a-active", "2", false),
					gapRunningPod("a-exec-2", "pg-a-active", "2", false),
					gapRunningPod("b-exec-1", "pg-b-leaf", "2", true),
					gapRunningPod("b-exec-2", "pg-b-leaf", "2", false),
					aAsk,
				},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgAActive, pgAStandby, pgBLeaf},
				Queues:          twoTenantQueues("8"),
				ExpectPipeLined: map[string][]string{"ns1/pg-a-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/b-exec-1"},
			},
		},

		// ---------------------------------------------------------------- F. infra and guarantee
		{
			// Documenting. A resident infra pod at its guarantee (and at its deserved) is never a
			// victim; the eviction comes from the over-deserved active queue. Note that active can
			// only be reclaimed down to its deserved (2c), so the 4c ask also needs 2c free on the
			// node: with a full 6c node the single admissible victim is not enough and nothing is
			// committed (that is A5 again), hence the 8c node here.
			//
			//	tenant   deserved 8c, cap 8c
			//	├── infra    deserved 2c, cap 4c, guarantee 2c   holds 2c
			//	├── active   deserved 2c, cap 4c                 holds 4c (exec-2 protected)
			//	└── standby  deserved 4c, cap 4c                 asks 4c
			//	node n1: 8c, 2c free
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "F1: the guarantee floor protects a resident infra pod",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("prober", "pg-infra", "2", true),
					exec1,
					exec2Protected,
					gapPendingPod("standby-driver", "pg-standby", "4"),
				},
				Nodes:     []*corev1.Node{n1},
				PodGroups: []*schedulingv1beta1.PodGroup{gapRunningPG("pg-infra", "infra"), pgActive, pgStandby},
				Queues: []*schedulingv1beta1.Queue{
					gapQueue("root", "", "", ""),
					gapQueue("tenant", "root", "8", "8"),
					infraWithGuarantee("2"),
					gapQueue("active", "tenant", "2", "4"),
					gapQueue("standby", "tenant", "4", "4"),
				},
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-1"},
			},
		},
		{
			// Requirement if guarantee is ever set. Same as F1 with infra deserved 0c: the
			// capacity plugin raises deserved to max(deserved, guarantee), so the guarantee alone
			// keeps the prober out of the victim set. For reclaim this means a guarantee below
			// deserved is a no-op and a guarantee above deserved simply becomes the deserved.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "F1b: a guarantee with zero deserved still protects the resident infra pod",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("prober", "pg-infra", "2", true),
					exec1,
					exec2Protected,
					gapPendingPod("standby-driver", "pg-standby", "4"),
				},
				Nodes:     []*corev1.Node{n1},
				PodGroups: []*schedulingv1beta1.PodGroup{gapRunningPG("pg-infra", "infra"), pgActive, pgStandby},
				Queues: []*schedulingv1beta1.Queue{
					gapQueue("root", "", "", ""),
					gapQueue("tenant", "root", "8", "8"),
					infraWithGuarantee("0"),
					gapQueue("active", "tenant", "2", "4"),
					gapQueue("standby", "tenant", "4", "4"),
				},
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-1"},
			},
		},
		{
			// Requirement. An infra restart is an under-deserved asker like any other: with the
			// node and the tenant both full, it reclaims from the over-deserved active queue.
			//
			//	tenant   deserved 4c, cap 4c
			//	├── infra    deserved 2c, cap 4c   asks 2c
			//	└── active   deserved 2c, cap 4c   holds 4c
			//	node n1: 4c, full
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "F2: an infra restart reclaims from the over-deserved queue",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapPendingPod("prober", "pg-infra", "2"),
					exec1,
					exec2Protected,
				},
				Nodes:     []*corev1.Node{gapNode("n1", "4")},
				PodGroups: []*schedulingv1beta1.PodGroup{gapAskPG("pg-infra", "infra"), pgActive},
				Queues: []*schedulingv1beta1.Queue{
					gapQueue("root", "", "", ""),
					gapQueue("tenant", "root", "4", "4"),
					gapQueue("infra", "tenant", "2", "4"),
					gapQueue("active", "tenant", "2", "4"),
				},
				ExpectPipeLined: map[string][]string{"ns1/pg-infra": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-1"},
			},
		},

		// ---------------------------------------------------------------- G. enqueue-side
		{
			// Documenting. The ask carries minResources (2c) and the tenant is at cap, but the
			// enqueue gate subtracts the holders' elastic usage (allocated above their own
			// minResources). active's PodGroup has no minResources, so all of its 4c is elastic,
			// the gate admits the job, and reclaim then serves it exactly as in A1. For PodGroups
			// auto-created per owner (no minResources on the holders) the enqueue-side form of
			// volcano-sh/volcano#4817 therefore does not arise.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:            "G1: an ask with minResources is admitted at enqueue when the holders' usage is elastic, then reclaimed",
				Plugins:         plugins,
				Pods:            []*corev1.Pod{exec1, exec2Protected, ask},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandbyMinRes},
				Queues:          defaultGapTree.queues(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-1"},
			},
		},
		{
			// EXPECT-FAIL. Same as G1 but the holder also declares minResources (4c, vcjob
			// style), so nothing is elastic: capacity's jobEnqueueable sees the tenant at cap,
			// the PodGroup stays Pending and reclaim never sees the job. Desired: enqueued, then
			// reclaimed as in A1. This needs the enqueue-side follow-up (permit only when the leaf
			// is under deserved on every requested dimension and the only failing check is an
			// ancestor cap; strictly all-dimension, unlike volcano-sh/volcano#4825). See
			// volcano-sh/volcano#4817.
			skip: "EXPECT-FAIL: enqueue gate rejects a minResources PodGroup blocked only by an ancestor cap when the holders have no elastic usage (volcano-sh/volcano#4817, follow-up to the reclaim fix)",
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:            "G2: an ask with minResources blocked by an ancestor cap held by non-elastic jobs should be enqueued and then reclaimed",
				Plugins:         plugins,
				Pods:            []*corev1.Pod{exec1, exec2Protected, ask},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActiveMinRes, pgStandbyMinRes},
				Queues:          defaultGapTree.queues(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-1"},
			},
		},
	}

	for i, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			if c.skip != "" {
				t.Skip(c.skip)
			}
			c.RegisterSession(gapTiers(c.level, c.priority, c.gangReclaim, c.starvingByPending), nil)
			defer c.Close()
			c.Run(actions)
			if err := c.CheckAll(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}

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
	"time"

	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/actions/dequeue"
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
	// enable the capacity plugin's enqueueAncestorCapReclaim argument (enqueue-side admission).
	enqueueReclaim bool
	// actions overrides the default enqueue, reclaim, allocate sequence; config is passed to
	// RegisterSession for action arguments.
	actions []framework.Action
	config  []conf.Configuration
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

func gapTiers(level int, withPriority, gangReclaim, starvingByPending, enqueueReclaim bool) []conf.Tier {
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
	if level > 0 || enqueueReclaim {
		capacityOpt.Arguments = framework.Arguments{}
		if level > 0 {
			capacityOpt.Arguments[ancestorReclaimLevelKey] = level
		}
		if enqueueReclaim {
			capacityOpt.Arguments[enqueueAncestorCapReclaimKey] = true
		}
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
	// G3: the shape the PodGroup controller produces for bare pods (pod == PodGroup): one Running
	// PodGroup per holder pod, minMember 1, minResources equal to that pod's request.
	pgExec1MinRes := util.BuildPodGroupWithMinResources("pg-exec-1", gapNS, "active", 1, nil, cpuMem("2"), schedulingv1beta1.PodGroupRunning)
	pgExec2MinRes := util.BuildPodGroupWithMinResources("pg-exec-2", gapNS, "active", 1, nil, cpuMem("2"), schedulingv1beta1.PodGroupRunning)
	pgProberMinRes := util.BuildPodGroupWithMinResources("pg-prober", gapNS, "infra", 1, nil, cpuMem("2"), schedulingv1beta1.PodGroupRunning)
	pgStandbyMinRes4 := util.BuildPodGroupWithMinResources("pg-standby", gapNS, "standby", 1, nil, cpuMem("4"), schedulingv1beta1.PodGroupPending)
	// G14: the same 4c ask, already Inqueue for two hours without progress.
	pgStandbyStuck := util.BuildPodGroupWithMinResources("pg-standby", gapNS, "standby", 1, nil, cpuMem("4"), schedulingv1beta1.PodGroupInqueue)
	pgStandbyStuck.Status.Conditions = []schedulingv1beta1.PodGroupCondition{{
		Type:               schedulingv1beta1.PodGroupConditionType(api.PodGroupInqueueType),
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
	}}
	// G9: a cpu+gpu ask. Only cpu has reclaimable slack under the tenant.
	gpu := func(c, g string) corev1.ResourceList {
		return api.BuildResourceList(c, c+"Gi", []api.ScalarResource{{Name: "nvidia.com/gpu", Value: g}}...)
	}
	gpuNode := util.BuildNode("n1", api.BuildResourceList("8", "8Gi", []api.ScalarResource{{Name: "pods", Value: "10"}, {Name: "nvidia.com/gpu", Value: "4"}}...), map[string]string{})
	gpuQueues := []*schedulingv1beta1.Queue{
		gapQueue("root", "", "", ""),
		buildQueueWithParents("tenant", "root", gpu("4", "1"), gpu("4", "1")),
		buildQueueWithParents("active", "tenant", gpu("2", "1"), gpu("4", "1")),
		buildQueueWithParents("standby", "tenant", gpu("2", "1"), gpu("4", "1")),
	}
	pgExec1Gpu := util.BuildPodGroupWithMinResources("pg-exec-1", gapNS, "active", 1, nil, gpu("2", "1"), schedulingv1beta1.PodGroupRunning)
	pgStandbyGpu := util.BuildPodGroupWithMinResources("pg-standby", gapNS, "standby", 1, nil, gpu("2", "1"), schedulingv1beta1.PodGroupPending)
	// G10: slack spread over two sibling queues.
	//
	//	tenant   deserved 6c, cap 6c
	//	├── a        deserved 1c, cap 6c   holds exec-a1 2c (preemptable) + exec-a2 1c (protected)
	//	├── b        deserved 1c, cap 6c   holds exec-b1 2c (preemptable) + exec-b2 1c (protected)
	//	└── standby  deserved 4c, cap 6c   asks 4c
	//	node n1: 8c, 2c free
	twoSiblingQueues := []*schedulingv1beta1.Queue{
		gapQueue("root", "", "", ""),
		gapQueue("tenant", "root", "6", "6"),
		gapQueue("a", "tenant", "1", "6"),
		gapQueue("b", "tenant", "1", "6"),
		gapQueue("standby", "tenant", "4", "6"),
	}
	twoSiblingPGs := []*schedulingv1beta1.PodGroup{
		util.BuildPodGroupWithMinResources("pg-a1", gapNS, "a", 1, nil, cpuMem("2"), schedulingv1beta1.PodGroupRunning),
		util.BuildPodGroupWithMinResources("pg-a2", gapNS, "a", 1, nil, cpuMem("1"), schedulingv1beta1.PodGroupRunning),
		util.BuildPodGroupWithMinResources("pg-b1", gapNS, "b", 1, nil, cpuMem("2"), schedulingv1beta1.PodGroupRunning),
		util.BuildPodGroupWithMinResources("pg-b2", gapNS, "b", 1, nil, cpuMem("1"), schedulingv1beta1.PodGroupRunning),
		pgStandbyMinRes4,
	}
	// G7: the standby leaf's deserved is short on memory only.
	standbyShortMemory := buildQueueWithParents("standby", "tenant", api.BuildResourceList("2", "1Gi"), cpuMem("4"))

	withGuarantee := func(q *schedulingv1beta1.Queue, c string) *schedulingv1beta1.Queue {
		q.Spec.Guarantee = schedulingv1beta1.Guarantee{Resource: cpuMem(c)}
		return q
	}
	// fiveHolders builds five 2c running pods in queue, each in its own PodGroup with
	// minResources equal to its request (the pod == PodGroup shape).
	fiveHolders := func(prefix, queue string) ([]*corev1.Pod, []*schedulingv1beta1.PodGroup) {
		var pods []*corev1.Pod
		var pgs []*schedulingv1beta1.PodGroup
		for i := 1; i <= 5; i++ {
			pg := prefix + "-pg-" + strconv.Itoa(i)
			pods = append(pods, gapRunningPod(prefix+"-"+strconv.Itoa(i), pg, "2", true))
			pgs = append(pgs, util.BuildPodGroupWithMinResources(pg, gapNS, queue, 1, nil, cpuMem("2"), schedulingv1beta1.PodGroupRunning))
		}
		return pods, pgs
	}
	// G11: the scenario reported in volcano-sh/volcano#4817, scaled to a 10c node. The root
	// capability is set explicitly: the reporter hit this on 1.13, where the root's realCapability
	// was the cluster total; on master an unset root capability is infinite, so the same block
	// now needs a capability on the root or on an intermediate ancestor.
	//
	//	root        capability 10c
	//	├── org-1   no guarantee, no deserved
	//	│   └── org-1-team-1  capability 10c, no deserved   holds 10c (5 × 2c)
	//	└── org-2   guarantee 4c
	//	    └── org-2-team-1  guarantee 4c, deserved 4c       asks 4c
	issueQueues := []*schedulingv1beta1.Queue{
		gapQueue("root", "", "", "10"),
		gapQueue("org-1", "root", "", ""),
		gapQueue("org-1-team-1", "org-1", "", "10"),
		withGuarantee(gapQueue("org-2", "root", "", ""), "4"),
		withGuarantee(gapQueue("org-2-team-1", "org-2", "4", ""), "4"),
	}
	org1Pods, org1PGs := fiveHolders("org1", "org-1-team-1")
	pgOrg2Ask := util.BuildPodGroupWithMinResources("pg-org2", gapNS, "org-2-team-1", 1, nil, cpuMem("4"), schedulingv1beta1.PodGroupPending)
	// G12: the example from volcano-sh/volcano#4825, scaled to a 10c node. leaf-1 holds more
	// than its effective capability (6c), the state that arises when leaf-2 is created after
	// leaf-1 already borrowed the whole parent.
	//
	//	parent      guarantee 10c, capability 10c
	//	├── leaf-1  guarantee 6c, deserved 6c   holds 10c (5 × 2c)
	//	└── leaf-2  guarantee 4c, deserved 4c   asks 4c
	prExampleQueues := []*schedulingv1beta1.Queue{
		gapQueue("root", "", "", ""),
		withGuarantee(gapQueue("parent", "root", "", "10"), "10"),
		withGuarantee(gapQueue("leaf-1", "parent", "6", ""), "6"),
		withGuarantee(gapQueue("leaf-2", "parent", "4", ""), "4"),
	}
	leaf1Pods, leaf1PGs := fiveHolders("l1", "leaf-1")
	pgLeaf2Ask := util.BuildPodGroupWithMinResources("pg-leaf-2", gapNS, "leaf-2", 1, nil, cpuMem("4"), schedulingv1beta1.PodGroupPending)

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
			// Requirement. Same as G1 but the holder also declares minResources (4c, vcjob
			// style), so nothing is elastic and the default gate rejects the ask at the tenant.
			// With enqueueAncestorCapReclaim the gate sees that the leaf stays under deserved and
			// that active's reclaimable slack (exec-1, 2c over deserved) covers the 2c shortfall,
			// admits the job, and reclaim serves it as in A1.
			enqueueReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:            "G2: an ask with minResources blocked by an ancestor cap held by non-elastic jobs is enqueued on reclaimable slack and then reclaimed",
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
		{
			// Requirement. The pod == PodGroup shape: every pod runs without a pre-created
			// PodGroup, so the controller gives each one a minMember-1 PodGroup whose
			// minResources equal the pod's own request. A single-pod PodGroup has no elastic
			// usage, so by default a failover ask is rejected at enqueue even though reclaim could
			// serve it (see G3-off). With enqueueAncestorCapReclaim it is admitted and reclaimed.
			enqueueReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "G3: pod == PodGroup shape, failover ask blocked at enqueue by an ancestor cap is enqueued on reclaimable slack and then reclaimed",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("exec-1", "pg-exec-1", "2", true),
					gapRunningPod("exec-2", "pg-exec-2", "2", false),
					ask,
				},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgExec1MinRes, pgExec2MinRes, pgStandbyMinRes},
				Queues:          defaultGapTree.queues(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-1"},
			},
		},
		{
			// Documenting. G3 with the flag off: the default gate is unchanged, the PodGroup stays
			// Pending and nothing is evicted.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "G3-off: pod == PodGroup shape without enqueueAncestorCapReclaim stays Pending",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("exec-1", "pg-exec-1", "2", true),
					gapRunningPod("exec-2", "pg-exec-2", "2", false),
					ask,
				},
				Nodes:          []*corev1.Node{n1},
				PodGroups:      []*schedulingv1beta1.PodGroup{pgExec1MinRes, pgExec2MinRes, pgStandbyMinRes},
				Queues:         defaultGapTree.queues(),
				ExpectStatus:   map[api.JobID]scheduling.PodGroupPhase{"ns1/pg-standby": scheduling.PodGroupPending},
				ExpectEvictNum: 0,
			},
		},
		{
			// Requirement. Flag on, but every holder is non-preemptable: no reclaimable slack, so
			// the ask is not admitted. This is the "admitted but never served" case the gate must
			// not produce.
			enqueueReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "G4: no admission when the holders are non-preemptable",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("exec-1", "pg-exec-1", "2", false),
					gapRunningPod("exec-2", "pg-exec-2", "2", false),
					ask,
				},
				Nodes:          []*corev1.Node{n1},
				PodGroups:      []*schedulingv1beta1.PodGroup{pgExec1MinRes, pgExec2MinRes, pgStandbyMinRes},
				Queues:         defaultGapTree.queues(),
				ExpectStatus:   map[api.JobID]scheduling.PodGroupPhase{"ns1/pg-standby": scheduling.PodGroupPending},
				ExpectEvictNum: 0,
			},
		},
		{
			// Requirement. Flag on, but the holder queue has reclaimable=false: its usage is not
			// slack.
			enqueueReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "G5: no admission when the holder queue is not reclaimable",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("exec-1", "pg-exec-1", "2", true),
					gapRunningPod("exec-2", "pg-exec-2", "2", false),
					ask,
				},
				Nodes:     []*corev1.Node{n1},
				PodGroups: []*schedulingv1beta1.PodGroup{pgExec1MinRes, pgExec2MinRes, pgStandbyMinRes},
				Queues: []*schedulingv1beta1.Queue{
					gapQueue("root", "", "", ""),
					gapQueue("tenant", "root", "4", "4"),
					activeNotReclaimable,
					gapQueue("standby", "tenant", "2", "4"),
				},
				ExpectStatus:   map[api.JobID]scheduling.PodGroupPhase{"ns1/pg-standby": scheduling.PodGroupPending},
				ExpectEvictNum: 0,
			},
		},
		{
			// Requirement. Flag on, but the holders sit exactly at their deserved (active 2c of
			// 2c, infra 2c of 2c): reclaim would find no victim, so nothing is admitted. The
			// children's deserved sum exceeds the tenant's here; that misconfiguration is exactly
			// when a deserved-only rule would admit a hopeless job.
			enqueueReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "G6: no admission when the holders are at their deserved",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("exec-1", "pg-exec-1", "2", true),
					gapRunningPod("prober", "pg-prober", "2", true),
					ask,
				},
				Nodes:     []*corev1.Node{n1},
				PodGroups: []*schedulingv1beta1.PodGroup{pgExec1MinRes, pgProberMinRes, pgStandbyMinRes},
				Queues: []*schedulingv1beta1.Queue{
					gapQueue("root", "", "", ""),
					gapQueue("tenant", "root", "4", "4"),
					gapQueue("active", "tenant", "2", "4"),
					gapQueue("infra", "tenant", "2", "4"),
					gapQueue("standby", "tenant", "2", "4"),
				},
				ExpectStatus:   map[api.JobID]scheduling.PodGroupPhase{"ns1/pg-standby": scheduling.PodGroupPending},
				ExpectEvictNum: 0,
			},
		},
		{
			// Requirement. Flag on, cpu slack exists, but the ask would take the leaf over its
			// deserved on memory (deserved 1Gi, ask 2Gi). The rule is all-dimension: not admitted.
			enqueueReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "G7: no admission when the leaf would exceed deserved on any requested dimension",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("exec-1", "pg-exec-1", "2", true),
					gapRunningPod("exec-2", "pg-exec-2", "2", false),
					ask,
				},
				Nodes:     []*corev1.Node{n1},
				PodGroups: []*schedulingv1beta1.PodGroup{pgExec1MinRes, pgExec2MinRes, pgStandbyMinRes},
				Queues: []*schedulingv1beta1.Queue{
					gapQueue("root", "", "", ""),
					gapQueue("tenant", "root", "4", "4"),
					gapQueue("active", "tenant", "2", "4"),
					standbyShortMemory,
				},
				ExpectStatus:   map[api.JobID]scheduling.PodGroupPhase{"ns1/pg-standby": scheduling.PodGroupPending},
				ExpectEvictNum: 0,
			},
		},
		{
			// Requirement. Flag on, but the 4c ask's shortfall exceeds the 2c of reclaimable
			// slack (only exec-1 is preemptable): not admitted, so no half-useful eviction can
			// follow either.
			enqueueReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "G8: no admission when the shortfall exceeds the reclaimable slack",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("exec-1", "pg-exec-1", "2", true),
					gapRunningPod("exec-2", "pg-exec-2", "2", false),
					gapPendingPod("standby-driver", "pg-standby", "4"),
				},
				Nodes:          []*corev1.Node{n1},
				PodGroups:      []*schedulingv1beta1.PodGroup{pgExec1MinRes, pgExec2MinRes, pgStandbyMinRes4},
				Queues:         gapTree{"4", "4", "0", "4", "4", "4"}.queues(),
				ExpectStatus:   map[api.JobID]scheduling.PodGroupPhase{"ns1/pg-standby": scheduling.PodGroupPending},
				ExpectEvictNum: 0,
			},
		},
		{
			// Requirement. Flag on, two-dimension ask (cpu + gpu). active is over deserved on cpu
			// only (4c of 2c; 1 gpu of 1 gpu), so the tenant's gpu shortfall has no slack behind
			// it. The rule is all-dimension: not admitted.
			enqueueReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "G9: no admission when only some requested dimensions have reclaimable slack",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					util.BuildPod(gapNS, "exec-1", "n1", corev1.PodRunning, gpu("2", "1"), "pg-exec-1", preemptableLabel(true), map[string]string{}),
					gapRunningPod("exec-2", "pg-exec-2", "2", false),
					util.BuildPod(gapNS, "standby-driver", "", corev1.PodPending, gpu("2", "1"), "pg-standby", map[string]string{}, map[string]string{}),
				},
				Nodes:          []*corev1.Node{gpuNode},
				PodGroups:      []*schedulingv1beta1.PodGroup{pgExec1Gpu, pgExec2MinRes, pgStandbyGpu},
				Queues:         gpuQueues,
				ExpectStatus:   map[api.JobID]scheduling.PodGroupPhase{"ns1/pg-standby": scheduling.PodGroupPending},
				ExpectEvictNum: 0,
			},
		},
		{
			// Requirement. Flag on, the 4c shortfall is covered only by adding the slack of two
			// sibling queues (2c each). Admitted, then reclaim evicts one pod from each sibling
			// and pipelines the ask.
			enqueueReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "G10: admission on slack spread over two sibling queues, then reclaim from both",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("exec-a1", "pg-a1", "2", true),
					gapRunningPod("exec-a2", "pg-a2", "1", false),
					gapRunningPod("exec-b1", "pg-b1", "2", true),
					gapRunningPod("exec-b2", "pg-b2", "1", false),
					gapPendingPod("standby-driver", "pg-standby", "4"),
				},
				Nodes:           []*corev1.Node{n1},
				PodGroups:       twoSiblingPGs,
				Queues:          twoSiblingQueues,
				ExpectStatus:    map[api.JobID]scheduling.PodGroupPhase{"ns1/pg-standby": scheduling.PodGroupInqueue},
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  2,
				ExpectEvicted:   []string{"ns1/exec-a1", "ns1/exec-b1"},
			},
		},
		{
			// Requirement. The reporter's scenario from volcano-sh/volcano#4817: org-2-team-1 has a
			// 4c guarantee, org-1-team-1 borrowed the whole cluster with no deserved at all, and
			// the ask is blocked at the root. With the flag the root's shortfall (4c) is covered by
			// org-1-team-1's slack (10c, all of it over its empty deserved), the job is admitted,
			// and reclaim evicts two 2c pods.
			enqueueReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:            "G11: issue 4817 scenario, guaranteed child blocked at the root by a borrower with no deserved",
				Plugins:         plugins,
				Pods:            append(append([]*corev1.Pod{}, org1Pods...), gapPendingPod("team2-driver", "pg-org2", "4")),
				Nodes:           []*corev1.Node{gapNode("n1", "10")},
				PodGroups:       append(append([]*schedulingv1beta1.PodGroup{}, org1PGs...), pgOrg2Ask),
				Queues:          issueQueues,
				ExpectStatus:    map[api.JobID]scheduling.PodGroupPhase{"ns1/pg-org2": scheduling.PodGroupInqueue},
				ExpectPipeLined: map[string][]string{"ns1/pg-org2": {"n1"}},
				ExpectEvictNum:  2,
				ExpectEvicted:   []string{"ns1/org1-5", "ns1/org1-4"},
			},
		},
		{
			// Requirement. The example from volcano-sh/volcano#4825 in its guarantee form. leaf-2
			// is under its deserved and guarantee, the parent is full, and leaf-1 is 4c over its
			// deserved: admitted, then reclaim takes leaf-1 down to its deserved and no further
			// (the guarantee floor and the deserved check agree here).
			enqueueReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:            "G12: PR 4825 example, guaranteed sibling reclaims an over-deserved borrower under a full parent",
				Plugins:         plugins,
				Pods:            append(append([]*corev1.Pod{}, leaf1Pods...), gapPendingPod("leaf2-driver", "pg-leaf-2", "4")),
				Nodes:           []*corev1.Node{gapNode("n1", "10")},
				PodGroups:       append(append([]*schedulingv1beta1.PodGroup{}, leaf1PGs...), pgLeaf2Ask),
				Queues:          prExampleQueues,
				ExpectStatus:    map[api.JobID]scheduling.PodGroupPhase{"ns1/pg-leaf-2": scheduling.PodGroupInqueue},
				ExpectPipeLined: map[string][]string{"ns1/pg-leaf-2": {"n1"}},
				ExpectEvictNum:  2,
				ExpectEvicted:   []string{"ns1/l1-5", "ns1/l1-4"},
			},
		},
		{
			// Documenting a limitation. The slack estimate is cluster-wide, but reclaim evicts
			// only on the one node it is trying, and the hierarchical shortfall must be covered by
			// victims on that node. Here active's two 2c pods sit on different nodes, the 4c ask
			// needs both, so each per-node attempt frees only 2c, is discarded, and nothing is
			// evicted. The job is admitted on 4c of slack, stays Inqueue, and is never served.
			enqueueReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "G13: admitted on cluster-wide slack, but the victims are spread over nodes and reclaim cannot serve it",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("exec-1", "pg-exec-1", "2", true),
					util.BuildPod(gapNS, "exec-2", "n2", corev1.PodRunning, cpuMem("2"), "pg-exec-2", preemptableLabel(true), map[string]string{}),
					gapPendingPod("standby-driver", "pg-standby", "4"),
				},
				Nodes:          []*corev1.Node{n1, gapNode("n2", "8")},
				PodGroups:      []*schedulingv1beta1.PodGroup{pgExec1MinRes, pgExec2MinRes, pgStandbyMinRes4},
				Queues:         gapTree{"4", "4", "0", "4", "4", "4"}.queues(),
				ExpectStatus:   map[api.JobID]scheduling.PodGroupPhase{"ns1/pg-standby": scheduling.PodGroupInqueue},
				ExpectEvictNum: 0,
			},
		},
		{
			// Documenting the safety net. The G13 group, already Inqueue for longer than the
			// dequeue action's timeout with no task placed: reclaim still cannot serve it, and the
			// dequeue action moves it back to Pending, releasing its reservation. Nothing is evicted.
			enqueueReclaim: true,
			actions:        []framework.Action{enqueue.New(), reclaim.New(), allocate.New(), dequeue.New()},
			config:         []conf.Configuration{{Name: dequeue.Dequeue, Arguments: map[string]interface{}{dequeue.InqueueTimeoutKey: "1h"}}},
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:    "G14: a group stuck Inqueue past the dequeue timeout is moved back to Pending",
				Plugins: plugins,
				Pods: []*corev1.Pod{
					gapRunningPod("exec-1", "pg-exec-1", "2", true),
					util.BuildPod(gapNS, "exec-2", "n2", corev1.PodRunning, cpuMem("2"), "pg-exec-2", preemptableLabel(true), map[string]string{}),
					gapPendingPod("standby-driver", "pg-standby", "4"),
				},
				Nodes:          []*corev1.Node{n1, gapNode("n2", "8")},
				PodGroups:      []*schedulingv1beta1.PodGroup{pgExec1MinRes, pgExec2MinRes, pgStandbyStuck},
				Queues:         gapTree{"4", "4", "0", "4", "4", "4"}.queues(),
				ExpectStatus:   map[api.JobID]scheduling.PodGroupPhase{"ns1/pg-standby": scheduling.PodGroupPending},
				ExpectEvictNum: 0,
			},
		},
	}

	for i, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			if c.skip != "" {
				t.Skip(c.skip)
			}
			c.RegisterSession(gapTiers(c.level, c.priority, c.gangReclaim, c.starvingByPending, c.enqueueReclaim), c.config)
			defer c.Close()
			caseActions := actions
			if c.actions != nil {
				caseActions = c.actions
			}
			c.Run(caseActions)
			if err := c.CheckAll(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func Test_capacityPlugin_parseEnqueueAncestorCapReclaim(t *testing.T) {
	cases := []struct {
		name string
		args framework.Arguments
		want bool
	}{
		{name: "default off", args: framework.Arguments{}, want: false},
		{name: "enabled", args: framework.Arguments{enqueueAncestorCapReclaimKey: true}, want: true},
		{name: "explicitly off", args: framework.Arguments{enqueueAncestorCapReclaimKey: false}, want: false},
		{name: "invalid value falls back to off", args: framework.Arguments{enqueueAncestorCapReclaimKey: "nope"}, want: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cp := New(c.args).(*capacityPlugin)
			cp.parseArguments()
			if cp.enqueueAncestorCapReclaim != c.want {
				t.Fatalf("%s=%v: want %t, got %t", enqueueAncestorCapReclaimKey, c.args[enqueueAncestorCapReclaimKey], c.want, cp.enqueueAncestorCapReclaim)
			}
		})
	}
}

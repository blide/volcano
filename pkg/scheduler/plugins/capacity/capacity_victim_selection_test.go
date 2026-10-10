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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

// Victim selection by the reclaim action under hierarchical capacity queues: which of a node's
// victims are offered to the plugins first, and which node's victims are committed. The fixture
// is a tenant failing over: queue active drains (deserved 0 where the case says so) while queue
// standby asks for its share, with other tenants next to it where the case needs them.

const vsNS = "ns1"

// vsCase is one scheduling session: pods and nodes, the queue tree, plugin switches, the reclaim
// action's arguments, and the expected binds, pipelines and evictions.
type vsCase struct {
	uthelper.TestCommonStruct
	// priority enables the priority plugin's job and task order (victim cost is pod priority).
	priority bool
	// gangReclaim enables gang's ReclaimableFn (the minAvailable veto).
	gangReclaim bool
	// bestFit sets the reclaim action's victimSelection to bestFit; maxCandidateNodes is passed
	// along when non-zero.
	bestFit           bool
	maxCandidateNodes int
	// actions overrides the action sequence; the default is enqueue, reclaim, allocate.
	actions []framework.Action
	// check runs extra assertions on the session after CheckAll.
	check func(t *testing.T, ssn *framework.Session)
}

func vsRes(c string) corev1.ResourceList {
	if c == "" {
		return nil
	}
	return api.BuildResourceList(c, c+"Gi")
}

func vsNode(name, c string) *corev1.Node {
	return util.BuildNode(name, api.BuildResourceList(c, c+"Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{})
}

func vsQueue(name, parent, deserved, capability string) *schedulingv1beta1.Queue {
	return buildQueueWithParents(name, parent, vsRes(deserved), vsRes(capability))
}

func vsPreemptable(preemptable bool) map[string]string {
	return map[string]string{schedulingv1beta1.PodPreemptable: strconv.FormatBool(preemptable)}
}

// vsRunning is a Running pod on node; prio nil leaves the pod without a priority.
func vsRunning(name, pg, node, c string, preemptable bool, prio *int32) *corev1.Pod {
	return util.BuildPodWithPriority(vsNS, name, node, corev1.PodRunning, vsRes(c), pg, vsPreemptable(preemptable), map[string]string{}, prio)
}

func vsPending(name, pg, c string) *corev1.Pod {
	return util.BuildPod(vsNS, name, "", corev1.PodPending, vsRes(c), pg, map[string]string{}, map[string]string{})
}

// vsNominated is a pending ask that a previous session's reclaim pipelined on node after
// evicting for it: the cache wrote the node into the pod's nominatedNodeName.
func vsNominated(name, pg, c, node string) *corev1.Pod {
	pod := vsPending(name, pg, c)
	pod.Status.NominatedNodeName = node
	return pod
}

// vsEvicted is a victim the scheduler evicted in a previous session that is still terminating on
// node: deletion timestamp set, DisruptionTarget condition as Volcano's evictor writes it.
func vsEvicted(name, pg, node, c string) *corev1.Pod {
	pod := vsRunning(name, pg, node, c, true, nil)
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:   corev1.DisruptionTarget,
		Status: corev1.ConditionTrue,
		Reason: corev1.PodReasonPreemptionByScheduler,
	})
	return pod
}

func vsRunningPG(name, queue string) *schedulingv1beta1.PodGroup {
	return util.BuildPodGroup(name, vsNS, queue, 1, nil, schedulingv1beta1.PodGroupRunning)
}

func vsAskPG(name, queue string) *schedulingv1beta1.PodGroup {
	return util.BuildPodGroup(name, vsNS, queue, 1, nil, schedulingv1beta1.PodGroupInqueue)
}

// vsTree is root -> tenant -> {active, standby}; the strings are deserved and capability.
func vsTree(tenantDeserved, tenantCap, activeDeserved, activeCap, standbyDeserved, standbyCap string) []*schedulingv1beta1.Queue {
	return []*schedulingv1beta1.Queue{
		vsQueue("root", "", "", ""),
		vsQueue("tenant", "root", tenantDeserved, tenantCap),
		vsQueue("active", "tenant", activeDeserved, activeCap),
		vsQueue("standby", "tenant", standbyDeserved, standbyCap),
	}
}

// vsTwoTenants is a second tenant next to the failover one. t1 is failing over: active1 drains
// (deserved 0) while standby1 ramps; t2 is a plain tenant over its deserved.
func vsTwoTenants() []*schedulingv1beta1.Queue {
	return []*schedulingv1beta1.Queue{
		vsQueue("root", "", "", ""),
		vsQueue("t1", "root", "4", "10"),
		vsQueue("active1", "t1", "0", "10"),
		vsQueue("standby1", "t1", "4", "10"),
		vsQueue("t2", "root", "3", "8"),
		vsQueue("active2", "t2", "0", "8"),
	}
}

func (c vsCase) tiers() []conf.Tier {
	trueValue := true
	opts := []conf.PluginOption{
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
	}
	gangOpt := conf.PluginOption{Name: gang.PluginName, EnabledJobStarving: &trueValue}
	if c.gangReclaim {
		gangOpt.EnabledReclaimable = &trueValue
	}
	opts = append(opts, gangOpt)
	if c.priority {
		opts = append(opts, conf.PluginOption{
			Name:             priority.PluginName,
			EnabledJobOrder:  &trueValue,
			EnabledTaskOrder: &trueValue,
		})
	}
	return []conf.Tier{{Plugins: opts}}
}

func Test_capacityPlugin_ReclaimVictimSelection(t *testing.T) {
	plugins := map[string]framework.PluginBuilder{
		PluginName:            New,
		predicates.PluginName: predicates.New,
		gang.PluginName:       gang.New,
		priority.PluginName:   priority.New,
	}
	defaultActions := []framework.Action{enqueue.New(), reclaim.New(), allocate.New()}
	// The documented order: allocate ahead of reclaim.
	documentedActions := []framework.Action{enqueue.New(), allocate.New(), reclaim.New()}

	prioHigh, prioLow := int32(1000), int32(100)
	pcHigh := util.BuildPriorityClass("high", prioHigh)
	pcLow := util.BuildPriorityClass("low", prioLow)
	priClasses := []*schedulingv1.PriorityClass{pcHigh, pcLow}

	// tenant 4/4, active deserved 2 cap 4, standby deserved 2 cap 4: the tenant is full, active
	// is over its deserved, standby asks for its share.
	defaultTree := vsTree("4", "4", "2", "4", "2", "4")
	pgActive := vsRunningPG("pg-active", "active")
	pgStandby := vsAskPG("pg-standby", "standby")
	standbyOnN1 := map[string][]string{"ns1/pg-standby": {"n1"}}

	cases := []vsCase{
		// ------------------------------------------------------- reclaimee order on a node
		{
			// Requirement. Gang's reclaim veto admits victims in arrival order until the job would
			// drop below minAvailable: the job has two ready tasks and minAvailable 1, so exactly
			// one victim is admitted and the other vetoed. Reclaim must hand the reclaimees over
			// cheapest first, so the veto lands on the driver and never on the executor.
			priority:    true,
			gangReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "O1: with gang's veto the executor is evicted before the driver",
				Pods: []*corev1.Pod{
					vsRunning("driver-a", "pg-active", "n1", "2", true, &prioHigh),
					vsRunning("exec-a", "pg-active", "n1", "2", true, &prioLow),
					vsPending("standby-driver", "pg-standby", "2"),
				},
				Nodes:           []*corev1.Node{vsNode("n1", "4")},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          defaultTree,
				PriClass:        priClasses,
				ExpectPipeLined: standbyOnN1,
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-a"},
			},
		},
		// ------------------------------------------------------- bestFit across nodes
		{
			// Requirement. Across nodes, bestFit picks the node whose victims cost least: the
			// driver filling n1 and an executor filling n2 both free the 2c the ask needs; the
			// executor's node wins. First-fit would take whichever node came first.
			priority: true,
			bestFit:  true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "B1: bestFit evicts the executor on another node rather than the driver on the first node",
				Pods: []*corev1.Pod{
					vsRunning("driver-a", "pg-active", "n1", "2", true, &prioHigh),
					vsRunning("exec-a", "pg-active", "n2", "2", true, &prioLow),
					vsPending("standby-driver", "pg-standby", "2"),
				},
				Nodes:           []*corev1.Node{vsNode("n1", "2"), vsNode("n2", "2")},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          defaultTree,
				PriClass:        priClasses,
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n2"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-a"},
			},
		},
		{
			// Requirement. A node that serves the ask without any eviction beats every other
			// plan. Active is 1c over its deserved, so capacity admits its executors as victims on
			// both nodes; the tenant still has room for the ask. n1 (3c) has 1c idle and would need
			// its 2c executor evicted, n2 has 3c idle and serves the ask as it is. Only reclaim runs,
			// so the choice is reclaim's alone. First-fit evicts on n1 whenever n1 comes first.
			bestFit: true,
			actions: []framework.Action{enqueue.New(), reclaim.New()},
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "B2: bestFit prefers the node that needs no eviction",
				Pods: []*corev1.Pod{
					vsRunning("exec-a1", "pg-active", "n1", "2", true, nil),
					vsRunning("exec-a2", "pg-active", "n2", "1", true, nil),
					vsPending("standby-driver", "pg-standby", "2"),
				},
				Nodes:           []*corev1.Node{vsNode("n1", "3"), vsNode("n2", "4")},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          vsTree("6", "6", "2", "6", "2", "6"),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n2"}},
				ExpectEvictNum:  0,
			},
		},
		{
			// Requirement. At equal priority the plan with fewer victims wins: n1 must lose both
			// of its 1c executors to free 2c, n2 one 2c executor.
			bestFit: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "B3: bestFit prefers fewer victims at equal priority",
				Pods: []*corev1.Pod{
					vsRunning("exec-a1", "pg-active", "n1", "1", true, nil),
					vsRunning("exec-a2", "pg-active", "n1", "1", true, nil),
					vsRunning("exec-b", "pg-active", "n2", "2", true, nil),
					vsPending("standby-driver", "pg-standby", "2"),
				},
				Nodes:           []*corev1.Node{vsNode("n1", "2"), vsNode("n2", "2")},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          defaultTree,
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n2"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-b"},
			},
		},
		// ------------------------------------------------------- the ladder is fenced by queue
		{
			// Requirement. The PriorityClass ladder is local to the tenant that uses it. Across
			// tenants the victim queue order decides, as it does within a node: for the capacity
			// plugin the queue nearest the asker in the hierarchy first. t1's own active queue is
			// nearer to standby1 than t2 is, so its high-priority executor on n1 is evicted
			// although t2's low-priority executor on n2 would score cheaper by class value.
			priority: true,
			bestFit:  true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "F1: bestFit ranks victim queues before pod priority across tenants",
				Pods: []*corev1.Pod{
					vsRunning("exec-a", "pg-active1", "n1", "2", true, &prioHigh),
					vsRunning("exec-t2", "pg-active2", "n2", "2", true, &prioLow),
					vsPending("standby-driver", "pg-standby1-ask", "2"),
				},
				Nodes: []*corev1.Node{vsNode("n1", "2"), vsNode("n2", "2")},
				PodGroups: []*schedulingv1beta1.PodGroup{
					vsRunningPG("pg-active1", "active1"), vsAskPG("pg-standby1-ask", "standby1"), vsRunningPG("pg-active2", "active2"),
				},
				Queues:          vsTwoTenants(),
				PriClass:        priClasses,
				ExpectPipeLined: map[string][]string{"ns1/pg-standby1-ask": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-a"},
			},
		},
		{
			// Requirement. Between two foreign tenants at the same distance the victim queue
			// order falls back to the reverse of the allocation order: the tenant with the
			// higher share is evicted first. t2 (allocated 4 of deserved 2) ranks above t3
			// (allocated 3 of deserved 2), so t2's high-priority pod on n1 is evicted although
			// t3's low-priority pod on n2 would score cheaper by class value.
			priority: true,
			bestFit:  true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "F2: between foreign tenants the higher share is evicted first regardless of class",
				Pods: []*corev1.Pod{
					vsRunning("t2-x1", "pg-t2", "n1", "2", true, &prioHigh),
					vsRunning("t2-x2", "pg-t2", "n3", "2", false, nil),
					vsRunning("t3-x1", "pg-t3", "n2", "2", true, &prioLow),
					vsRunning("t3-x2", "pg-t3", "n4", "1", false, nil),
					vsPending("t1-driver", "pg-t1-ask", "2"),
				},
				Nodes: []*corev1.Node{vsNode("n1", "2"), vsNode("n2", "2"), vsNode("n3", "2"), vsNode("n4", "1")},
				PodGroups: []*schedulingv1beta1.PodGroup{
					vsAskPG("pg-t1-ask", "t1-leaf"), vsRunningPG("pg-t2", "t2-leaf"), vsRunningPG("pg-t3", "t3-leaf"),
				},
				Queues: []*schedulingv1beta1.Queue{
					vsQueue("root", "", "", ""),
					vsQueue("t1", "root", "4", "8"), vsQueue("t1-leaf", "t1", "4", "8"),
					vsQueue("t2", "root", "2", "8"), vsQueue("t2-leaf", "t2", "2", "8"),
					vsQueue("t3", "root", "2", "8"), vsQueue("t3-leaf", "t3", "2", "8"),
				},
				PriClass:        priClasses,
				ExpectPipeLined: map[string][]string{"ns1/pg-t1-ask": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/t2-x1"},
			},
		},
		// ------------------------------------------------------- waiting on the nominated node
		{
			// Requirement. The ask was reclaimed for in a previous session: its victim on n1 is
			// still terminating and the pod carries n1 as its nominated node. n1 has no other
			// reclaimable pod. In the documented action order allocate runs before reclaim,
			// places the ask on its nominated node onto the room the victim is freeing; the job
			// is no longer starving and reclaim evicts nothing.
			actions: documentedActions,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "R1: an ask waits on its nominated node while the victims reclaimed for it terminate",
				Pods: []*corev1.Pod{
					vsEvicted("victim-a", "pg-active", "n1", "2"),
					vsRunning("holder-a", "pg-active", "n1", "2", false, nil),
					vsRunning("exec-b", "pg-active", "n2", "2", true, nil),
					vsRunning("holder-b", "pg-active", "n2", "2", false, nil),
					vsNominated("standby-driver", "pg-standby", "2", "n1"),
				},
				Nodes:           []*corev1.Node{vsNode("n1", "4"), vsNode("n2", "4")},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          vsTree("8", "8", "0", "8", "4", "8"),
				ExpectPipeLined: standbyOnN1,
				ExpectEvictNum:  0,
			},
		},
		{
			// Requirement. The terminating room on n1 is held for the ask that paid for it:
			// the reserved pass pipelines the nominated ask there before any regular ask is
			// considered, and the second ask reclaims its own room on n2.
			actions: documentedActions,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "R2: the waiting ask reserves the terminating room against other askers",
				Pods: []*corev1.Pod{
					vsEvicted("victim-a", "pg-active", "n1", "2"),
					vsRunning("holder-a", "pg-active", "n1", "2", false, nil),
					vsRunning("exec-b", "pg-active", "n2", "2", true, nil),
					vsRunning("holder-b", "pg-active", "n2", "2", false, nil),
					vsNominated("standby-driver", "pg-standby", "2", "n1"),
					vsPending("s1-driver", "pg-s1", "2"),
				},
				Nodes:           []*corev1.Node{vsNode("n1", "4"), vsNode("n2", "4")},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby, vsRunningPG("pg-s1", "standby")},
				Queues:          vsTree("8", "8", "0", "8", "4", "8"),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}, "ns1/pg-s1": {"n2"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-b"},
			},
		},
		{
			// Requirement. A stale nomination is no protection: the nominated node's terminating
			// pod is gone and its room was taken, so the ask is planned afresh and evicts on n2.
			actions: documentedActions,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "R3: a stale nomination falls through to reclaim",
				Pods: []*corev1.Pod{
					vsRunning("holder-a1", "pg-active", "n1", "2", false, nil),
					vsRunning("holder-a2", "pg-active", "n1", "2", false, nil),
					vsRunning("exec-b", "pg-active", "n2", "2", true, nil),
					vsRunning("holder-b", "pg-active", "n2", "2", false, nil),
					vsNominated("standby-driver", "pg-standby", "2", "n1"),
				},
				Nodes:           []*corev1.Node{vsNode("n1", "4"), vsNode("n2", "4")},
				PodGroups:       []*schedulingv1beta1.PodGroup{pgActive, pgStandby},
				Queues:          vsTree("8", "8", "0", "8", "4", "8"),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n2"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-b"},
			},
		},
		{
			// Requirement. The room reclaim freed goes to the ask it was freed for, not to the
			// first ask in queue order. That order compares the tenants' shares where two leaves
			// diverge, and the failing-over tenant t1 looks the most over its deserved of all,
			// because its draining active pods still count as allocated, so another tenant's
			// over-deserved replacement would sort first and take n1, and the ask that paid for
			// n1 would reclaim a second time, on n2. The reserved pass places the nominated ask
			// on n1 before any regular ask; the replacement finds no room and, being over its
			// deserved, cannot reclaim. Nothing is evicted.
			actions: documentedActions,
			check: func(t *testing.T, ssn *framework.Session) {
				if n := len(ssn.Jobs["ns1/pg-active2"].TaskStatusIndex[api.Pipelined]); n != 0 {
					t.Errorf("pg-active2 must not be pipelined, got %d tasks", n)
				}
			},
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "R4: the ask reclaim evicted for is served before an over-deserved tenant's ask",
				Pods: []*corev1.Pod{
					vsEvicted("victim-a", "pg-active1", "n1", "2"),
					vsRunning("holder-a", "pg-active1", "n1", "2", false, nil),
					vsRunning("holder-a2", "pg-active1", "n3", "2", false, nil),
					vsRunning("s1-running", "pg-standby1-run", "n3", "2", false, nil),
					vsRunning("t2-x1", "pg-active2", "n2", "2", true, nil),
					vsRunning("t2-x2", "pg-active2", "n2", "2", false, nil),
					vsNominated("standby-driver", "pg-standby1-ask", "2", "n1"),
					vsPending("t2-exec-new", "pg-active2", "2"),
				},
				Nodes: []*corev1.Node{vsNode("n1", "4"), vsNode("n2", "4"), vsNode("n3", "4")},
				PodGroups: []*schedulingv1beta1.PodGroup{
					vsRunningPG("pg-active1", "active1"), vsRunningPG("pg-standby1-run", "standby1"),
					vsAskPG("pg-standby1-ask", "standby1"), vsRunningPG("pg-active2", "active2"),
				},
				Queues:          vsTwoTenants(),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby1-ask": {"n1"}},
				ExpectEvictNum:  0,
			},
		},
		{
			// Requirement. The reserved pass binds, not only pipelines: when the nominated node
			// already has idle room next to the terminating victim, the ask is bound there and
			// nothing else moves.
			actions: documentedActions,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "R5: a reserved ask that already fits the idle room is bound",
				Pods: []*corev1.Pod{
					vsEvicted("victim-a", "pg-active", "n1", "2"),
					vsNominated("standby-driver", "pg-standby", "2", "n1"),
					vsPending("s1-driver", "pg-s1", "2"),
				},
				Nodes:          []*corev1.Node{vsNode("n1", "4")},
				PodGroups:      []*schedulingv1beta1.PodGroup{pgActive, pgStandby, vsRunningPG("pg-s1", "standby")},
				Queues:         vsTree("8", "8", "0", "8", "4", "8"),
				ExpectBindsNum: 1,
				ExpectBindMap:  map[string]string{"ns1/standby-driver": "n1"},
				ExpectEvictNum: 0,
			},
		},
	}

	for i, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			c.Plugins = plugins
			var config []conf.Configuration
			if c.bestFit {
				args := map[string]interface{}{reclaim.VictimSelectionKey: reclaim.VictimSelectionBestFit}
				if c.maxCandidateNodes > 0 {
					args[reclaim.MaxCandidateNodesKey] = c.maxCandidateNodes
				}
				config = []conf.Configuration{{Name: reclaim.New().Name(), Arguments: args}}
			}
			ssn := c.RegisterSession(c.tiers(), config)
			defer c.Close()
			actions := defaultActions
			if c.actions != nil {
				actions = c.actions
			}
			c.Run(actions)
			if c.check != nil {
				c.check(t, ssn)
			}
			if err := c.CheckAll(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}

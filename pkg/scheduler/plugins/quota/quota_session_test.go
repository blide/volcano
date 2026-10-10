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
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/actions/enqueue"
	"volcano.sh/volcano/pkg/scheduler/actions/reclaim"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/capacity"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/plugins/priority"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// One scheduling session per case on an unmodified scheduler: the upstream enqueue, allocate and
// reclaim actions, capacity for share, this plugin for quota, in the last tier. The plugin's
// session-close pass is run explicitly after the actions so its outcome can be checked before the
// session is torn down.
//
// The fixture is a tenant failing over: queue active drains (deserved 0) while queue standby asks
// for its share, under a tenant at its capability.

const qNS = "ns1"

type qCase struct {
	uthelper.TestCommonStruct
	priority    bool
	gangReclaim bool
	level       int
	// plugin arguments
	trial          bool
	reserve        bool
	quotaReclaim   bool
	maxVictims     int
	enqueueBackoff string
	inqueueTimeout string
	// actions overrides the documented order enqueue, allocate, reclaim.
	actions []framework.Action
	check   func(t *testing.T, ssn *framework.Session)
}

func qRes(c string) corev1.ResourceList {
	if c == "" {
		return nil
	}
	return api.BuildResourceList(c, c+"Gi")
}

func qNode(name, c string) *corev1.Node {
	return util.BuildNode(name, api.BuildResourceList(c, c+"Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{})
}

func qQueue(name, parent, deserved, capability string) *schedulingv1beta1.Queue {
	q := util.BuildQueueWithResourcesQuantity(name, qRes(deserved), qRes(capability))
	q.Spec.Parent = parent
	return q
}

func qPreemptable(p bool) map[string]string {
	return map[string]string{schedulingv1beta1.PodPreemptable: strconv.FormatBool(p)}
}

func qRunning(name, pg, node, c string, preemptable bool, prio *int32) *corev1.Pod {
	return util.BuildPodWithPriority(qNS, name, node, corev1.PodRunning, qRes(c), pg, qPreemptable(preemptable), map[string]string{}, prio)
}

func qPending(name, pg, c string) *corev1.Pod {
	return util.BuildPod(qNS, name, "", corev1.PodPending, qRes(c), pg, map[string]string{}, map[string]string{})
}

// qNominated is a pending ask a previous session's reclaim pipelined on node after evicting.
func qNominated(name, pg, c, node string) *corev1.Pod {
	pod := qPending(name, pg, c)
	pod.Status.NominatedNodeName = node
	return pod
}

// qEvicted is a victim the scheduler evicted in a previous session, still terminating on node.
func qEvicted(name, pg, node, c string) *corev1.Pod {
	pod := qRunning(name, pg, node, c, true, nil)
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: corev1.PodReasonPreemptionByScheduler,
	})
	return pod
}

func qPG(name, queue, minRes string, phase schedulingv1beta1.PodGroupPhase) *schedulingv1beta1.PodGroup {
	return util.BuildPodGroupWithMinResources(name, qNS, queue, 1, nil, qRes(minRes), phase)
}

func qRunningPG(name, queue string) *schedulingv1beta1.PodGroup {
	return util.BuildPodGroup(name, qNS, queue, 1, nil, schedulingv1beta1.PodGroupRunning)
}

// withCondition adds a PodGroup condition stamped age ago.
func withCondition(pg *schedulingv1beta1.PodGroup, condType, reason string, status corev1.ConditionStatus, age time.Duration) *schedulingv1beta1.PodGroup {
	pg.Status.Conditions = append(pg.Status.Conditions, schedulingv1beta1.PodGroupCondition{
		Type:               schedulingv1beta1.PodGroupConditionType(condType),
		Status:             status,
		Reason:             reason,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-age)),
	})
	return pg
}

// failoverTree: root -> tenant (4/4) -> active (0/4), standby (4/4).
func failoverTree() []*schedulingv1beta1.Queue {
	return []*schedulingv1beta1.Queue{
		qQueue("root", "", "", ""),
		qQueue("tenant", "root", "4", "4"),
		qQueue("active", "tenant", "0", "4"),
		qQueue("standby", "tenant", "4", "4"),
	}
}

func (c qCase) tiers() []conf.Tier {
	trueValue := true
	capacityOpt := conf.PluginOption{
		Name:               capacity.PluginName,
		EnablePreemptive:   &trueValue,
		EnabledReclaimable: &trueValue,
		EnabledQueueOrder:  &trueValue,
		EnabledHierarchy:   &trueValue,
	}
	if c.level > 0 {
		capacityOpt.Arguments = framework.Arguments{"ancestorReclaimLevel": c.level}
	}
	gangOpt := conf.PluginOption{Name: gang.PluginName, EnabledJobStarving: &trueValue}
	if c.gangReclaim {
		gangOpt.EnabledReclaimable = &trueValue
	}
	opts := []conf.PluginOption{capacityOpt, {Name: predicates.PluginName, EnabledPredicate: &trueValue}, gangOpt}
	if c.priority {
		opts = append(opts, conf.PluginOption{Name: priority.PluginName, EnabledJobOrder: &trueValue, EnabledTaskOrder: &trueValue})
	}
	args := framework.Arguments{
		EnqueueAncestorCapReclaimKey: c.trial,
		ReserveDeservedKey:           c.reserve,
		QuotaReclaimKey:              c.quotaReclaim,
	}
	if c.maxVictims > 0 {
		args[MaxCrossNodeVictimsKey] = c.maxVictims
	}
	if c.enqueueBackoff != "" {
		args[EnqueueBackoffKey] = c.enqueueBackoff
	}
	if c.inqueueTimeout != "" {
		args[InqueueTimeoutKey] = c.inqueueTimeout
	}
	// The quota plugin last: its open pass uses the predicates and order functions registered
	// before it.
	opts = append(opts, conf.PluginOption{
		Name:               PluginName,
		EnabledAllocatable: &trueValue,
		EnabledJobEnqueued: &trueValue,
		EnabledHierarchy:   &trueValue,
		Arguments:          args,
	})
	return []conf.Tier{{Plugins: opts}}
}

func Test_quotaPlugin_Sessions(t *testing.T) {
	documented := []framework.Action{enqueue.New(), allocate.New(), reclaim.New()}
	prioHigh, prioLow := int32(1000), int32(100)
	pcHigh, pcLow := util.BuildPriorityClass("high", prioHigh), util.BuildPriorityClass("low", prioLow)
	priClasses := []*schedulingv1.PriorityClass{pcHigh, pcLow}

	pgActive := qRunningPG("pg-active", "active")
	// activeMin4 holds 4c with minResources 4c: no elastic credit, so the strict gate cannot admit
	// an ask on it and the trial admission is what gets the ask in.
	activeMin4 := qPG("pg-active", "active", "4", schedulingv1beta1.PodGroupRunning)
	// tenant5 gives the tenant 5c: room for a 3c ask once one 2c holder is gone.
	tenant5 := []*schedulingv1beta1.Queue{
		qQueue("root", "", "", ""), qQueue("tenant", "root", "5", "5"), qQueue("active", "tenant", "0", "5"), qQueue("standby", "tenant", "5", "5"),
	}
	standbyPending2 := func() *schedulingv1beta1.PodGroup {
		return qPG("pg-standby", "standby", "2", schedulingv1beta1.PodGroupPending)
	}
	standbyPending4 := func() *schedulingv1beta1.PodGroup {
		return qPG("pg-standby", "standby", "4", schedulingv1beta1.PodGroupPending)
	}
	standbyInqueue2 := func() *schedulingv1beta1.PodGroup {
		return qPG("pg-standby", "standby", "2", schedulingv1beta1.PodGroupInqueue)
	}
	standby := map[api.JobID]scheduling.PodGroupPhase{}
	phase := func(p scheduling.PodGroupPhase) map[api.JobID]scheduling.PodGroupPhase {
		return map[api.JobID]scheduling.PodGroupPhase{"ns1/pg-standby": p}
	}
	_ = standby
	dequeuedWith := func(reason string) func(t *testing.T, ssn *framework.Session) {
		return func(t *testing.T, ssn *framework.Session) {
			cond := findCondition(ssn.Jobs["ns1/pg-standby"], PodGroupDequeuedType)
			if cond == nil || cond.Status != corev1.ConditionTrue || cond.Reason != reason {
				t.Fatalf("want Dequeued condition with reason %s, got %+v", reason, cond)
			}
		}
	}
	notDequeued := func(t *testing.T, ssn *framework.Session) {
		if cond := findCondition(ssn.Jobs["ns1/pg-standby"], PodGroupDequeuedType); cond != nil && cond.Status == corev1.ConditionTrue {
			t.Fatalf("unexpected Dequeued condition: %+v", cond)
		}
	}
	notPipelined := func(jobID string) func(t *testing.T, ssn *framework.Session) {
		return func(t *testing.T, ssn *framework.Session) {
			if n := len(ssn.Jobs[api.JobID(jobID)].TaskStatusIndex[api.Pipelined]); n != 0 {
				t.Fatalf("%s must not be pipelined, got %d tasks", jobID, n)
			}
		}
	}

	cases := []qCase{
		// ------------------------------------------------------------ admission trial
		{
			// The ask is entitled (standby is under its deserved) and blocked only by the tenant's
			// capability, so it is admitted for a trial. n1 is full, so the reclaim action evicts
			// active's over-deserved pod for it and pipelines it there. Nothing of this plugin's
			// own reclaim is needed.
			trial: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "T1: a trial admission is served by the reclaim action on the node",
				Pods: []*corev1.Pod{
					qRunning("a1", "pg-active", "n1", "2", true, nil),
					qRunning("a2", "pg-active", "n1", "2", false, nil),
					qPending("standby-driver", "pg-standby", "2"),
				},
				Nodes:           []*corev1.Node{qNode("n1", "4")},
				PodGroups:       []*schedulingv1beta1.PodGroup{activeMin4, standbyPending2()},
				Queues:          failoverTree(),
				ExpectStatus:    phase(scheduling.PodGroupInqueue),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/a1"},
			},
		},
		{
			// volcano-sh/volcano#4817 on an unmodified reclaim: n1 has room next to active's pod,
			// so reclaim pipelines the ask there with nothing evicted although the tenant refuses
			// it; the next allocate would refuse it forever. The plugin's close pass sees a
			// pipelined task its hierarchy refuses and relieves the tenant with active's
			// over-deserved pod.
			trial:        true,
			quotaReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "T2: a pipelined ask the hierarchy refuses is served by quota-aware reclaim",
				Pods: []*corev1.Pod{
					qRunning("a1", "pg-active", "n1", "2", true, nil),
					qRunning("a2", "pg-active", "n2", "2", false, nil),
					qPending("standby-driver", "pg-standby", "2"),
				},
				Nodes:           []*corev1.Node{qNode("n1", "6"), qNode("n2", "2")},
				PodGroups:       []*schedulingv1beta1.PodGroup{activeMin4, standbyPending2()},
				Queues:          failoverTree(),
				ExpectStatus:    phase(scheduling.PodGroupInqueue),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/a1"},
			},
		},
		{
			// T2 without quotaReclaim: upstream pipelines the ask with nothing evicted, and on
			// upstream alone that repeats every session. The plugin sees a pipelined task its
			// hierarchy refuses, which is not progress, and reverts the trial at session close.
			trial: true,
			check: dequeuedWith(DequeuedReasonReclaimFailed),
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "T2b: without quotaReclaim a hierarchy-refused pipeline reverts the trial",
				Pods: []*corev1.Pod{
					qRunning("a1", "pg-active", "n1", "2", true, nil),
					qRunning("a2", "pg-active", "n2", "2", false, nil),
					qPending("standby-driver", "pg-standby", "2"),
				},
				Nodes:          []*corev1.Node{qNode("n1", "6"), qNode("n2", "2")},
				PodGroups:      []*schedulingv1beta1.PodGroup{activeMin4, standbyPending2()},
				Queues:         failoverTree(),
				ExpectStatus:   phase(scheduling.PodGroupPending),
				ExpectEvictNum: 0,
			},
		},
		{
			// Victims spread over nodes: the tenant is 4c over for the 4c ask, active holds 2c on
			// each of two small nodes, and only the empty n1 can host the ask. The upstream
			// reclaim action skips n1 (no reclaimees there) and cannot fit the ask on n2 or n3,
			// so it leaves the trial admission unserved; the plugin's planner places the ask on
			// n1 and evicts both holders wherever they run.
			trial:        true,
			quotaReclaim: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "T3: victims spread over nodes are evicted for the ask's quota",
				Pods: []*corev1.Pod{
					qRunning("a1", "pg-active", "n2", "2", true, nil),
					qRunning("a2", "pg-active", "n3", "2", true, nil),
					qPending("standby-driver", "pg-standby", "4"),
				},
				Nodes:           []*corev1.Node{qNode("n1", "4"), qNode("n2", "2"), qNode("n3", "2")},
				PodGroups:       []*schedulingv1beta1.PodGroup{activeMin4, standbyPending4()},
				Queues:          failoverTree(),
				ExpectStatus:    phase(scheduling.PodGroupInqueue),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  2,
				ExpectEvicted:   []string{"ns1/a1", "ns1/a2"},
			},
		},
		{
			// Only victims under the blocking ancestor count. t1 is at its cap with 4c of usage,
			// 2c of it reclaimable; the 4c ask needs 4c gone. The upstream reclaim action, which
			// stops at node fit, evicts t2's over-deserved pod on n1 and pipelines the ask there
			// although t1 still refuses it; that eviction is upstream's and the plugin cannot undo
			// it. The plugin's quota round takes nothing outside t1's subtree, finds t1 cannot be
			// relieved, rolls its own evictions back, and reverts the trial: a pipelined task the
			// hierarchy refuses is not progress.
			trial:        true,
			quotaReclaim: true,
			check:        dequeuedWith(DequeuedReasonReclaimFailed),
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "T4: no eviction outside the blocked subtree by the plugin; the trial is reverted",
				Pods: []*corev1.Pod{
					qRunning("t2-x", "pg-active2", "n1", "2", true, nil),
					qRunning("exec-a", "pg-active1", "n2", "2", true, nil),
					qRunning("holder-a", "pg-active1", "n3", "2", false, nil),
					qPending("standby-driver", "pg-standby", "4"),
				},
				Nodes: []*corev1.Node{qNode("n1", "4"), qNode("n2", "4"), qNode("n3", "2")},
				PodGroups: []*schedulingv1beta1.PodGroup{
					qPG("pg-active1", "active1", "4", schedulingv1beta1.PodGroupRunning), qPG("pg-standby", "standby1", "4", schedulingv1beta1.PodGroupPending), qPG("pg-active2", "active2", "2", schedulingv1beta1.PodGroupRunning),
				},
				Queues: []*schedulingv1beta1.Queue{
					qQueue("root", "", "", ""),
					qQueue("t1", "root", "4", "4"), qQueue("active1", "t1", "0", "4"), qQueue("standby1", "t1", "4", "4"),
					qQueue("t2", "root", "2", "8"), qQueue("active2", "t2", "0", "8"),
				},
				ExpectStatus:   phase(scheduling.PodGroupPending),
				ExpectEvictNum: 1,
				ExpectEvicted:  []string{"ns1/t2-x"},
			},
		},
		{
			// The quota round stops at the first pass: the tenant (5c) is 2c short for the 3c ask
			// and active holds two 2c executors on small full nodes; the ask fits only the empty
			// n1, where upstream reclaim does not look. Only the cheaper executor is evicted.
			trial:        true,
			quotaReclaim: true,
			priority:     true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "T5: the quota round evicts only what the shortfall needs",
				Pods: []*corev1.Pod{
					qRunning("a1", "pg-active", "n2", "2", true, &prioLow),
					qRunning("a2", "pg-active", "n3", "2", true, &prioHigh),
					qPending("standby-driver", "pg-standby", "3"),
				},
				Nodes:           []*corev1.Node{qNode("n1", "4"), qNode("n2", "2"), qNode("n3", "2")},
				PodGroups:       []*schedulingv1beta1.PodGroup{activeMin4, qPG("pg-standby", "standby", "3", schedulingv1beta1.PodGroupPending)},
				Queues:          tenant5,
				PriClass:        priClasses,
				ExpectStatus:    phase(scheduling.PodGroupInqueue),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/a1"},
			},
		},
		{
			// Gang's veto applies across nodes: the active job has a driver on n2 and an executor
			// on n3, minAvailable 1, so gang admits exactly one victim in arrival order; the
			// candidates arrive cheapest first, so the executor goes and the driver stays.
			trial:        true,
			quotaReclaim: true,
			priority:     true,
			gangReclaim:  true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "T6: gang's veto lands on the driver, not the executor, across nodes",
				Pods: []*corev1.Pod{
					qRunning("driver-a", "pg-active", "n2", "2", true, &prioHigh),
					qRunning("exec-a", "pg-active", "n3", "2", true, &prioLow),
					qPending("standby-driver", "pg-standby", "3"),
				},
				Nodes:           []*corev1.Node{qNode("n1", "4"), qNode("n2", "2"), qNode("n3", "2")},
				PodGroups:       []*schedulingv1beta1.PodGroup{activeMin4, qPG("pg-standby", "standby", "3", schedulingv1beta1.PodGroupPending)},
				Queues:          tenant5,
				PriClass:        priClasses,
				ExpectStatus:    phase(scheduling.PodGroupInqueue),
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  1,
				ExpectEvicted:   []string{"ns1/exec-a"},
			},
		},
		{
			// Not entitled: standby already holds its deserved, so the ask would take it over and
			// the strict gate's rejection stands (the holders carry minResources, so the gate has
			// no elastic credit to admit on). No trial, no Dequeued condition.
			trial:        true,
			quotaReclaim: true,
			check:        notDequeued,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "T7: a leaf over its deserved is not admitted on entitlement",
				Pods: []*corev1.Pod{
					qRunning("a1", "pg-active", "n1", "2", true, nil),
					qRunning("s-run", "pg-s-run", "n2", "2", false, nil),
					qPending("standby-driver", "pg-standby", "2"),
				},
				Nodes:     []*corev1.Node{qNode("n1", "4"), qNode("n2", "4")},
				PodGroups: []*schedulingv1beta1.PodGroup{qPG("pg-active", "active", "2", schedulingv1beta1.PodGroupRunning), qPG("pg-s-run", "standby", "2", schedulingv1beta1.PodGroupRunning), standbyPending2()},
				Queues: []*schedulingv1beta1.Queue{
					qQueue("root", "", "", ""),
					qQueue("tenant", "root", "4", "4"),
					qQueue("active", "tenant", "0", "4"),
					qQueue("standby", "tenant", "2", "4"),
				},
				ExpectStatus:   phase(scheduling.PodGroupPending),
				ExpectEvictNum: 0,
			},
		},
		// ------------------------------------------------------------ backoff and timeout
		{
			// Dequeued ten seconds ago with a one-minute backoff: admissible, but rejected.
			trial:          true,
			enqueueBackoff: "1m",
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:           "B1: a recently dequeued PodGroup stays Pending for the backoff",
				Pods:           []*corev1.Pod{qPending("standby-driver", "pg-standby", "2")},
				Nodes:          []*corev1.Node{qNode("n1", "4")},
				PodGroups:      []*schedulingv1beta1.PodGroup{withCondition(standbyPending2(), string(PodGroupDequeuedType), DequeuedReasonReclaimFailed, corev1.ConditionTrue, 10*time.Second)},
				Queues:         failoverTree(),
				ExpectStatus:   phase(scheduling.PodGroupPending),
				ExpectBindsNum: 0,
			},
		},
		{
			// The same, dequeued two minutes ago: admitted and bound.
			trial:          true,
			enqueueBackoff: "1m",
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:           "B2: after the backoff the PodGroup is admitted again",
				Pods:           []*corev1.Pod{qPending("standby-driver", "pg-standby", "2")},
				Nodes:          []*corev1.Node{qNode("n1", "4")},
				PodGroups:      []*schedulingv1beta1.PodGroup{withCondition(standbyPending2(), string(PodGroupDequeuedType), DequeuedReasonReclaimFailed, corev1.ConditionTrue, 2*time.Minute)},
				Queues:         failoverTree(),
				ExpectBindsNum: 1,
				ExpectBindMap:  map[string]string{"ns1/standby-driver": "n1"},
			},
		},
		{
			// An admitted PodGroup with no pods (a Volcano Job before its controller acts) and no
			// verdict is stamped on first sight and returned to Pending after the timeout.
			inqueueTimeout: "1m",
			check:          dequeuedWith(DequeuedReasonInqueueTimeout),
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:         "B3: an Inqueue PodGroup with nothing placed times out",
				Nodes:        []*corev1.Node{qNode("n1", "4")},
				PodGroups:    []*schedulingv1beta1.PodGroup{pgActive, withCondition(standbyInqueue2(), string(PodGroupInqueueType), InqueueReasonWaiting, corev1.ConditionTrue, 2*time.Minute)},
				Queues:       failoverTree(),
				ExpectStatus: phase(scheduling.PodGroupPending),
			},
		},
		{
			inqueueTimeout: "1m",
			check:          notDequeued,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:         "B4: before the timeout the PodGroup stays Inqueue",
				Nodes:        []*corev1.Node{qNode("n1", "4")},
				PodGroups:    []*schedulingv1beta1.PodGroup{pgActive, standbyInqueue2()},
				Queues:       failoverTree(),
				ExpectStatus: phase(scheduling.PodGroupInqueue),
			},
		},
		// ------------------------------------------------------------ reserved asks
		{
			// The ask was reclaimed for in a previous session: its victim on n1 is terminating and
			// the pod carries n1 as its nominated node. The open pass pipelines it there before the
			// actions run; the job is not starving and reclaim evicts nothing more.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "R1: an ask waits on its nominated node while the victims reclaimed for it terminate",
				Pods: []*corev1.Pod{
					qEvicted("victim-a", "pg-active", "n1", "2"),
					qRunning("holder-a", "pg-active", "n1", "2", false, nil),
					qRunning("exec-b", "pg-active", "n2", "2", true, nil),
					qRunning("holder-b", "pg-active", "n2", "2", false, nil),
					qNominated("standby-driver", "pg-standby", "2", "n1"),
				},
				Nodes:     []*corev1.Node{qNode("n1", "4"), qNode("n2", "4")},
				PodGroups: []*schedulingv1beta1.PodGroup{pgActive, standbyInqueue2()},
				Queues: []*schedulingv1beta1.Queue{
					qQueue("root", "", "", ""), qQueue("tenant", "root", "8", "8"), qQueue("active", "tenant", "0", "8"), qQueue("standby", "tenant", "4", "8"),
				},
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  0,
			},
		},
		{
			// The room reclaim freed goes to the ask it was freed for: the failing-over tenant
			// looks the most over its deserved of all, so another tenant's over-deserved
			// replacement would sort first and take n1 in allocate. The open pass places the
			// nominated ask on n1 first; the replacement finds no room and, over its deserved,
			// cannot reclaim.
			check: notPipelined("ns1/pg-active2"),
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "R2: the ask reclaim evicted for is served before an over-deserved tenant's ask",
				Pods: []*corev1.Pod{
					qEvicted("victim-a", "pg-active1", "n1", "2"),
					qRunning("holder-a", "pg-active1", "n1", "2", false, nil),
					qRunning("holder-a2", "pg-active1", "n3", "2", false, nil),
					qRunning("s1-running", "pg-standby1-run", "n3", "2", false, nil),
					qRunning("t2-x1", "pg-active2", "n2", "2", true, nil),
					qRunning("t2-x2", "pg-active2", "n2", "2", false, nil),
					qNominated("standby-driver", "pg-standby", "2", "n1"),
					qPending("t2-exec-new", "pg-active2", "2"),
				},
				Nodes: []*corev1.Node{qNode("n1", "4"), qNode("n2", "4"), qNode("n3", "4")},
				PodGroups: []*schedulingv1beta1.PodGroup{
					qRunningPG("pg-active1", "active1"), qRunningPG("pg-standby1-run", "standby1"),
					qPG("pg-standby", "standby1", "2", schedulingv1beta1.PodGroupInqueue), qRunningPG("pg-active2", "active2"),
				},
				Queues: []*schedulingv1beta1.Queue{
					qQueue("root", "", "", ""),
					qQueue("t1", "root", "4", "10"), qQueue("active1", "t1", "0", "10"), qQueue("standby1", "t1", "4", "10"),
					qQueue("t2", "root", "3", "8"), qQueue("active2", "t2", "0", "8"),
				},
				ExpectPipeLined: map[string][]string{"ns1/pg-standby": {"n1"}},
				ExpectEvictNum:  0,
			},
		},
		{
			// The open pass binds, not only pipelines: the nominated node has idle room next to
			// the terminating victim.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "R3: a reserved ask that already fits the idle room is bound",
				Pods: []*corev1.Pod{
					qEvicted("victim-a", "pg-active", "n1", "2"),
					qNominated("standby-driver", "pg-standby", "2", "n1"),
				},
				Nodes:     []*corev1.Node{qNode("n1", "4")},
				PodGroups: []*schedulingv1beta1.PodGroup{pgActive, standbyInqueue2()},
				Queues: []*schedulingv1beta1.Queue{
					qQueue("root", "", "", ""), qQueue("tenant", "root", "8", "8"), qQueue("active", "tenant", "0", "8"), qQueue("standby", "tenant", "4", "8"),
				},
				ExpectBindsNum: 1,
				ExpectBindMap:  map[string]string{"ns1/standby-driver": "n1"},
				ExpectEvictNum: 0,
			},
		},
		// ------------------------------------------------------------ reserve
		{
			// The session after a reclaim: a1 is terminating, its replacement is pending, and
			// active sorts first in allocate (share 0.5 against standby's 0.6). With the reserve
			// the tenant refuses the replacement the share standby's admitted ask is owed, and the
			// ask takes the freed room.
			reserve: true,
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "S1: the owed share is kept for the admitted ask; the replacement waits",
				Pods: []*corev1.Pod{
					qEvicted("a1", "pg-active", "n1", "3"),
					qRunning("a2", "pg-active", "n1", "2", false, nil),
					qRunning("s1", "pg-s1", "n1", "3", false, nil),
					qPending("a1-replacement", "pg-active", "3"),
					qPending("standby-driver", "pg-standby", "2"),
				},
				Nodes:     []*corev1.Node{qNode("n1", "12")},
				PodGroups: []*schedulingv1beta1.PodGroup{pgActive, qRunningPG("pg-s1", "standby"), standbyInqueue2()},
				Queues: []*schedulingv1beta1.Queue{
					qQueue("root", "", "", ""), qQueue("tenant", "root", "9", "9"), qQueue("active", "tenant", "4", "9"), qQueue("standby", "tenant", "5", "9"),
				},
				ExpectBindsNum: 1,
				ExpectBindMap:  map[string]string{"ns1/standby-driver": "n1"},
				ExpectEvictNum: 0,
			},
		},
		{
			// S1 without the reserve: the replacement takes the room and the eviction was spent
			// for nothing.
			TestCommonStruct: uthelper.TestCommonStruct{
				Name: "S1b: without the reserve the replacement takes the freed room",
				Pods: []*corev1.Pod{
					qEvicted("a1", "pg-active", "n1", "3"),
					qRunning("a2", "pg-active", "n1", "2", false, nil),
					qRunning("s1", "pg-s1", "n1", "3", false, nil),
					qPending("a1-replacement", "pg-active", "3"),
					qPending("standby-driver", "pg-standby", "2"),
				},
				Nodes:     []*corev1.Node{qNode("n1", "12")},
				PodGroups: []*schedulingv1beta1.PodGroup{pgActive, qRunningPG("pg-s1", "standby"), standbyInqueue2()},
				Queues: []*schedulingv1beta1.Queue{
					qQueue("root", "", "", ""), qQueue("tenant", "root", "9", "9"), qQueue("active", "tenant", "4", "9"), qQueue("standby", "tenant", "5", "9"),
				},
				ExpectBindsNum: 1,
				ExpectBindMap:  map[string]string{"ns1/a1-replacement": "n1"},
				ExpectEvictNum: 0,
			},
		},
	}

	for i, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			var qp *quotaPlugin
			c.Plugins = map[string]framework.PluginBuilder{
				capacity.PluginName:   capacity.New,
				predicates.PluginName: predicates.New,
				gang.PluginName:       gang.New,
				priority.PluginName:   priority.New,
				PluginName: func(args framework.Arguments) framework.Plugin {
					qp = New(args).(*quotaPlugin)
					return qp
				},
			}
			ssn := c.RegisterSession(c.tiers(), nil)
			defer c.Close()
			actions := documented
			if c.actions != nil {
				actions = c.actions
			}
			c.Run(actions)
			// The session-close pass, run here so its outcome is visible to the checks; the
			// deferred Close runs it again as a no-op.
			qp.OnSessionClose(ssn)
			if c.check != nil {
				c.check(t, ssn)
			}
			if err := c.CheckAll(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}

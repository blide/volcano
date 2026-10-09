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
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/actions/enqueue"
	"volcano.sh/volcano/pkg/scheduler/actions/reclaim"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// Unit tests for the reserveDeserved arithmetic and a two-session end-to-end run of the drain
// window it is meant for. The single-session shapes are in capacity_gap_reclaim_test.go (H*).

func res(cpu, mem float64, scalars ...float64) *api.Resource {
	r := &api.Resource{MilliCPU: cpu, Memory: mem}
	if len(scalars) > 0 {
		r.SetScalar("nvidia.com/gpu", scalars[0])
	}
	return r
}

func Test_leafOwed(t *testing.T) {
	cases := []struct {
		name                          string
		deserved, allocated, unplaced *api.Resource
		want                          *api.Resource
	}{
		{"within deserved: owed the whole unplaced", res(5000, 5), res(3000, 3), res(2000, 2), res(2000, 2)},
		{"partly over deserved: owed up to deserved", res(4000, 4), res(3000, 3), res(2000, 2), res(1000, 1)},
		{"already over deserved: owed nothing", res(2000, 2), res(3000, 3), res(2000, 2), res(0, 0)},
		{"nothing unplaced", res(5000, 5), res(3000, 3), res(0, 0), res(0, 0)},
		{"dimension missing from deserved is owed nothing", res(5000, 0), res(0, 0), res(2000, 2), res(2000, 0)},
		{"scalar within deserved", res(5000, 5, 2), res(0, 0, 1), res(1000, 1, 1), res(1000, 1, 1)},
		{"scalar over deserved", res(5000, 5, 1), res(0, 0, 1), res(1000, 1, 1), res(1000, 1)},
		{"scalar missing from deserved", res(5000, 5), res(0, 0), res(1000, 1, 1), res(1000, 1)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := leafOwed(c.deserved, c.allocated, c.unplaced)
			if !got.Equal(c.want, api.Zero) {
				t.Fatalf("want %v, got %v", c.want, got)
			}
		})
	}
}

func Test_parentReserve(t *testing.T) {
	cases := []struct {
		name                string
		deserved, allocated *api.Resource
		children            []*api.Resource
		want                *api.Resource
	}{
		{"sum of children within the parent's headroom", res(9000, 9), res(5000, 5), []*api.Resource{res(2000, 2), res(1000, 1)}, res(3000, 3)},
		{"capped by the parent's deserved minus allocated", res(6000, 6), res(5000, 5), []*api.Resource{res(2000, 2)}, res(1000, 1)},
		{"parent over its deserved reserves nothing", res(4000, 4), res(5000, 5), []*api.Resource{res(2000, 2)}, res(0, 0)},
		{"no deserved on the parent passes the sum through", res(0, 0), res(5000, 5), []*api.Resource{res(2000, 2)}, res(2000, 2)},
		{"no children", res(9000, 9), res(5000, 5), nil, res(0, 0)},
		{"scalar capped", res(9000, 9, 1), res(0, 0, 0), []*api.Resource{res(0, 0, 2)}, res(0, 0, 1)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parentReserve(c.deserved, c.allocated, c.children)
			if !got.Equal(c.want, api.Zero) {
				t.Fatalf("want %v, got %v", c.want, got)
			}
		})
	}
}

func Test_jobUnplaced(t *testing.T) {
	minRes := cpuMem("4")
	job := api.NewJobInfo("ns1/pg")
	job.PodGroup = &api.PodGroup{PodGroup: scheduling.PodGroup{
		Spec:   scheduling.PodGroupSpec{MinMember: 2, MinResources: &minRes},
		Status: scheduling.PodGroupStatus{Phase: scheduling.PodGroupInqueue},
	}}
	pending := api.NewTaskInfo(util.BuildPod("ns1", "p1", "", corev1.PodPending, cpuMem("2"), "pg", nil, nil))
	job.AddTaskInfo(pending)
	if got := jobUnplaced(job); got.MilliCPU != 4000 {
		t.Fatalf("nothing placed: want 4000m, got %v", got)
	}
	// A pipelined task counts as placed although JobInfo.Allocated does not include it.
	pipelined := api.NewTaskInfo(util.BuildPod("ns1", "p2", "", corev1.PodPending, cpuMem("1"), "pg", nil, nil))
	job.AddTaskInfo(pipelined)
	job.UpdateTaskStatus(pipelined, api.Pipelined)
	if got := jobUnplaced(job); got.MilliCPU != 3000 {
		t.Fatalf("one pipelined: want 3000m, got %v", got)
	}
	job.UpdateTaskStatus(pending, api.Allocated)
	if got := jobUnplaced(job); got.MilliCPU != 1000 {
		t.Fatalf("one pipelined, one allocated: want 1000m, got %v", got)
	}
}

// Test_capacityPlugin_ReserveDeservedAcrossSessions runs the drain window end to end over one
// cache: session 1 reclaims a1 for standby's ask and pipelines it; between the sessions a1 goes
// terminating and its replacement appears; session 2 runs allocate before reclaim. With
// reserveDeserved the ask gets the freed space and nothing else is evicted; without it the
// replacement takes the space and the eviction was spent for nothing.
func Test_capacityPlugin_ReserveDeservedAcrossSessions(t *testing.T) {
	for _, reserve := range []bool{false, true} {
		t.Run(fmt.Sprintf("reserveDeserved=%t", reserve), func(t *testing.T) {
			framework.RegisterPluginBuilder(PluginName, New)
			framework.RegisterPluginBuilder(predicates.PluginName, predicates.New)
			framework.RegisterPluginBuilder(gang.PluginName, gang.New)
			defer framework.CleanupPluginBuilders()

			binder := util.NewFakeBinder(0)
			evictor := util.NewFakeEvictor(0)
			schedulerCache := cache.NewCustomMockSchedulerCache("ut-reserve-deserved", binder, evictor, &util.FakeStatusUpdater{}, nil, nil)
			stop := make(chan struct{})
			defer close(stop)
			schedulerCache.Run(stop)
			schedulerCache.WaitForCacheSync(stop)

			if err := schedulerCache.AddOrUpdateNode(gapNode("n1", "12")); err != nil {
				t.Fatal(err)
			}
			for _, q := range []*schedulingv1beta1.Queue{
				gapQueue("root", "", "", ""),
				gapQueue("tenant", "root", "9", "9"),
				gapQueue("active", "tenant", "4", "9"),
				gapQueue("standby", "tenant", "5", "9"),
			} {
				schedulerCache.AddQueueV1beta1(q)
			}
			schedulerCache.AddPodGroupV1beta1(gapRunningPG("pg-active", "active"))
			schedulerCache.AddPodGroupV1beta1(gapRunningPG("pg-s1", "standby"))
			schedulerCache.AddPodGroupV1beta1(util.BuildPodGroupWithMinResources("pg-standby", gapNS, "standby", 1, nil, cpuMem("2"), schedulingv1beta1.PodGroupInqueue))
			// active holds 5c against deserved 4c under a full tenant (cap 9c): a1 is the only
			// admissible victim for standby's 2c ask.
			a1 := gapRunningPod("a1", "pg-active", "3", true)
			for _, p := range []*corev1.Pod{
				a1,
				gapRunningPod("a2", "pg-active", "2", false),
				gapRunningPod("s1", "pg-s1", "3", false),
				gapPendingPod("standby-driver", "pg-standby", "2"),
			} {
				schedulerCache.AddPod(p)
			}

			tiers := gapTiers(0, false, false, false, false, false, reserve)
			actions := []framework.Action{enqueue.New(), allocate.New(), reclaim.New()}
			runSession := func() {
				ssn := framework.OpenSession(schedulerCache, tiers, []conf.Configuration{})
				for _, a := range actions {
					a.Execute(ssn)
				}
				framework.CloseSession(ssn)
			}
			recv := func(ch chan string, want string) {
				t.Helper()
				select {
				case got := <-ch:
					if got != want {
						t.Fatalf("want %s, got %s", want, got)
					}
				case <-time.After(time.Second):
					t.Fatalf("timed out waiting for %s", want)
				}
			}
			quiet := func(ch chan string) {
				t.Helper()
				select {
				case got := <-ch:
					t.Fatalf("unexpected %s", got)
				case <-time.After(300 * time.Millisecond):
				}
			}

			// Session 1: allocate cannot place the ask (tenant 8 + 2 > 9); reclaim evicts a1.
			runSession()
			recv(evictor.Channel, "ns1/a1")
			quiet(binder.Channel)

			// Between the sessions: a1 is terminating and its replacement is created.
			a1Terminating := a1.DeepCopy()
			now := metav1.Now()
			a1Terminating.DeletionTimestamp = &now
			schedulerCache.UpdatePod(a1, a1Terminating)
			schedulerCache.AddPod(gapPendingPod("a1-replacement", "pg-active", "3"))

			// Session 2: active sorts first (share 0.5 against standby's 0.6).
			runSession()
			if reserve {
				recv(binder.Channel, "ns1/standby-driver")
			} else {
				recv(binder.Channel, "ns1/a1-replacement")
			}
			quiet(binder.Channel)
			quiet(evictor.Channel)
			if got := evictor.Evicts(); len(got) != 1 {
				t.Fatalf("want exactly one eviction over both sessions, got %v", got)
			}
		})
	}
}

func Test_capacityPlugin_parseReserveDeserved(t *testing.T) {
	cases := []struct {
		name string
		args framework.Arguments
		want bool
	}{
		{name: "default off", args: framework.Arguments{}, want: false},
		{name: "enabled", args: framework.Arguments{reserveDeservedKey: true}, want: true},
		{name: "explicitly off", args: framework.Arguments{reserveDeservedKey: false}, want: false},
		{name: "invalid value falls back to off", args: framework.Arguments{reserveDeservedKey: "nope"}, want: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cp := New(c.args).(*capacityPlugin)
			cp.parseArguments()
			if cp.reserveDeserved != c.want {
				t.Fatalf("%s=%v: want %t, got %t", reserveDeservedKey, c.args[reserveDeservedKey], c.want, cp.reserveDeserved)
			}
		})
	}
}

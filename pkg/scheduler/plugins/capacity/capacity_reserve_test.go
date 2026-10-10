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

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/actions/enqueue"
	"volcano.sh/volcano/pkg/scheduler/actions/reclaim"
	"volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/plugins/quota"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// A two-session end-to-end run of the drain window the quota plugin's reserveDeserved is meant for.
// The single-session shapes are the H cases in capacity_gap_reclaim_test.go; the arithmetic is unit
// tested in the quota package.

// recv waits for want on ch.
func recv(t *testing.T, ch chan string, want string) {
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

// quiet fails if either channel delivers anything within 300ms.
func quiet(t *testing.T, binds, evicts chan string) {
	t.Helper()
	select {
	case got := <-binds:
		t.Fatalf("unexpected bind %s", got)
	case got := <-evicts:
		t.Fatalf("unexpected eviction %s", got)
	case <-time.After(300 * time.Millisecond):
	}
}

// Test_capacityPlugin_ReserveDeservedAcrossSessions runs the drain window end to end over one
// cache, on the H-section tree: session 1 reclaims a1 for standby's ask and pipelines it; between
// the sessions a1 goes terminating and its replacement appears; session 2 runs allocate before
// reclaim. With reserveDeserved the ask gets the freed space and nothing else is evicted; without
// it the replacement takes the space and the eviction was spent for nothing.
func Test_capacityPlugin_ReserveDeservedAcrossSessions(t *testing.T) {
	for _, reserve := range []bool{false, true} {
		t.Run(fmt.Sprintf("reserveDeserved=%t", reserve), func(t *testing.T) {
			framework.RegisterPluginBuilder(PluginName, New)
			framework.RegisterPluginBuilder(quota.PluginName, quota.New)
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
			for _, q := range reserveTree("9", "9") {
				schedulerCache.AddQueueV1beta1(q)
			}
			schedulerCache.AddPodGroupV1beta1(gapRunningPG("pg-active", "active"))
			schedulerCache.AddPodGroupV1beta1(gapRunningPG("pg-s1", "standby"))
			schedulerCache.AddPodGroupV1beta1(gapMinResPG("pg-standby", "standby", cpuMem("2"), schedulingv1beta1.PodGroupInqueue))
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

			tiers := gapCase{reserveDeserved: reserve}.tiers()
			actions := []framework.Action{enqueue.New(), allocate.New(), reclaim.New()}
			runSession := func() {
				ssn := framework.OpenSession(schedulerCache, tiers, []conf.Configuration{})
				for _, a := range actions {
					a.Execute(ssn)
				}
				framework.CloseSession(ssn)
			}

			// Session 1: allocate cannot place the ask (tenant 8 + 2 > 9); reclaim evicts a1.
			runSession()
			recv(t, evictor.Channel, gapNS+"/a1")
			quiet(t, binder.Channel, evictor.Channel)

			// Between the sessions: a1 is terminating and its replacement is created.
			a1Terminating := a1.DeepCopy()
			now := metav1.Now()
			a1Terminating.DeletionTimestamp = &now
			schedulerCache.UpdatePod(a1, a1Terminating)
			schedulerCache.AddPod(gapPendingPod("a1-replacement", "pg-active", "3"))

			// Session 2: active sorts first (share 0.5 against standby's 0.6).
			runSession()
			if reserve {
				recv(t, binder.Channel, gapNS+"/standby-driver")
			} else {
				recv(t, binder.Channel, gapNS+"/a1-replacement")
			}
			quiet(t, binder.Channel, evictor.Channel)
			if got := evictor.Evicts(); len(got) != 1 {
				t.Fatalf("want exactly one eviction over both sessions, got %v", got)
			}
		})
	}
}

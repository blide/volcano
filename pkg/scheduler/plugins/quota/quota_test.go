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
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// Unit tests for the reserve arithmetic and the arguments. The end-to-end shapes run through the
// capacity plugin's gap table, which configures this plugin next to capacity.

const quotaNS = "ns1"

func quotaRes(c string) corev1.ResourceList {
	return api.BuildResourceList(c, c+"Gi")
}

func res(milliCPU, mem float64) *api.Resource {
	return &api.Resource{MilliCPU: milliCPU, Memory: mem}
}

func resGPU(milliCPU, mem, gpu float64) *api.Resource {
	r := res(milliCPU, mem)
	r.SetScalar("nvidia.com/gpu", gpu)
	return r
}

// attrWith builds a queueAttr with the given deserved and allocated and one child per reserve.
func attrWith(deserved, allocated *api.Resource, childReserves ...*api.Resource) *queueAttr {
	attr := &queueAttr{deserved: deserved, allocated: allocated, children: map[api.QueueID]*queueAttr{}}
	for i, r := range childReserves {
		attr.children[api.QueueID(fmt.Sprintf("c%d", i))] = &queueAttr{reserve: r}
	}
	return attr
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
		{"scalar within deserved", resGPU(5000, 5, 2), resGPU(0, 0, 1), resGPU(1000, 1, 1), resGPU(1000, 1, 1)},
		{"scalar over deserved", resGPU(5000, 5, 1), resGPU(0, 0, 1), resGPU(1000, 1, 1), res(1000, 1)},
		{"scalar missing from deserved", res(5000, 5), res(0, 0), resGPU(1000, 1, 1), res(1000, 1)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := leafOwed(c.deserved, c.allocated, c.unplaced); !got.Equal(c.want, api.Zero) {
				t.Fatalf("want %v, got %v", c.want, got)
			}
		})
	}
}

func Test_parentReserve(t *testing.T) {
	cases := []struct {
		name string
		attr *queueAttr
		want *api.Resource
	}{
		{"sum of children within the parent's headroom", attrWith(res(9000, 9), res(5000, 5), res(2000, 2), res(1000, 1)), res(3000, 3)},
		{"capped by the parent's deserved minus allocated", attrWith(res(6000, 6), res(5000, 5), res(2000, 2)), res(1000, 1)},
		{"parent over its deserved reserves nothing", attrWith(res(4000, 4), res(5000, 5), res(2000, 2)), res(0, 0)},
		{"no deserved on the parent passes the sum through", attrWith(res(0, 0), res(5000, 5), res(2000, 2)), res(2000, 2)},
		{"no children", attrWith(res(9000, 9), res(5000, 5)), res(0, 0)},
		{"scalar capped", attrWith(resGPU(9000, 9, 1), resGPU(0, 0, 0), resGPU(0, 0, 2)), resGPU(0, 0, 1)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := parentReserve(c.attr); !got.Equal(c.want, api.Zero) {
				t.Fatalf("want %v, got %v", c.want, got)
			}
		})
	}
}

func Test_jobUnplaced(t *testing.T) {
	minRes := quotaRes("4")
	job := api.NewJobInfo(quotaNS + "/pg")
	job.PodGroup = &api.PodGroup{PodGroup: scheduling.PodGroup{
		Spec:   scheduling.PodGroupSpec{MinMember: 2, MinResources: &minRes},
		Status: scheduling.PodGroupStatus{Phase: scheduling.PodGroupInqueue},
	}}
	pending := api.NewTaskInfo(util.BuildPod(quotaNS, "p1", "", corev1.PodPending, quotaRes("2"), "pg", nil, nil))
	job.AddTaskInfo(pending)
	if got := jobUnplaced(job); got.MilliCPU != 4000 {
		t.Fatalf("nothing placed: want 4000m, got %v", got)
	}
	// A pipelined task counts as placed although JobInfo.Allocated does not include it.
	pipelined := api.NewTaskInfo(util.BuildPod(quotaNS, "p2", "", corev1.PodPending, quotaRes("1"), "pg", nil, nil))
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

func Test_quotaPlugin_parseArguments(t *testing.T) {
	cases := []struct {
		name string
		args framework.Arguments
		want bool
	}{
		{name: "default off", args: framework.Arguments{}, want: false},
		{name: "enabled", args: framework.Arguments{ReserveDeservedKey: true}, want: true},
		{name: "explicitly off", args: framework.Arguments{ReserveDeservedKey: false}, want: false},
		{name: "invalid value falls back to off", args: framework.Arguments{ReserveDeservedKey: "nope"}, want: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cp := New(c.args).(*quotaPlugin)
			cp.parseArguments()
			if cp.reserveDeserved != c.want {
				t.Fatalf("%s=%v: want %t, got %t", ReserveDeservedKey, c.args[ReserveDeservedKey], c.want, cp.reserveDeserved)
			}
		})
	}
}

func Test_quotaPlugin_parseEnqueueAncestorCapReclaim(t *testing.T) {
	cases := []struct {
		name string
		args framework.Arguments
		want bool
	}{
		{name: "default off", args: framework.Arguments{}, want: false},
		{name: "enabled", args: framework.Arguments{EnqueueAncestorCapReclaimKey: true}, want: true},
		{name: "explicitly off", args: framework.Arguments{EnqueueAncestorCapReclaimKey: false}, want: false},
		{name: "invalid value falls back to off", args: framework.Arguments{EnqueueAncestorCapReclaimKey: "nope"}, want: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			qp := New(c.args).(*quotaPlugin)
			qp.parseArguments()
			if qp.enqueueAncestorCapReclaim != c.want {
				t.Fatalf("%s=%v: want %t, got %t", EnqueueAncestorCapReclaimKey, c.args[EnqueueAncestorCapReclaimKey], c.want, qp.enqueueAncestorCapReclaim)
			}
		})
	}
}

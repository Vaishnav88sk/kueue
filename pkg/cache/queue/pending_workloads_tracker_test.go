/*
Copyright The Kubernetes Authors.

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

package queue

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"

	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	utilworkload "sigs.k8s.io/kueue/pkg/workload"
)

func TestPendingWorkloadsTracker(t *testing.T) {
	keyFunc := workloadKey
	lessFunc := func(a, b *utilworkload.Info) bool {
		return a.Obj.Name < b.Obj.Name
	}
	cpu := corev1.ResourceCPU

	testCases := []struct {
		name     string
		actions  func(*pendingWorkloadsTracker, *utilworkload.Info, *utilworkload.Info)
		wantRes  map[corev1.ResourceName]int64
		wantAct  int
		wantInad int
		wantInf  bool
	}{
		{
			name: "add to active",
			actions: func(tracker *pendingWorkloadsTracker, w1, w2 *utilworkload.Info) {
				tracker.PushOrUpdateActive(w1)
				tracker.PushOrUpdateActive(w2)
			},
			wantRes:  map[corev1.ResourceName]int64{cpu: 3000}, // w1: 1, w2: 2
			wantAct:  2,
			wantInad: 0,
			wantInf:  false,
		},
		{
			name: "add to inadmissible",
			actions: func(tracker *pendingWorkloadsTracker, w1, w2 *utilworkload.Info) {
				tracker.InsertInadmissible(w1)
				tracker.InsertInadmissible(w2)
			},
			wantRes:  map[corev1.ResourceName]int64{cpu: 3000},
			wantAct:  0,
			wantInad: 2,
			wantInf:  false,
		},
		{
			name: "pop sets inflight and removes resources",
			actions: func(tracker *pendingWorkloadsTracker, w1, w2 *utilworkload.Info) {
				tracker.PushOrUpdateActive(w1)
				tracker.PushOrUpdateActive(w2)
				tracker.Pop() // pops w1 since name "w1" < "w2"
			},
			wantRes:  map[corev1.ResourceName]int64{cpu: 2000}, // w2 remaining
			wantAct:  1,
			wantInad: 0,
			wantInf:  true,
		},
		{
			name: "duplicate add updates correctly",
			actions: func(tracker *pendingWorkloadsTracker, w1, w2 *utilworkload.Info) {
				tracker.PushOrUpdateActive(w1)
				// Re-pushing the exact same should preserve total resource tracking
				tracker.PushOrUpdateActive(w1)
			},
			wantRes:  map[corev1.ResourceName]int64{cpu: 1000},
			wantAct:  1,
			wantInad: 0,
			wantInf:  false,
		},
		{
			name: "delete active workload removes resources",
			actions: func(tracker *pendingWorkloadsTracker, w1, w2 *utilworkload.Info) {
				tracker.PushOrUpdateActive(w1)
				tracker.Delete(utilworkload.Key(w1.Obj))
			},
			wantRes:  map[corev1.ResourceName]int64{cpu: 0},
			wantAct:  0,
			wantInad: 0,
			wantInf:  false,
		},
		{
			name: "delete inflight clears inflight but resources already zeroed by pop",
			actions: func(tracker *pendingWorkloadsTracker, w1, w2 *utilworkload.Info) {
				tracker.PushOrUpdateActive(w1)
				tracker.Pop() // w1 becomes inflight, pending resource goes to 0
				tracker.Delete(utilworkload.Key(w1.Obj))
			},
			wantRes:  map[corev1.ResourceName]int64{cpu: 0},
			wantAct:  0,
			wantInad: 0,
			wantInf:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tracker := newPendingWorkloadsTracker(keyFunc, lessFunc)
			tracker.InitializeResource(cpu)

			w1 := utilworkload.NewInfo(utiltestingapi.MakeWorkload("w1", "").
				PodSets(*utiltestingapi.MakePodSet("ps1", 1).
					Request(cpu, "1").Obj()).
				Obj())
			w2 := utilworkload.NewInfo(utiltestingapi.MakeWorkload("w2", "").
				PodSets(*utiltestingapi.MakePodSet("ps1", 1).
					Request(cpu, "2").Obj()).
				Obj())

			tc.actions(tracker, w1, w2)

			if diff := cmp.Diff(tc.wantRes, tracker.PendingResources()); diff != "" {
				t.Errorf("Unexpected pending resources (-want,+got): %s", diff)
			}
			if tracker.ActiveLen() != tc.wantAct {
				t.Errorf("Expected %d active workloads, got %d", tc.wantAct, tracker.ActiveLen())
			}
			if tracker.InadmissibleLen() != tc.wantInad {
				t.Errorf("Expected %d inadmissible workloads, got %d", tc.wantInad, tracker.InadmissibleLen())
			}
			if (tracker.Inflight() != nil) != tc.wantInf {
				t.Errorf("Expected inflight presence to be %v", tc.wantInf)
			}
		})
	}
}

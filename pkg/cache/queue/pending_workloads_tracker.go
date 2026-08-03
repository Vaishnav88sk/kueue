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
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/kueue/pkg/util/heap"
	"sigs.k8s.io/kueue/pkg/workload"
)

// pendingWorkloadsTracker encapsulates the state and resource accounting for workloads
// that are pending in a ClusterQueue (active, inadmissible, or inflight).
// It maintains the invariant that each pending workload is in exactly one location,
// and correctly updates pending-resource totals upon state transitions.
type pendingWorkloadsTracker struct {
	// active is the heap of workloads waiting to be scheduled.
	active heap.Heap[workload.Info, workload.Reference]

	// inadmissible are workloads that have been tried at least once and couldn't be admitted.
	inadmissible inadmissibleWorkloads

	// inflight is non-nil when a workload has been popped by the scheduler but
	// not yet requeued or deleted.
	inflight *workload.Info

	// pendingResourcesTotal is the incremental sum of TotalRequests across workloads
	// in active and inadmissible (not inflight). Updated at each mutation site so
	// pendingResources() is O(1) rather than O(N).
	pendingResourcesTotal map[corev1.ResourceName]int64
}

func newPendingWorkloadsTracker(keyFunc func(*workload.Info) workload.Reference, lessFunc func(a, b *workload.Info) bool) *pendingWorkloadsTracker {
	return &pendingWorkloadsTracker{
		active:                *heap.New(keyFunc, lessFunc),
		inadmissible:          make(inadmissibleWorkloads),
		pendingResourcesTotal: make(map[corev1.ResourceName]int64),
	}
}

func (t *pendingWorkloadsTracker) addPendingResources(wInfo *workload.Info) {
	for _, ps := range wInfo.TotalRequests {
		if ps.Requests != nil {
			ps.Requests.ForEach(func(name corev1.ResourceName, q int64) {
				t.pendingResourcesTotal[name] += q
			})
		}
	}
}

func (t *pendingWorkloadsTracker) subtractPendingResources(wInfo *workload.Info) {
	for _, ps := range wInfo.TotalRequests {
		if ps.Requests != nil {
			ps.Requests.ForEach(func(name corev1.ResourceName, q int64) {
				t.pendingResourcesTotal[name] -= q
			})
		}
	}
}

// PushOrUpdateActive pushes the workload to the active heap or updates it if it exists.
// It also updates the pending resources accordingly.
func (t *pendingWorkloadsTracker) PushOrUpdateActive(wInfo *workload.Info) {
	key := workload.Key(wInfo.Obj)
	if oldHeapInfo := t.active.GetByKey(key); oldHeapInfo != nil {
		t.subtractPendingResources(oldHeapInfo)
	}
	t.active.PushOrUpdate(wInfo)
	t.addPendingResources(wInfo)
}

// PushIfNotPresentActive pushes the workload if it's not already in the active heap.
func (t *pendingWorkloadsTracker) PushIfNotPresentActive(wInfo *workload.Info) bool {
	if t.active.GetByKey(workload.Key(wInfo.Obj)) != nil {
		return false
	}
	t.active.PushOrUpdate(wInfo)
	t.addPendingResources(wInfo)
	return true
}

// InsertInadmissible inserts a workload to the inadmissible set.
func (t *pendingWorkloadsTracker) InsertInadmissible(wInfo *workload.Info) {
	key := workload.Key(wInfo.Obj)
	if oldInfo := t.inadmissible.get(key); oldInfo != nil {
		t.subtractPendingResources(oldInfo)
	}
	t.inadmissible.insert(key, wInfo)
	t.addPendingResources(wInfo)
}

// RemoveFromInadmissible removes a workload from the inadmissible set and updates pending resources.
func (t *pendingWorkloadsTracker) RemoveFromInadmissible(key workload.Reference) {
	if wInfo := t.inadmissible.get(key); wInfo != nil {
		t.inadmissible.delete(key)
		t.subtractPendingResources(wInfo)
	}
}

// Delete removes a workload from any state it might be in (active, inadmissible, inflight).
func (t *pendingWorkloadsTracker) Delete(key workload.Reference) {
	if t.inflight != nil && workload.Key(t.inflight.Obj) == key {
		t.inflight = nil
		return
	}
	if wInfo := t.inadmissible.get(key); wInfo != nil {
		t.RemoveFromInadmissible(key)
		return
	}
	if wInfo := t.active.GetByKey(key); wInfo != nil {
		t.active.Delete(key)
		t.subtractPendingResources(wInfo)
		return
	}
}

// Pop pops the head of the active heap, sets it as inflight, and returns it.
func (t *pendingWorkloadsTracker) Pop() *workload.Info {
	if t.active.Len() == 0 {
		return nil
	}
	wInfo := t.active.Pop()
	t.subtractPendingResources(wInfo)
	t.inflight = wInfo
	return wInfo
}

// DeleteFromInflight clears the inflight state if it matches the key.
func (t *pendingWorkloadsTracker) DeleteFromInflight(key workload.Reference) {
	if t.inflight != nil && workload.Key(t.inflight.Obj) == key {
		t.inflight = nil
	}
}

// GetActive returns the active workload by key.
func (t *pendingWorkloadsTracker) GetActive(key workload.Reference) *workload.Info {
	return t.active.GetByKey(key)
}

// ActiveList returns all active workloads.
func (t *pendingWorkloadsTracker) ActiveList() []*workload.Info {
	return t.active.List()
}

// DeleteActive deletes a workload from the active heap.
func (t *pendingWorkloadsTracker) DeleteActive(key workload.Reference) {
	if wInfo := t.active.GetByKey(key); wInfo != nil {
		t.active.Delete(key)
		t.subtractPendingResources(wInfo)
	}
}

// ActiveLen returns the number of active workloads.
func (t *pendingWorkloadsTracker) ActiveLen() int {
	return t.active.Len()
}

// GetInadmissible returns the inadmissible workload by key.
func (t *pendingWorkloadsTracker) GetInadmissible(key workload.Reference) *workload.Info {
	return t.inadmissible.get(key)
}

// InadmissibleLen returns the number of inadmissible workloads.
func (t *pendingWorkloadsTracker) InadmissibleLen() int {
	return t.inadmissible.len()
}

// InadmissibleEmpty returns true if inadmissible is empty.
func (t *pendingWorkloadsTracker) InadmissibleEmpty() bool {
	return t.inadmissible.empty()
}

// InadmissibleList returns all inadmissible workloads.
func (t *pendingWorkloadsTracker) InadmissibleList() []*workload.Info {
	list := make([]*workload.Info, 0, t.inadmissible.len())
	for _, wInfo := range t.inadmissible {
		list = append(list, wInfo)
	}
	return list
}

// InadmissibleMap returns the underlying inadmissible workloads map for iteration.
func (t *pendingWorkloadsTracker) InadmissibleMap() inadmissibleWorkloads {
	return t.inadmissible
}

// HasInadmissible returns true if the workload is in the inadmissible set.
func (t *pendingWorkloadsTracker) HasInadmissible(key workload.Reference) bool {
	return t.inadmissible.hasKey(key)
}

// Inflight returns the inflight workload.
func (t *pendingWorkloadsTracker) Inflight() *workload.Info {
	return t.inflight
}

// SetInflight sets the inflight workload.
func (t *pendingWorkloadsTracker) SetInflight(wInfo *workload.Info) {
	t.inflight = wInfo
}

// PendingResources returns a map of total pending resources.
func (t *pendingWorkloadsTracker) PendingResources() map[corev1.ResourceName]int64 {
	return t.pendingResourcesTotal
}

// InitializeResource initializes the pending resource total for a given resource.
func (t *pendingWorkloadsTracker) InitializeResource(name corev1.ResourceName) {
	if _, exists := t.pendingResourcesTotal[name]; !exists {
		t.pendingResourcesTotal[name] = 0
	}
}

// DeleteResource deletes a pending resource total.
func (t *pendingWorkloadsTracker) DeleteResource(name corev1.ResourceName) {
	delete(t.pendingResourcesTotal, name)
}

// ReplaceInadmissible replaces the entire inadmissible map and updates pending resources.
func (t *pendingWorkloadsTracker) ReplaceInadmissible(newInadmissible inadmissibleWorkloads) {
	for _, wInfo := range t.inadmissible {
		t.subtractPendingResources(wInfo)
	}
	t.inadmissible = newInadmissible
	for _, wInfo := range t.inadmissible {
		t.addPendingResources(wInfo)
	}
}

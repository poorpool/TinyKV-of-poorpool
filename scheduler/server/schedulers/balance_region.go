// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type storeSlice []*core.StoreInfo

func (s storeSlice) Len() int {
	return len(s)
}

func (s storeSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s storeSlice) Less(i, j int) bool {
	return s[i].GetRegionSize() > s[j].GetRegionSize()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	maxDownTime := cluster.GetMaxStoreDownTime()
	var stores storeSlice
	for _, v := range cluster.GetStores() {
		if v.IsUp() && v.DownTime() <= maxDownTime {
			stores = append(stores, v)
		}
	}
	if len(stores) == 0 {
		return nil
	}
	sort.Sort(stores) // 按照 regionSize 降序排序
	var region *core.RegionInfo
	var fromStore, toStore *core.StoreInfo
	i := 0
	for i, v := range stores {
		cluster.GetPendingRegionsWithLock(v.GetID(), func(rc core.RegionsContainer) {
			region = rc.RandomRegion(nil, nil)
		})
		if region != nil {
			fromStore = stores[i]
			break
		}
		cluster.GetFollowersWithLock(v.GetID(), func(rc core.RegionsContainer) {
			region = rc.RandomRegion(nil, nil)
		})
		if region != nil {
			fromStore = stores[i]
			break
		}
		cluster.GetLeadersWithLock(v.GetID(), func(rc core.RegionsContainer) {
			region = rc.RandomRegion(nil, nil)
		})
		if region != nil {
			fromStore = stores[i]
			break
		}
	}
	if region == nil || fromStore == nil {
		return nil
	}
	regionStoreIds := region.GetStoreIds()
	if len(regionStoreIds) < cluster.GetMaxReplicas() { // 不到必要的时候不去新 store
		return nil
	}
	for j := len(stores) - 1; j > i; j-- {
		if _, ok := regionStoreIds[stores[j].GetID()]; !ok { // toStore 不能在 region 已有的里面
			toStore = stores[j]
			break
		}
	}
	if toStore == nil {
		return nil
	}
	if fromStore.GetRegionSize()-toStore.GetRegionSize() <= 2*region.GetApproximateSize() {
		return nil
	}
	peer, err := cluster.AllocPeer(toStore.GetID())
	if err != nil {
		return nil
	}
	oper, err := operator.CreateMovePeerOperator("", cluster, region, operator.OpBalance, fromStore.GetID(), toStore.GetID(), peer.GetId())
	if err != nil {
		return nil
	}
	return oper
}

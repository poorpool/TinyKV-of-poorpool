// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"log"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage // storage 和 entries 可能交也可能不交

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry // entries[i]'s index is first+i, index 连续

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	fi, err := storage.FirstIndex()
	if err != nil {
		log.Println(err)
	}
	ls, err := storage.LastIndex()
	if err != nil {
		log.Println(err)
	}
	entries, err := storage.Entries(fi, ls+1)
	if err != nil {
		log.Println(err)
	}
	l := &RaftLog{
		entries:    entries,
		storage:    storage,
		firstIndex: fi,
		applied:    fi - 1,
		committed:  fi - 1, // fixme: 是吗？
		stabled:    ls,
	}
	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	first, _ := l.storage.FirstIndex() // 没进入 snapshot 的第一个元素
	if first > l.firstIndex {
		for len(l.entries) > 0 && l.firstIndex < first { // 删掉进了 snapshot 的元素
			l.entries = l.entries[1:]
			if len(l.entries) == 0 {
				l.firstIndex = 0
			} else {
				l.firstIndex = l.entries[0].GetIndex()
			}
		}
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 && l.stabled-l.firstIndex+1 <= uint64(len(l.entries)) {
		return l.entries[l.stabled-l.firstIndex+1:]
	}
	return []pb.Entry{}
}

func (l *RaftLog) unstableEntryPointersFromIndex(i uint64) []*pb.Entry {
	var entries []*pb.Entry
	for _, v := range l.entries {
		if v.GetIndex() >= i {
			entries = append(entries, &pb.Entry{
				EntryType: v.GetEntryType(),
				Term:      v.GetTerm(),
				Index:     v.GetIndex(),
				Data:      v.GetData(),
			})
		}
	}
	return entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	var entries []pb.Entry
	for _, v := range l.entries {
		if v.GetIndex() <= l.committed && v.GetIndex() > l.applied {
			entries = append(entries, v)
		}
	}
	return entries
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	lastIndex := uint64(0)
	if !IsEmptySnap(l.pendingSnapshot) {
		lastIndex = l.pendingSnapshot.GetMetadata().GetIndex()
	}
	if len(l.entries) > 0 { // 最大值要么在 snapshot 里，要么：有 entries 就肯定在 entries，否则 storage
		return max(lastIndex, l.entries[len(l.entries)-1].GetIndex())
	} else {
		index, _ := l.storage.LastIndex()
		return max(lastIndex, index)
	}
}

func (l *RaftLog) FirstIndex() uint64 {
	return l.firstIndex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.GetIndex() {
			return l.pendingSnapshot.Metadata.GetTerm(), nil
		}
	}
	for _, v := range l.entries {
		if v.Index == i {
			return v.Term, nil
		}
	}
	return l.storage.Term(i)
}

func (l *RaftLog) AppendEntriesWithTheirOwnTermAndIndex(pendingConfIndex *uint64, entries []*pb.Entry) {
	for _, v := range entries {
		if v.GetEntryType() == pb.EntryType_EntryConfChange {
			if *pendingConfIndex != 0 {
				continue
			} else {
				*pendingConfIndex = v.GetIndex()
			}
		}
		l.entries = append(l.entries, pb.Entry{
			EntryType: v.GetEntryType(),
			Term:      v.GetTerm(),
			Index:     v.GetIndex(),
			Data:      v.GetData(),
		})
	}
}

func (l *RaftLog) AppendEntries(pendingConfIndex *uint64, entries []*pb.Entry, term uint64) {
	for _, v := range entries {
		if v.GetEntryType() == pb.EntryType_EntryConfChange {
			if *pendingConfIndex != 0 {
				continue
			} else {
				*pendingConfIndex = v.GetIndex()
			}
		}
		l.entries = append(l.entries, pb.Entry{
			EntryType: v.GetEntryType(),
			Term:      term,
			Index:     l.LastIndex() + 1,
			Data:      v.GetData(),
		})
	}
}

func (l *RaftLog) DeleteFromIndex(index uint64) {
	for i, v := range l.entries {
		if v.GetIndex() == index {
			l.entries = l.entries[:i]
			break
		}
	}
	lastIndex := l.LastIndex()
	l.committed = min(l.committed, lastIndex)
	l.applied = min(l.applied, lastIndex)
	l.stabled = min(l.stabled, lastIndex)
}

func (l *RaftLog) SetFirstIndex(index uint64) {
	l.firstIndex = index
}

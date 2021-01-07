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
	storage Storage

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
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	lastIndex uint64 // 最后一个元素的 index
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
	return &RaftLog{
		entries:   entries,
		lastIndex: ls,
		stabled:   ls,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	entries := []pb.Entry{} // 应当这么写
	for _, v := range l.entries {
		if v.GetIndex() > l.stabled {
			entries = append(entries, v)
		}
	}
	return entries
}

func (l *RaftLog) unstableEntryPointersFromIndexWithPrevIndexAndTerm(i uint64) ([]*pb.Entry, uint64, uint64) {
	var entries []*pb.Entry
	var index, logTerm uint64
	for _, v := range l.entries {
		if v.GetIndex() >= i {
			entries = append(entries, &pb.Entry{
				Term:  v.GetTerm(),
				Index: v.GetIndex(),
				Data:  v.GetData(),
			})
		} else {
			index = v.GetIndex()
			logTerm = v.GetTerm()
		}
	}
	return entries, index, logTerm
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
	return l.lastIndex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	for _, v := range l.entries {
		if v.Index == i {
			return v.Term, nil
		}
	}
	return 0, nil
}

func (l *RaftLog) AppendEntriesWithTheirOwnTerm(entries []*pb.Entry) {
	for _, v := range entries {
		l.lastIndex++
		l.entries = append(l.entries, pb.Entry{
			Term:  v.GetTerm(),
			Index: l.lastIndex,
			Data:  v.GetData(),
		})
	}
}

func (l *RaftLog) AppendEntries(entries []*pb.Entry, term uint64) {
	for _, v := range entries {
		l.lastIndex++
		l.entries = append(l.entries, pb.Entry{
			Term:  term,
			Index: l.lastIndex,
			Data:  v.GetData(),
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
	if len(l.entries) == 0 {
		l.lastIndex = 0
	} else {
		l.lastIndex = min(l.lastIndex, l.entries[len(l.entries)-1].Index)
	}
	l.committed = min(l.committed, l.lastIndex)
	l.applied = min(l.applied, l.lastIndex)
	l.stabled = min(l.stabled, l.lastIndex)
}

/*
func (l *RaftLog) removeNoopEntries(entries []pb.Entry) []pb.Entry {
	var ent []pb.Entry
	for _, v := range entries {
		if v.GetData() != nil {
			ent = append(ent, v)
		}
	}
	return ent
}*/

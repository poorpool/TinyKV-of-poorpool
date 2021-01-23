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
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int

	// baseline of election interval
	electionTimeout int

	// electionTimeout 是无消息到开始选举的时间，electionRandomTimeout 是自己加的选举超时时间，在 [electionTimeout, 2 * electionTimeout) 之间
	electionRandomTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int

	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// 领导转换的过去的 tick 数，防止一直领导转换
	transferLeaderElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	rand.Seed(time.Now().UnixNano())
	hardState, confState, _ := c.Storage.InitialState()
	r := &Raft{
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		Vote:             hardState.GetVote(),
		RaftLog:          newLog(c.Storage),
		Prs:              map[uint64]*Progress{},
		votes:            map[uint64]bool{},
		leadTransferee:   None,
	}
	r.Term = hardState.GetTerm()
	if r.Term == 0 {
		r.Term, _ = r.RaftLog.Term(r.RaftLog.LastIndex())
	}
	r.electionRandomTimeout = rand.Intn(r.electionTimeout) + r.electionTimeout
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	for _, v := range c.peers {
		if v == r.id {
			r.Prs[v] = &Progress{
				Match: r.RaftLog.LastIndex(),
				Next:  r.RaftLog.LastIndex() + 1,
			}
		} else {
			r.Prs[v] = &Progress{
				Match: 0,
				Next:  1,
			}
		}
	}
	r.RaftLog.committed = hardState.GetCommit()
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevIndex := r.Prs[to].Next - 1
	prevTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		r.sendSnapshot(to) // 已不可取得 Term，说明 to 落后太多，必须发快照
		return false
	}
	entries := r.RaftLog.unstableEntryPointersFromIndex(r.Prs[to].Next)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevTerm,  // 要发送的entries前一个term
		Index:   prevIndex, // 要发送的entries前一个index
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

func (r *Raft) sendSnapshot(to uint64) bool {
	// Your Code Here (2A).
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return false // 也许 snapshot 还没准备好
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
	r.Prs[to].Next = snapshot.GetMetadata().GetIndex() + 1
	return true
}

func (r *Raft) sendAppendResponse(to uint64, reject bool, term uint64, index uint64) bool {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    term,
		Reject:  reject,
		Index:   index,
	})
	// 其实 reject 为 true 的情况下没有必要发送 index
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		To:      to,
		From:    r.id,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
	}
	r.electionElapsed++
	if r.State == StateLeader && r.heartbeatElapsed >= r.heartbeatTimeout { // 定期心跳
		r.heartbeatElapsed = 0
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			From:    r.id,
			To:      r.id,
		})
	}
	if r.electionRandomTimeout <= r.electionElapsed && r.State != StateLeader { // 太久没收到有效消息，自己发起选举
		r.electionElapsed = 0
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
			To:      r.id,
		})
	}
	if r.leadTransferee != None {
		r.transferLeaderElapsed++
		if r.transferLeaderElapsed >= 2*r.electionElapsed { // 领导转换超时，中止
			r.leadTransferee = None
			r.transferLeaderElapsed = 0
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = 0
	r.votes = map[uint64]bool{}
	r.electionRandomTimeout = rand.Intn(r.electionTimeout) + r.electionTimeout
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++ // 自增任期号，开始选举
	r.Vote = 0
	r.Lead = 0
	r.votes = map[uint64]bool{}
	r.electionRandomTimeout = rand.Intn(r.electionTimeout) + r.electionTimeout
	r.electionElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.leadTransferee = None
	r.Lead = r.id
	for k := range r.Prs {
		r.Prs[k] = &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1, // raft 论文
		}
	}
	r.RaftLog.AppendEntriesWithTheirOwnTermAndIndex(&r.PendingConfIndex, []*pb.Entry{
		{EntryType: pb.EntryType_EntryNormal, Index: r.RaftLog.LastIndex() + 1, Term: r.Term},
	})
	if 1 > len(r.Prs)/2 { // 只有自己的特殊情况
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	r.Prs[r.id] = &Progress{
		Match: r.RaftLog.LastIndex(),
		Next:  r.RaftLog.LastIndex() + 1,
	}
	for k := range r.Prs {
		if k != r.id {
			r.sendAppend(k) // 成为领导人立刻开始发送附加日志 RPC
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	_, ok := r.Prs[r.id]
	if !ok && m.GetMsgType() == pb.MessageType_MsgTimeoutNow {
		return nil // 自己给 remove 掉了
	}
	if m.GetTerm() > r.Term {
		r.becomeFollower(m.GetTerm(), None) // 这里一定是 None，参见 notes.md
	}
	switch r.State { // 分状态讨论，方便 drop 掉不应该处理的消息
	case StateFollower:
		switch m.GetMsgType() {
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgHup:
			r.handleHup(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		case pb.MessageType_MsgTimeoutNow:
			r.handleHup(m)
		}
	case StateCandidate:
		switch m.GetMsgType() {
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgAppend:
			if m.GetTerm() == r.Term {
				r.becomeFollower(m.GetTerm(), m.GetFrom())
			}
			r.handleAppendEntries(m)
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgHup:
			r.handleHup(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgHeartbeat:
			if m.GetTerm() == r.Term {
				r.becomeFollower(m.GetTerm(), m.GetFrom())
			}
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		case pb.MessageType_MsgTimeoutNow:
			r.handleHup(m)
		}
	case StateLeader:
		switch m.GetMsgType() {
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		case pb.MessageType_MsgHup:
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgBeat:
			r.handleBeat(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.sendAppend(m.GetFrom())
		case pb.MessageType_MsgTransferLeader:
			r.handleTransferLeader(m)
		case pb.MessageType_MsgTimeoutNow:
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	reject := false
	r.electionElapsed = 0
	if m.GetTerm() > 0 && m.GetTerm() < r.Term {
		r.sendAppendResponse(m.GetFrom(), true, r.Term, 0) // 只是为了让这个 leader 下台，index 填 0
		return
	}
	r.Lead = m.GetFrom()
	if m.GetIndex() > r.RaftLog.LastIndex() { // 日志压根不重叠，重发
		r.sendAppendResponse(m.From, true, r.Term, r.RaftLog.LastIndex())
		return
	}
	prevLogIndex, prevLogTerm := m.GetIndex(), m.GetLogTerm()
	findTerm, _ := r.RaftLog.Term(prevLogIndex)
	if prevLogIndex >= r.RaftLog.FirstIndex() && findTerm != prevLogTerm { // 不匹配
		reject = true
		r.sendAppendResponse(m.GetFrom(), reject, r.Term, r.RaftLog.LastIndex())
		return
	}
	if !reject {
		for i, v := range m.GetEntries() {
			if v.GetIndex() < r.RaftLog.FirstIndex() {
				continue
			} else if v.GetIndex() <= r.RaftLog.LastIndex() { // 可能要删自己的日志
				findTerm, _ := r.RaftLog.Term(v.GetIndex())
				if findTerm != v.GetTerm() {
					r.RaftLog.DeleteFromIndex(v.GetIndex())
					r.RaftLog.entries = append(r.RaftLog.entries, *v)
					r.RaftLog.stabled = min(r.RaftLog.stabled, v.GetIndex()-1)
				}
			} else {
				r.RaftLog.AppendEntriesWithTheirOwnTermAndIndex(&r.PendingConfIndex, m.Entries[i:])
				break
			}
		}
		if m.GetCommit() > r.RaftLog.committed { // raft 论文
			r.RaftLog.committed = min(m.GetCommit(), m.GetIndex()+uint64(len(m.GetEntries())))
		}
	}
	r.sendAppendResponse(m.GetFrom(), reject, r.Term, r.RaftLog.LastIndex())
}

// handlePropose handle Propose RPC request
func (r *Raft) handlePropose(m pb.Message) {
	if r.leadTransferee != None {
		return // 领导转换时不再接收 propose
	}
	entries := m.GetEntries()
	r.RaftLog.AppendEntries(&r.PendingConfIndex, entries, r.Term)
	for k := range r.Prs {
		if k == r.id {
			continue
		}
		r.sendAppend(k)
	}
	lastIndex := r.RaftLog.LastIndex()
	r.Prs[r.id] = &Progress{
		Match: lastIndex,
		Next:  lastIndex + 1,
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = lastIndex
	}
}

// handleHup handle Hup request
func (r *Raft) handleHup(m pb.Message) {
	r.becomeCandidate()

	if r.Vote == 0 {
		r.Vote = r.id
		r.votes[r.id] = true
	}
	if 1 > len(r.Prs)/2 { // 只有一个人
		r.becomeLeader()
		return
	}
	for k := range r.Prs {
		if k != r.id {
			index := r.RaftLog.LastIndex()
			logTerm, _ := r.RaftLog.Term(index)
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      k,
				From:    r.id,
				Term:    r.Term,
				Index:   index,
				LogTerm: logTerm,
			})
		}
	}
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	r.electionElapsed = 0
	if m.GetTerm() > 0 && m.GetTerm() < r.Term {
		r.msgs = append(r.msgs, pb.Message{ // 不直接 return，让发起者早点失败
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.GetFrom(),
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
	}
	willVote := false
	if r.Vote == 0 || r.Vote == m.GetFrom() { // 日志要至少和自己的一样新
		mindex, mterm := m.GetIndex(), m.GetLogTerm()
		rindex := r.RaftLog.LastIndex()
		rterm, _ := r.RaftLog.Term(rindex)
		if mterm > rterm || (mterm == rterm && mindex >= rindex) {
			willVote = true
		}
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.GetFrom(),
		From:    r.id,
		Term:    r.Term,
		Reject:  !willVote,
	})
	if willVote {
		r.Vote = m.GetFrom()
	}
}

// handleRequestVoteResponse handle RequestVoteResponse RPC request
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.electionElapsed = 0
	if m.GetTerm() > 0 && m.GetTerm() < r.Term {
		return
	}
	r.votes[m.GetFrom()] = !m.GetReject()
	cntTrue, cntFalse := 0, 0
	for _, v := range r.votes {
		if v {
			cntTrue++
		} else {
			cntFalse++
		}
	}
	if cntTrue > len(r.Prs)/2 {
		r.becomeLeader()
	}
	if len(r.Prs)-cntFalse <= len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

// handleBeat handle Beat request
func (r *Raft) handleBeat(m pb.Message) {
	r.heartbeatElapsed = 0
	for k := range r.Prs {
		if k != r.id {
			r.sendHeartbeat(k)
		}
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	r.electionElapsed = 0
	if m.GetTerm() > 0 && m.GetTerm() < r.Term {
		return
	}
	if m.GetReject() {
		r.Prs[m.GetFrom()].Next-- // 简单地减一
		r.sendAppend(m.GetFrom())
		return
	}
	if m.GetIndex() > r.Prs[m.GetFrom()].Match {
		r.Prs[m.GetFrom()].Next = m.GetIndex() + 1
		r.Prs[m.GetFrom()].Match = m.GetIndex()
		r.checkLeaderCommit()
	}
	if m.GetFrom() == r.leadTransferee { // 如果领导转换受体已经具备成为领导的条件，立刻让它超时发起选举，自己放弃领导地位
		if r.Prs[m.GetFrom()].Match >= r.RaftLog.LastIndex() {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgTimeoutNow,
				To:      m.GetFrom(),
				From:    r.id,
				Term:    r.Term,
			})
		}
		r.leadTransferee = None
		r.becomeFollower(r.Term, r.leadTransferee)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	if m.GetTerm() >= r.Term { // 小于的时候不要丢弃，要发送回复让任期比自己低的 leader 下台
		r.Lead = m.GetFrom()
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.GetFrom(),
		From:    r.id,
		Term:    r.Term,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	r.electionElapsed = 0
	snap := m.GetSnapshot()
	if m.GetTerm() < r.Term && snap.Metadata.GetIndex() <= r.RaftLog.committed {
		r.sendAppendResponse(m.GetFrom(), true, r.Term, r.RaftLog.LastIndex())
		return
	}
	r.becomeFollower(m.GetTerm(), m.GetFrom())
	r.RaftLog.entries = []pb.Entry{}
	r.RaftLog.SetFirstIndex(snap.GetMetadata().GetIndex() + 1)
	r.RaftLog.applied = snap.GetMetadata().GetIndex()
	r.RaftLog.committed = snap.GetMetadata().GetIndex()
	r.RaftLog.stabled = snap.GetMetadata().GetIndex()
	r.Prs = map[uint64]*Progress{}
	if confState := snap.Metadata.GetConfState(); confState != nil && len(confState.Nodes) > 0 {
		for _, v := range confState.Nodes {
			r.Prs[v] = &Progress{
				//Match: r.RaftLog.LastIndex(),
				//Next:  r.RaftLog.LastIndex() + 1,
			}
		}
	}
	r.RaftLog.pendingSnapshot = snap
	r.sendAppendResponse(m.GetFrom(), false, r.Term, r.RaftLog.LastIndex()) // 考虑一下是 lastindex吗
}

// 领导收到 appendresponse 或 removenode 后检查 commit 有没有变化，有就广播
func (r *Raft) checkLeaderCommit() {
	if r.State != StateLeader {
		return
	}
	changed := false
	for i := r.RaftLog.committed + 1; i <= r.RaftLog.LastIndex(); i++ {
		cnt := 0
		for k, v := range r.Prs {
			if k != r.id && v.Match >= i {
				cnt++
			}
		}
		logTerm, _ := r.RaftLog.Term(i)
		if logTerm == r.Term && cnt+1 > len(r.Prs)/2 {
			r.RaftLog.committed = i
			changed = true
		}
	}
	if changed {
		for k := range r.Prs {
			if k != r.id {
				r.sendAppend(k)
			}
		}
	}
}

// 处理 transferleader 消息
func (r *Raft) handleTransferLeader(m pb.Message) {
	_, ok := r.Prs[m.GetFrom()] // 首先要存在
	if !ok {
		return
	}
	if m.GetFrom() == r.id {
		return
	}
	if r.leadTransferee == m.GetFrom() {
		return
	}
	r.transferLeaderElapsed = 0
	r.leadTransferee = m.GetFrom()
	r.sendAppend(r.leadTransferee)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.PendingConfIndex = 0
	_, isExist := r.Prs[id]
	if isExist {
		return
	}
	r.Prs[id] = &Progress{ // 虽说只有 leader keep 这些信息，但是都 keep 上也无妨
		Match: 0,
		Next:  1,
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	r.PendingConfIndex = 0
	_, isExist := r.Prs[id]
	if !isExist {
		return
	}
	delete(r.Prs, id)
	//log.Printf("%d has %d r.Prs\n", r.id, len(r.Prs))
	r.checkLeaderCommit()
}

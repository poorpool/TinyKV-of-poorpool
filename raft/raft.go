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
	"log"
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
	r := &Raft{
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		Vote:             0,
		RaftLog:          newLog(c.Storage),
		Prs:              map[uint64]*Progress{},
		votes:            map[uint64]bool{},
	}
	r.electionRandomTimeout = rand.Intn(r.electionTimeout) + r.electionTimeout
	for _, v := range c.peers {
		r.Prs[v] = &Progress{
			Match: 0,
			Next:  1,
		}
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	entries, index, logTerm := r.RaftLog.unstableEntryPointersFromIndexWithPrevIndexAndTerm(r.Prs[to].Next)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm, // 要发送的entries前一个term
		Index:   index,   // 要发送的entries前一个index
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})
	log.Printf("%d send to %d %d entries\n", r.id, to, len(entries))
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
	if r.State == StateLeader && r.heartbeatElapsed == r.heartbeatTimeout {
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			From:    r.id,
			To:      r.id,
		})
	}
	if r.electionRandomTimeout == r.electionElapsed && r.State != StateLeader {
		r.electionElapsed = 0
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
			To:      r.id,
		})
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
	for k, _ := range r.Prs {
		r.Prs[k] = &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1, // raft 论文
		}
	}
	r.RaftLog.AppendEntries([]*pb.Entry{
		{Data: nil, Term: r.Term, Index: r.RaftLog.LastIndex() + 1},
	}, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//log.Println(m.GetMsgType(), m.GetFrom(), m.GetTo())
	if m.From != r.id { // 不处理自己给自己发消息
		r.electionElapsed = 0
		var fromTerm = m.GetTerm()
		if fromTerm > r.Term {
			if r.State != StateFollower {
				r.becomeFollower(fromTerm, m.GetFrom())
			}
			r.Term = fromTerm
		} else if fromTerm > 0 && fromTerm < r.Term {
			return nil // 过期了
		}
	}
	switch m.GetMsgType() { // todo: 在这儿使用反射
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m) // fixme: 这儿暂时过不去leadercycle,因为没进行noop entry的复制,做完2ab回来修
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgBeat:
		r.handleBeat(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	}

	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term >= r.Term && r.State == StateCandidate {
		r.becomeFollower(m.Term, m.GetFrom())
	}
	switch r.State {
	case StateFollower:
	case StateCandidate:
	case StateLeader:
	}
}

// handlePropose handle Propose RPC request
func (r *Raft) handlePropose(m pb.Message) {
	if r.State != StateLeader {
		return
	}
	entries := m.GetEntries()
	r.RaftLog.AppendEntries(entries, r.Term)
	for k, _ := range r.Prs {
		if k != r.id {
			r.sendAppend(k)
		}
	}
}

// handleHup handle Hup request
func (r *Raft) handleHup(m pb.Message) {
	if m.GetFrom() != r.id {
		return
	}

	if r.State == StateLeader {
		return
	}
	r.electionElapsed = 0
	r.becomeCandidate()

	if r.Vote == 0 {
		r.Vote = r.id
		r.votes[r.id] = true
	}
	if 1 > len(r.Prs)/2 { // 只有一个人
		r.becomeLeader()
		return
	}
	for k, _ := range r.Prs {
		if k != r.id {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      k,
				From:    r.id,
				Term:    r.Term,
			})
		}
	}
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	willVote := false
	if r.Term <= m.GetTerm() && (r.Vote == 0 || r.Vote == m.GetFrom()) {
		mindex, mterm := m.GetIndex(), m.GetLogTerm()
		rindex := r.RaftLog.LastIndex()
		rterm, _ := r.RaftLog.Term(rindex)
		//log.Println("mterm, ", mterm, "rterm, ", rterm, "mindex, ", mindex, "rindex, ", rindex)
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
	//log.Println(m.GetFrom(), " request ", r.id, " to vote, ", willVote, ", after that term is ", r.Term)
	if willVote {
		r.Vote = m.GetFrom()
	}
}

// handleRequestVoteResponse handle RequestVoteResponse RPC request
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.GetFrom()] = !m.GetReject()
	cnt := 0
	for _, v := range r.votes {
		if v {
			cnt++
		}
	}
	if cnt > len(r.Prs)/2 && r.State != StateLeader {
		r.becomeLeader()
	}
}

// handleBeat handle Beat request
func (r *Raft) handleBeat(m pb.Message) {
	if r.State == StateLeader && m.GetFrom() == r.id {
		r.heartbeatElapsed = 0
		for k, _ := range r.Prs {
			if k != r.id {
				r.sendHeartbeat(k)
			}
		}
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	// log.Printf("Received from %d, index is %d\n", m.GetFrom(), m.GetIndex())
	r.Prs[m.GetFrom()].Next = m.GetIndex() + 1
	r.RaftLog.committed = m.GetIndex()
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) { // fixme: 处理更高任期的心跳似乎在Step里处理了
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

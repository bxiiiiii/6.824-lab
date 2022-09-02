package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	"math/big"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	// sync "github.com/sasha-s/go-deadlock"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//2A
	CurrentTerm int
	VotedFor    int
	Log         map[int]Entries
	state       int

	leader int
	votes  int
	// heartbeatInerval int
	timeout int
	timer   int

	//2B
	commitIndex int
	lastApplied int

	nextIndex  map[int]int
	matchIndex map[int]int

	cond    *sync.Cond
	applyCh chan ApplyMsg

	LastLogIndex int

	LastIncludedIndex int
	LastIncludedTerm  int
	SnapshotData      []byte
}

type Entries struct {
	Command interface{}
	Term    int
	Index   int
}

const (
	Sfollower  = 0
	Scandidate = 1
	Sleader    = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.CurrentTerm
	if rf.state == Sleader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Panicing %s\r\n", err)
		}
	}()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	tem := make(map[int]Entries)
	for k, v := range rf.Log {
		entry := Entries{
			Command: v.Command,
			Index:   v.Index,
			Term:    v.Term,
		}
		tem[k] = entry
	}
	// rf.mu.Lock()
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(tem)
	e.Encode(rf.LastLogIndex)
	data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.Persister.SaveStateAndSnapshot(data, rf.SnapshotData)
	// rf.mu.Unlock()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	log := make(map[int]Entries)
	var lastLogIndex int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastLogIndex) != nil {
		DEBUG(dError, "S%v readPersist failed", rf.me)
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		for _, en := range log {
			rf.Log[en.Index] = en
		}
		rf.LastLogIndex = lastLogIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var index int
	err := d.Decode(&index)
	if err != nil {
		DEBUG(dError, "S%v readSnapshotPersist failed\n%v", rf.me, err)
	} else {
		if index == -1 {
			return
		}
		rf.mu.Lock()
		rf.LastIncludedIndex = index
		rf.LastIncludedTerm = rf.Log[index].Term
		rf.SnapshotData = data
		rf.mu.Unlock()
	}

	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.LastIncludedIndex != -1 {

			applyMsg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.SnapshotData,
				SnapshotIndex: rf.LastIncludedIndex,
				SnapshotTerm:  rf.LastIncludedTerm,
			}
			rf.applyCh <- applyMsg
			DEBUG(dCommit, "S%v apply: %v", rf.me, applyMsg.SnapshotIndex)
			//TODO: committed ?
			rf.lastApplied = rf.LastIncludedIndex
			rf.commitIndex = rf.lastApplied
		}
	}()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	DEBUG(dSnap, "S%v snapshot : idx:%v", rf.me, index)
	rf.mu.Lock()
	if index > rf.LastIncludedIndex {
		rf.SnapshotData = snapshot
		rf.LastIncludedIndex = index
		rf.LastIncludedTerm = rf.Log[index].Term
		for _, entry := range rf.Log {
			if entry.Index < index {
				delete(rf.Log, entry.Index)
			}
		}
		if rf.lastApplied < rf.LastIncludedIndex {
			rf.lastApplied = rf.LastIncludedIndex
		}
		if rf.commitIndex < rf.lastApplied {
			rf.commitIndex = rf.lastApplied
		}
		DEBUG(dPersist, "S%v [after snap]la:%v log:%v %v %v", rf.me, rf.lastApplied, rf.Log, rf.LastIncludedIndex, rf.LastIncludedTerm)
		rf.persist()
	}
	rf.mu.Unlock()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//2A
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	CommitIndex int
	CommitTerm  int
}

type AppendEntiresArgs struct {
	Term          int
	LeaderId      int
	PrevLogIndex  int
	PrevLogTerm   int
	AppendEntries []Entries
	LeaderCommit  int
	SnapShot      int
}

type AppendEntiresReply struct {
	ConflictLogIndex int
	ConflictLogTerm  int
	CommitIndex      int
	CommitTerm       int
	Term             int
	Success          bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	// Offset            int
	Data []byte
	// Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//2A
	DEBUG(dVote, "S%v C%v asking for vote pi:%v pt:%v", rf.me, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	rf.mu.Lock()

	if rf.CurrentTerm < args.Term {
		// DEBUG(dTerm, "S%v Term is lower, updating (%v < %v)", rf.me, rf.CurrentTerm, args.Term)
		// DEBUG(dVote, "S%v c:T:%v I:%v, m: T:%v I:%v", rf.me, args.LastLogTerm, args.LastLogIndex, rf.Log[rf.LastLogIndex].Term, rf.Log[len(rf.Log)-1].Index)

		if rf.Log[rf.LastLogIndex].Term > args.LastLogTerm {
			reply.VoteGranted = false
		} else if rf.Log[rf.LastLogIndex].Term < args.LastLogTerm {
			reply.VoteGranted = true
		} else {
			if rf.LastLogIndex > args.LastLogIndex {
				reply.VoteGranted = false
			} else {
				reply.VoteGranted = true
			}
		}
		if reply.VoteGranted {
			rf.becomeFowllower(args.CandidateId, args.Term)
			rf.VotedFor = args.CandidateId
		} else {
			if rf.state == Sleader {
				rf.leader = -1
			}
			rf.state = Sfollower
			rf.VotedFor = -1
			rf.CurrentTerm = args.Term
		}
	} else if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		reply.Term = args.Term
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
			if rf.Log[rf.LastLogIndex].Term > args.LastLogTerm {
				reply.VoteGranted = false
			} else if rf.Log[rf.LastLogIndex].Term < args.LastLogTerm {
				reply.VoteGranted = true
				rf.becomeFowllower(args.CandidateId, args.Term)
				rf.VotedFor = args.CandidateId
			} else {
				if rf.LastLogIndex > args.LastLogIndex {
					reply.VoteGranted = false
				} else {
					reply.VoteGranted = true
					rf.becomeFowllower(args.CandidateId, args.Term)
					rf.VotedFor = args.CandidateId
				}
			}
		} else {
			reply.VoteGranted = false
		}
	}
	reply.CommitIndex = rf.commitIndex
	reply.CommitTerm = rf.Log[rf.commitIndex].Term
	DEBUG(dVote, "S%v T%v Granting Vote to S%v", rf.me, rf.CurrentTerm, rf.VotedFor)
	rf.persist()
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntiresArgs, reply *AppendEntiresReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	DEBUG(dSnap, "S%v get snapshot : idx:%v", rf.me, args.LastIncludedIndex)
	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	} else if args.Term > rf.CurrentTerm {
		rf.becomeFowllower(args.LeaderId, args.Term)
	} else {
		if rf.state == Sfollower && rf.leader != args.LeaderId {
			if rf.leader == -1 {
				rf.becomeFowllower(args.LeaderId, rf.CurrentTerm)
			} else {
				reply.Term = rf.CurrentTerm
				rf.mu.Unlock()
				return
			}
			// } else {
			// 	rf.becomeFowllower(args.LeaderId, args.Term)
		}
	}
	if rf.commitIndex > args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	for _, en := range rf.Log {
		if en.Index <= args.LastIncludedIndex {
			delete(rf.Log, en.Index)
		}
	}
	if _, ok := rf.Log[args.LastIncludedIndex]; !ok {
		rf.Log[args.LastIncludedIndex] = Entries{
			Term:  args.LastIncludedTerm,
			Index: args.LastIncludedIndex,
		}
		DEBUG(dSnap, "S%v snap entry: %v", rf.me, rf.Log[args.LastIncludedIndex])
	}
	rf.SnapshotData = args.Data
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = rf.Log[args.LastIncludedIndex].Term

	if rf.LastLogIndex < rf.LastIncludedIndex {
		rf.LastLogIndex = rf.LastIncludedIndex
	}
	rf.persist()
	// rf.mu.Unlock()
	DEBUG(dClient, "S%v after snap: %v", rf.me, rf.Log)
	// go func() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if rf.lastApplied < args.LastIncludedIndex {
		applyMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.SnapshotData,
			SnapshotIndex: rf.LastIncludedIndex,
			SnapshotTerm:  rf.LastIncludedTerm,
		}

		rf.lastApplied = applyMsg.SnapshotIndex
		if rf.commitIndex < rf.lastApplied {
			rf.commitIndex = rf.lastApplied
		}
		DEBUG(dCommit, "S%v apply: %v %v ", rf.me, rf.lastApplied, rf.commitIndex)
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.cond.Signal()
	} else {
		rf.mu.Unlock()
	}
	// }()
	// rf.mu.Unlock()

}

func (rf *Raft) AppendEntries(args *AppendEntiresArgs, reply *AppendEntiresReply) {
	rf.cond.Signal()
	rf.mu.Lock()
	// ice := false
	if args.SnapShot != 0 {
		// for {
		// 	rf.mu.Lock()
		// 	if args.SnapShot > rf.lastApplied {
		// 		rf.mu.Unlock()
		// 		time.Sleep(30 * time.Millisecond)
		// 	} else {
		// 		break
		// 	}
		// }
		// ice = true
		for args.SnapShot > rf.lastApplied {
			rf.cond.Wait()
		}
	}
	DEBUG(dLeader, "S%v %v %v %v %v %v %v", rf.me, rf.commitIndex, rf.lastApplied, rf.LastLogIndex, rf.LastIncludedIndex, args.PrevLogIndex)
	DEBUG(dLog, "S%v get hbt or ae from %v", rf.me, args.LeaderId)
	// DEBUG(dError, "S%v leader: %v log: %v at T:%v", rf.me, rf.leader, rf.Log, rf.CurrentTerm)
	if args.Term > rf.CurrentTerm {
		DEBUG(dTrace, "S%v becomeF in ae", rf.me)
		rf.becomeFowllower(args.LeaderId, args.Term)
		reply.Success = false
		reply.Term = rf.CurrentTerm
		reply.CommitIndex = rf.commitIndex
		reply.CommitTerm = rf.Log[rf.commitIndex].Term
		reply.ConflictLogIndex = rf.LastLogIndex
		rf.persist()
		rf.mu.Unlock()
		return
	} else if args.Term < rf.CurrentTerm {
		DEBUG(dTrace, "S%v reject term in ae f-l:%v %v", rf.me, rf.CurrentTerm, args.Term)
		reply.Success = false
		reply.Term = rf.CurrentTerm
		reply.CommitIndex = rf.commitIndex
		reply.CommitTerm = rf.Log[rf.commitIndex].Term
		rf.persist()
		rf.mu.Unlock()
		return
	} else {
		if rf.state == Sfollower && rf.leader != args.LeaderId {
			DEBUG(dTrace, "S%v reject leader in ae f-l:%v %v ll:%v", rf.me, rf.leader, args.LeaderId, rf.LastLogIndex)
			if rf.leader == -1 {
				rf.becomeFowllower(args.LeaderId, rf.CurrentTerm)
			} else {
				reply.Success = false
				reply.Term = rf.CurrentTerm
				DEBUG(dTimer, "S%v timer: %v, timeout: %v", rf.me, rf.timer, rf.timeout)
				rf.mu.Unlock()
				return
			}

		}
		if rf.LastLogIndex < args.PrevLogIndex {
			DEBUG(dTrace, "S%v comp index failed in ae f-l:%v %v ll:%v", rf.me, rf.LastLogIndex, args.PrevLogIndex, rf.LastLogIndex)
			reply.Success = false
			reply.Term = rf.CurrentTerm
			minidx := rf.LastLogIndex
			for _, entry := range rf.Log {
				if entry.Term == rf.CurrentTerm && entry.Index < minidx {
					minidx = entry.Index
				}
			}
			reply.ConflictLogIndex = minidx + 1
		} else {
			if _, ok := rf.Log[args.PrevLogIndex]; ok {
				if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
					DEBUG(dTrace, "S%v comp term failed in ae f-l:%v %v ll:%v", rf.me, rf.Log[args.PrevLogIndex].Term, args.PrevLogTerm, rf.LastLogIndex)
					reply.Success = false
					reply.Term = rf.CurrentTerm
					minidx := rf.LastLogIndex
					for _, entry := range rf.Log {
						if entry.Index < minidx {
							minidx = entry.Index
						}
					}
					reply.ConflictLogIndex = minidx + 1
				} else {
					for i, entry := range args.AppendEntries {
						if rf.LastLogIndex+1 <= entry.Index {
							for _, en := range args.AppendEntries[i:] {
								rf.Log[en.Index] = en
							}
							rf.LastLogIndex = args.AppendEntries[len(args.AppendEntries)-1].Index
							break
						}
						if rf.Log[entry.Index].Term != entry.Term {
							for _, en := range rf.Log {
								if en.Index >= entry.Index {
									delete(rf.Log, en.Index)
								}
							}
							for _, en := range args.AppendEntries[i:] {
								rf.Log[en.Index] = en
							}
							rf.LastLogIndex = args.AppendEntries[len(args.AppendEntries)-1].Index
							break
						}
					}
					reply.Success = true
					reply.Term = rf.CurrentTerm
					DEBUG(dTrace, "S%v copy succ in ae log :ll :%v", rf.me, rf.LastLogIndex)
					// DEBUG(dTrace, "S%v copy succ in ae log : %v ll :%v", rf.me, rf.Log, rf.LastLogIndex)
				}
			} else {
				reply.Success = false
				reply.Term = rf.CurrentTerm
				minidx := rf.LastLogIndex
				for _, entry := range rf.Log {
					if entry.Index < minidx {
						minidx = entry.Index
					}
				}
				DEBUG(dTrace, "S%v comp index failed in ae f-l:%v %v min:%v", rf.me, rf.LastLogIndex, args.PrevLogIndex, minidx)
				reply.ConflictLogIndex = minidx + 1
			}
		}
		rf.persist()
		rf.becomeFowllower(args.LeaderId, args.Term)
		if !reply.Success {
			rf.mu.Unlock()
			return
		}
	}

	//update commitindex
	if args.LeaderCommit > rf.commitIndex {
		if len(args.AppendEntries) != 0 {
			if args.LeaderCommit < args.AppendEntries[len(args.AppendEntries)-1].Index {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = args.AppendEntries[len(args.AppendEntries)-1].Index
			}
		} else {
			if args.LeaderCommit < rf.Log[rf.LastLogIndex].Index {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.Log[rf.LastLogIndex].Index
			}
		}
		DEBUG(dCommit, "S%v update commitindex to %v", rf.me, rf.commitIndex)

	}
	rf.cond.Signal()
	rf.mu.Unlock()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	DEBUG(dClient, "S%v <- client isleader:%v", rf.me, isLeader)
	if isLeader {
		rf.mu.Lock()
		rf.LastLogIndex++
		index = rf.LastLogIndex
		en := Entries{command, term, index}
		rf.Log[index] = en
		// DEBUG(dLog2, "S%v log: %v", rf.me, rf.Log)
		// rf.StartSendAppendEntries(rf.CurrentTerm)
		// rf.timer = 0
		DEBUG(dLeader, "S%v start get %v", rf.me, en)
		if rf.timer > 50 {
			go rf.StartSendAppendEntries(rf.CurrentTerm)
			rf.timer = 0
		}
		rf.persist()
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// start:=time.Now().UnixMilli()

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		_, isleader := rf.GetState()
		rf.mu.Lock()
		tmterm := rf.CurrentTerm
		if rf.timer >= rf.timeout {
			if isleader {
				rf.timer = 0
				rf.persist()
				go func() {
					rf.StartSendAppendEntries(tmterm)

				}()
			} else {
				rf.becomeCandidate()
				go rf.sendvote()
			}
		}
		rf.timer++
		// DEBUG(dTimer, "S%v timer: %v", rf.me, rf.timer)
		rf.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.Persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//2A
	LOGinit()
	rf.becomeFowllower(-1, 0)
	rf.commitIndex = 0
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.votes = 0
	rf.Log = make(map[int]Entries)
	rf.Log[0] = Entries{0, 0, 0}
	rf.LastLogIndex = 0
	// sync.Opts.DeadlockTimeout = time.Mil                                                                                                                              lisecond * 1000
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.applyCh = applyCh
	rf.SnapshotData = nil
	rf.LastIncludedIndex = -1
	rf.LastIncludedTerm = -1
	rf.cond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyGoro(applyCh)
	DEBUG(dTrace, "S%v Started at T:%v TI: %v log: %v ll: %v li: %v la:%v", me, rf.CurrentTerm, rf.timeout, rf.Log, rf.LastLogIndex, rf.LastIncludedIndex, rf.lastApplied)
	return rf
}

func (rf *Raft) becomeLeader() {
	rf.timeout = 95
	rf.timer = 0
	for i := range rf.matchIndex {
		delete(rf.matchIndex, rf.matchIndex[i])
	}
	for i := range rf.nextIndex {
		delete(rf.nextIndex, rf.nextIndex[i])
	}
	rf.state = Sleader
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.LastLogIndex + 1
		rf.matchIndex[i] = 0
	}
}
func (rf *Raft) becomeCandidate() {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	rand.Seed(x)
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.state = Scandidate
	rf.votes = 1
	rf.timeout = rand.Intn(200) + 400
	rf.timer = 0
	// tmterm := rf.CurrentTerm
	// DEBUG(dTimer, "S%v timeout Reset: %v", rf.me, rf.timeout)
	DEBUG(dTerm, "S%v Converting to Candidate, calling election T:%v", rf.me, rf.CurrentTerm)
	rf.persist()
}

func (rf *Raft) becomeFowllower(leaderId int, Term int) {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	rand.Seed(x)
	if Term > rf.CurrentTerm {
		rf.VotedFor = -1
		rf.CurrentTerm = Term
		rf.persist()
	}
	rf.state = Sfollower
	rf.leader = leaderId

	rf.timeout = rand.Intn(200) + 400
	rf.timer = 0
	// DEBUG(dTimer, "S%v timeout Reset: %v", rf.me, rf.timeout)
}

func (rf *Raft) sendvote() {
	var lock sync.Mutex
	var cond sync.WaitGroup
	truevotes := 1
	cond.Add(len(rf.peers) - 1)
	rf.mu.Lock()
	tmTerm := rf.CurrentTerm
	log := make(map[int]Entries)
	for k, v := range rf.Log {
		log[k] = Entries{
			Command: v.Command,
			Index:   v.Index,
			Term:    v.Term,
		}
	}
	maxlog := rf.LastLogIndex
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			args := RequestVoteArgs{}
			reply := RequestVoteReply{}
			args.CandidateId = rf.me
			args.Term = tmTerm
			args.LastLogIndex = maxlog
			args.LastLogTerm = log[maxlog].Term
			rf.sendRequestVote(i, &args, &reply)
			// ok := rf.sendRequestVote(i, &args, &reply)
			// if !ok {
			// 	DEBUG(dError, "S%v -> S%v voterpc failed at T: %v", rf.me, i, rf.CurrentTerm)
			// }
			if reply.VoteGranted {
				DEBUG(dVote, "S%v <- S%v Got vote", rf.me, i)
				lock.Lock()
				truevotes++
				num := len(rf.peers)
				if truevotes >= (num+1)/2 {
					rf.mu.Lock()
					if tmTerm == rf.CurrentTerm && rf.state == Scandidate {
						DEBUG(dLeader, "S%v Achieved Majority for T:%v(%v %v), converting to Leader", rf.me, rf.CurrentTerm, truevotes, num)
						rf.becomeLeader()
						rf.StartSendAppendEntries(rf.CurrentTerm)
					}
					rf.mu.Unlock()
				}
				lock.Unlock()
			} else {
				rf.mu.Lock()
				if reply.Term > rf.CurrentTerm {
					rf.becomeFowllower(i, reply.Term)
				}
				rf.mu.Unlock()
			}
			cond.Done()
		}(i)
	}

	cond.Wait()
}

func (rf *Raft) StartSendAppendEntries(tmterm int) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.SendAppendEntriesTo(i, tmterm)
	}
}

func (rf *Raft) applyGoro(applyCh chan ApplyMsg) {

	for {
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.cond.Wait()
		}
		// lastApplied := rf.lastApplied
		// commitIndex := rf.commitIndex
		// log := make(map[int]Entries)
		// for _, en := range rf.Log {
		// 	entry := Entries{
		// 		Command: en.Command,
		// 		Index: en.Index,
		// 		Term: en.Term,
		// 	}
		// 	log[en.Index] = entry
		// }
		// rf.mu.Unlock()
		for rf.lastApplied < rf.commitIndex {
			// rf.mu.Lock()
			if _, ok := rf.Log[rf.lastApplied+1]; ok {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.Log[rf.lastApplied+1].Command,
					CommandIndex: rf.lastApplied + 1,
				}
				rf.lastApplied++
				rf.mu.Unlock()
				applyCh <- applyMsg
				rf.mu.Lock()
				DEBUG(dCommit, "S%v apply: %v", rf.me, applyMsg)
			} else {
				break
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) SendAppendEntriesTo(i int, tmterm int) {
	rf.mu.Lock()
	args := AppendEntiresArgs{}
	reply := AppendEntiresReply{}
	args.Term = tmterm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	snapflag := 0
	if rf.LastLogIndex >= rf.matchIndex[i] {
		if _, ok := rf.Log[rf.matchIndex[i]]; !ok {
			snapshotArgs := InstallSnapshotArgs{
				Term:              rf.CurrentTerm,
				LeaderId:          rf.me,
				Data:              rf.SnapshotData,
				LastIncludedIndex: rf.LastIncludedIndex,
				LastIncludedTerm:  rf.LastIncludedTerm,
			}
			snapshotReply := InstallSnapshotReply{}
			go rf.sendSnapshot(i, &snapshotArgs, &snapshotReply)
			rf.matchIndex[i] = rf.LastIncludedIndex
			rf.nextIndex[i] = rf.LastIncludedIndex + 1
			snapflag = rf.LastIncludedIndex
		}
		for idx := rf.nextIndex[i]; idx <= rf.LastLogIndex; idx++ {
			args.AppendEntries = append(args.AppendEntries, Entries{
				Command: rf.Log[idx].Command,
				Term:    rf.Log[idx].Term,
				Index:   rf.Log[idx].Index,
			})
		}
	}
	if rf.nextIndex[i] == 0 {
		args.PrevLogIndex = 0
	} else {
		args.PrevLogIndex = rf.nextIndex[i] - 1
	}
	args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
	args.SnapShot = snapflag
	// DEBUG(dLog2, "S%v -> %v send AE pi: %v, pt: %v log: %v at T:%v", rf.me, i, args.PrevLogIndex, args.PrevLogTerm, args.AppendEntries, rf.CurrentTerm)
	DEBUG(dLog2, "S%v n: %v m: %v", rf.me, rf.nextIndex, rf.matchIndex)
	rf.mu.Unlock()

	rf.sendAppendEntries(i, &args, &reply)
	DEBUG(dLog2, "S%v ->%v appendentry is %v", rf.me, i, reply.Success)
	rf.mu.Lock()
	if rf.state == Sleader && rf.CurrentTerm == tmterm {

		if reply.Success {
			if len(args.AppendEntries) != 0 {
				rf.matchIndex[i] = args.AppendEntries[len(args.AppendEntries)-1].Index
			} else {
				rf.matchIndex[i] = args.PrevLogIndex
			}
			rf.nextIndex[i] = rf.matchIndex[i] + 1

			DEBUG(dLog, "S%v commitidx: %v, matchidx: %v", rf.me, rf.commitIndex, rf.matchIndex[i])
			if rf.commitIndex < rf.matchIndex[i] && rf.CurrentTerm == rf.Log[rf.matchIndex[i]].Term {
				num := 1
				for _, v := range rf.matchIndex {
					if v >= rf.matchIndex[i] {
						num++
					}
				}
				// DEBUG(dError, "S%v num: %v", rf.me, num)
				if num >= (len(rf.peers)+1)/2 {
					rf.commitIndex = rf.matchIndex[i]
					rf.cond.Signal()
				}
			}
			// DEBUG(dLog2, "S%v n: %v m: %v ci: %v", rf.me, rf.nextIndex, rf.matchIndex, rf.commitIndex)
		} else {
			// DEBUG(dTrace, "S%v ae failed F-L:%v-%v", rf.me, reply.CommitIndex, rf.commitIndex)
			if rf.LastLogIndex < reply.CommitIndex {
				for _, en := range rf.Log {
					if en.Index > rf.commitIndex {
						delete(rf.Log, en.Index)
					}
				}
				rf.LastLogIndex = rf.commitIndex
				rf.becomeFowllower(-1, reply.Term)
				rf.persist()
			} else if rf.Log[reply.CommitIndex].Term != reply.CommitTerm {
				if _, ok := rf.Log[reply.CommitIndex]; ok {
					for _, en := range rf.Log {
						if en.Index > rf.commitIndex {
							delete(rf.Log, en.Index)
						}
					}
					rf.LastLogIndex = rf.commitIndex
					rf.becomeFowllower(-1, reply.Term)
					rf.persist()
				}
			}
			// DEBUG(dLog2, "S%v log: %v", rf.me, rf.Log)
			if reply.Term > rf.CurrentTerm {
				rf.becomeFowllower(-1, reply.Term)
				rf.persist()
			} else {
				rf.matchIndex[i] = reply.ConflictLogIndex - 1
				rf.nextIndex[i] = rf.matchIndex[i] + 1
				if rf.matchIndex[i] < 0 {
					rf.matchIndex[i] = 0
					rf.nextIndex[i] = 1
				}
				// DEBUG(dError, "S%v i: %v, nextidx: %v", rf.me, i, rf.nextIndex[i])
			}
		}
	}
	rf.mu.Unlock()
}

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
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//2A
	CurrentTerm int
	VotedFor    int
	Log         []Entries
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

	applymsg chan ApplyMsg
	cond     *sync.Cond
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.mu.Lock()
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	rf.mu.Unlock()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	var log []Entries
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil{
		DEBUG(dError, "S%v readPersist failed", rf.me)
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		rf.mu.Unlock()
	}
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
}

type AppendEntiresArgs struct {
	Term          int
	LeaderId      int
	PrevLogIndex  int
	PrevLogTerm   int
	AppendEntries []Entries
	LeaderCommit  int
}

type AppendEntiresReply struct {
	Maxindex int
	Term     int
	Success  bool
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
		DEBUG(dTerm, "S%v Term is lower, updating (%v < %v)", rf.me, rf.CurrentTerm, args.Term)
		DEBUG(dVote, "S%v c:T:%v I:%v, m: T:%v I:%v", rf.me, args.LastLogTerm, args.LastLogIndex, rf.Log[len(rf.Log)-1].Term, rf.Log[len(rf.Log)-1].Index)

		if rf.Log[len(rf.Log)-1].Term > args.LastLogTerm {
			reply.VoteGranted = false
		} else if rf.Log[len(rf.Log)-1].Term < args.LastLogTerm {
			reply.VoteGranted = true
		} else {
			if len(rf.Log)-1 > args.LastLogIndex {
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
			rf.CurrentTerm = args.Term
		}
	} else if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		reply.Term = args.Term
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
			if rf.Log[len(rf.Log)-1].Term > args.LastLogTerm {
				reply.VoteGranted = false
			} else if rf.Log[len(rf.Log)-1].Term < args.LastLogTerm {
				reply.VoteGranted = true
				rf.becomeFowllower(args.CandidateId, args.Term)
				rf.VotedFor = args.CandidateId
			} else {
				if len(rf.Log)-1 > args.LastLogIndex {
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
	DEBUG(dVote, "S%v T%v Granting Vote to S%v", rf.me, rf.CurrentTerm, rf.VotedFor)
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

func (rf *Raft) AppendEntries(args *AppendEntiresArgs, reply *AppendEntiresReply) {
	rf.cond.Signal()
	rf.mu.Lock()
	DEBUG(dLog, "S%v get hbt or ae ", rf.me)
	DEBUG(dError, "S%v leader: %v log: %v at T:%v", rf.me, rf.leader, rf.Log, rf.CurrentTerm)
	if args.Term > rf.CurrentTerm {
		rf.becomeFowllower(args.LeaderId, args.Term)
		rf.mu.Unlock()
		return
	} else if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	} else {
		if rf.leader != args.LeaderId {
			reply.Success = false
			reply.Term = rf.CurrentTerm
			DEBUG(dTimer, "S%v timer: %v, timeout: %v", rf.me, rf.timer, rf.timeout)
			rf.mu.Unlock()
			return
		}
		if len(rf.Log)-1 < args.PrevLogIndex {
			reply.Success = false
			reply.Term = rf.CurrentTerm
			// reply.Maxindex = len(rf.Log) - 1
			ppterm := args.PrevLogTerm - 1
			for i := 0; i < len(rf.Log); i++ {
				if rf.Log[i].Term == ppterm {
					reply.Maxindex = rf.Log[i].Index
					break
				}
			}
		} else {
			if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
				reply.Success = false
				reply.Term = rf.CurrentTerm
				ppterm := args.PrevLogTerm - 1
				for i := 0; i < len(rf.Log); i++ {
					if rf.Log[i].Term == ppterm {
						reply.Maxindex = rf.Log[i].Index
						break
					}
				}
			} else {
				for i, j := range args.AppendEntries {
					if len(rf.Log) <= j.Index {
						rf.Log = append(rf.Log, args.AppendEntries[i:]...)
						break
					}
					if rf.Log[j.Index].Term != j.Term {
						rf.Log = rf.Log[:j.Index]
						rf.Log = append(rf.Log, args.AppendEntries[i:]...)
					}
				}
				reply.Success = true
				reply.Term = rf.CurrentTerm
				
				// rf.persist()
			}
		}
		if len(args.AppendEntries) != 0 {
			reply.Maxindex = args.AppendEntries[len(args.AppendEntries)-1].Index
		} else {
			reply.Maxindex = -1
		}
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
			if args.LeaderCommit < rf.Log[len(rf.Log)-1].Index {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.Log[len(rf.Log)-1].Index
			}
		}
		DEBUG(dCommit, "S%v update commitindex to %v", rf.me, rf.commitIndex)

	}
	rf.cond.Signal()
	//apply
	// for rf.commitIndex > rf.lastApplied {
	// 	rf.lastApplied++
	// 	applyMsg := ApplyMsg{
	// 		CommandValid: true,
	// 		CommandIndex: rf.lastApplied,
	// 		Command:      rf.Log[rf.lastApplied].Command,
	// 	}
	// 	rf.applymsg <- applyMsg
	// 	DEBUG(dCommit, "S%v apply: %v", rf.me, applyMsg)
	// }
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
		index = len(rf.Log)
		en := Entries{command, term, index}
		rf.Log = append(rf.Log, en)
		DEBUG(dLog2, "S%v log: %v", rf.me, rf.Log)
		// rf.StartSendAppendEntries(rf.CurrentTerm)
		// rf.timer = 0
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
		if rf.timer >= rf.timeout {
			if isleader {
				rf.timer = 0
				go func() {
					// rf.sendheartbeat(rf.CurrentTerm)
					rf.StartSendAppendEntries(rf.CurrentTerm)

				}()
			} else {
				rf.becomeCandidate()
				go func() {
					rf.sendvote()
				}()
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
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//2A
	LOGinit()
	rf.becomeFowllower(-1, 0)
	rf.commitIndex = 0
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.votes = 0
	rf.Log = append(rf.Log, Entries{0, 0, 0})
	// sync.Opts.DeadlockTimeout = time.Millisecond * 1000
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	// rf.applymsg = applyCh
	rf.cond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyGoro(applyCh)
	DEBUG(dClient, "S%v Started at T:%v TI: %v", me, rf.CurrentTerm, rf.timeout)
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
		rf.nextIndex[i] = len(rf.Log)
		rf.matchIndex[i] = 0
	}
}
func (rf *Raft) becomeCandidate() {
	rand.Seed(time.Now().Local().UnixMicro())
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.state = Scandidate
	rf.votes = 1
	rf.timeout = rand.Intn(400) + 100
	rf.timer = 0
	// tmterm := rf.CurrentTerm
	DEBUG(dTimer, "S%v timeout Reset: %v", rf.me, rf.timeout)
	DEBUG(dTerm, "S%v Converting to Candidate, calling election T:%v", rf.me, rf.CurrentTerm)

}

func (rf *Raft) becomeFowllower(leaderId int, Term int) {
	rand.Seed(time.Now().Local().UnixMicro())
	if Term > rf.CurrentTerm {
		rf.VotedFor = -1
		rf.CurrentTerm = Term
	}
	rf.state = Sfollower
	rf.leader = leaderId

	rf.timeout = rand.Intn(400) + 100
	rf.timer = 0
	DEBUG(dTimer, "S%v timeout Reset: %v", rf.me, rf.timeout)
}

func (rf *Raft) sendheartbeat(tmterm int) {
	DEBUG(dTimer, "S%v Broadcast, resetting HTB", rf.me)
	DEBUG(dTimer, "S%v HTB time: %v", rf.me, time.Now().UnixMilli())
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int, tmterm int) {
			args := AppendEntiresArgs{}
			reply := AppendEntiresReply{}
			args.LeaderId = rf.me
			args.Term = tmterm
			args.LeaderCommit = rf.commitIndex

			args.PrevLogIndex = 0
			args.PrevLogTerm = 0
			DEBUG(dLog, "S%v -> S%v SendingHTB PLI: %v PLT:%v LC:%v - %v at T: %v", rf.me, i, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.AppendEntries, tmterm)
			rf.sendAppendEntries(i, &args, &reply)
			// ok := rf.sendAppendEntries(i, &args, &reply)
			// if !ok {
			// 	DEBUG(dError, "S%v -> S%v sendingHTB failed at T: %v", rf.me, i, rf.CurrentTerm)
			// }
			term, isleader := rf.GetState()
			if isleader && term == tmterm {
				DEBUG(dLog, "S%v <- S%v %v Append MI: %v", rf.me, i, reply.Success, reply.Term)
				if reply.Term > term {
					rf.mu.Lock()
					rf.becomeFowllower(-1, reply.Term)
					rf.mu.Unlock()
				}
			}
		}(i, tmterm)
	}
}

func (rf *Raft) sendvote() {
	var lock sync.Mutex
	var cond sync.WaitGroup
	truevotes := 1
	cond.Add(len(rf.peers) - 1)
	rf.mu.Lock()
	tmTerm := rf.CurrentTerm
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
			rf.mu.Lock()
			if len(rf.Log) == 1 {
				args.LastLogIndex = 0
			} else {
				args.LastLogIndex = len(rf.Log) - 1

			}
			args.LastLogTerm = rf.Log[args.LastLogIndex].Term
			rf.mu.Unlock()
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
	// rf.persist()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			args := AppendEntiresArgs{}
			reply := AppendEntiresReply{}
			args.Term = tmterm
			args.LeaderId = rf.me
			rf.mu.Lock()
			args.LeaderCommit = rf.commitIndex
			if rf.nextIndex[i] == 0 {
				args.PrevLogIndex = 0
			} else {
				args.PrevLogIndex = rf.nextIndex[i] - 1
			}
			if len(rf.Log)-1 >= rf.nextIndex[i] {
				args.AppendEntries = rf.Log[rf.nextIndex[i]:]
			}
			args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
			DEBUG(dLog2, "S%v -> %v send AE pi: %v, pt: %v log: %v at T:%v", rf.me, i, args.PrevLogIndex, args.PrevLogTerm, args.AppendEntries, rf.CurrentTerm)
			DEBUG(dLog2, "S%v n: %v m: %v", rf.me, rf.nextIndex, rf.matchIndex)
			rf.mu.Unlock()

			rf.sendAppendEntries(i, &args, &reply)
			DEBUG(dLog2, "S%v ->%v appendentry is %v", rf.me, i, reply.Success)
			rf.mu.Lock()
			if rf.state == Sleader && rf.CurrentTerm == tmterm {

				if reply.Success {
					if len(args.AppendEntries) != 0 {
						rf.matchIndex[i] = args.AppendEntries[len(args.AppendEntries)-1].Index
					}
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					DEBUG(dLog, "S%v commitidx: %v, matchidx: %v", rf.me, rf.commitIndex, rf.matchIndex[i])
					if rf.commitIndex < rf.matchIndex[i] {
						num := 1
						for _, v := range rf.matchIndex {
							if v >= rf.matchIndex[i] {
								num++
							}
						}
						DEBUG(dError, "S%v num: %v", rf.me, num)
						if num >= (len(rf.peers)+1)/2 {
							rf.commitIndex = rf.matchIndex[i]
							// for rf.lastApplied < rf.commitIndex {
							// 	rf.lastApplied++
							// 	applyMsg := ApplyMsg{
							// 		CommandValid: true,
							// 		Command:      rf.Log[rf.lastApplied].Command,
							// 		CommandIndex: rf.lastApplied,
							// 	}

							// 	rf.applymsg <- applyMsg
							// 	DEBUG(dCommit, "S%v commit: %v", rf.me, applyMsg)
							// }
							rf.cond.Signal()
						}
					}
					DEBUG(dLog2, "S%v n: %v m: %v ci: %v", rf.me, rf.nextIndex, rf.matchIndex, rf.commitIndex)
				} else {
					if reply.Term > rf.CurrentTerm {
						rf.becomeFowllower(-1, reply.Term)
					} else {
						rf.nextIndex[i] = reply.Maxindex
						if rf.nextIndex[i] == -1 {
							rf.nextIndex[i] = 0
						}
						DEBUG(dError, "S%v i: %v, nextidx: %v", rf.me, i, rf.nextIndex[i])

					}

				}
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) applyGoro(applyCh chan ApplyMsg) {

	for {
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.cond.Wait()
		}
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		log := rf.Log
		rf.mu.Unlock()
		for lastApplied < commitIndex {
			lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      log[lastApplied].Command,
				CommandIndex: lastApplied,
			}
			applyCh <- applyMsg
			rf.persist()
		}
		rf.mu.Lock()
		rf.lastApplied = lastApplied
		rf.mu.Unlock()
	}
}

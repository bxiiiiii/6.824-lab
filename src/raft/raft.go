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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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
	currentTerm int
	votedFor    int
	log         []Entries
	state       int
	leader      int
	votes       int
	done        bool
	timerMutex sync.Mutex

	commitIndex int
	lastApplied int
	// heartbeatInerval int
	timeout int
	timer   int

	nextIndex  map[int]int
	matchIndex map[int]int
}

type Entries struct {
	Log   string
	Term  int
	Index int
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
	term = rf.currentTerm
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//2A
	DEBUG(dVote, "S%v C%v asking for vote", rf.me, args.CandidateId)
	if rf.currentTerm < args.Term {
		DEBUG(dTerm, "S%v Term is lower, updating (%v < %v)", rf.me, rf.currentTerm, args.Term)
		rf.becomeFowllower(args.CandidateId, args.Term)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		reply.Term = args.Term
		//logTerm and logIndex compare candidate
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}
	DEBUG(dVote, "S%v T%v Granting Vote to S%v", rf.me, rf.currentTerm, rf.votedFor)
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
	if args.Term >= rf.currentTerm {
		if len(args.AppendEntries) == 0 && rf.currentTerm == args.Term {
			DEBUG(dLog, "S%v get heartbeat from S%v at T: %v", rf.me, args.LeaderId, args.Term)
			rf.becomeFowllower(args.LeaderId, args.Term)
			reply.Success = true
		}
	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
	}

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
		// DEBUG(dTimer, "S%v start: %v", rf.me, start)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		_, isleader := rf.GetState()
		rf.mu.Lock()
		if rf.timer >= rf.timeout {
			if isleader {
				go rf.becomeLeader()
			} else {
				go rf.becomeCandidate()
			}
		}
		// ti:=time.Since(start).Milliseconds()
		
		rf.timer++
		// 
		// end := time.Now().UnixMilli()
		// DEBUG(dError, "S%v timer: %v end: %v", rf.me, rf.timer, end)
		// rf.timer += int(end) - int(start)
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
	LOGinit()
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//2A
	rf.becomeFowllower(-1, 0)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votes = 0
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	DEBUG(dClient, "S%v Started at T:%v TI: %v", me, rf.currentTerm, rf.timeout)

	return rf
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.state = Sleader
	rf.timeout = 95
	rf.timer = 0
	tmterm := rf.currentTerm
	rf.mu.Unlock()
	var cond sync.WaitGroup
	DEBUG(dTimer, "S%v Broadcast, resetting HTB", rf.me)
	DEBUG(dTimer, "S%v HTB time: %v", rf.me, time.Now().UnixMilli())
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		cond.Add(1)
		go func(i int) {

			args := AppendEntiresArgs{}
			reply := AppendEntiresReply{}
			args.LeaderId = rf.me
			args.Term = tmterm
			commitid := 0
			if len(rf.log) > 0 {
				commitid = rf.log[len(rf.log)-1].Index
			} else {
				commitid = 0
			}
			args.LeaderCommit = commitid
			args.PrevLogIndex = 0
			args.PrevLogTerm = 0
			DEBUG(dLog, "S%v -> S%v SendingHTB PLI: %v PLT:%v LC:%v - %v at T: %v", rf.me, i, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.AppendEntries, tmterm)
			rf.sendAppendEntries(i, &args, &reply)
			// ok := rf.sendAppendEntries(i, &args, &reply)
			// if !ok {
			// 	DEBUG(dError, "S%v -> S%v sendingHTB failed at T: %v", rf.me, i, rf.currentTerm)
			// }
			term, isleader := rf.GetState()
			if isleader && term == tmterm {
				DEBUG(dLog, "S%v <- S%v %v Append MI: %v", rf.me, i, reply.Success, reply.Term)
				if reply.Term > term {
					rf.becomeFowllower(-1, reply.Term)
				}
				rf.mu.Lock()
				rf.nextIndex[i] = rf.commitIndex + 1
				rf.matchIndex[i] = 0
				rf.mu.Unlock()
			}
		
			cond.Done()
		}(i)
	}
	cond.Wait()
}
func (rf *Raft) becomeCandidate() {
	rand.Seed(time.Now().Local().UnixMicro())
	var lock sync.Mutex
	var cond sync.WaitGroup
	rf.mu.Lock()
	rf.currentTerm++
	tmTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.state = Scandidate
	rf.votes = 1
	rf.timeout = rand.Intn(200) + 200
	rf.timer = 0
	// tmterm := rf.currentTerm
	rf.mu.Unlock()
	DEBUG(dTimer, "S%v timeout Reset: %v", rf.me, rf.timeout)
	DEBUG(dTerm, "S%v Converting to Candidate, calling election T:%v", rf.me, rf.currentTerm)

	truevotes := 1
	falsevotes := 0

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		cond.Add(1)
		go func(i int) {
			args := RequestVoteArgs{}
			reply := RequestVoteReply{}
			args.CandidateId = rf.me
			args.Term = tmTerm

			args.LastLogIndex = rf.commitIndex
			if rf.commitIndex == 0 {
				args.LastLogTerm = 0
			} else {
				args.LastLogTerm = rf.log[rf.commitIndex].Term
			}
			rf.sendRequestVote(i, &args, &reply)
			// ok := rf.sendRequestVote(i, &args, &reply)
			// if !ok {
			// 	DEBUG(dError, "S%v -> S%v voterpc failed at T: %v", rf.me, i, rf.currentTerm)
			// }
			if reply.VoteGranted {
				DEBUG(dVote, "S%v <- S%v Got vote", rf.me, i)
				// atomic.AddInt64(&truevotes, 1)
				lock.Lock()
				truevotes++
				lock.Unlock()
			} else {
				if reply.Term > rf.currentTerm {
					rf.becomeFowllower(i, reply.Term)
				} else {
					lock.Lock()
					falsevotes++
					// num := len(rf.peers)
					// if int(truevotes) > num/2 {
					// 	rf.becomeFowllower(reply.)
					// 	return
					// }
					lock.Unlock()
				}
			}
			cond.Done()
		}(i)
	}

	for {
		num := len(rf.peers)
		lock.Lock()
		if truevotes > num/2 {
			if tmTerm == rf.currentTerm {
				DEBUG(dLeader, "S%v Achieved Majority for T:%v(%v %v), converting to Leader", rf.me, rf.currentTerm, truevotes, num)
				rf.becomeLeader()
				lock.Unlock()
				break
			}
		}
		lock.Unlock()
	}
	cond.Wait()
	term, isleader := rf.GetState()
	if !isleader {
		DEBUG(dVote, "S%v election failed at T: %v", rf.me, term)
	}
}

func (rf *Raft) becomeFowllower(leaderId int, Term int) {
	rand.Seed(time.Now().Local().UnixMicro())
	rf.mu.Lock()
	if Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = Term
	}
	rf.state = Sfollower
	rf.leader = leaderId

	rf.timeout = rand.Intn(200) + 200
	rf.timer = 0
	rf.mu.Unlock()
	DEBUG(dTimer, "S%v timeout Reset: %v", rf.me, rf.timeout)
}

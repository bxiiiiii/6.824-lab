package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

import dsync "github.com/sasha-s/go-deadlock"

const (
	Completed     = "Completed"
	InProgress    = "InProgress"
	ErrorOccurred = "ErrorOccurred"
	ErrorTimeDeny = "ErrorTimeDeny"
)

type ShardCtrler struct {
	mu      dsync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	rqRecord   map[int64]RequestInfo
	startTimer int64
	index      int
	cond       *sync.Cond

	applyIndex int
}

type Op struct {
	// Your data here.
	Type  string
	Index int64

	Servers map[int][]string

	GIDs []int

	Shard int
	GID   int

	Num int
}

type RequestInfo struct {
	Status  string
	Rqindex int
	// Type   string
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if _, ok := sc.rqRecord[sc.startTimer]; !ok {
		DEBUG(dPersist, "S%v join deny %v", sc.me, args.Index)
		reply.Err = ErrorTimeDeny
		sc.mu.Unlock()
		return
	}

	for k, v := range sc.rqRecord {
		if k == args.Index {
			switch v.Status {
			case Completed:
				DEBUG(dCommit, "S%v join %v is ok", sc.me, args.Index)
				reply.Err = OK
			case InProgress:
				DEBUG(dDrop, "S%v join %v is waiting", sc.me, args.Index)
				for sc.rqRecord[args.Index].Status == InProgress {
					sc.cond.Wait()
				}
				switch sc.rqRecord[args.Index].Status {
				case ErrorOccurred:
					DEBUG(dCommit, "S%v join %v is failed", sc.me, args.Index)
					reply.Err = ErrorOccurred
				case Completed:
					DEBUG(dCommit, "S%v join %v is ok", sc.me, args.Index)
					reply.Err = OK
				}
			case ErrorOccurred:
				break
			}
			sc.mu.Unlock()
			return
		}
	}

	operation := Op{
		Type:    "Join",
		Index:   args.Index,
		Servers: args.Servers,
	}

	i, _, isLeader := sc.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	DEBUG(dLog2, "S%v join %v", sc.me, i)
	sc.rqRecord[args.Index] = RequestInfo{
		Status:  InProgress,
		Rqindex: i,
	}

	for sc.rqRecord[args.Index].Status == InProgress {
		sc.cond.Wait()
	}
	switch sc.rqRecord[args.Index].Status {
	case ErrorOccurred:
		DEBUG(dCommit, "S%v join %v is failed", sc.me, args.Index)
		reply.Err = ErrorOccurred
	case Completed:
		DEBUG(dInfo, "S%v join %v is ok", sc.me, i)
		reply.Err = OK
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if _, ok := sc.rqRecord[sc.startTimer]; !ok {
		DEBUG(dPersist, "S%v leave deny %v", sc.me, args.Index)
		reply.Err = ErrorTimeDeny
		sc.mu.Unlock()
		return
	}

	for k, v := range sc.rqRecord {
		if k == args.Index {
			switch v.Status {
			case Completed:
				DEBUG(dCommit, "S%v leave %v is ok", sc.me, args.Index)
				reply.Err = OK
			case InProgress:
				DEBUG(dDrop, "S%v leave %v is waiting", sc.me, args.Index)
				for sc.rqRecord[args.Index].Status == InProgress {
					sc.cond.Wait()
				}
				switch sc.rqRecord[args.Index].Status {
				case ErrorOccurred:
					DEBUG(dCommit, "S%v leave %v is failed", sc.me, args.Index)
					reply.Err = ErrorOccurred
				case Completed:
					DEBUG(dCommit, "S%v leave %v is ok", sc.me, args.Index)
					reply.Err = OK
				}
			case ErrorOccurred:
				break
			}
			sc.mu.Unlock()
			return
		}
	}

	operation := Op{
		Type:  "Leave",
		Index: args.Index,
		GIDs:  args.GIDs,
	}

	i, _, isLeader := sc.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	DEBUG(dLog2, "S%v leave %v", sc.me, i)
	sc.rqRecord[args.Index] = RequestInfo{
		Status:  InProgress,
		Rqindex: i,
	}

	for sc.rqRecord[args.Index].Status == InProgress {
		sc.cond.Wait()
	}
	switch sc.rqRecord[args.Index].Status {
	case ErrorOccurred:
		DEBUG(dInfo, "S%v leave %v is failed", sc.me, i)
		reply.Err = ErrorOccurred
	case Completed:
		DEBUG(dInfo, "S%v leave %v is ok", sc.me, i)
		reply.Err = OK
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if _, ok := sc.rqRecord[sc.startTimer]; !ok {
		DEBUG(dPersist, "S%v move deny %v", sc.me, args.Index)
		reply.Err = ErrorTimeDeny
		sc.mu.Unlock()
		return
	}

	for k, v := range sc.rqRecord {
		if k == args.Index {
			switch v.Status {
			case Completed:
				DEBUG(dCommit, "S%v move %v is ok", sc.me, args.Index)
				reply.Err = OK
				reply.Err = OK
			case InProgress:
				DEBUG(dDrop, "S%v move %v is waiting", sc.me, args.Index)
				for sc.rqRecord[args.Index].Status == InProgress {
					sc.cond.Wait()
				}
				switch sc.rqRecord[args.Index].Status {
				case ErrorOccurred:
					DEBUG(dCommit, "S%v move %v is failed", sc.me, args.Index)
					reply.Err = ErrorOccurred
				case Completed:
					DEBUG(dCommit, "S%v move %v is ok", sc.me, args.Index)
					reply.Err = OK
				}
			case ErrorOccurred:
				break
			}
			sc.mu.Unlock()
			return
		}
	}

	operation := Op{
		Type:  "Move",
		Index: args.Index,
		Shard: args.Shard,
		GID:   args.GID,
	}

	i, _, isLeader := sc.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	DEBUG(dLog2, "S%v move %v", sc.me, i)
	sc.rqRecord[args.Index] = RequestInfo{
		Status:  InProgress,
		Rqindex: i,
	}

	for sc.rqRecord[args.Index].Status == InProgress {
		sc.cond.Wait()
	}
	switch sc.rqRecord[args.Index].Status {
	case ErrorOccurred:
		DEBUG(dInfo, "S%v move %v is failed", sc.me, i)
		reply.Err = ErrorOccurred
	case Completed:
		DEBUG(dInfo, "S%v move %v is ok", sc.me, i)
		reply.Err = OK
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if _, ok := sc.rqRecord[sc.startTimer]; !ok {
		DEBUG(dPersist, "S%v query deny %v", sc.me, args.Index)
		reply.Err = ErrorTimeDeny
		sc.mu.Unlock()
		return
	}

	for k, v := range sc.rqRecord {
		if k == args.Index {
			switch v.Status {
			case Completed:
				DEBUG(dCommit, "S%v query %v is ok", sc.me, args.Index)
				reply.Err = OK
			case InProgress:
				DEBUG(dDrop, "S%v query %v is waiting", sc.me, args.Index)
				for sc.rqRecord[args.Index].Status == InProgress {
					sc.cond.Wait()
				}
				switch sc.rqRecord[args.Index].Status {
				case ErrorOccurred:
					DEBUG(dCommit, "S%v query %v is failed", sc.me, args.Index)
					reply.Err = ErrorOccurred
				case Completed:
					DEBUG(dCommit, "S%v query %v is ok", sc.me, args.Index)
					reply.Err = OK
					if args.Num == -1 || args.Num >= len(sc.configs) {
						reply.Config = sc.configs[len(sc.configs)-1]
					} else {
						reply.Config = sc.configs[args.Num]
					}
				}
			case ErrorOccurred:
				break
			}
			sc.mu.Unlock()
			return
		}
	}

	operation := Op{
		Type:  "Query",
		Index: args.Index,
		Num:   args.Num,
	}

	i, _, isLeader := sc.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	DEBUG(dLog2, "S%v query %v", sc.me, i)
	sc.rqRecord[args.Index] = RequestInfo{
		Status:  InProgress,
		Rqindex: i,
	}

	for sc.rqRecord[args.Index].Status == InProgress {
		sc.cond.Wait()
	}
	switch sc.rqRecord[args.Index].Status {
	case ErrorOccurred:
		DEBUG(dCommit, "S%v query %v is failed", sc.me, args.Index)
		reply.Err = ErrorOccurred
	case Completed:
		DEBUG(dInfo, "S%v query %v is ok", sc.me, i)
		reply.Err = OK
		if args.Num == -1 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
	}
	sc.mu.Unlock()
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.applyIndex = 0
	sc.cond = sync.NewCond(&sc.mu)
	sc.startTimer = -1
	sc.rqRecord = make(map[int64]RequestInfo)
	// sync.Opts.DeadlockTimeout = time.Second*1
	var newShards [NShards]int
	for i := range newShards {
		newShards[i] = 0
	}
	// newGroups := make(map[int][]string)
	sc.configs[0] = Config{
		Num:    0,
		Shards: newShards,
	}
	LOGinit()
	go sc.Timer()
	go sc.Apply()
	return sc
}

func (sc *ShardCtrler) Apply() {
	for entry := range sc.applyCh {
		if entry.CommandValid {
			sc.mu.Lock()
			if entry.CommandIndex <= sc.applyIndex {
				sc.mu.Unlock()
				continue
			}
			op := (entry.Command).(Op)
			switch op.Type {
			case "Join":
				sc.ApplyJoin(op.Servers)
			case "Leave":
				sc.ApplyLeave(op.GIDs)
			case "Move":
				sc.ApplyMove(op.Shard, op.GID)
			case "LeaderTimer":
				if sc.index == entry.CommandIndex || sc.startTimer == -1 {
					sc.startTimer = op.Index
					DEBUG(dClient, "S%v timer: %v record:%v", sc.me, sc.startTimer, sc.rqRecord)
				}
			}
			for k, v := range sc.rqRecord {
				if v.Rqindex == entry.CommandIndex {
					if k != op.Index {
						sc.rqRecord[k] = RequestInfo{
							Status:  ErrorOccurred,
							Rqindex: v.Rqindex,
						}
					}
				}
			}
			sc.rqRecord[op.Index] = RequestInfo{
				Status:  Completed,
				Rqindex: entry.CommandIndex,
			}
			DEBUG(dError, "S%v after apply [%v]", sc.me, sc.rqRecord)
			sc.applyIndex = entry.CommandIndex
			sc.mu.Unlock()
			sc.cond.Broadcast()
		}
	}
}

func (sc *ShardCtrler) ApplyJoin(servers map[int][]string) {
	// lastConfigNum := len(sc.configs) - 1
	newConfigNum := len(sc.configs)
	newGroups := make(map[int][]string)

	for k, v := range sc.configs[newConfigNum-1].Groups {
		newGroups[k] = v
	}
	for k, v := range servers {
		newGroups[k] = v
	}

	sortedGroups := []int{}
	for k := range newGroups {
		sortedGroups = append(sortedGroups, k)
	}
	sort.Ints(sortedGroups)

	sc.configs = append(sc.configs, Config{
		Num:    newConfigNum,
		Shards: sc.AllocShard(sortedGroups, sc.configs[newConfigNum-1].Shards),
		Groups: newGroups,
	})
	DEBUG(dError, "S%v [after join]%v", sc.me, sc.configs[len(sc.configs)-1])
}

func (sc *ShardCtrler) ApplyLeave(gids []int) {
	DEBUG(dClient, "S%v [apply leave] gids: %v", sc.me, gids)
	newConfigNum := len(sc.configs)
	newGroups := make(map[int][]string)

	for k, v := range sc.configs[newConfigNum-1].Groups {
		ice := true
		for _, gid := range gids {
			if k == gid {
				ice = false
				break
			}
		}
		if ice {
			newGroups[k] = v
		}
	}

	if len(newGroups) == 0 {
		sc.configs = append(sc.configs, Config{
			Num: newConfigNum,
		})
		return
	}

	sortedGroups := []int{}
	for k := range newGroups {
		sortedGroups = append(sortedGroups, k)
	}
	sort.Ints(sortedGroups)

	sc.configs = append(sc.configs, Config{
		Num:    newConfigNum,
		Shards: sc.AllocShard(sortedGroups, sc.configs[newConfigNum-1].Shards),
		Groups: newGroups,
	})
	DEBUG(dError, "S%v [after leave]%v", sc.me, sc.configs[len(sc.configs)-1])
}

func (sc *ShardCtrler) ApplyMove(shard int, gid int) {
	newConfigNum := len(sc.configs)
	newGroups := make(map[int][]string)
	var newShards [NShards]int

	for k, v := range sc.configs[newConfigNum-1].Groups {
		newGroups[k] = v
	}
	for k, v := range sc.configs[newConfigNum-1].Shards {
		if shard == k {
			newShards[k] = gid
		} else {
			newShards[k] = v
		}
	}

	sc.configs = append(sc.configs, Config{
		Num:    newConfigNum,
		Shards: newShards,
		Groups: newGroups,
	})
}

func (sc *ShardCtrler) Timer() {
	for {
		// if sc.killed() {
		// 	return
		// }
		sc.mu.Lock()
		if sc.startTimer != -1 {
			sc.mu.Unlock()
			break
		}
		sc.mu.Unlock()
		Operation := Op{
			Type:  "LeaderTimer",
			Index: nrand(),
		}
		i, _, isleader := sc.rf.Start(Operation)
		if isleader {
			sc.mu.Lock()
			sc.index = i
			sc.startTimer = Operation.Index
			sc.mu.Unlock()
			break
		} else {
			time.Sleep(time.Millisecond * 500)
		}
	}
	for {
		operation := Op{
			Type:  "Timer",
			Index: nrand(),
		}
		i, _, isleader := sc.rf.Start(operation)
		if isleader {
		DEBUG(dTimer, "S%v add a Timer i:%v", sc.me, i)
		}
		time.Sleep(time.Second)
	}
}

func (sc *ShardCtrler) AllocShard(group []int, preShards [NShards]int) [NShards]int {
	groupNum := len(group)
	min := NShards / groupNum
	remain := NShards % groupNum

	//judge gid is exist to add -1 to shard
	for k, v := range preShards {
		isExist := false
		for _, gid := range group {
			if gid == v {
				isExist = true
				break
			}
		}
		if !isExist {
			preShards[k] = -1
		}
	}

	//sort shards to gid
	newShards := make(map[int][]int)
	for k, v := range preShards {
		if v == -1 {
			continue
		}
		if _, ok := newShards[v]; ok {
			newShards[v] = append(newShards[v], k)
		} else {
			array := [...]int{k}
			newShards[v] = array[:]
		}
	}
	for _, v := range group {
		if _, ok := newShards[v]; !ok {
			var array []int
			newShards[v] = array
		}
	}

	sortedGroups := []int{}
	for k := range newShards {
		sortedGroups = append(sortedGroups, k)
	}
	sort.Ints(sortedGroups)
	//judge more than min/min+1 to cut
	for _, gid := range sortedGroups {
		if remain > 0 {
			if len(newShards[gid]) > min+1 {
				remain--
				release := newShards[gid][min+1:]
				for _, v := range release {
					preShards[v] = -1
				}
				newShards[gid] = newShards[gid][0 : min+1]
			}
		} else {
			if len(newShards[gid]) > min {
				release := newShards[gid][min:]
				for _, v := range release {
					preShards[v] = -1
				}
				newShards[gid] = newShards[gid][0:min]
			}
		}
	}

	//alloc remaining part
	for _, gid := range sortedGroups {
		if remain > 0 {
			if num := min + 1 - len(newShards[gid]); num > 0 {
				remain--
				for shardNum, nogid := range preShards {
					if nogid == -1 && num > 0{
						newShards[gid] = append(newShards[gid], shardNum)
						preShards[shardNum] = gid
						num--
					}
				}
			}
		} else {
			if num := min - len(newShards[gid]); num > 0 {
				for shardNum, nogid := range preShards {
					if nogid == -1 && num > 0 {
						newShards[gid] = append(newShards[gid], shardNum)
						preShards[shardNum] = gid
						num--
					}
				}
			}
		}

	}

	for _, v := range preShards {
		if v == -1 {
			panic(preShards)
		}
	}
	return preShards
}

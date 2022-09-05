package shardkv

import (
	"bytes"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Index int64

	ShardNum int
	Key      string
	Value    string

	Config shardctrler.Config

	Shard Shard
}

const (
	Completed     = "Completed"
	InProgress    = "InProgress"
	ErrorOccurred = "ErrorOccurred"
	ErrorTimeDeny = "ErrorTimeDeny"
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck        *shardctrler.Clerk
	storage    map[string]string
	rqRecord   map[int64]RequestInfo
	shards     map[int]Shard
	startTimer int64
	startIndex int
	cond       *sync.Cond

	applyIndex    int
	snapshotIndex int
	snapshotTerm  int

	curConfig  shardctrler.Config
	lastConfig shardctrler.Config
}

type RequestInfo struct {
	Status  string
	Rqindex int
	// Type   string
}

type Shard struct {
	ShardNum int
	Storage  map[string]string
	RqRecord map[int64]RequestInfo
	Status   string
}

func (kv *ShardKV) GetConfig() {
	for {
		lastConfig := kv.mck.Query(-1)
		kv.mu.Lock()
		if lastConfig.Num > kv.curConfig.Num {
			operation := Op{
				Type:   "ConfigChange",
				Index:  nrand(),
				Config: lastConfig,
			}
			kv.rf.Start(operation)
		}
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if _, ok := kv.rqRecord[kv.startTimer]; !ok {
		reply.Err = ErrorTimeDeny
		DEBUG(dPersist, "S%v get deny %v", kv.me, args.Index)
		kv.mu.Unlock()
		return
	}
	// DEBUG(dClient, "S%v get %v")
	for k, v := range kv.shards[args.ShardNum].RqRecord {
		if k == args.Index {
			if v.Status == Completed {
				if _, ok := kv.shards[args.ShardNum].Storage[args.Key]; ok {
					DEBUG(dCommit, "S%v get %v is ok", kv.me, args.Index)
					reply.Err = OK
					reply.Value = kv.shards[args.ShardNum].Storage[args.Key]
				} else {
					reply.Err = ErrNoKey
					reply.Value = ""
				}
			} else if v.Status == InProgress {
				DEBUG(dDrop, "S%v get %v is waiting", kv.me, args.Index)
				for kv.shards[args.ShardNum].RqRecord[args.Index].Status == InProgress {
					kv.cond.Wait()
				}
				if kv.shards[args.ShardNum].RqRecord[args.Index].Status == Completed {
					DEBUG(dCommit, "S%v get %v is ok", kv.me, args.Index)
					if _, ok := kv.shards[args.ShardNum].Storage[args.Key]; ok {
						reply.Err = OK
						reply.Value = kv.shards[args.ShardNum].Storage[args.Key]
					} else {
						reply.Err = ErrNoKey
						reply.Value = ""
					}
				} else if kv.shards[args.ShardNum].RqRecord[args.Index].Status == ErrorOccurred {
					DEBUG(dCommit, "S%v get %v is failed", kv.me, args.Index)
					// delete(kv.rqRecord, args.Index)
					reply.Err = ErrorOccurred
				}
			} else if v.Status == ErrorOccurred {
				break
			}
			kv.mu.Unlock()
			return
		}
	}

	operation := Op{
		Type:  "Get",
		Key:   args.Key,
		Index: args.Index,
	}
	// fmt.Println("----", kv.storage)
	i, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DEBUG(dLog2, "S%v get %v", kv.me, i)
	kv.shards[args.ShardNum].RqRecord[args.Index] = RequestInfo{
		Rqindex: i,
		Status:  InProgress,
	}

	for kv.shards[args.ShardNum].RqRecord[args.Index].Status == InProgress {
		// DEBUG(dPersist, "S%v %v", kv.me, kv.rqRecord)
		// DEBUG(dInfo, "S%v %v get is working status:%v", kv.me, args.Index, kv.rqRecord[args.Index].Status)
		kv.cond.Wait()
	}
	if kv.shards[args.ShardNum].RqRecord[args.Index].Status == ErrorOccurred {
		DEBUG(dCommit, "S%v get %v is failed", kv.me, args.Index)
		// delete(kv.rqRecord, args.Index)
		reply.Err = ErrorOccurred
	} else if kv.shards[args.ShardNum].RqRecord[args.Index].Status == Completed {
		if _, ok := kv.shards[args.ShardNum].Storage[args.Key]; ok {
			reply.Err = OK
			reply.Value = kv.shards[args.ShardNum].Storage[args.Key]
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
		DEBUG(dInfo, "S%v get %v is ok", kv.me, i)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if _, ok := kv.rqRecord[kv.startTimer]; !ok {
		DEBUG(dPersist, "S%v p/a deny %v", kv.me, args.Index)
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}
	// DEBUG(dClient, "S%v g/a %v--[%v][%v]", kv.me, args.Index, args.Key, args.Value)
	// DEBUG(dLog, "S%v %v", kv.me, kv.rqRecord)
	for k, v := range kv.shards[args.ShardNum].RqRecord {
		if k == args.Index {
			if v.Status == Completed {
				DEBUG(dCommit, "S%v p/a %v is ok", kv.me, args.Index)
				reply.Err = OK
			} else if v.Status == InProgress {
				DEBUG(dDrop, "S%v p/a %v is waiting", kv.me, args.Index)
				for kv.shards[args.ShardNum].RqRecord[args.Index].Status == InProgress {
					kv.cond.Wait()
				}
				if kv.shards[args.ShardNum].RqRecord[args.Index].Status == Completed {
					DEBUG(dCommit, "S%v p/a %v is ok", kv.me, args.Index)
					reply.Err = OK
				} else if kv.shards[args.ShardNum].RqRecord[args.Index].Status == ErrorOccurred {
					DEBUG(dCommit, "S%v p/a %v is failed", kv.me, args.Index)
					// delete(kv.rqRecord, args.Index)
					reply.Err = ErrorOccurred
				}
			} else if v.Status == ErrorOccurred {
				break
			}
			kv.mu.Unlock()
			return
		}
	}

	operation := Op{
		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
		Index: args.Index,
	}
	i, _, isleader := kv.rf.Start(operation)
	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DEBUG(dLog2, "S%v p/a %v", kv.me, i)
	kv.shards[args.ShardNum].RqRecord[args.Index] = RequestInfo{
		Rqindex: i,
		Status:  InProgress,
	}
	for kv.shards[args.ShardNum].RqRecord[args.Index].Status == InProgress {
		// DEBUG(dPersist, "S%v %v", kv.me, kv.rqRecord)
		// DEBUG(dInfo, "S%v %v p/a is working status:%v", kv.me, args.Index, kv.rqRecord[args.Index].Status)
		kv.cond.Wait()
	}
	if kv.shards[args.ShardNum].RqRecord[args.Index].Status == ErrorOccurred {
		DEBUG(dInfo, "S%v p/a %v is failed", kv.me, i)
		// delete(kv.rqRecord, args.Index)
		reply.Err = ErrorOccurred
	} else if kv.shards[args.ShardNum].RqRecord[args.Index].Status == Completed {
		DEBUG(dInfo, "S%v p/a %v is ok", kv.me, i)
		reply.Err = OK
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) HandleRequireShard(args *RequireShardArgs, reply *RequireShardReply) {
	kv.mu.Lock()
	if _, ok := kv.rqRecord[kv.startTimer]; !ok {
		DEBUG(dPersist, "S%v p/a deny %v", kv.me, args.Index)
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}

	//Todo: some check

	// DEBUG(dClient, "S%v get %v")
	for k, v := range kv.rqRecord {
		if k == args.Index {
			if v.Status == Completed {
				DEBUG(dCommit, "S%v get %v is ok", kv.me, args.Index)
				reply.Err = OK
				reply.Shard = kv.shards[args.ShardNum]
			} else if v.Status == InProgress {
				DEBUG(dDrop, "S%v get %v is waiting", kv.me, args.Index)
				for kv.rqRecord[args.Index].Status == InProgress {
					kv.cond.Wait()
				}
				if kv.rqRecord[args.Index].Status == Completed {
					DEBUG(dCommit, "S%v get %v is ok", kv.me, args.Index)
					reply.Err = OK
					reply.Shard = kv.shards[args.ShardNum]
				} else if kv.rqRecord[args.Index].Status == ErrorOccurred {
					DEBUG(dCommit, "S%v get %v is failed", kv.me, args.Index)
					// delete(kv.record, args.Index)
					reply.Err = ErrorOccurred
				}
			} else if v.Status == ErrorOccurred {
				break
			}
			kv.mu.Unlock()
			return
		}
	}

	operation := Op{
		Type:     "GetShard",
		ShardNum: args.ShardNum,
		Index:    args.Index,
	}
	// fmt.Println("----", kv.storage)
	i, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DEBUG(dLog2, "S%v get %v", kv.me, i)
	kv.rqRecord[args.Index] = RequestInfo{
		Rqindex: i,
		Status:  InProgress,
	}

	for kv.rqRecord[args.Index].Status == InProgress {
		// DEBUG(dPersist, "S%v %v", kv.me, kv.record)
		// DEBUG(dInfo, "S%v %v get is working status:%v", kv.me, args.Index, kv.record[args.Index].Status)
		kv.cond.Wait()
	}
	if kv.rqRecord[args.Index].Status == ErrorOccurred {
		DEBUG(dCommit, "S%v get %v is failed", kv.me, args.Index)
		// delete(kv.record, args.Index)
		reply.Err = ErrorOccurred
	} else if kv.rqRecord[args.Index].Status == Completed {
		reply.Err = OK
		reply.Shard = kv.shards[args.ShardNum]
		DEBUG(dInfo, "S%v get %v is ok", kv.me, i)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) HandleAppendShard(args *AppendShardArgs, reply *AppendShardReply) {
	kv.mu.Lock()
	if _, ok := kv.rqRecord[kv.startTimer]; !ok {
		DEBUG(dPersist, "S%v p/a deny %v", kv.me, args.Index)
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}

	//Todo: some check

	// DEBUG(dClient, "S%v get %v")
	for k, v := range kv.rqRecord {
		if k == args.Index {
			if v.Status == Completed {
				DEBUG(dCommit, "S%v get %v is ok", kv.me, args.Index)
				reply.Err = OK
			} else if v.Status == InProgress {
				DEBUG(dDrop, "S%v get %v is waiting", kv.me, args.Index)
				for kv.rqRecord[args.Index].Status == InProgress {
					kv.cond.Wait()
				}
				if kv.rqRecord[args.Index].Status == Completed {
					DEBUG(dCommit, "S%v get %v is ok", kv.me, args.Index)
					reply.Err = OK
				} else if kv.rqRecord[args.Index].Status == ErrorOccurred {
					DEBUG(dCommit, "S%v get %v is failed", kv.me, args.Index)
					// delete(kv.record, args.Index)
					reply.Err = ErrorOccurred
				}
			} else if v.Status == ErrorOccurred {
				break
			}
			kv.mu.Unlock()
			return
		}
	}

	operation := Op{
		Type:  "AppendShard",
		Index: args.Index,
		Shard: args.Shard,
	}
	// fmt.Println("----", kv.storage)
	i, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DEBUG(dLog2, "S%v get %v", kv.me, i)
	kv.rqRecord[args.Index] = RequestInfo{
		Rqindex: i,
		Status:  InProgress,
	}

	for kv.rqRecord[args.Index].Status == InProgress {
		// DEBUG(dPersist, "S%v %v", kv.me, kv.record)
		// DEBUG(dInfo, "S%v %v get is working status:%v", kv.me, args.Index, kv.record[args.Index].Status)
		kv.cond.Wait()
	}
	if kv.rqRecord[args.Index].Status == ErrorOccurred {
		DEBUG(dCommit, "S%v get %v is failed", kv.me, args.Index)
		// delete(kv.record, args.Index)
		reply.Err = ErrorOccurred
	} else if kv.rqRecord[args.Index].Status == Completed {
		reply.Err = OK
		DEBUG(dInfo, "S%v get %v is ok", kv.me, i)
	}
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.storage = make(map[string]string)
	kv.rqRecord = make(map[int64]RequestInfo)
	kv.startTimer = -1
	kv.cond = sync.NewCond(&kv.mu)
	kv.applyIndex = 0

	go kv.Timer()
	go kv.Apply()
	go kv.GetConfig()
	return kv
}

func (kv *ShardKV) Apply() {
	for entry := range kv.applyCh {
		if entry.CommandValid {
			if entry.CommandIndex <= kv.applyIndex {
				kv.mu.Unlock()
				continue
			}
			kv.mu.Lock()
			op := (entry.Command).(Op)
			switch op.Type {
			case "Put":
				kv.shards[op.ShardNum].Storage[op.Key] = op.Value
			case "Append":
				kv.shards[op.ShardNum].Storage[op.Key] += op.Value
			case "LeaderTimer":
				if kv.startIndex == entry.CommandIndex || kv.startTimer == -1 {
					kv.startTimer = op.Index
					DEBUG(dClient, "S%v timer: %v rqRecord:%v", kv.me, kv.startTimer, kv.rqRecord)
				}
				kv.applyIndex = entry.CommandIndex
				continue
			case "ConfigChange":
				kv.ApplyConfigChange(op.Config)
			case "AppendShard":
				kv.shards[op.Shard.ShardNum] = Shard{
					ShardNum: op.Shard.ShardNum,
					Storage:  op.Shard.Storage,
					RqRecord: op.Shard.RqRecord,
					Status:   "Working",
				}
			}

			for k, v := range kv.rqRecord {
				if v.Rqindex == entry.CommandIndex {
					if k != op.Index {
						kv.rqRecord[k] = RequestInfo{
							Status:  ErrorOccurred,
							Rqindex: v.Rqindex,
						}
					}
				}
			}
			for _, shard := range kv.shards {
				for k, v := range shard.RqRecord {
					if v.Rqindex == entry.CommandIndex {
						if k != op.Index {
							kv.shards[shard.ShardNum].RqRecord[k] = RequestInfo{
								Status: ErrorOccurred,
								Rqindex: v.Rqindex,
							}
						}
					}
				}
			}

			switch op.Type {
			case "Put", "Append", "Get":
				kv.shards[op.ShardNum].RqRecord[op.Index] = RequestInfo{
					Status:  Completed,
					Rqindex: entry.CommandIndex,
				}
			case "ConfigChange", "RequireShard", "AppendShard":
				kv.rqRecord[op.Index] = RequestInfo{
					Status:  Completed,
					Rqindex: entry.CommandIndex,
				}
			}

			// DEBUG(dError, "S%v after apply [%v]", kv.me, kv.rqRecord)
			kv.applyIndex = entry.CommandIndex
			kv.mu.Unlock()
			kv.cond.Broadcast()
		} else if entry.SnapshotValid {
			kv.mu.Lock()
			kv.snapshotIndex = entry.SnapshotIndex
			kv.snapshotTerm = entry.SnapshotTerm
			r := bytes.NewBuffer(entry.Snapshot)
			d := labgob.NewDecoder(r)
			var applyIndex int
			shards := make(map[int]Shard)
			rqRecord := make(map[int64]RequestInfo)
			if d.Decode(&applyIndex) != nil || d.Decode(&shards) != nil || d.Decode(&rqRecord) != nil {
				panic("decode fail")
			} else {
				kv.applyIndex = entry.SnapshotIndex
				for k, shard := range shards{
					kv.shards[k] = shard
				}

				for k, v := range kv.rqRecord {
					if v.Rqindex <= applyIndex && v.Status == InProgress {
						kv.rqRecord[k] = RequestInfo{
							Rqindex: v.Rqindex,
							Status: ErrorOccurred,
						}
					}
				}
				for _, shard := range kv.shards {
					for k, v := range shard.RqRecord {
						if v.Rqindex <= applyIndex && v.Status == InProgress {
							kv.rqRecord[k] = RequestInfo{
								Rqindex: v.Rqindex,
								Status: ErrorOccurred,
							}
						}
					}
				}

				for k, v := range rqRecord {
					kv.rqRecord[k] = v
				}
			}
			kv.mu.Unlock()
			kv.cond.Broadcast()
		}
	}
}

func (kv *ShardKV) ApplyConfigChange(lastConfig shardctrler.Config) {
	kv.curConfig = lastConfig
	// var need2Control []int
	for shardNum, gid := range lastConfig.Shards {
		if kv.gid == gid {
			// need2Control = append(need2Control, shardNum)
			if _, ok := kv.shards[shardNum]; !ok {
				kv.shards[shardNum] = Shard{
					ShardNum: shardNum,
					Storage:  make(map[string]string),
					RqRecord: make(map[int64]RequestInfo),
					Status:   "WaitForAdd",
				}
			}
			go kv.RequireShard(shardNum)
		} else {
			if _, ok := kv.shards[shardNum]; ok {
				kv.shards[shardNum] = Shard{
					ShardNum: kv.shards[shardNum].ShardNum,
					Storage:  kv.shards[shardNum].Storage,
					RqRecord: kv.shards[shardNum].RqRecord,
					Status:   "Deleting",
				}
			}
		}
	}
	// Todo: delete
}

func (kv *ShardKV) RequireShard(shardNum int) {
	args := RequireShardArgs{
		ShardNum: shardNum,
		Index:    nrand(),
	}
	kv.mu.Lock()
	curConfig := kv.curConfig
	kv.mu.Unlock()

	for {
		gid := curConfig.Shards[shardNum]
		if servers, ok := kv.curConfig.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply RequireShardReply
				ok := kv.CallRequireShard(srv, &args, &reply, 0)
				if ok {
					kv.shards[shardNum] = Shard{
						ShardNum: reply.Shard.ShardNum,
						Storage:  reply.Shard.Storage,
						RqRecord: reply.Shard.RqRecord,
						Status:   "Working",
					}
					kv.AppendShard(kv.shards[shardNum])
				}
			}
		}
	}
}

func (kv *ShardKV) CallRequireShard(srv *labrpc.ClientEnd, args *RequireShardArgs, reply *RequireShardReply, timer int) bool {
	if timer > 5 {
		return false
	}
	ok := srv.Call("ShardKV.HandleRequireShard", args, reply)
	if !ok {
		newreply := RequireShardReply{}
		return kv.CallRequireShard(srv, args, &newreply, timer+1)
	} else {
		if reply.Err == OK {
			return true
		} else if reply.Err == ErrWrongLeader {
			return false
		} else if reply.Err == ErrorOccurred {
			return false
		} else if reply.Err == ErrorTimeDeny {
			newreply := RequireShardReply{}
			return kv.CallRequireShard(srv, args, &newreply, timer+1)
		}
	}
	return false
}

func (kv *ShardKV) AppendShard(shard Shard) {
	args := AppendShardArgs{
		Shard: shard,
		Index: nrand(),
	}

	for {
		gid := kv.curConfig.Shards[shard.ShardNum]
		if servers, ok := kv.curConfig.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply AppendShardReply
				ok := kv.CallAppendShard(srv, &args, &reply, 0)
				if ok {
					return
				}
			}
		}
	}
}

func (kv *ShardKV) CallAppendShard(srv *labrpc.ClientEnd, args *AppendShardArgs, reply *AppendShardReply, timer int) bool {
	if timer > 5 {
		return false
	}
	ok := srv.Call("ShardKV.HandleAppendShard", args, reply)
	if !ok {
		newreply := AppendShardReply{}
		return kv.CallAppendShard(srv, args, &newreply, timer+1)
	} else {
		if reply.Err == OK {
			return true
		} else if reply.Err == ErrWrongLeader {
			return false
		} else if reply.Err == ErrorOccurred {
			return false
		} else if reply.Err == ErrorTimeDeny {
			newreply := AppendShardReply{}
			return kv.CallAppendShard(srv, args, &newreply, timer+1)
		}
	}
	return false
}

func (kv *ShardKV) Timer() {
	for {
		// if kv.killed() {
		// 	return
		// }
		kv.mu.Lock()
		if kv.startTimer != -1 {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
		Operation := Op{
			Type:  "LeaderTimer",
			Index: nrand(),
		}
		i, _, isleader := kv.rf.Start(Operation)
		if isleader {
			kv.mu.Lock()
			kv.startIndex = i
			kv.startTimer = Operation.Index
			kv.mu.Unlock()
			break
		} else {
			time.Sleep(time.Millisecond * 500)
		}
	}
	for {
		// if kv.killed() {
		// 	return
		// }
		operation := Op{
			Type:  "Timer",
			Index: nrand(),
		}
		i, _, isleader := kv.rf.Start(operation)
		if isleader {
			DEBUG(dTimer, "S%v add a Timer i:%v", kv.me, i)
		}
		time.Sleep(time.Second)
	}
}

func (kv *ShardKV) Snap() {
	if kv.maxraftstate == -1 {
		return
	}
	for {
		kv.mu.Lock()
		if kv.applyIndex > kv.snapshotIndex && kv.rf.Persister.RaftStateSize() >= kv.maxraftstate {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.applyIndex)
			e.Encode(kv.shards)
			e.Encode(kv.rqRecord)
			data := w.Bytes()
			kv.snapshotIndex = kv.applyIndex
			kv.rf.Snapshot(kv.applyIndex, data)
			kv.mu.Unlock()
		} else {
			kv.mu.Unlock()
		}
		time.Sleep(time.Millisecond * 50)
	}
}

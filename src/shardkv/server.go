package shardkv

import (
	"bytes"
	dsync "sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	sync "github.com/sasha-s/go-deadlock"
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

	PreConfig shardctrler.Config
	CurConfig shardctrler.Config

	Shards map[int]Shard
}

const (
	Completed            = "Completed"
	InProgress           = "InProgress"
	ErrorOccurred        = "ErrorOccurred"
	ErrorTimeDeny        = "ErrorTimeDeny"
	ErrorConfigOutOfDate = "ErrorConfigOutOfDate"
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
	rqRecord   map[int64]RequestInfo
	shards     map[int]Shard
	startTimer int64
	startIndex int
	cond       *dsync.Cond

	applyIndex    int
	snapshotIndex int
	snapshotTerm  int

	curConfig shardctrler.Config
	preConfig shardctrler.Config
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
		// kv.mu.Lock()
		// if _, ok := kv.rqRecord[kv.startTimer]; !ok {
		// 	// DEBUG(dPersist, "S%v[shardkv][gid:%v] get deny %v", kv.me, kv.gid, args.Index)
		// 	kv.mu.Unlock()
		// 	continue
		// }
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
	if kv.curConfig.Shards[args.ShardNum] != kv.gid {
		DEBUG(dError, "S%v[shardkv][gid:%v] get wrongGroup %v %v %v", kv.me, kv.gid, kv.curConfig.Shards[args.ShardNum], args.Index, kv.curConfig.Shards)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.shards[args.ShardNum].Status != "Working" {
		DEBUG(dError, "S%v[shardkv][gid:%v] get not ready %v %v", kv.me, kv.gid, kv.shards[args.ShardNum].Status, args.Index)
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.rqRecord[kv.startTimer]; !ok {
		reply.Err = ErrorTimeDeny
		// DEBUG(dPersist, "S%v[shardkv][gid:%v] get deny %v", kv.me, kv.gid, args.Index)
		kv.mu.Unlock()
		return
	}
	DEBUG(dClient, "S%v[shardkv][gid:%v] get %v", kv.me, kv.gid, args.Index)
	for k, v := range kv.shards[args.ShardNum].RqRecord {
		if k == args.Index {
			if v.Status == Completed {
				if _, ok := kv.shards[args.ShardNum].Storage[args.Key]; ok {
					DEBUG(dCommit, "S%v[shardkv][gid:%v] get %v is ok", kv.me, kv.gid, args.Index)
					reply.Err = OK
					reply.Value = kv.shards[args.ShardNum].Storage[args.Key]
				} else {
					reply.Err = ErrNoKey
					reply.Value = ""
				}
			} else if v.Status == InProgress {
				DEBUG(dDrop, "S%v[shardkv][gid:%v] get %v is waiting", kv.me, kv.gid, args.Index)
				for kv.shards[args.ShardNum].RqRecord[args.Index].Status == InProgress {
					kv.cond.Wait()
				}
				if kv.shards[args.ShardNum].RqRecord[args.Index].Status == Completed {
					DEBUG(dCommit, "S%v[shardkv][gid:%v] get %v is ok", kv.me, kv.gid, args.Index)
					if _, ok := kv.shards[args.ShardNum].Storage[args.Key]; ok {
						reply.Err = OK
						reply.Value = kv.shards[args.ShardNum].Storage[args.Key]
					} else {
						reply.Err = ErrNoKey
						reply.Value = ""
					}
				} else if kv.shards[args.ShardNum].RqRecord[args.Index].Status == ErrorOccurred {
					DEBUG(dCommit, "S%v[shardkv][gid:%v] get %v is failed", kv.me, kv.gid, args.Index)
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
		Type:     "Get",
		Key:      args.Key,
		Index:    args.Index,
		ShardNum: args.ShardNum,
	}
	// fmt.Println("----", kv.storage)
	i, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DEBUG(dLog2, "S%v[shardkv][gid:%v] get %v", kv.me, kv.gid, i)
	kv.shards[args.ShardNum].RqRecord[args.Index] = RequestInfo{
		Rqindex: i,
		Status:  InProgress,
	}

	for kv.shards[args.ShardNum].RqRecord[args.Index].Status == InProgress {
		// DEBUG(dPersist, "S%v[shardkv][gid:%v] %v", kv.me, kv.gid, kv.rqRecord)
		// DEBUG(dInfo, "S%v[shardkv][gid:%v] %v get is working status:%v", kv.me, kv.gid, args.Index, kv.rqRecord[args.Index].Status)
		kv.cond.Wait()
	}
	if kv.shards[args.ShardNum].RqRecord[args.Index].Status == ErrorOccurred {
		DEBUG(dCommit, "S%v[shardkv][gid:%v] get %v is failed", kv.me, kv.gid, args.Index)
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
		DEBUG(dInfo, "S%v[shardkv][gid:%v] get %v is ok", kv.me, kv.gid, i)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.curConfig.Shards[args.ShardNum] != kv.gid {
		DEBUG(dLog, "S%v[shardkv][gid:%v] p/a wrongGroup %v %v %v", kv.me, kv.gid, kv.curConfig.Shards[args.ShardNum], args.Index, kv.curConfig.Shards)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.shards[args.ShardNum].Status != "Working" {
		DEBUG(dLog, "S%v[shardkv][gid:%v] p/a not ready %v %v %v", kv.me, kv.gid, kv.shards[args.ShardNum].Status, args.ShardNum, args.Index)
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.rqRecord[kv.startTimer]; !ok {
		DEBUG(dPersist, "S%v[shardkv][gid:%v] p/a deny %v %v", kv.me, kv.gid, args.Index, kv.startTimer)
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}
	DEBUG(dClient, "S%v[shardkv][gid:%v] p/a %v--[%v][%v]", kv.me, kv.gid, args.Index, args.Key, args.Value)
	// DEBUG(dLog, "S%v[shardkv][gid:%v] %v", kv.me, kv.gid, kv.rqRecord)
	for k, v := range kv.shards[args.ShardNum].RqRecord {
		if k == args.Index {
			if v.Status == Completed {
				DEBUG(dCommit, "S%v[shardkv][gid:%v] p/a %v is ok", kv.me, kv.gid, args.Index)
				reply.Err = OK
			} else if v.Status == InProgress {
				DEBUG(dDrop, "S%v[shardkv][gid:%v] p/a %v is waiting", kv.me, kv.gid, args.Index)
				for kv.shards[args.ShardNum].RqRecord[args.Index].Status == InProgress {
					kv.cond.Wait()
				}
				if kv.shards[args.ShardNum].RqRecord[args.Index].Status == Completed {
					DEBUG(dCommit, "S%v[shardkv][gid:%v] p/a %v is ok", kv.me, kv.gid, args.Index)
					reply.Err = OK
				} else if kv.shards[args.ShardNum].RqRecord[args.Index].Status == ErrorOccurred {
					DEBUG(dCommit, "S%v[shardkv][gid:%v] p/a %v is failed", kv.me, kv.gid, args.Index)
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
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		Index:    args.Index,
		ShardNum: args.ShardNum,
	}
	i, _, isleader := kv.rf.Start(operation)
	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DEBUG(dLog2, "S%v[shardkv][gid:%v] p/a %v", kv.me, kv.gid, i)
	kv.shards[args.ShardNum].RqRecord[args.Index] = RequestInfo{
		Rqindex: i,
		Status:  InProgress,
	}
	for kv.shards[args.ShardNum].RqRecord[args.Index].Status == InProgress {
		// DEBUG(dPersist, "S%v[shardkv][gid:%v] %v", kv.me, kv.gid, kv.rqRecord)
		// DEBUG(dInfo, "S%v[shardkv][gid:%v] %v p/a is working status:%v", kv.me, kv.gid, args.Index, kv.shards)
		kv.cond.Wait()
	}
	if kv.shards[args.ShardNum].RqRecord[args.Index].Status == ErrorOccurred {
		DEBUG(dInfo, "S%v[shardkv][gid:%v] p/a %v is failed", kv.me, kv.gid, i)
		// delete(kv.rqRecord, args.Index)
		reply.Err = ErrorOccurred
	} else if kv.shards[args.ShardNum].RqRecord[args.Index].Status == Completed {
		DEBUG(dInfo, "S%v[shardkv][gid:%v] p/a %v is ok", kv.me, kv.gid, i)
		reply.Err = OK
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) HandleRequireShard(args *RequireShardArgs, reply *RequireShardReply) {
	kv.mu.Lock()
	if kv.curConfig.Num > args.ConfigNum {
		DEBUG(dCommit, "S%v[shardkv][gid:%v] RequireShard is failed, conNum:m-o:%v-%v", kv.me, kv.gid, kv.curConfig.Num, args.ConfigNum)
		reply.Err = ErrorConfigOutOfDate
		kv.mu.Unlock()
		return
	} else if kv.curConfig.Num < args.ConfigNum {
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.rqRecord[kv.startTimer]; !ok {
		// DEBUG(dPersist, "S%v[shardkv][gid:%v] p/a deny %v", kv.me, kv.gid, args.Index)
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}

	//Todo: some check

	// DEBUG(dClient, "S%v[shardkv][gid:%v] RequireShard %v", )
	// for k, v := range kv.rqRecord {
	// 	if k == args.Index {
	// 		if v.Status == Completed {
	// 			DEBUG(dCommit, "S%v[shardkv][gid:%v] RequireShard is ok %v", kv.me, kv.gid, args.Index)
	// 			reply.Err = OK
	// 			reply.Shard = kv.shards[args.ShardNum]
	// 		} else if v.Status == InProgress {
	// 			DEBUG(dDrop, "S%v[shardkv][gid:%v] RequireShard %v is waiting", kv.me, kv.gid, args.Index)
	// 			for kv.rqRecord[args.Index].Status == InProgress {
	// 				kv.cond.Wait()
	// 			}
	// 			if kv.rqRecord[args.Index].Status == Completed {
	// 				DEBUG(dCommit, "S%v[shardkv][gid:%v] RequireShard %v is ok", kv.me, kv.gid, args.Index)
	// 				reply.Err = OK
	// 				reply.Shard = kv.shards[args.ShardNum]
	// 			} else if kv.rqRecord[args.Index].Status == ErrorOccurred {
	// 				DEBUG(dCommit, "S%v[shardkv][gid:%v] RequireShard %v is failed", kv.me, kv.gid, args.Index)
	// 				// delete(kv.record, args.Index)
	// 				reply.Err = ErrorOccurred
	// 			}
	// 		} else if v.Status == ErrorOccurred {
	// 			break
	// 		}
	// 		kv.mu.Unlock()
	// 		return
	// 	}
	// }

	operation := Op{
		Type:     "RequireShard",
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
	DEBUG(dLog2, "S%v[shardkv][gid:%v] RequireShard %v %v", kv.me, kv.gid, i, args.ShardNum)
	kv.rqRecord[args.Index] = RequestInfo{
		Rqindex: i,
		Status:  InProgress,
	}

	for kv.rqRecord[args.Index].Status == InProgress {
		// DEBUG(dPersist, "S%v[shardkv][gid:%v] %v", kv.me, kv.gid, kv.record)
		// DEBUG(dInfo, "S%v[shardkv][gid:%v] %v get is working status:%v", kv.me, kv.gid, args.Index, kv.record[args.Index].Status)
		kv.cond.Wait()
	}
	if kv.rqRecord[args.Index].Status == ErrorOccurred {
		DEBUG(dCommit, "S%v[shardkv][gid:%v] RequireShard %v is failed", kv.me, kv.gid, args.Index)
		// delete(kv.record, args.Index)
		reply.Err = ErrorOccurred
	} else if kv.rqRecord[args.Index].Status == Completed {
		reply.Err = OK
		// reply.Shard = kv.shards[args.ShardNum]
		if _, ok := kv.shards[args.ShardNum]; !ok {
			shard := Shard{
				ShardNum: args.ShardNum,
				Storage:  make(map[string]string),
				RqRecord: make(map[int64]RequestInfo),
			}
			reply.Shard = shard
		} else {
			reply.Shard = kv.shards[args.ShardNum]
		}
		DEBUG(dInfo, "S%v[shardkv][gid:%v] RequireShard %v is ok", kv.me, kv.gid, i)
	}
	kv.mu.Unlock()
	DEBUG(dLeader, "S%v[shardkv][gid:%v] RequireShard reply:%v", kv.me, kv.gid, reply)
}

func (kv *ShardKV) HandleAppendShard(args *AppendShardArgs, reply *AppendShardReply) {
	kv.mu.Lock()
	// if kv.curConfig.Num != args.PreConfig.Num {
	// 	reply.Err = ErrorTimeDeny
	// 	kv.mu.Unlock()
	// 	return
	// }
	if _, ok := kv.rqRecord[kv.startTimer]; !ok {
		// DEBUG(dPersist, "S%v[shardkv][gid:%v] p/a deny %v", kv.me, kv.gid, args.Index)
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}

	//Todo: some check

	// DEBUG(dClient, "S%v[shardkv][gid:%v] get %v")
	for k, v := range kv.rqRecord {
		if k == args.Index {
			if v.Status == Completed {
				DEBUG(dCommit, "S%v[shardkv][gid:%v] get %v is ok", kv.me, kv.gid, args.Index)
				reply.Err = OK
			} else if v.Status == InProgress {
				DEBUG(dDrop, "S%v[shardkv][gid:%v] get %v is waiting", kv.me, kv.gid, args.Index)
				for kv.rqRecord[args.Index].Status == InProgress {
					kv.cond.Wait()
				}
				if kv.rqRecord[args.Index].Status == Completed {
					DEBUG(dCommit, "S%v[shardkv][gid:%v] get %v is ok", kv.me, kv.gid, args.Index)
					reply.Err = OK
				} else if kv.rqRecord[args.Index].Status == ErrorOccurred {
					DEBUG(dCommit, "S%v[shardkv][gid:%v] get %v is failed", kv.me, kv.gid, args.Index)
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
		Type:      "AppendShard",
		Index:     args.Index,
		Shards:    args.Shards,
		CurConfig: args.CurConfig,
		PreConfig: args.PreConfig,
	}
	// fmt.Println("----", kv.storage)
	i, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DEBUG(dLog2, "S%v[shardkv][gid:%v] get %v", kv.me, kv.gid, i)
	kv.rqRecord[args.Index] = RequestInfo{
		Rqindex: i,
		Status:  InProgress,
	}

	for kv.rqRecord[args.Index].Status == InProgress {
		// DEBUG(dPersist, "S%v[shardkv][gid:%v] %v", kv.me, kv.gid, kv.record)
		// DEBUG(dInfo, "S%v[shardkv][gid:%v] %v get is working status:%v", kv.me, kv.gid, args.Index, kv.record[args.Index].Status)
		kv.cond.Wait()
	}
	if kv.rqRecord[args.Index].Status == ErrorOccurred {
		DEBUG(dCommit, "S%v[shardkv][gid:%v] get %v is failed", kv.me, kv.gid, args.Index)
		// delete(kv.record, args.Index)
		reply.Err = ErrorOccurred
	} else if kv.rqRecord[args.Index].Status == Completed {
		reply.Err = OK
		DEBUG(dInfo, "S%v[shardkv][gid:%v] get %v is ok", kv.me, kv.gid, i)
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
	kv.rqRecord = make(map[int64]RequestInfo)
	kv.shards = make(map[int]Shard)
	kv.startTimer = -1
	kv.cond = dsync.NewCond(&kv.mu)
	kv.applyIndex = 0
	sync.Opts.DeadlockTimeout = time.Second
	LOGinit()
	DEBUG(dInfo, "S%v[shardkv][gid:%v] Started", kv.me, kv.gid)
	go kv.Timer()
	go kv.Apply()
	go kv.GetConfig()
	go kv.Snap()
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
			DEBUG(dClient, "S%v[shardkv][gid:%v] apply receive [%v]%v", kv.me, kv.gid, entry.CommandIndex, op)
			switch op.Type {
			case "Put":
				for kv.shards[op.ShardNum].Status != "Working" {
					kv.cond.Wait()
				}
				// kv.TaskQueue[op.ShardNum] = append(kv.TaskQueue[op.ShardNum], op)
				if kv.shards[op.ShardNum].RqRecord[op.Index].Status == Completed {
					kv.mu.Unlock()
					continue
				}
				kv.shards[op.ShardNum].Storage[op.Key] = op.Value
			case "Append":
				for kv.shards[op.ShardNum].Status != "Working" {
					kv.cond.Wait()
				}
				// kv.TaskQueue[op.ShardNum] = append(kv.TaskQueue[op.ShardNum], op)
				if kv.shards[op.ShardNum].RqRecord[op.Index].Status == Completed {
					kv.mu.Unlock()
					continue
				}
				kv.shards[op.ShardNum].Storage[op.Key] += op.Value
			case "LeaderTimer":
				if kv.startIndex == entry.CommandIndex || kv.startTimer == -1 {
					kv.startTimer = op.Index
					kv.startIndex = entry.CommandIndex
					// DEBUG(dClient, "S%v[shardkv][gid:%v] timer: %v rqRecord:%v", kv.me, kv.gid, kv.startTimer, kv.rqRecord)
				}
			case "ConfigChange":
				if kv.curConfig.Num < op.Config.Num {
					kv.ApplyConfigChange(op.Config)
				}
			case "AppendShard":
				DEBUG(dSnap, "S%v[shardkv][gid:%v] AppendShard %v", kv.me, kv.gid, op)
				if kv.curConfig.Num < op.Config.Num {
					kv.shards = make(map[int]Shard)
					for k, v := range op.Shards {
						kv.shards[k] = v
					}
					// kv.curConfig = shardctrler.Config{Num: op.CurConfig.Num, Shards: op.CurConfig.Shards, Groups: make(map[int][]string)}
					// for k, v := range op.CurConfig.Groups {
					// 	kv.curConfig.Groups[k] = v
					// }
					// kv.preConfig = shardctrler.Config{Num: op.PreConfig.Num, Shards: op.PreConfig.Shards, Groups: make(map[int][]string)}
					// for k, v := range op.PreConfig.Groups {
					// 	kv.preConfig.Groups[k] = v
					// }
					kv.curConfig = op.CurConfig
					kv.preConfig = op.PreConfig
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
								Status:  ErrorOccurred,
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
				DEBUG(dLog, "S%v[shardkv][gid:%v]%v %v", kv.me, kv.gid, op.Type, kv.shards)
			case "LeaderTimer", "Timer", "ConfigChange", "RequireShard", "AppendShard":
				// DEBUG(dLog, "[]%v", op.Type)
				kv.rqRecord[op.Index] = RequestInfo{
					Status:  Completed,
					Rqindex: entry.CommandIndex,
				}
			}

			// DEBUG(dError, "S%v[shardkv][gid:%v] after apply [%v]", kv.me, kv.gid, kv.rqRecord)
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
			var curConfig shardctrler.Config
			var preConfig shardctrler.Config
			if d.Decode(&applyIndex) != nil || d.Decode(&shards) != nil || d.Decode(&rqRecord) != nil || d.Decode(&curConfig) != nil || d.Decode(&preConfig) != nil {
				panic("decode fail")
			} else {
				DEBUG(dSnap, "S%v [shardkv][gid:%v] apply snap %v", kv.me, kv.gid, applyIndex)
				kv.applyIndex = entry.SnapshotIndex
				// kv.curConfig = shardctrler.Config{Num: curConfig.Num, Shards: curConfig.Shards, Groups: make(map[int][]string)}
				// for k, v := range curConfig.Groups {
				// 	kv.curConfig.Groups[k] = v
				// }
				// kv.preConfig = shardctrler.Config{Num: preConfig.Num, Shards: preConfig.Shards, Groups: make(map[int][]string)}
				// for k, v := range preConfig.Groups {
				// 	kv.preConfig.Groups[k] = v
				// }
				kv.curConfig = curConfig
				kv.preConfig = preConfig
				for k, shard := range shards {
					kv.shards[k] = shard
				}

				for k, v := range kv.rqRecord {
					if v.Rqindex <= applyIndex && v.Status == InProgress {
						kv.rqRecord[k] = RequestInfo{
							Rqindex: v.Rqindex,
							Status:  ErrorOccurred,
						}
					}
				}
				for _, shard := range kv.shards {
					for k, v := range shard.RqRecord {
						if v.Rqindex <= applyIndex && v.Status == InProgress {
							kv.rqRecord[k] = RequestInfo{
								Rqindex: v.Rqindex,
								Status:  ErrorOccurred,
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
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	var Preneed2Control []int
	for shardNum, gid := range kv.curConfig.Shards {
		if kv.gid == gid {
			Preneed2Control = append(Preneed2Control, shardNum)
		}
	}
	if kv.curConfig.Num != 0 {
		for _, shardNum := range Preneed2Control {
			for kv.shards[shardNum].Status != "Working" {
				kv.cond.Wait()
			}
		}
	}
	DEBUG(dError, "S%v [shardkv][gid:%v] -pre-cur:%v-%v", kv.me, kv.gid, kv.preConfig, kv.curConfig)
	kv.preConfig, kv.curConfig = kv.curConfig, lastConfig
	// kv.curConfig = lastConfig
	DEBUG(dError, "S%v [shardkv][gid:%v] pre-cur:%v-%v", kv.me, kv.gid, kv.preConfig, kv.curConfig)
	if kv.curConfig.Num != kv.preConfig.Num+1 {
		kv.preConfig = kv.mck.Query(kv.curConfig.Num - 1)
	}
	var Curneed2Control []int
	for shardNum, gid := range lastConfig.Shards {
		if kv.gid == gid {
			Curneed2Control = append(Curneed2Control, shardNum)
			if _, ok := kv.shards[shardNum]; !ok {
				kv.shards[shardNum] = Shard{
					ShardNum: shardNum,
					Storage:  make(map[string]string),
					RqRecord: make(map[int64]RequestInfo),
					Status:   "WaitForAdd",
				}
			} else {
				if kv.shards[shardNum].Status != "Working" {
					shard := Shard{
						ShardNum: shardNum,
						Storage:  make(map[string]string),
						RqRecord: make(map[int64]RequestInfo),
						Status:   "WaitForAdd",
					}
					shard.Storage = kv.shards[shardNum].Storage
					shard.RqRecord = kv.shards[shardNum].RqRecord
					kv.shards[shardNum] = shard
				}
			}

			if kv.preConfig.Shards[shardNum] != 0 {
				if kv.preConfig.Shards[shardNum] != kv.gid {
					go kv.RequireShard(shardNum)
				} else {
					shard := Shard{
						ShardNum: shardNum,
						Storage:  make(map[string]string),
						RqRecord: make(map[int64]RequestInfo),
						Status:   "Working",
					}
					shard.Storage = kv.shards[shardNum].Storage
					shard.RqRecord = kv.shards[shardNum].RqRecord
					kv.shards[shardNum] = shard
				}
			} else {
				kv.shards[shardNum] = Shard{
					ShardNum: shardNum,
					Storage:  make(map[string]string),
					RqRecord: make(map[int64]RequestInfo),
					Status:   "Working",
				}
				// shard := Shard{
				// 	ShardNum: shardNum,
				// 	Storage:  make(map[string]string),
				// 	RqRecord: make(map[int64]RequestInfo),
				// 	Status:   "Working",
				// }
				// go kv.AppendShard(shard)
			}
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
	DEBUG(dClient, "S%v[shardkv][gid:%v]---%v", kv.me, kv.gid, kv.shards)
	if kv.curConfig.Num != 0 {
		for _, shardNum := range Curneed2Control {
			for kv.shards[shardNum].Status != "Working" {
				kv.cond.Wait()
			}
		}
	}
	go kv.AppendShard()
	// Todo: delete
}

func (kv *ShardKV) RequireShard(shardNum int) {
	kv.mu.Lock()
	preConfig := kv.preConfig
	// curConfig := kv.curConfig
	configNum := kv.curConfig.Num
	kv.mu.Unlock()
	DEBUG(dTrace, "S%v[shardkv][gid:%v] RequireShard to %v", kv.me, kv.gid, preConfig)

	args := RequireShardArgs{
		ShardNum:  shardNum,
		Index:     nrand(),
		ConfigNum: configNum,
	}

	for {
		gid := preConfig.Shards[shardNum]
		if servers, ok := preConfig.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply RequireShardReply
				ok := kv.CallRequireShard(srv, &args, &reply, 0)
				if ok {
					if reply.Shard.RqRecord == nil || reply.Shard.Storage == nil {
						continue
					}
					kv.mu.Lock()
					shard := Shard{
						ShardNum: reply.Shard.ShardNum,
						Storage:  make(map[string]string),
						RqRecord: make(map[int64]RequestInfo),
						Status:   "Working",
					}
					shard.Storage = reply.Shard.Storage
					shard.RqRecord = reply.Shard.RqRecord
					kv.shards[shardNum] = shard
					DEBUG(dLog2, "S%v[shardkv][gid:%v] RequireShard success %v", kv.me, kv.gid, shard)
					kv.mu.Unlock()
					kv.cond.Broadcast()
					// shard := Shard{
					// 	ShardNum: reply.Shard.ShardNum,
					// 	Storage:  make(map[string]string),
					// 	RqRecord: make(map[int64]RequestInfo),
					// 	Status:   "Working",
					// }
					// shard.Storage = reply.Shard.Storage
					// shard.RqRecord = reply.Shard.RqRecord
					// go kv.AppendShard(shard)
					return
				} else {
					if reply.Err == ErrorConfigOutOfDate {
						return
					}
					// } else if reply.Err == ErrWrongGroup {
					// 	kv.RequireShard2Group(reply.Gid)
					// 	return
					// }
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
		} else if reply.Err == ErrorConfigOutOfDate {
			return false
		} else if reply.Err == ErrWrongGroup {
			return false
		}
	}
	return false
}

// func (kv *ShardKV) RequireShard2Group(gid int) {
// 	for si := 0; si <
// }

func (kv *ShardKV) AppendShard() {
	kv.mu.Lock()
	shards := make(map[int]Shard)
	for k, v := range kv.shards {
		shards[k] = v
	}
	args := AppendShardArgs{
		Shards: shards,
		Index:  nrand(),
		// 	CurConfig: shardctrler.Config{Num: kv.curConfig.Num, Shards: kv.curConfig.Shards, Groups: make(map[int][]string)},
		// 	PreConfig: shardctrler.Config{Num: kv.preConfig.Num, Shards: kv.preConfig.Shards, Groups: make(map[int][]string)},
		CurConfig: kv.curConfig,
		PreConfig: kv.preConfig,
	}
	// for k, v := range kv.curConfig.Groups {
	// 	args.CurConfig.Groups[k] = v
	// }
	// for k, v := range kv.preConfig.Groups {
	// args.PreConfig.Groups[k] = v
	// }
	DEBUG(dLeader, "S%v [shardkv][gid:%v] send AppendShard %v", kv.me, kv.gid, args)
	// preConfig := kv.preConfig
	curConfig := kv.curConfig
	kv.mu.Unlock()

	for {
		if servers, ok := curConfig.Groups[kv.gid]; ok {
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
			DEBUG(dTimer, "S%v[shardkv][gid:%v] add a Timer i:%v", kv.me, kv.gid, i)
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
			DEBUG(dDrop, "S%v[shardkv][gid:%v] snap %v", kv.me, kv.gid, kv.applyIndex)
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.applyIndex)
			e.Encode(kv.shards)
			e.Encode(kv.rqRecord)
			e.Encode(kv.curConfig)
			e.Encode(kv.preConfig)
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

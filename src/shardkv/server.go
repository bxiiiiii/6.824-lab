package shardkv

import (
	"bytes"
	dsync "sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	// sync "github.com/sasha-s/go-deadlock"
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

	// LastConfig shardctrler.Config
	CurConfig shardctrler.Config

	Shards map[int]Shard

	ShardsNum []int

	Receiver  int
	ConfigNum int

	Rq [shardctrler.NShards]rqShardInfo
}

const (
	Completed            = "Completed"
	InProgress           = "InProgress"
	ErrorOccurred        = "ErrorOccurred"
	ErrorTimeDeny        = "ErrorTimeDeny"
	ErrorConfigOutOfDate = "ErrorConfigOutOfDate"
)

type ShardKV struct {
	mu           dsync.Mutex
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

	curConfig  shardctrler.Config
	LastConfig shardctrler.Config
	preConfig  shardctrler.Config

	temShards map[int]Shard
	isWorking bool

	rqShardRecord [shardctrler.NShards]rqShardInfo

	isDead    bool
	isChanged bool
	temConfig shardctrler.Config
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

type rqShardInfo struct {
	Receiver  int
	ConfigNum int
}

func (kv *ShardKV) GetConfig() {
	for {
		kv.mu.Lock()
		if _, ok := kv.rqRecord[kv.startTimer]; !ok {
			// DEBUG(dPersist, "S%v[shardkv][gid:%v] get deny %v", kv.me, kv.gid, args.Index)
			kv.mu.Unlock()
			time.Sleep(time.Millisecond * 100)
			continue
		}
		lastConfig := kv.mck.Query(-1)
		// kv.mu.Lock()
		ice := true
		// if kv.LastConfig.Num != 0 {
		// 	var Preneed2Control []int
		// 	for shardNum, gid := range kv.LastConfig.Shards {
		// 		if kv.gid == gid {
		// 			Preneed2Control = append(Preneed2Control, shardNum)
		// 		}
		// 	}
		// 	if kv.LastConfig.Num != 0 {
		// 		for _, shardNum := range Preneed2Control {
		// 			if kv.shards[shardNum].Status != "Working" && kv.shards[shardNum].Status != "Failed" {
		// 				// kv.cond.Wait()
		// 				ice = false
		// 			}
		// 		}
		// 	}
		// }
		if kv.LastConfig.Num != kv.curConfig.Num {
			ice = false
		}
		// DEBUG(dDrop,"S%v [shardkv][gid:%v] ice: %v last-kv.last:%v-%v isworking:%v", kv.me, kv.gid, ice, lastConfig.Num, kv.LastConfig.Num, kv.isWorking)
		if ice && lastConfig.Num > kv.LastConfig.Num && kv.isWorking {
			operation := Op{
				Type:   "ConfigChange",
				Index:  nrand(),
				Config: lastConfig,
			}
			i, _, isLeader := kv.rf.Start(operation)
			if isLeader {
				kv.isWorking = false
				DEBUG(dTrace, "S%v [shardkv][gid:%v] get new config %v %v ", kv.me, kv.gid, i, lastConfig)
			}
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
	if !kv.isWorking {
		DEBUG(dLog, "S%v[shardkv][gid:%v] get configChanging %v %v %v", kv.me, kv.gid, kv.shards[args.ShardNum].Status, args.ShardNum, args.Index)
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
		DEBUG(dInfo, "S%v[shardkv][gid:%v] %v get is working status:%v", kv.me, kv.gid, args.Index, kv.shards[args.ShardNum].RqRecord[args.Index].Status)
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
		// DEBUG(dLog, "S%v[shardkv][gid:%v] p/a wrongGroup %v %v %v", kv.me, kv.gid, kv.curConfig.Shards[args.ShardNum], args.Index, kv.curConfig.Shards)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.shards[args.ShardNum].Status != "Working" {
		// DEBUG(dLog, "S%v[shardkv][gid:%v] p/a not ready %v %v %v", kv.me, kv.gid, kv.shards[args.ShardNum].Status, args.ShardNum, args.Index)
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}
	if !kv.isWorking {
		// DEBUG(dLog, "S%v[shardkv][gid:%v] p/a configChanging %v %v %v", kv.me, kv.gid, kv.shards[args.ShardNum].Status, args.ShardNum, args.Index)
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.rqRecord[kv.startTimer]; !ok {
		// DEBUG(dPersist, "S%v[shardkv][gid:%v] p/a deny %v %v", kv.me, kv.gid, args.Index, kv.startTimer)
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
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	// DEBUG(dLog, "S%v[shardkv][gid:%v] handle rq %v from %v", kv.me, kv.gid, args.ShardsNum, args.Sender)
	if kv.LastConfig.Num > args.ConfigNum {
		DEBUG(dError, "S%v [shardkv][gid:%v] handle rq outofdate from lower config:%v %v", kv.me, kv.gid, args, kv.rqShardRecord)
		for _, v := range args.ShardsNum {
			replyShard := Shard{
				ShardNum: v,
				Storage:  make(map[string]string),
				RqRecord: make(map[int64]RequestInfo),
				Status:   kv.shards[v].Status,
			}
			replyShard.Storage = kv.shards[v].Storage
			replyShard.RqRecord = kv.shards[v].RqRecord
			if args.Sender == kv.rqShardRecord[v].Receiver && args.ConfigNum == kv.rqShardRecord[v].ConfigNum {
				reply.Err = OK
				replyShard.Status = "Working"
			} else {
				DEBUG(dCommit, "S%v[shardkv][gid:%v] RequireShard is failed, conNum:m-o:%v-%v %v", kv.me, kv.gid, kv.LastConfig.Num, args.ConfigNum, args.ShardsNum)
				reply.Err = ErrorConfigOutOfDate
				reply.ConfigNum = kv.LastConfig.Num
			}
			reply.Shards = append(reply.Shards, replyShard)
		}
		kv.mu.Unlock()
		return
	} else if kv.LastConfig.Num < args.ConfigNum {
		// DEBUG(dCommit, "S%v[shardkv][gid:%v] ddRequireShard is failed, conNum:m-o:%v-%v %v from %v", kv.me, kv.gid, kv.LastConfig.Num, args.ConfigNum, args.ShardsNum, args.Sender)
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.rqRecord[kv.startTimer]; !ok {
		// DEBUG(dPersist, "S%v[shardkv][gid:%v] RequireShard deny %v", kv.me, kv.gid, args.Index)
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}

	operation := Op{
		Type:      "RequireShard",
		ShardsNum: args.ShardsNum,
		Index:     args.Index,

		Receiver:  args.Sender,
		ConfigNum: args.ConfigNum,
	}
	// fmt.Println("----", kv.storage)
	i, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DEBUG(dLog2, "S%v[shardkv][gid:%v] RequireShard %v %v from %v", kv.me, kv.gid, i, args.ShardsNum, args.Sender)
	kv.rqRecord[args.Index] = RequestInfo{
		Rqindex: i,
		Status:  InProgress,
	}

	for kv.rqRecord[args.Index].Status == InProgress {
		// DEBUG(dPersist, "S%v[shardkv][gid:%v] %v", kv.me, kv.gid, kv.record)
		// DEBUG(dInfo, "S%v[shardkv][gid:%v] %v rq is working status:%v", kv.me, kv.gid, args.Index, kv.record[args.Index].Status)
		kv.cond.Wait()
	}
	if kv.rqRecord[args.Index].Status == ErrorOccurred {
		DEBUG(dCommit, "S%v[shardkv][gid:%v] RequireShard %v is failed", kv.me, kv.gid, args.Index)
		// delete(kv.record, args.Index)
		reply.Err = ErrorOccurred
	} else if kv.rqRecord[args.Index].Status == Completed {
		reply.Err = OK
		// reply.Shard = kv.shards[args.ShardNum]
		var needAppend []int
		for _, v := range args.ShardsNum {
			if kv.LastConfig.Num > args.ConfigNum {
				DEBUG(dCommit, "S%v[shardkv][gid:%v] dddRequireShard is failed, conNum:m-o:%v-%v", kv.me, kv.gid, kv.LastConfig.Num, args.ConfigNum)
				reply.Err = ErrorConfigOutOfDate
				reply.ConfigNum = kv.LastConfig.Num
				if kv.rqShardRecord[v].Receiver == args.Sender && kv.rqShardRecord[v].ConfigNum == args.ConfigNum {
					reply.Err = OK
					replyShard := Shard{
						ShardNum: v,
						Storage:  make(map[string]string),
						RqRecord: make(map[int64]RequestInfo),
						Status:   kv.shards[v].Status,
					}
					replyShard.Storage = kv.shards[v].Storage
					replyShard.RqRecord = kv.shards[v].RqRecord
					DEBUG(dInfo, "S%v [shardkv][gid:%v] require again sucess from lower %v %v", kv.me, kv.gid, args.Sender, kv.rqShardRecord)
					replyShard.Status = "Working"
				}
				kv.mu.Unlock()
				return
			}

			if _, ok := kv.shards[v]; !ok {
				shard := Shard{
					ShardNum: v,
					Storage:  make(map[string]string),
					RqRecord: make(map[int64]RequestInfo),
					Status:   "NotExist",
				}
				reply.Shards = append(reply.Shards, shard)
			} else {
				if kv.shards[v].Status == "Failed" {
					shard := Shard{
						ShardNum: v,
						Storage:  make(map[string]string),
						RqRecord: make(map[int64]RequestInfo),
						Status:   "NotExist",
					}
					reply.Shards = append(reply.Shards, shard)
				} else if kv.shards[v].Status == "OutOfDate" {
					DEBUG(dError, "S%v [shardkv][gid:%v] handle rq outofdate %v %v", kv.me, kv.gid, v, kv.rqShardRecord)
					replyShard := Shard{
						ShardNum: v,
						Storage:  make(map[string]string),
						RqRecord: make(map[int64]RequestInfo),
						Status:   kv.shards[v].Status,
					}
					replyShard.Storage = kv.shards[v].Storage
					replyShard.RqRecord = kv.shards[v].RqRecord
					if kv.rqShardRecord[v].Receiver == args.Sender && kv.rqShardRecord[v].ConfigNum == args.ConfigNum {
						DEBUG(dInfo, "S%v [shardkv][gid:%v] require again sucess %v %v", kv.me, kv.gid, args.Sender, kv.rqShardRecord)
						replyShard.Status = "Working"
					}
					reply.Shards = append(reply.Shards, replyShard)
				} else {
					replyShard := Shard{
						ShardNum: v,
						Storage:  make(map[string]string),
						RqRecord: make(map[int64]RequestInfo),
						Status:   kv.shards[v].Status,
					}
					replyShard.Storage = kv.shards[v].Storage
					replyShard.RqRecord = kv.shards[v].RqRecord
					reply.Shards = append(reply.Shards, replyShard)

					shard := Shard{
						ShardNum: v,
						Storage:  kv.shards[v].Storage,
						RqRecord: kv.shards[v].RqRecord,
						Status:   "OutOfDate",
					}
					kv.shards[v] = shard
					needAppend = append(needAppend, v)
					kv.rqShardRecord[v] = rqShardInfo{
						Receiver:  args.Sender,
						ConfigNum: args.ConfigNum,
					}
				}
				DEBUG(dLog2, "S%v[shardkv][gid:%v] rqShard: %v", kv.me, kv.gid, kv.rqShardRecord)
			}
		}
		if len(needAppend) > 0 {
			go kv.UpdateShard(needAppend, args.Sender, args.ConfigNum)
		}
		DEBUG(dInfo, "S%v[shardkv][gid:%v] RequireShard %v is ok", kv.me, kv.gid, i)
	}
	kv.mu.Unlock()
	DEBUG(dLeader, "S%v[shardkv][gid:%v] RequireShard reply:%v", kv.me, kv.gid, reply)
}

func (kv *ShardKV) HandleAppendShard(args *AppendShardArgs, reply *AppendShardReply) {
	DEBUG(dClient, "S%v[shardkv][gid:%v] HandleAppendShard %v from %v", kv.me, kv.gid)
	kv.mu.Lock()
	// if kv.curConfig.Num != args.PreConfig.Num {
	// 	reply.Err = ErrorTimeDeny
	// 	kv.mu.Unlock()
	// 	return
	// }
	DEBUG(dClient, "S%v[shardkv][gid:%v] HandleAppendShard", kv.me, kv.gid)
	if _, ok := kv.rqRecord[kv.startTimer]; !ok {
		// DEBUG(dPersist, "S%v[shardkv][gid:%v] p/a deny %v", kv.me, kv.gid, args.Index)
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}

	//Todo: some check

	DEBUG(dClient, "S%v[shardkv][gid:%v] HandleAppendShard", kv.me, kv.gid)
	for k, v := range kv.rqRecord {
		if k == args.Index {
			if v.Status == Completed {
				DEBUG(dCommit, "S%v[shardkv][gid:%v] HandleAppendShard %v is ok", kv.me, kv.gid, args.Index)
				reply.Err = OK
			} else if v.Status == InProgress {
				DEBUG(dDrop, "S%v[shardkv][gid:%v] HandleAppendShard %v is waiting", kv.me, kv.gid, args.Index)
				for kv.rqRecord[args.Index].Status == InProgress {
					kv.cond.Wait()
				}
				if kv.rqRecord[args.Index].Status == Completed {
					DEBUG(dCommit, "S%v[shardkv][gid:%v] HandleAppendShard %v is ok", kv.me, kv.gid, args.Index)
					reply.Err = OK
				} else if kv.rqRecord[args.Index].Status == ErrorOccurred {
					DEBUG(dCommit, "S%v[shardkv][gid:%v] HandleAppendShard %v is failed", kv.me, kv.gid, args.Index)
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
		// PreConfig: args.PreConfig,
	}
	// fmt.Println("----", kv.storage)
	i, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DEBUG(dLog2, "S%v[shardkv][gid:%v] HandleAppendShard %v", kv.me, kv.gid, i)
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
		DEBUG(dCommit, "S%v[shardkv][gid:%v] HandleAppendShard %v is failed", kv.me, kv.gid, args.Index)
		// delete(kv.record, args.Index)
		reply.Err = ErrorOccurred
	} else if kv.rqRecord[args.Index].Status == Completed {
		reply.Err = OK
		DEBUG(dInfo, "S%v[shardkv][gid:%v] HandleAppendShard %v is ok", kv.me, kv.gid, i)
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
	kv.isDead = true
}

func (kv *ShardKV) killed() bool {
	return kv.isDead
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
	kv.temShards = make(map[int]Shard)
	kv.startTimer = -1
	kv.cond = dsync.NewCond(&kv.mu)
	kv.applyIndex = 0
	kv.isWorking = true
	kv.isChanged = true
	for i := 0; i < shardctrler.NShards; i++ {
		kv.rqShardRecord[i].Receiver = -1
	}
	// sync.Opts.DeadlockTimeout = time.Second
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
			DEBUG(dClient, "S%v[shardkv][gid:%v] apply receive [%v]%v", kv.me, kv.gid, entry.CommandIndex, op.Type)
			switch op.Type {
			case "Put":
				for kv.shards[op.ShardNum].Status != "Working" {
					DEBUG(dClient, "S%v[shardkv][gid:%v] apply put status [%v]", kv.me, kv.gid, kv.shards[op.ShardNum].Status)
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
				DEBUG(dLog, "S%v[shardkv][gid:%v] apply new config: %v %v %v %v", kv.me, kv.gid, op.Config.Num, kv.LastConfig.Num, kv.curConfig.Num, kv.isChanged)
				// if kv.LastConfig.Num <= op.Config.Num && kv.isChanged {
				if kv.LastConfig.Num <= op.Config.Num {
					kv.LastConfig = op.Config
					kv.isWorking = false
					kv.isChanged = false
					kv.ApplyConfigChange(op.Config)
				}
			case "AppendShard":
				// DEBUG(dSnap, "S%v[shardkv][gid:%v] AppendShard %v", kv.me, kv.gid, op)
				if kv.curConfig.Num <= op.CurConfig.Num {
					kv.shards = make(map[int]Shard)
					kv.isWorking = true
					kv.isChanged = true
					for k, v := range op.Shards {
						if v.Status == "WorkingWaitForAdd" {
							v.Status = "Working"
						}
						shard := Shard{
							ShardNum: v.ShardNum,
							Storage:  make(map[string]string),
							RqRecord: make(map[int64]RequestInfo),
							Status:   v.Status,
						}
						for i, j := range v.Storage {
							shard.Storage[i] = j
						}
						for i, j := range v.RqRecord {
							shard.RqRecord[i] = j
						}
						kv.shards[k] = shard
					}
					kv.curConfig = op.CurConfig
					kv.LastConfig = op.CurConfig
					kv.temConfig = op.Config
				}
			case "UpdateShard":
				for k, v := range op.Shards {
					shard := Shard{
						ShardNum: v.ShardNum,
						Storage:  make(map[string]string),
						RqRecord: make(map[int64]RequestInfo),
						Status:   v.Status,
					}
					shard.Storage = v.Storage
					shard.RqRecord = v.RqRecord
					kv.shards[k] = shard
					kv.rqShardRecord[k].Receiver = op.Receiver
					kv.rqShardRecord[k].ConfigNum = op.ConfigNum
					// kv.rqShardRecord = op.Rq
					// for k, v := range op.Rq {
					// 	if v.Receiver != 0 && v.Receiver != -1 && kv.shards[k].Status != "OutOfDate" {
					// 		shard := Shard{
					// 			ShardNum: kv.shards[k].ShardNum,
					// 			Storage:  kv.shards[k].Storage,
					// 			RqRecord: kv.shards[k].RqRecord,
					// 			Status:   "OutOfDate",
					// 		}
					// 		kv.shards[k] = shard
					// 	}
					// }
				}
			case "RequireShardd":
				if kv.LastConfig.Num <= op.ConfigNum {
					if _, ok := kv.shards[op.ShardsNum[0]]; ok {
						if kv.shards[op.ShardsNum[0]].Status != "Failed" && kv.shards[op.ShardsNum[0]].Status != "OutOfDate" {
							shard := Shard{
								ShardNum: op.ShardsNum[0],
								Storage:  kv.shards[op.ShardsNum[0]].Storage,
								RqRecord: kv.shards[op.ShardsNum[0]].RqRecord,
								Status:   "OutOfDate",
							}
							kv.shards[op.ShardsNum[0]] = shard
						}
					}
				}
				kv.rqShardRecord[op.ShardsNum[0]].Receiver = op.Receiver
				kv.rqShardRecord[op.ShardsNum[0]].ConfigNum = op.ConfigNum
			}

			switch op.Type {
			case "Put", "Append", "Get":
				kv.shards[op.ShardNum].RqRecord[op.Index] = RequestInfo{
					Status:  Completed,
					Rqindex: entry.CommandIndex,
				}
				// DEBUG(dLog, "S%v[shardkv][gid:%v]%v %v", kv.me, kv.gid, op.Type, kv.shards)
			case "LeaderTimer", "Timer", "ConfigChange", "RequireShard", "AppendShard", "UpdateShard":
				kv.rqRecord[op.Index] = RequestInfo{
					Status:  Completed,
					Rqindex: entry.CommandIndex,
				}
			}

			// DEBUG(dError, "S%v[shardkv][gid:%v] after apply [%v]", kv.me, kv.gid, kv.rqRecord)
			kv.applyIndex = entry.CommandIndex
			if op.Type != "Timer" {
				// DEBUG(dError, "S%v[shardkv][gid:%v] after apply [%v]", kv.me, kv.gid, kv.shards)
			}
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
			var lastConfig shardctrler.Config
			var rqShardRecord [shardctrler.NShards]rqShardInfo
			if d.Decode(&applyIndex) != nil || d.Decode(&shards) != nil || d.Decode(&rqRecord) != nil || d.Decode(&curConfig) != nil || d.Decode(&lastConfig) != nil || d.Decode(&rqShardRecord) != nil {
				panic("decode fail")
			} else {
				DEBUG(dSnap, "S%v [shardkv][gid:%v] apply snap %v", kv.me, kv.gid, applyIndex)
				kv.applyIndex = entry.SnapshotIndex
				kv.curConfig = curConfig
				kv.LastConfig = lastConfig

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
				for k, v := range rqShardRecord {
					kv.rqShardRecord[k] = v
				}
			}
			DEBUG(dDrop, "S%v [shardkv][gid:%v] restart apply  check %v %v, %v", kv.me, kv.gid, kv.isWorking, kv.LastConfig, kv.curConfig)
			// if kv.isWorking && kv.LastConfig.Num != kv.curConfig.Num {
			// 	kv.isWorking = false
			// 	opertion := Op{
			// 		Type:   "ConfigChange",
			// 		Index:  nrand(),
			// 		Config: kv.LastConfig,
			// 	}
			// 	i, _, isLeader := kv.rf.Start(opertion)
			// 	if isLeader {
			// 		// kv.isWorking = false
			// 		DEBUG(dTrace, "S%v [shardkv][gid:%v] restart and    %v %v ", kv.me, kv.gid, i, lastConfig)
			// 	}
			// }
			DEBUG(dDrop, "S%v [shardkv][gid:%v] apply snapshot curConfig:%v lastConfig:%v", kv.me, kv.gid, kv.curConfig, kv.LastConfig)
			kv.mu.Unlock()
			kv.cond.Broadcast()
		}
	}
}

func (kv *ShardKV) ApplyConfigChange(lastConfig shardctrler.Config) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	if _, ok := kv.rqRecord[kv.startTimer]; !ok {
		kv.curConfig.Num = kv.LastConfig.Num
		// kv.temConfig = kv.LastConfig
		// kv.isChanged = true
		return
	}
	kv.isWorking = false
	kv.LastConfig = lastConfig
	kv.temShards = make(map[int]Shard)
	for k, v := range kv.shards {
		shard := Shard{
			ShardNum: v.ShardNum,
			Storage:  make(map[string]string),
			RqRecord: make(map[int64]RequestInfo),
			Status:   v.Status,
		}
		for i, j := range v.RqRecord {
			shard.RqRecord[i] = j
		}
		for i, j := range v.Storage {
			shard.Storage[i] = j
		}
		kv.temShards[k] = shard
	}
	DEBUG(dError, "S%v [shardkv][gid:%v] -pre-cur:%v-%v", kv.me, kv.gid, kv.LastConfig, kv.curConfig)
	// kv.preConfig, kv.curConfig = kv.curConfig, lastConfig
	// kv.curConfig = lastConfig
	if kv.curConfig.Num != kv.LastConfig.Num-1 {
		preConfig := kv.mck.Query(kv.LastConfig.Num - 1)
		for preConfig.Num != kv.LastConfig.Num-1 {
			DEBUG(dError, "S%v [shardkv][gid:%v] query failed.", kv.me, kv.gid)
			preConfig = kv.mck.Query(kv.LastConfig.Num - 1)
		}
		kv.preConfig = preConfig
	} else {
		kv.preConfig = kv.curConfig
	}
	DEBUG(dError, "S%v [shardkv][gid:%v] pre:%v", kv.me, kv.gid, kv.preConfig)
	var Curneed2Control []int
	need2Require2gid := make(map[int][]int)
	var need2Require []int
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
				kv.temShards[shardNum] = Shard{
					ShardNum: shardNum,
					Storage:  make(map[string]string),
					RqRecord: make(map[int64]RequestInfo),
					Status:   "WaitForAdd",
				}
			} else {
				// if kv.shards[shardNum].Status != "Working" {
				if kv.shards[shardNum].Status == "Working" || kv.shards[shardNum].Status == "WorkingWaitForAdd" {
					shard := Shard{
						ShardNum: shardNum,
						Storage:  make(map[string]string),
						RqRecord: make(map[int64]RequestInfo),
						Status:   "WorkingWaitForAdd",
					}
					shard.Storage = kv.shards[shardNum].Storage
					shard.RqRecord = kv.shards[shardNum].RqRecord
					kv.shards[shardNum] = shard
					continue
				} else if kv.shards[shardNum].Status == "Failed" {
					shard := Shard{
						ShardNum: shardNum,
						Storage:  make(map[string]string),
						RqRecord: make(map[int64]RequestInfo),
						Status:   "WaitForAdd",
					}
					shard.Storage = kv.shards[shardNum].Storage
					shard.RqRecord = kv.shards[shardNum].RqRecord
					kv.shards[shardNum] = shard
					temShard := Shard{
						ShardNum: shardNum,
						Storage:  make(map[string]string),
						RqRecord: make(map[int64]RequestInfo),
						Status:   "WaitForAdd",
					}
					temShard.Storage = kv.temShards[shardNum].Storage
					temShard.RqRecord = kv.temShards[shardNum].RqRecord
					kv.temShards[shardNum] = temShard
				}

				// }
			}

			if kv.preConfig.Shards[shardNum] != 0 {
				if kv.curConfig.Shards[shardNum] != kv.gid {
					need2Require = append(need2Require, shardNum)
				} else {
					if _, ok := kv.shards[shardNum]; !ok {
						need2Require = append(need2Require, shardNum)
					} else {
						if kv.shards[shardNum].Status != "Working" && kv.shards[shardNum].Status != "WorkingForAdd" {
							need2Require = append(need2Require, shardNum)
						} else {
							shard := Shard{
								ShardNum: shardNum,
								Storage:  make(map[string]string),
								RqRecord: make(map[int64]RequestInfo),
								Status:   "Working",
							}

							shard.Storage = kv.shards[shardNum].Storage
							shard.RqRecord = kv.shards[shardNum].RqRecord
							kv.temShards[shardNum] = shard
						}
					}
				}
			} else {
				kv.temShards[shardNum] = Shard{
					ShardNum: shardNum,
					Storage:  make(map[string]string),
					RqRecord: make(map[int64]RequestInfo),
					Status:   "Working",
				}
			}
		} else {
			if _, ok := kv.shards[shardNum]; ok && (kv.shards[shardNum].Status == "Working" || kv.shards[shardNum].Status == "Deleting") {
				kv.shards[shardNum] = Shard{
					ShardNum: kv.shards[shardNum].ShardNum,
					Storage:  kv.shards[shardNum].Storage,
					RqRecord: kv.shards[shardNum].RqRecord,
					Status:   "Deleting",
				}
				kv.temShards[shardNum] = Shard{
					ShardNum: kv.temShards[shardNum].ShardNum,
					Storage:  kv.temShards[shardNum].Storage,
					RqRecord: kv.temShards[shardNum].RqRecord,
					Status:   "Deleting",
				}
			}
		}
	}
	// DEBUG(dClient, "S%v[shardkv][gid:%v]---%v", kv.me, kv.gid, kv.shards)
	// DEBUG(dClient, "S%v[shardkv][gid:%v]---%v", kv.me, kv.gid, kv.temShards)
	for _, shardNum := range need2Require {
		if _, ok := need2Require2gid[kv.preConfig.Shards[shardNum]]; ok {
			need2Require2gid[kv.preConfig.Shards[shardNum]] = append(need2Require2gid[kv.preConfig.Shards[shardNum]], shardNum)
		} else {
			array := [...]int{shardNum}
			need2Require2gid[kv.preConfig.Shards[shardNum]] = array[:]
		}
	}

	DEBUG(dLeader, "S%v [shardkv][gid:%v] -----%v", kv.me, kv.gid, need2Require2gid)
	for k, v := range need2Require2gid {
		if kv.preConfig.Num == 0 {
			for _, num := range v {
				shard := Shard{
					ShardNum: num,
					Storage:  kv.shards[num].Storage,
					RqRecord: kv.shards[num].RqRecord,
					Status:   "Working",
				}
				kv.temShards[num] = shard
			}
		} else {
			for _, shardNum := range v {
				go kv.RequireShard(k, []int{shardNum}, kv.preConfig)
			}
		}
	}
	go func() {
		kv.mu.Lock()
		if kv.preConfig.Num != 0 {
			for _, shardNum := range Curneed2Control {
				for kv.temShards[shardNum].Status != "Working" && kv.temShards[shardNum].Status != "WorkingWaitForAdd" && kv.temShards[shardNum].Status != "Failed" {
					DEBUG(dLog2, "S%v [shardkv][gid:%v] wait requireShard %v %v ", kv.me, kv.gid, shardNum, kv.temShards[shardNum].Status)
					for k, v := range kv.temShards {
						DEBUG(dLog, "S%v [shardkv][gid:%v] temStatus: %v %v", kv.me, kv.gid, k, v.Status)
					}
					kv.cond.Wait()
				}
				if kv.temShards[shardNum].Status == "WorkingWaitForAdd" {
					kv.temShards[shardNum] = Shard{
						ShardNum: kv.temShards[shardNum].ShardNum,
						Storage:  kv.temShards[shardNum].Storage,
						RqRecord: kv.temShards[shardNum].RqRecord,
						Status:   "Working",
					}
				}
			}
		}
		kv.mu.Unlock()
		kv.AppendShard()
	}()
	// if kv.curConfig.Num != 0 {
	// 	for _, shardNum := range Curneed2Control {
	// 		for kv.temShards[shardNum].Status != "Working" && kv.temShards[shardNum].Status != "Failed" {
	// 			kv.cond.Wait()
	// 		}
	// 	}
	// }
	// go kv.AppendShard()
	// Todo: delete
}

func (kv *ShardKV) RequireShard(gid int, shardsNum []int, config shardctrler.Config) {
	kv.mu.Lock()
	preConfig := config
	if gid == kv.gid {
		for _, v := range shardsNum {
			if kv.shards[v].Status == "Deleting" && kv.rqShardRecord[v].Receiver == -1 {
				shard := Shard{
					ShardNum: v,
					Storage:  kv.shards[v].Storage,
					RqRecord: kv.shards[v].RqRecord,
					Status:   "Working",
				}
				kv.temShards[v] = shard
				DEBUG(dLog, "S%v [shardkv][gid:%v] require to self %v sucess", kv.me, kv.gid, v)
			} else {
				preConfig := kv.mck.Query(preConfig.Num - 1)
				for preConfig.Num != config.Num-1 {
					DEBUG(dError, "S%v [shardkv][gid:%v] query failed.", kv.me, kv.gid)
					preConfig = kv.mck.Query(config.Num - 1)
				}
				DEBUG(dError, "S%v [shardkv][gid:%v] pre-%v", kv.me, kv.gid, preConfig)
				if preConfig.Num == 0 {
					shard := Shard{
						ShardNum: v,
						Storage:  kv.shards[v].Storage,
						RqRecord: kv.shards[v].RqRecord,
						Status:   "Working",
					}
					kv.temShards[v] = shard
				} else {
					go kv.RequireShard(preConfig.Shards[v], []int{v}, preConfig)
				}
			}
		}
		kv.mu.Unlock()
		return
	}

	// curConfig := kv.curConfig
	configNum := kv.LastConfig.Num
	kv.mu.Unlock()
	DEBUG(dTrace, "S%v[shardkv][gid:%v] RequireShard to %v %v %v", kv.me, kv.gid, gid, shardsNum, preConfig)

	args := RequireShardArgs{
		ShardsNum: shardsNum,
		Index:     nrand(),
		ConfigNum: configNum,
		Sender:    kv.gid,
	}

	for {
		if servers, ok := preConfig.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply RequireShardReply
				ok := kv.CallRequireShard(srv, &args, &reply, 0)
				if ok {
					if reply.Shards == nil {
						continue
					}
					kv.mu.Lock()
					for _, v := range reply.Shards {
						if v.Status == "NotExist" || v.Status == "OutOfDate" || v.Status == "WaitForAdd" {
							// preConfig := kv.mck.Query(preConfig.Num - 1)
							preConfig := kv.mck.Query(preConfig.Num - 1)
							for preConfig.Num != config.Num-1 {
								DEBUG(dError, "S%v [shardkv][gid:%v] query failed.", kv.me, kv.gid)
								preConfig = kv.mck.Query(config.Num - 1)
							}

							DEBUG(dError, "S%v [shardkv][gid:%v] pre:%v", kv.me, kv.gid, preConfig)
							if preConfig.Num == 0 {
								shard := Shard{
									ShardNum: v.ShardNum,
									Storage:  make(map[string]string),
									RqRecord: make(map[int64]RequestInfo),
									Status:   "Working",
								}
								kv.temShards[v.ShardNum] = shard
								kv.rqShardRecord[v.ShardNum].Receiver = -1
							} else {
								go kv.RequireShard(preConfig.Shards[v.ShardNum], []int{v.ShardNum}, preConfig)
							}
						} else {
							shard := Shard{
								ShardNum: v.ShardNum,
								Storage:  make(map[string]string),
								RqRecord: make(map[int64]RequestInfo),
								Status:   "Working",
							}
							shard.Storage = v.Storage
							shard.RqRecord = v.RqRecord
							kv.temShards[v.ShardNum] = shard

							kv.rqShardRecord[v.ShardNum].Receiver = -1
							DEBUG(dVote, "S%v [shardkv][gid:%v] rq success %v", kv.me, kv.gid, kv.temShards[v.ShardNum])
						}
					}
					DEBUG(dLog2, "S%v[shardkv][gid:%v] RequireShard success %v", kv.me, kv.gid, reply.Shards)
					kv.mu.Unlock()
					kv.cond.Broadcast()

					return
				} else {
					// DEBUG(dError, "S%v [shardkv][gid:%v] rq shard rpc failed. send to %v %v %v", kv.me, kv.gid, gid, shardsNum, reply.Err)
					if reply.Err == ErrorConfigOutOfDate {
						kv.mu.Lock()
						for _, v := range shardsNum {
							if kv.shards[v].Status == "Deleting" && kv.rqShardRecord[v].Receiver == -1 {
								shard := Shard{
									ShardNum: v,
									Storage:  kv.shards[v].Storage,
									RqRecord: kv.shards[v].RqRecord,
									Status:   "Working",
								}
								kv.temShards[v] = shard
								DEBUG(dLog, "S%v [shardkv][gid:%v] require to self %v sucess", kv.me, kv.gid, v)
							} else {
								shard := Shard{
									ShardNum: v,
									Storage:  make(map[string]string),
									RqRecord: make(map[int64]RequestInfo),
									Status:   "Failed",
								}
								shard.Storage = kv.temShards[v].Storage
								shard.RqRecord = kv.temShards[v].RqRecord
								kv.temShards[v] = shard
							}
							DEBUG(dLog2, "S%v[shardkv][gid:%v] RequireShard failed %v %v", kv.me, kv.gid, shardsNum, reply.ConfigNum)
						}
						kv.mu.Unlock()
						kv.cond.Broadcast()
						return
					}
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
		} else if reply.Err == ErrorTimeDeny {
			// time.Sleep(500 * time.Microsecond)
			newreply := RequireShardReply{}
			return kv.CallRequireShard(srv, args, &newreply, timer+1)
		} else if reply.Err == ErrorConfigOutOfDate {
			return false
		}
	}
	return false
}

func (kv *ShardKV) AppendShard() {
	kv.mu.Lock()
	shards := make(map[int]Shard)
	for k, v := range kv.temShards {
		shard := Shard{
			ShardNum: v.ShardNum,
			Storage:  make(map[string]string),
			RqRecord: make(map[int64]RequestInfo),
			Status:   v.Status,
		}
		for i, j := range v.Storage {
			shard.Storage[i] = j
		}
		for i, j := range v.RqRecord {
			shard.RqRecord[i] = j
		}
		shards[k] = shard
	}
	args := AppendShardArgs{
		Shards:    shards,
		Index:     nrand(),
		CurConfig: kv.LastConfig,
	}
	DEBUG(dLeader, "S%v [shardkv][gid:%v] send AppendShard %v", kv.me, kv.gid, args)
	operation := Op{
		Type:      "AppendShard",
		Index:     args.Index,
		Shards:    args.Shards,
		CurConfig: args.CurConfig,
	}
	kv.rf.Start(operation)
	kv.mu.Unlock()
}

func (kv *ShardKV) UpdateShard(shardNum []int, sender int, configNum int) {
	kv.mu.Lock()
	shards := make(map[int]Shard)
	for _, shardnum := range shardNum {
		shard := Shard{
			ShardNum: shardnum,
			Storage:  make(map[string]string),
			RqRecord: make(map[int64]RequestInfo),
			Status:   "OutOfDate",
		}
		shard.Storage = kv.shards[shardnum].Storage
		shard.RqRecord = kv.shards[shardnum].RqRecord
		shards[shardnum] = shard
	}
	args := AppendShardArgs{
		Shards:    shards,
		Index:     nrand(),
		CurConfig: kv.LastConfig,
	}
	DEBUG(dLeader, "S%v [shardkv][gid:%v] send UpdateShard %v", kv.me, kv.gid, args)
	operation := Op{
		Type:      "UpdateShard",
		Index:     args.Index,
		Shards:    args.Shards,
		CurConfig: args.CurConfig,
		Receiver:  sender,
		ConfigNum: configNum,
		Rq:        kv.rqShardRecord,
	}
	kv.rf.Start(operation)
	kv.mu.Unlock()
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
		if kv.killed() {
			return
		}
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
		if kv.killed() {
			return
		}
		operation := Op{
			Type:  "Timer",
			Index: nrand(),
		}
		kv.mu.Lock()
		if kv.isWorking && kv.LastConfig.Num > kv.curConfig.Num {
			operation := Op{
				Type:   "ConfigChange",
				Index:  nrand(),
				Config: kv.LastConfig,
			}
			i, _, isLeader := kv.rf.Start(operation)
			if isLeader {
				kv.isWorking = false
				DEBUG(dWarn, "S%v [shardkv][gid:%v] miss config %v %v", kv.me, kv.gid, i, kv.LastConfig)
			}
		}
		// if kv.curConfig.Num < kv.temConfig.Num {
		if !kv.isWorking && kv.LastConfig.Num == kv.curConfig.Num {
			operation := Op{
				Type:   "ConfigChange",
				Index:  nrand(),
				Config: kv.LastConfig,
			}
			i, _, isLeader := kv.rf.Start(operation)
			if isLeader {
				kv.isWorking = false
				DEBUG(dWarn, "S%v [shardkv][gid:%v] !miss config %v %v", kv.me, kv.gid, i, kv.LastConfig)
			}
		}
		i, _, isleader := kv.rf.Start(operation)
		if isleader {
			DEBUG(dTimer, "S%v[shardkv][gid:%v] add a Timer i:%v isworking:%v last:%v cur:%v", kv.me, kv.gid, i, kv.isWorking, kv.LastConfig.Num, kv.curConfig.Num)
		}
		kv.mu.Unlock()
		time.Sleep(time.Second)
	}
}

func (kv *ShardKV) Snap() {
	if kv.maxraftstate == -1 {
		return
	}
	for {
		kv.mu.Lock()
		if kv.killed() {
			return
		}

		if kv.applyIndex > kv.snapshotIndex && kv.rf.Persister.RaftStateSize() >= kv.maxraftstate {
			// DEBUG(dDrop, "S%v[shardkv][gid:%v] snap %v", kv.me, kv.gid, kv.applyIndex)
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.applyIndex)
			e.Encode(kv.shards)
			e.Encode(kv.rqRecord)
			e.Encode(kv.curConfig)
			e.Encode(kv.LastConfig)
			e.Encode(kv.rqShardRecord)
			data := w.Bytes()
			DEBUG(dDrop, "S%v[shardkv][gid:%v] snap %v %v ", kv.me, kv.gid, kv.applyIndex, len(data))
			kv.snapshotIndex = kv.applyIndex
			kv.rf.Snapshot(kv.applyIndex, data)
			kv.mu.Unlock()
		} else {
			kv.mu.Unlock()
		}
		time.Sleep(time.Millisecond * 50)
	}
}

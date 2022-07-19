package kvraft

import (
	"fmt"
	"log"
	"time"

	dsync "sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	sync "github.com/sasha-s/go-deadlock"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string
	Index int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage  map[string]string
	ApplyIdx int
	cond     *dsync.Cond
	record   map[int64]RequestInfo
}

type RequestInfo struct {
	Status bool
	Rindex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, Rstatus := kv.rf.GetState()
	if !Rstatus {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	// DEBUG(dClient, "S%v get %v")
	temRecord := make(map[int64]RequestInfo)
	for k, v := range kv.record {
		temRecord[k] = v
	}
	kv.mu.Unlock()

	for k, v := range temRecord {
		if k == args.Index {
			if v.Status {
				if _, ok := kv.storage[args.Key]; ok {
					reply.Err = OK
					reply.Value = kv.storage[args.Key]
				} else {
					reply.Err = ErrNoKey
					reply.Value = ""
				}
			} else {
				kv.mu.Lock()
				for !kv.record[args.Index].Status {
					kv.cond.Wait()
				}
				if _, ok := kv.storage[args.Key]; ok {
					reply.Err = OK
					reply.Value = kv.storage[args.Key]
				} else {
					reply.Err = ErrNoKey
					reply.Value = ""
				}
				kv.mu.Unlock()
			}
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
		return
	}
	DEBUG(dLog2, "S%v get %v", kv.me, i)
	kv.mu.Lock()
	kv.record[args.Index] = RequestInfo{
		Status: false,
		Rindex: i,
	}
	kv.mu.Unlock()

	kv.mu.Lock()
	for _, ok := kv.record[args.Index]; ok && !kv.record[args.Index].Status; {
		DEBUG(dPersist, "S%v %v", kv.me, kv.record)
		DEBUG(dInfo, "S%v %v get is working ok:%v status:%v", kv.me, args.Index, ok, kv.record[args.Index].Status)
		kv.cond.Wait()
	}
	if _, ok := kv.record[args.Index]; !ok {
		reply.Err = ErrWrongLeader
	} else {
		if _, ok := kv.storage[args.Key]; ok {
			reply.Err = OK
			reply.Value = kv.storage[args.Key]
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
		DEBUG(dInfo, "S%v %v get is ok", kv.me, i)
	}

	for _, v := range kv.record {
		fmt.Print(v)
	}
	fmt.Println()
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, Rstatus := kv.rf.GetState()
	if !Rstatus {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	// fmt.Println("PUT/APPEND: ", kv.record)
	// DEBUG(dClient, "S%v g/a %v--[%v][%v]", kv.me, args.Index, args.Key, args.Value)
	// DEBUG(dLog, "S%v %v", kv.me, kv.record)
	temRecord := make(map[int64]RequestInfo)
	for k, v := range kv.record {
		temRecord[k] = v
	}
	kv.mu.Unlock()

	for k, v := range temRecord {
		if k == args.Index {
			if v.Status {
				DEBUG(dCommit, "S%v %v is ok", kv.me, args.Index)
				reply.Err = OK
			} else {
				DEBUG(dDrop, "S%v %v is waiting", kv.me, args.Index)
				kv.mu.Lock()
				for !kv.record[args.Index].Status {
					kv.cond.Wait()
				}
				// if kv.record[]
				DEBUG(dCommit, "S%v %v is ok", kv.me, args.Index)
				reply.Err = OK
				kv.mu.Unlock()
			}
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
		return
	}
	DEBUG(dLog2, "S%v p/a %v", kv.me, i)
	kv.mu.Lock()
	kv.record[args.Index] = RequestInfo{
		Status: false,
		Rindex: i,
	}
	kv.mu.Unlock()

	kv.mu.Lock()
	for _, ok := kv.record[args.Index]; ok && !kv.record[args.Index].Status; {
		DEBUG(dPersist, "S%v %v", kv.me, kv.record)
		DEBUG(dInfo, "S%v %v p/a is working ok:%v status:%v", kv.me, args.Index, ok, kv.record[args.Index].Status)
		kv.cond.Wait()
	}
	if _, ok := kv.record[args.Index]; !ok {
		reply.Err = ErrWrongLeader
		DEBUG(dInfo, "S%v %v p/a is failed", kv.me, i)
	} else {
		reply.Err = OK
		DEBUG(dInfo, "S%v %v p/a is ok", kv.me, i)
	}
	for _, v := range kv.record {
		fmt.Print(v)
	}
	fmt.Println()
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.storage = make(map[string]string)
	kv.ApplyIdx = 0
	kv.cond = dsync.NewCond(&kv.mu)
	sync.Opts.DeadlockTimeout = time.Second

	kv.record = make(map[int64]RequestInfo)
	go kv.Apply()
	return kv
}

func (kv *KVServer) Apply() {
	for entry := range kv.applyCh {
		if entry.CommandValid {
			kv.mu.Lock()
			op := (entry.Command).(Op)
			if op.Type == "Put" {
				kv.storage[op.Key] = op.Value
			} else if op.Type == "Append" {
				kv.storage[op.Key] += op.Value
			}
			DEBUG(dLeader, "S%v apply [%v][%v]%v", kv.me, entry.CommandIndex, op.Index, kv.record[op.Index].Status)
			kv.record[op.Index] = RequestInfo{
				Status: true,
				Rindex: entry.CommandIndex,
			}
			for k, v := range kv.record {
				if v.Rindex == entry.CommandIndex {
					DEBUG(dError, "S%v apply [%v][%v]%v", kv.me, entry.CommandIndex, k, v.Status)
					if k != op.Index{
						delete(kv.record, k)
					}
				}
			}
			DEBUG(dLog2, "S%v %v", kv.me, kv.record)
			// kv.ApplyIdx = entry.CommandIndex
			kv.cond.Broadcast()
			// fmt.Println("--", kv.storage)
			kv.mu.Unlock()
		}
	}
}

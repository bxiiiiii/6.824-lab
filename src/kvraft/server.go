package kvraft

import (
	"fmt"
	"log"
	// "sync"
	"sync/atomic"
	"time"

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
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	operation := Op{
		Type: "Get",
		Key:  args.Key,
	}
	fmt.Println("----",kv.storage)
	_, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = "failed"
		reply.Value = ""
	} else {
		reply.Err = ""
		kv.mu.Lock()
		if _, ok := kv.storage[args.Key]; ok {
			reply.Value = kv.storage[args.Key]
		} else {
			reply.Value = ""
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	operation := Op{
		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
	}
	idx, _, isleader := kv.rf.Start(operation)
	if !isleader {
		reply.Err = "failed"
		return
	}
	time.Sleep(50 * time.Millisecond)
	for m := range kv.applyCh {
		fmt.Println(m, "*", idx)
		if m.CommandValid {
			if m.CommandIndex == idx {
				kv.mu.Lock()
				// if _, ok := kv.storage[args.Key]; ok {
				// 	kv.storage[args.Key] += args.Value
				// } else {
				// 	kv.storage[args.Key] = args.Value
				// }
				if args.Op == "Put"{
					kv.storage[args.Key] = args.Value
				} else if args.Op == "Append"{
					kv.storage[args.Key] += args.Value
				} else {
					fmt.Println("unknown type")
				}
				kv.mu.Unlock()
				// fmt.Println("*****",kv.storage)
				reply.Err = ""
				return
			}
		}
	}
	reply.Err = "failed"
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
	return kv
}

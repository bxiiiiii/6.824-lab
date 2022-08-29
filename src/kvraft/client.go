package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	index := nrand()
	DEBUG(dTrace, "S0 rq get %v", index)
	for {
		for i := range ck.servers {
			args := GetArgs{
				Key:   key,
				Index: index,
			}
			reply := GetReply{}
			ok, value := ck.CallGet(i, &args, &reply, 0)
			if ok {
				DEBUG(dInfo, "S%v get client ok %v", i, index)
				return value
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) CallGet(i int, args *GetArgs, reply *GetReply, timer int) (bool, string) {
	if timer > 3 {
		return false, ""
	}
	ok := ck.servers[i].Call("KVServer.Get", args, reply)
	// DEBUG(dError, "S%v get %v ok:%v err:%v", i, args.Index, ok, reply.Err)
	if !ok {
		newreply := GetReply{}
		return ck.CallGet(i, args, &newreply, timer+1)
	} else {
		if reply.Err == OK || reply.Err == ErrNoKey {
			DEBUG(dTrace, "S0 get %v is completed", args.Index)
			return true, reply.Value
		} else if reply.Err == ErrWrongLeader {
			return false, ""
		} else if reply.Err == ErrorOccurred {
			return false, ""
		} else if reply.Err == ErrorTimeDeny {
			time.Sleep(time.Millisecond * 250)
			newreply := GetReply{}
			return ck.CallGet(i, args, &newreply, timer+1)
		}
	}
	return false, ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	index := nrand()
	// DEBUG(dTrace, "S0 rq p/a %v", index)
	for {
		for i := range ck.servers {
			args := PutAppendArgs{
				Key:   key,
				Value: value,
				Op:    op,
				Index: index,
			}
			reply := PutAppendReply{}
			ok := ck.CallPutAppend(i, &args, &reply, 0)
			if ok {
				DEBUG(dInfo, "S%v p/a client ok %v", i, index)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) CallPutAppend(i int, args *PutAppendArgs, reply *PutAppendReply, timer int) bool {
	if timer > 3 {
		return false
	}
	ok := ck.servers[i].Call("KVServer.PutAppend", args, reply)
	// DEBUG(dError, "S%v p/a %v ok:%v err:%v", i, args.Index, ok, reply.Err)
	if !ok {
		newreply := PutAppendReply{}
		return ck.CallPutAppend(i, args, &newreply, timer+1)
	} else {
		if reply.Err == OK {
			DEBUG(dTrace, "S0 p/a %v is completed", args.Index)
			return true
		} else if reply.Err == ErrWrongLeader {
			return false
		} else if reply.Err == ErrorOccurred {
			return false
		} else if reply.Err == ErrorTimeDeny {
			time.Sleep(time.Millisecond * 250)
			newreply := PutAppendReply{}
			return ck.CallPutAppend(i, args, &newreply, timer+1)
		}
	}
	return false
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

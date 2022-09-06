package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	shard := key2shard(key)
	args := GetArgs{
		Key:      key,
		Index:    nrand(),
		ShardNum: shard,
	}
	DEBUG(dLog, "[client]get %v %v", shard, args.Index)
	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok, value := ck.CallGet(srv, &args, &reply, 0)
				if ok {
					return value
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

func (ck *Clerk) CallGet(srv *labrpc.ClientEnd, args *GetArgs, reply *GetReply, timer int) (bool, string) {
	if timer > 5 {
		return false, ""
	}
	ok := srv.Call("ShardKV.Get", args, reply)
	if !ok {
		newreply := GetReply{}
		return ck.CallGet(srv, args, &newreply, timer+1)
	} else {
		switch reply.Err {
		case OK, ErrNoKey:
			return true, reply.Value
		case ErrWrongLeader, ErrorOccurred, ErrWrongGroup:
			return false, ""
		case ErrorTimeDeny:
			newreply := GetReply{}
			return ck.CallGet(srv, args, &newreply, timer+1)
		}
	}
	return false, ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	shard := key2shard(key)
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Index:    nrand(),
		ShardNum: shard,
	}
	DEBUG(dLog, "[client]p/a %v %v", shard, args.Index)
	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := ck.CallPutAppend(srv, &args, &reply, 0)
				if ok {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) CallPutAppend(srv *labrpc.ClientEnd, args *PutAppendArgs, reply *PutAppendReply, timer int) bool {
	if timer > 5 {
		return false
	}
	ok := srv.Call("ShardKV.PutAppend", args, reply)
	if !ok {
		newreply := PutAppendReply{}
		return ck.CallPutAppend(srv, args, &newreply, timer+1)
	} else {
		switch reply.Err {
		case OK:
			return true
		case ErrWrongLeader, ErrorOccurred, ErrWrongGroup:
			return false
		case ErrorTimeDeny:
			newreply := PutAppendReply{}
			return ck.CallPutAppend(srv, args, &newreply, timer+1)
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

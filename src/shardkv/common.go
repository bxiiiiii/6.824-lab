package shardkv

import "6.824/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Index    int64
	ShardNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Index    int64
	ShardNum int
}

type GetReply struct {
	Err   Err
	Value string
}

type RequireShardArgs struct {
	Index       int64
	ShardsNum   []int
	ConfigNum   int
	Sender      int
	RqConfigNum int
}

type RequireShardReply struct {
	Err       Err
	Shards    []Shard
	ConfigNum int
}

type AppendShardArgs struct {
	Index     int64
	Shards    map[int]Shard
	CurConfig shardctrler.Config
}

type AppendShardReply struct {
	Err Err
}

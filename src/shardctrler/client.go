package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.Index = nrand()

	for {
		// try each known server.
		for i, _ := range ck.servers {
			var reply QueryReply
			ok := ck.CallQuery(i, args, &reply, 0)
			if ok {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) CallQuery(i int, args *QueryArgs, reply *QueryReply, timer int) bool {
	if timer > 5 {
		return false
	}
	ok := ck.servers[i].Call("ShardCtrler.Query", args, reply)
	if !ok {
		newreply := QueryReply{}
		return ck.CallQuery(i, args, &newreply, timer+1)
	} else {
		if reply.Err == OK {
			return true
		} else if reply.Err == ErrWrongLeader {
			return false
		} else if reply.Err == ErrorOccurred {
			return false
		} else if reply.Err == ErrorTimeDeny {
			newreply := QueryReply{}
			return ck.CallQuery(i, args, &newreply, timer+1)
		}
	}
	return false
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.Index = nrand()

	for {
		// try each known server.
		for i, _ := range ck.servers {
			var reply JoinReply
			ok := ck.CallJoin(i, args, &reply, 0)
			if ok {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) CallJoin(i int, args *JoinArgs, reply *JoinReply, timer int) bool {
	if timer > 5 {
		return false
	}
	ok := ck.servers[i].Call("ShardCtrler.Join", args, reply)
	if !ok {
		newreply := JoinReply{}
		return ck.CallJoin(i, args, &newreply, timer+1)
	} else {
		if reply.Err == OK {
			return true
		} else if reply.Err == ErrWrongLeader {
			return false
		} else if reply.Err == ErrorOccurred {
			return false
		} else if reply.Err == ErrorTimeDeny {
			newreply := JoinReply{}
			return ck.CallJoin(i, args, &newreply, timer+1)
		}
	}
	return false
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.Index = nrand()

	for {
		// try each known server.
		for i, _ := range ck.servers {
			var reply LeaveReply
			ok := ck.CallLeave(i, args, &reply, 0)
			if ok {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) CallLeave(i int, args *LeaveArgs, reply *LeaveReply, timer int) bool {
	if timer > 5 {
		return false
	}
	ok := ck.servers[i].Call("ShardCtrler.Leave", args, reply)
	if !ok {
		newreply := LeaveReply{}
		return ck.CallLeave(i, args, &newreply, timer+1)
	} else {
		if reply.Err == OK {
			return true
		} else if reply.Err == ErrWrongLeader {
			return false
		} else if reply.Err == ErrorOccurred {
			return false
		} else if reply.Err == ErrorTimeDeny {
			newreply := LeaveReply{}
			return ck.CallLeave(i, args, &newreply, timer+1)
		}
	}
	return false
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.Index = nrand()

	for {
		// try each known server.
		for i, _ := range ck.servers {
			var reply MoveReply
			ok := ck.CallMove(i, args, &reply, 0)
			if ok {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) CallMove(i int, args *MoveArgs, reply *MoveReply, timer int) bool {
	if timer > 5 {
		return false
	}
	ok := ck.servers[i].Call("ShardCtrler.Move", args, reply)
	if !ok {
		newreply := MoveReply{}
		return ck.CallMove(i, args, &newreply, timer+1)
	} else {
		if reply.Err == OK {
			return true
		} else if reply.Err == ErrWrongLeader {
			return false
		} else if reply.Err == ErrorOccurred {
			return false
		} else if reply.Err == ErrorTimeDeny {
			newreply := MoveReply{}
			return ck.CallMove(i, args, &newreply, timer+1)
		}
	}
	return false
}

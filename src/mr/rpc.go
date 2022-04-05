package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MapArgs struct {
	MapperId int
	Applyorfinish int
	IntermediateFilename string
	// FileSize int
}

type MapReply struct {
	Filename string
	Content []byte
}

type ReduceArgs struct {
	RuducerId int
	Applyorfinish int
}

type ReduceReply struct{
	Filename string
}
// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

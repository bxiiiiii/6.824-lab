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
	MapperId string
	Applyorfinish int
	FinishedFile string
	IntermediateFilename []string
}

type MapReply struct {
	Filename string
	Content []byte
	Ret bool
}

type ReduceArgs struct {
	RuducerId string
	Applyorfinish int
	FinishedFile []string
	Sim uint8
}

type ReduceReply struct{
	Filesname []string
	Reserve bool
	Sim uint8
	Ret bool
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

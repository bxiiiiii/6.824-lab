package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	num_mapper := 5
	for i := 0; i < num_mapper; i++ {
		reply := MapReply{}
		go func (reply MapReply){
			MapCall(0, &reply)
			kva := mapf(reply.filename, reply.content)

			
		}(reply)
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func MapCall(aorf int, reply *MapReply){
	args := MapArgs{}
	args.applyorfinish = aorf

	ok := call("Coordinator.Map", &args, reply)
	if ok {
		
	} else {
		fmt.Printf("map call failed!\n")
	}

}

func ReduceCall(aorf int) {
	args := ReduceArgs{}
	args.applyorfinish = aorf
	reply := ReduceReply{}

	ok := call("Coordinator.Reduce", &args, &reply)
	if ok {

	} else {
		fmt.Printf("reduce call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

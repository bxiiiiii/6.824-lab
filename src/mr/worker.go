package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	num_mapper := 8
	for i := 0; i < num_mapper; i++ {
		reply := MapReply{}
		go func(reply MapReply, i int) {
			MapCall(0, &reply, i)
			kva := mapf(reply.Filename, string(reply.Content))
			for j := 0; j < len(kva); j++ {
				i := ihash(kva[j].Key) % 10
				oname := fmt.Sprintf("mr-%v%v", i, i)
				file, _ := os.OpenFile(oname, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
				enc := json.NewEncoder(file)
				err := enc.Encode(&kva[i])
				if err != nil {
					fmt.Fprintf(os.Stderr, "encode failed\n")
					os.Exit(1)
				}
				file.Close()
			}
			MapCall(1, &reply, i)
		}(reply, i)
	}
	

	for i := 0; i < 10; i++ {
		reply := ReduceReply{}
		go func(reply ReduceReply, i int) {
			ReduceCall(0, &reply, i)
			oname := fmt.Sprintf("mr-out-%v", i)
			file, _ := os.OpenFile(oname, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
			file1, _ := os.OpenFile(reply.Filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
			fmt.Println(reply.Filename)
			intermediate := []KeyValue{}
			dec := json.NewDecoder(file1)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}

			j := 0
			for j < len(intermediate) {
				p := j + 1
				for p < len(intermediate) && intermediate[p].Key == intermediate[j].Key {
					j++
				}
				values := []string{}
				for k := j; k < p; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[j].Key, values)
				fmt.Fprintf(file, "%v %v\n", intermediate[j].Key, output)
				j = p
			}
			ReduceCall(1, &reply, i)
		}(reply, i)
	}
	time.Sleep(time.Second)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func MapCall(aorf int, reply *MapReply, i int) {
	args := MapArgs{}
	args.Applyorfinish = aorf
	fmt.Println(args.Applyorfinish)

	ok := call("Coordinator.Map", &args, reply)
	if ok {

	} else {
		fmt.Printf("map call failed!\n")
	}

}

func ReduceCall(aorf int, reply *ReduceReply, i int) {
	args := ReduceArgs{}
	args.Applyorfinish = aorf
	args.RuducerId = i

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

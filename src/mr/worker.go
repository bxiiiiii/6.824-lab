package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
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
		go func(i int) {
			for {
				reply := MapReply{}
				args := MapArgs{}
				args.Applyorfinish = 0
				args.MapperId = i
				MapCall(args, &reply)
				if reply.Filename != "" {
					imfiles := make(map[string]int)
					kva := mapf(reply.Filename, string(reply.Content))
					for j := 0; j < len(kva); j++ {
						rid := ihash(kva[j].Key) % 10
						pid := os.Getpid()
						oname := fmt.Sprintf("mr-%v:%v-%v", pid, i, rid)
						// oname := fmt.Sprintf("mr-%v%v", i, rid)
						imfiles[oname] = 0
						file, _ := os.OpenFile(oname, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
						enc := json.NewEncoder(file)
						err := enc.Encode(&kva[j])
						if err != nil {
							fmt.Fprintf(os.Stdout, "encode failed\n")
							os.Exit(1)
						}
						file.Close()
					}

					reply2 := MapReply{}
					args2 := MapArgs{}
					args2.Applyorfinish = 1
					args2.FinishedFile = reply.Filename
					args2.MapperId = i
					args2.IntermediateFilename = imfiles
					MapCall(args2, &reply2)
				} else {
					time.Sleep(20 * time.Second)
				}
			}
		}(i)
	}

	// for {
	// 	reply3 := MapReply{}
	// 	args3 := MapArgs{}
	// 	args3.Applyorfinish = 2
	// 	MapCall(args3, &reply3)
	// 	if reply3.Ret {
	// 		break
	// 	}
	// }
	time.Sleep(5 * time.Second)
	for i := 0; i < 10; i++ {
		go func(i int) {
			for {
				reply := ReduceReply{}
				args := ReduceArgs{}
				args.Applyorfinish = 0
				args.RuducerId = i
				ReduceCall(args, &reply)
				if len(reply.Filesname) != 0 {
					oname := fmt.Sprintf("mr-out-%v", i)
					file, _ := os.OpenFile(oname, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
					intermediate := []KeyValue{}
					for _, filename := range reply.Filesname {
						file1, _ := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
						dec := json.NewDecoder(file1)
						for {
							var kv KeyValue
							if err := dec.Decode(&kv); err != nil {
								break
							}
							intermediate = append(intermediate, kv)
						}
						file1.Close()
					}

					sort.Sort(ByKey(intermediate))

					j := 0
					for j < len(intermediate) {
						p := j + 1
						for p < len(intermediate) && intermediate[p].Key == intermediate[j].Key {
							p++
						}
						values := []string{}
						for k := j; k < p; k++ {
							values = append(values, intermediate[k].Value)
						}
						output := reducef(intermediate[j].Key, values)
						fmt.Fprintf(file, "%v %v\n", intermediate[j].Key, output)
						j = p
					}
					file.Close()

					reply2 := ReduceReply{}
					args2 := ReduceArgs{}
					args2.Applyorfinish = 1
					args2.RuducerId = i
					args2.FinishedFile = reply.Filesname
					ReduceCall(args2, &reply2)
				} else {
					time.Sleep(5 * time.Second)
				}
			}
		}(i)
	}
	time.Sleep(time.Minute)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func MapCall(aorf int, reply *MapReply, i int, imfiles map[string]int) {
// args := MapArgs{}
// args.MapperId = i
// args.Applyorfinish = aorf
// args.IntermediateFilename = imfiles
func MapCall(args MapArgs, reply *MapReply) {
	ok := call("Coordinator.Map", &args, reply)
	if ok {

	} else {
		os.Exit(0)
		fmt.Printf("map call failed!\n")
	}

}

// func ReduceCall(aorf int, reply *ReduceReply, i int) {
// 	args := ReduceArgs{}
// 	args.Applyorfinish = aorf
// 	args.RuducerId = i
func ReduceCall(args ReduceArgs, reply *ReduceReply) {
	ok := call("Coordinator.Reduce", &args, reply)
	if ok {

	} else {
		os.Exit(0)
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

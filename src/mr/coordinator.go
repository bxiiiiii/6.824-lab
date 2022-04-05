package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	files  map[string]int
	intermediateFiles map[string]int
	mapper map[int]int
	reducer map[int]int
	mutex  sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Map(args *MapArgs, reply *MapReply) error{
	fmt.Println(args.Applyorfinish)
	if args.Applyorfinish == 0 {
		var filename string
		c.mutex.Lock()
		for filename = range c.files {
			if c.files[filename] == 0 {
				reply.Filename = filename
				c.files[filename] = 1
				c.mapper[args.MapperId] = 0
			}
		}
		c.mutex.Unlock()

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		reply.Content = content
		file.Close()
	} else if args.Applyorfinish == 1 {
		c.mutex.Lock()
		c.mapper[args.MapperId] = 1
		c.intermediateFiles[args.IntermediateFilename] = 0	
		c.mutex.Unlock()
	}
	return nil
}

func (c *Coordinator) Reduce(args *ReduceArgs, reply *ReduceReply) error{
	// fmt.Println(args.Applyorfinish)
	if args.Applyorfinish == 0{
		var filename string
		c.mutex.Lock()
		for filename = range c.intermediateFiles{
			// fmt.Println(args.RuducerId)
			// fmt.Println(reply.Filename)
			if filename[4] == byte(args.RuducerId){
				reply.Filename = filename
				c.intermediateFiles[filename] = 1
			}
		}
		c.mutex.Unlock()
	}else if args.Applyorfinish == 1{
		c.mutex.Lock()
		c.reducer[args.RuducerId] = 1
		c.mutex.Unlock()
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = make(map[string]int)
	c.intermediateFiles = make(map[string]int)
	c.mapper = make(map[int]int)
	c.reducer = make(map[int]int)
	fmt.Println(files)
	for _, filename := range files {
		c.files[filename] = 0
	}

	c.server()
	return &c
}

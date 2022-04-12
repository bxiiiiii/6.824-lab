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
	// "time"
)

type Coordinator struct {
	// Your definitions here.
	files        map[string]int
	siminFiles   map[uint8]int
	mapper       map[string]int
	mapjob       map[string]string
	activeMapper map[string]int

	// intermediateFiles  map[string]int
	intermediateFiles []string
	reducer           map[string]int
	reducejob         map[string]uint8
	activeReducer     map[string]int

	maptaskFinished    int
	reducetaskFinished int
	interval           int
	// time               int
	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Map(args *MapArgs, reply *MapReply) error {
	if args.Applyorfinish == 0 {
		filename := ""
		var v int
		c.mutex.Lock()
		ice := true
		for k := range c.mapper {
			if k == args.MapperId {
				ice = false
				break
			}
		}
		if ice {
			c.mapper[args.MapperId] = 0
		}
		if c.mapper[args.MapperId] != 3 {
			for filename, v = range c.files {
				// fmt.Println(c.files)
				if v == 0 {
					reply.Filename = filename
					c.mapper[args.MapperId] = 1
					c.activeMapper[args.MapperId] = 0
					c.files[filename] = 1
					c.mapjob[args.MapperId] = filename
					break
				}
			}
		}
		c.mutex.Unlock()
		if filename != "" {
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
		}
	} else if args.Applyorfinish == 1 {
		c.mutex.Lock()
		if c.mapper[args.MapperId] != 3 {
			c.mapper[args.MapperId] = 2
			c.files[args.FinishedFile] = 2
			for _, v := range args.IntermediateFilename {
				c.intermediateFiles = append(c.intermediateFiles, v)
				c.siminFiles[v[len(v)-1]-'0'] = 0
			}
			delete(c.activeMapper, args.MapperId)
			delete(c.mapjob, args.MapperId)
		}
		//+* fmt.Println(args.MapperId)
		// fmt.Println(c.files)
		// fmt.Println(c.intermediateFiles)
		c.mutex.Unlock()
	} else if args.Applyorfinish == 2 {
		c.mutex.Lock()
		for _, v := range c.files {
			if v != 2 {
				reply.Ret = false
				c.mutex.Unlock()
				return nil
			}
		}
		reply.Ret = true
		c.mutex.Unlock()
	}
	return nil
}

func (c *Coordinator) Reduce(args *ReduceArgs, reply *ReduceReply) error {
	// fmt.Println("~ ",args.Applyorfinish)
	if args.Applyorfinish == 0 {
		c.mutex.Lock()
		ice := true
		for k := range c.reducer {
			if k == args.RuducerId {
				ice = false
				break
			}
		}
		if ice {
			c.reducer[args.RuducerId] = 0
		}
		if c.reducer[args.RuducerId] != 3 {
			fmt.Println(c.siminFiles)
			fmt.Println(c.reducejob)
			for k, v := range c.siminFiles {
				fmt.Println(c.siminFiles)
				if v == 0 {
					for _, s := range c.intermediateFiles {
						if s[len(s)-1] == k+'0' {
							reply.Filesname = append(reply.Filesname, s)
						}
					}
					fmt.Println(reply.Filesname)
					c.reducer[args.RuducerId] = 1
					c.activeReducer[args.RuducerId] = 0
					c.siminFiles[k] = 1
					c.reducejob[args.RuducerId] = k
					reply.Sim = k
					break
				}
			}
		}
		c.mutex.Unlock()
	} else if args.Applyorfinish == 1 {
		c.mutex.Lock()
		// fmt.Println("~", args.RuducerId, c.reducer[args.RuducerId])
		// fmt.Println("************")
		if c.reducer[args.RuducerId] != 3 {
			c.reducer[args.RuducerId] = 2
			// fmt.Println(args.Sim)
			c.siminFiles[args.Sim] = 2
			delete(c.activeReducer, args.RuducerId)
			delete(c.reducejob, args.RuducerId)
			reply.Reserve = true
		} else {
			reply.Reserve = false
		}
		c.mutex.Unlock()
	} else if args.Applyorfinish == 2{
		c.mutex.Lock()
		for _, v := range c.siminFiles{
			if v != 2{
				reply.Ret = false
				c.mutex.Unlock()
				return nil
			}
		}
		reply.Ret = true
		c.mutex.Unlock()
	}
	return nil
}

// func (c *Coordinator) Finished(args *ReduceArgs, reply *ReduceReply) error {

// }
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
	// ret := false

	c.mutex.Lock()
	// fmt.Println("siminFiles   ",c.siminFiles)
	// fmt.Println("reducer      ",c.reducer)
	// fmt.Println("activeReducer",c.activeReducer)
	// fmt.Println("reducejob    ",c.reducejob)
	// fmt.Println("Files   ",c.files)
	// fmt.Println("mapper      ",c.mapper)
	// fmt.Println("activeMapper",c.activeMapper)
	// fmt.Println("mapjob    ",c.mapjob)

	for k, v := range c.activeMapper {
		if v+1 >= c.interval {
			c.mapper[k] = 3
			var i string
			var j string
			for i, j = range c.mapjob {
				if i == k {
					for x := range c.files {
						if x == j {
							c.files[x] = 0
						}
					}
				}
			}
			delete(c.activeMapper, k)
			delete(c.mapjob, k)
		}
		c.activeMapper[k]++
	}

	for k, v := range c.activeReducer {
		if v+1 >= c.interval {
			c.reducer[k] = 3
			var i string
			var j uint8
			for i, j = range c.reducejob {
				if i == k {
					for x := range c.siminFiles {
						if x == j {
							c.siminFiles[x] = 0
						}
					}
				}
			}
			delete(c.activeReducer, k)
			delete(c.reducejob, k)
		}
		c.activeReducer[k]++
	}
	// fmt.Println("Files   ", c.files)
	// fmt.Println("mapper      ", c.mapper)
	// fmt.Println("activeMapper", c.activeMapper)
	// fmt.Println("mapjob    ", c.mapjob)

	// fmt.Println("siminFiles   ",c.siminFiles)
	// fmt.Println("reducer      ",c.reducer)
	// fmt.Println("activeReducer",c.activeReducer)
	// fmt.Println("reducejob    ",c.reducejob)

	for _, v := range c.files {
		if v != 2 {
			c.mutex.Unlock()
			return false
		}
	}
	for _, v := range c.siminFiles {
		if v != 2 {
			c.mutex.Unlock()
			return false
		}
	}

	// for _, v := range c.intermediateFiles {
	// 	if v != 2 {
	// 		c.mutex.Unlock()
	// 		return false
	// 	}
	// }
	c.mutex.Unlock()
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = make(map[string]int)
	// c.intermediateFiles = make(map[string]int)
	c.mapper = make(map[string]int)
	c.mapjob = make(map[string]string)
	c.activeMapper = make(map[string]int)

	c.reducer = make(map[string]int)
	c.siminFiles = make(map[uint8]int)
	c.activeReducer = make(map[string]int)
	c.reducejob = make(map[string]uint8)

	c.maptaskFinished = 0
	c.reducetaskFinished = 0
	c.interval = 10
	// c.time = 0
	for _, filename := range files {
		c.files[filename] = 0
	}

	c.server()
	return &c
}

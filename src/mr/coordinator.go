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
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files              map[string]int
	intermediateFiles  map[string]int
	mapper             map[string]int
	reducer            map[string]int
	outfiles           map[string]int
	maptask            int
	reducetask         int
	maptaskFinished    int
	reducetaskFinished int
	interval           int
	time               int
	mutex              sync.Mutex
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
		c.mutex.Lock()
		c.mapper[args.MapperId] = 0
		for filename = range c.files {
			if c.files[filename] == 0 {
				reply.Filename = filename
				c.files[filename] = 1
				c.mapper[args.MapperId] = 1
				c.maptask++
				break
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
			c.maptask--
			for k, v := range args.IntermediateFilename {
				c.intermediateFiles[k] = v
			}
		}
		//+* fmt.Println(args.MapperId)
		//+* fmt.Println(c.files)
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
	if args.Applyorfinish == 0 {
		var filename string
		c.mutex.Lock()
		c.reducer[args.RuducerId] = 0
		for filename = range c.intermediateFiles {
			if c.intermediateFiles[filename] != 0 {
				continue
			}
			if filename[len(filename)-1] == args.RuducerId[len(args.RuducerId)-1] {
				reply.Filesname = append(reply.Filesname, filename)
				c.intermediateFiles[filename] = 1
			}
			c.reducer[args.RuducerId] = 1
		}
		c.mutex.Unlock()
	} else if args.Applyorfinish == 1 {
		c.mutex.Lock()
		if c.reducer[args.RuducerId] != 3 {
			c.reducetask--
			c.reducer[args.RuducerId] = 2
			for _, i := range args.FinishedFile {
				c.intermediateFiles[i] = 2
			}
			reply.reserve = true
		}
		reply.reserve = false
		fmt.Println(args.RuducerId)
		fmt.Println(c.reducer)
		//+* fmt.Println(c.intermediateFiles)
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
	c.time++
	if c.time >= c.interval {
		fmt.Println(c.intermediateFiles)
		fmt.Println(c.reducer)
		for k, v := range c.files {
			if v != 2 {
				c.files[k] = 0
			}
		}
		for k, v := range c.mapper {
			if v == 1 {
				c.mapper[k] = 3
			}
		}

		for k, v := range c.reducer {
			if v == 1 {
				// fmt.Println("**\n",)
				var idleReduce byte
				var k1 string
				var v1 int
				for k1, v1 = range c.reducer {
					if v1 == 2 {
						idleReduce = k1[len(k1)-1]
					}
				}
				fmt.Println("--  ", k1)
				c.reducer[k] = 3
				for i := range c.intermediateFiles {
					fmt.Println("==   ", i)
					fmt.Println("++   ", k)
					if i[len(i)-1] == k[len(k)-1] {
						// fmt.Println(i, idleReduce-'0')
						new := fmt.Sprintf("%v%v", i[0:len(i)-1], idleReduce-'0')
						fmt.Println(new)
						delete(c.intermediateFiles, i)
						// c.intermediateFiles[i] = -1
						c.intermediateFiles[new] = 0
						c.reducer[k1] = 1
					}
				}
			}
		}
		// for i, v := range c.intermediateFiles {
		// 	if v == 1 {
		// 		// fmt.Println(i, idleReduce)
		// 		// new := fmt.Sprintf("%v%v", i[0:len(i)-1], idleReduce-'0')
		// 		// fmt.Println(new)
		// 		// delete(c.intermediateFiles, i)
		// 		// c.intermediateFiles[new] = 0
		// 		c.intermediateFiles[i] = 0
		// 	}
		// }
		fmt.Println(c.intermediateFiles)
		fmt.Println(c.reducer)
		c.time = 0
		time.Sleep(5*time.Minute)
	}
	if len(c.mapper) == 0 || len(c.reducer) == 0 {
		c.mutex.Unlock()
		return false
	}
	// for _, j := range c.mapper{
	// 	if j == 0{
	// 		c.mutex.Unlock()
	// 		return false
	// 	}
	// }
	// for _, j := range c.reducer{
	// 	if j == 0{
	// 		c.mutex.Unlock()
	// 		return false
	// 	}
	// }

	// fmt.Println(c.files)
	// fmt.Println(c.intermediateFiles)
	for _, v := range c.files {
		if v != 2 {
			c.mutex.Unlock()
			return false
		}
	}

	for _, v := range c.intermediateFiles {
		if v != 2 {
			c.mutex.Unlock()
			return false
		}
	}
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
	c.intermediateFiles = make(map[string]int)
	c.mapper = make(map[string]int)
	c.reducer = make(map[string]int)
	c.outfiles = make(map[string]int)
	c.maptask = 0
	c.reducetask = nReduce
	c.maptaskFinished = 0
	c.reducetaskFinished = 0
	c.interval = 10
	c.time = 0
	for _, filename := range files {
		c.files[filename] = 0
		c.maptask++
	}

	c.server()
	return &c
}

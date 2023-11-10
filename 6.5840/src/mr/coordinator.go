package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type task struct {
	Id           int
	Filename     string
	WorkerStatus string
	TaskType     string
	Filepath     string
	NReduce      int
	TimeStamp    time.Time
}

type Coordinator struct {
	// Your definitions here.
	mapTaskList    []task
	reduceTaskList []task
	mapNum         int
	filenameList   []string
	lock           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) Allocate(args *ExampleArgs, reply *ExampleReply) error {

	//if args.TaskID != -1 {
	//	fmt.Printf(" master receive task %d, status: %s \n", args.TaskID, args.Status)
	//}

	if args.Status == "Finish" {
		c.lock.Lock()
		defer c.lock.Unlock()
		if args.TaskID >= len(c.mapTaskList) {
			c.reduceTaskList[args.TaskID-len(c.mapTaskList)].WorkerStatus = "Done"
		} else {
			c.mapTaskList[args.TaskID].WorkerStatus = "Done"
		}

		return nil
	}

	if args.Status == "Wrong" {
		c.lock.Lock()
		defer c.lock.Unlock()

		if args.TaskID >= len(c.mapTaskList) {
			c.reduceTaskList[args.TaskID-len(c.mapTaskList)].WorkerStatus = ""
		} else {
			c.mapTaskList[args.TaskID].WorkerStatus = ""
		}
		return nil
	}

	if c.isNoTaskLeft(append(c.mapTaskList, c.reduceTaskList...)) {
		reply.Close = true
		return nil
	}

	// should first map then reduce!!!
	c.lock.Lock()
	for i := 0; i < len(c.mapTaskList); i++ {
		if c.isTaskShouldBeAllocate(&c.mapTaskList[i]) {
			c.mapTaskList[i].WorkerStatus = "Run"
			c.mapTaskList[i].TimeStamp = time.Now()
			reply.Status = "Run"
			reply.FilenameList = c.filenameList
			reply.TaskReply = c.mapTaskList[i]
			c.lock.Unlock()
			return nil
		}
	}

	if c.isNoTaskLeft(c.mapTaskList) {
		for i := 0; i < len(c.reduceTaskList); i++ {
			if c.isTaskShouldBeAllocate(&c.reduceTaskList[i]) {
				c.reduceTaskList[i].WorkerStatus = "Run"
				c.reduceTaskList[i].TimeStamp = time.Now()
				reply.Status = "Run"
				reply.FilenameList = c.filenameList
				reply.TaskReply = c.reduceTaskList[i]
				c.lock.Unlock()
				return nil
			}
		}
	}

	c.lock.Unlock()

	return nil
}

func (c *Coordinator) isNoTaskLeft(list []task) bool {
	for i := 0; i < len(list); i++ {
		if list[i].WorkerStatus != "Done" {
			return false
		}
	}
	return true
}

func (c *Coordinator) isTaskShouldBeAllocate(t *task) bool {
	if t.WorkerStatus == "" {
		return true
	}

	// heartbeat
	endTime := time.Now()
	timeDiff := endTime.Sub(t.TimeStamp).Seconds()
	if t.WorkerStatus != "Done" && timeDiff > 10 {
		t.WorkerStatus = ""
		return true
	}

	return false
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
	for i := range c.mapTaskList {
		if c.mapTaskList[i].WorkerStatus != "Done" {
			return false
		}
	}

	for i := range c.reduceTaskList {
		if c.reduceTaskList[i].WorkerStatus != "Done" {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// 分发files任务
	//c.workerMap = make(map[string]string)
	c.mapTaskList = []task{}
	c.reduceTaskList = []task{}

	for i := 0; i < len(files); i++ {
		filename := files[i]
		mapTask := task{Id: i, Filename: filename, TaskType: "map", Filepath: strconv.Itoa(i), NReduce: nReduce}
		c.mapTaskList = append(c.mapTaskList, mapTask)
		c.mapNum = i + 1
		c.filenameList = append(c.filenameList, strconv.Itoa(i))
	}

	for i := 0; i < nReduce; i++ {
		reduceTask := task{Id: i + c.mapNum, TaskType: "reduce", Filepath: strconv.Itoa(i), NReduce: nReduce}
		c.reduceTaskList = append(c.reduceTaskList, reduceTask)
	}

	fmt.Println("begin>>>>>>>")
	c.server()
	return &c
}

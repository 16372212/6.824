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
)

type task struct {
	Id           int
	Filename     string
	WorkerPort   string
	WorkerStatus string
	TaskType     string
	Filepath     string
	NReduce      int
}

type Coordinator struct {
	// Your definitions here.
	taskList     []task
	mapNum       int
	filenameList []string
	lock         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) Allocate(args *ExampleArgs, reply *ExampleReply) error {

	fmt.Printf(" task:%d, status:%s\n", args.TaskID, args.Status)
	if args.Status == "Finish" {
		c.lock.Lock()
		defer c.lock.Unlock()
		c.taskList[args.TaskID].WorkerStatus = "Done"
		return nil
	}

	if args.Status == "Wrong" {
		c.lock.Lock()
		defer c.lock.Unlock()
		c.taskList[args.TaskID].WorkerPort = ""
		return nil
	}

	if c.isNoTaskLeft() {
		reply.Close = true
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	for i := 0; i < len(c.taskList); i++ {
		if c.taskList[i].WorkerPort == "" {
			c.taskList[i].WorkerPort = args.Port
			c.taskList[i].WorkerStatus = "Run"
			reply.Status = "Run"
			reply.FilenameList = c.filenameList
			reply.TaskReply = c.taskList[i]
			return nil
		}
	}

	return nil
}

func (c *Coordinator) isNoTaskLeft() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	for i := 0; i < len(c.taskList); i++ {
		if c.taskList[i].WorkerPort == "" || c.taskList[i].WorkerStatus != "Done" {
			return false
		}
	}
	return true
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
	// Your code here.
	// worker状态都是停止
	for i := range c.taskList {
		taskTemp := c.taskList[i]
		if taskTemp.WorkerStatus != "Done" {
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
	c.taskList = []task{}

	for i := 0; i < len(files); i++ {
		filename := files[i]
		mapTask := task{Id: i, Filename: filename, TaskType: "map", Filepath: strconv.Itoa(i), NReduce: nReduce}
		c.taskList = append(c.taskList, mapTask)
		c.mapNum = i + 1
		c.filenameList = append(c.filenameList, strconv.Itoa(i))
	}

	for i := 0; i < nReduce; i++ {
		reduceTask := task{Id: i + c.mapNum, TaskType: "reduce", Filepath: strconv.Itoa(i), NReduce: nReduce}
		c.taskList = append(c.taskList, reduceTask)
	}

	fmt.Println("begin>>>>>>>")
	c.server()
	return &c
}

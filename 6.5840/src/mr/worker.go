package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

var mrMap func(string, string) []KeyValue
var mrReduce func(string, []string) string

//for sorting by key.
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

	// Your worker implementation here.
	mrMap = mapf
	mrReduce = reducef
	for {
		status := Run()
		if status == "Close" {
			break
		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func Run() string {

	args := ExampleArgs{}
	args.Status = "Ready"
	args.Port = "1"
	args.TaskID = -1

	// declare a reply structure.
	reply := ExampleReply{}
	reply.Close = false
	ok := call("Coordinator.Allocate", &args, &reply)

	if ok {
		//fmt.Printf("---worker: get task %d \n", reply.TaskReply.Id)
		if reply.Close {
			return "Close"
		}
		if reply.TaskReply.TaskType == "map" {
			dealMap(reply.TaskReply)
		} else if reply.TaskReply.TaskType == "reduce" {
			dealReduce(reply.FilenameList, reply.TaskReply)
		}
		return reply.Status
	} else {
		fmt.Printf("[worker] call failed!\n")
		return "Wrong"
	}
}

func dealMap(taskReply task) {
	filename := taskReply.Filename
	file, err := os.Open(filename)

	if err != nil {
		SendBack(taskReply.Id, "Wrong")
		log.Println("map: cannot open %+v", filename)
		return
		//log.Fatalf("cannot open %+v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		SendBack(taskReply.Id, "Wrong")
		//log.Println("cannot read %v", filename)
		//return
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mrMap(filename, string(content))
	// todo sort.Sort(ByKey(kva))

	// bucket
	bucket := map[int][]KeyValue{}
	i := 0
	for i < len(kva) {
		key := kva[i].Key
		bucketID := ihash(key) % taskReply.NReduce
		bucket[bucketID] = append(bucket[bucketID], kva[i])
		i += 1
	}

	// write
	for bucketID, _ := range bucket {
		oname := "mr-" + taskReply.Filepath + "-" + strconv.Itoa(bucketID)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)

		for _, kva := range bucket[bucketID] {
			err := enc.Encode(&kva)
			if err != nil {
				SendBack(taskReply.Id, "Wrong")
				//log.Println("cannot write into %v", oname)
				log.Fatalf("cannot write into %v", oname)
			}
		}
		defer ofile.Close()
		bucketID += 1
	}

	// call back
	SendBack(taskReply.Id, "Finish")
}

func SendBack(taskID int, status string) {
	args := ExampleArgs{}
	args.Status = status
	args.Port = "1"
	args.TaskID = taskID

	// declare a reply structure.
	reply := ExampleReply{}
	reply.Close = false
	ok := call("Coordinator.Allocate", &args, &reply)
	for !ok {
		call("Coordinator.Allocate", &args, &reply)
	}
}

func dealReduce(filenameList []string, taskReply task) {
	// shuffle
	// all Filename:
	intermediate := []KeyValue{}

	for i := 0; i < len(filenameList); i++ {
		filename := filenameList[i]
		iname := "mr-" + filename + "-" + taskReply.Filepath
		ifile, err := os.Open(iname)
		if err != nil {
			// 说明可能这个文件就不存在呢
			if os.IsExist(err) {
				SendBack(taskReply.Id, "Wrong")
				//log.Println("cannot open file: %s, %v", iname, ifile)
				//return
				log.Fatalf("reduce: cannot open file: %s, %v", iname, ifile)
			} else {
				continue
			}
		}
		// read, for each key
		decoder := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			err = decoder.Decode(&kv)
			if err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ifile.Close()
	}

	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(taskReply.Id-len(filenameList))

	// to stop from this reduce work fail, use temp file
	ofile, _ := ioutil.TempFile("", oname+"*")
	defer ofile.Close()

	// put all same key together to map
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := mrReduce(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	os.Rename(ofile.Name(), oname)

	// call back
	SendBack(taskReply.Id, "Finish")
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {

	//c, err := rpc.DialHTTP("tcp", ":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		//fmt.Println("dialing:", err)
		//return false
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Printf("call failed, err: %v\n", err)
	return false
}

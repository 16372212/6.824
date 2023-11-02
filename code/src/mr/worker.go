package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

	fmt.Printf("run Worker\n")

	// Your worker implementation here.
	mrMap = mapf
	mrReduce = reducef
	for {
		status := Run()
		if status == "Close" || status == "Wrong" {
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

	// declare a reply structure.
	reply := ExampleReply{}
	reply.Close = false
	ok := call("Coordinator.Allocate", &args, &reply)

	if ok {
		if reply.Close {
			return "Close"
		}
		if reply.TaskReply.TaskType == "map" {
			dealMap(reply.TaskReply)
		} else {
			dealReduce(reply.FilenameList, reply.TaskReply)
		}
		return reply.Status
	} else {
		fmt.Printf("[worker] call failed!\n")
		return "Wrong"
	}
}

func dealMap(taskReply task) {
	fmt.Printf("task %d, dealing map %+v \n", taskReply.Id, taskReply)
	filename := taskReply.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %+v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mrMap(filename, string(content))
	fmt.Printf(" run mrMap\n")
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
	bucketID := 0
	for bucketID < len(bucket) {
		oname := "mr-" + taskReply.Filepath + "-" + strconv.Itoa(bucketID)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)

		for _, kva := range bucket[bucketID] {
			err := enc.Encode(&kva)
			if err != nil {
				log.Fatalf("cannot write into %v", oname)
			}
		}
		ofile.Close()
		bucketID += 1
	}

	fmt.Printf(" writing..., len(kva):%d \n", len(kva))

	// call back
	SendFinish(taskReply.Id)
}

func SendFinish(taskID int) {
	args := ExampleArgs{}
	args.Status = "Finish"
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
	fmt.Printf("task %d, dealing reduce %+v \n", taskReply.Id, taskReply)
	intermediate := []KeyValue{}

	for i := 0; i < len(filenameList); i++ {
		filename := filenameList[i]
		iname := "mr-" + filename + "-" + taskReply.Filepath
		ifile, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open file: %s, %v", iname, ifile)
		}
		// read, for each key
		defer ifile.Close()

		decoder := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
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
	SendFinish(taskReply.Id)
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

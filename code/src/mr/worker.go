package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
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

	//gob.Register(ExampleArgs{})
	//gob.Register(ExampleReply{})

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
	sort.Sort(ByKey(kva))
	// write

	i := 0
	fmt.Printf(" writing..., len(kva):%d \n", len(kva))
	for i < len(kva) {
		j := i + 1
		var key string
		for j < len(kva) && kva[j].Key == kva[i].Key {
			key = kva[i].Key
			j++
		}

		intermediate := []KeyValue{}
		for k := i; k < j; k++ {
			intermediate = append(intermediate, kva[k])
		}

		filepath := ihash(key) % taskReply.NReduce
		oname := "mr-out-" + taskReply.Filepath + "-" + strconv.Itoa(filepath)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		err := enc.Encode(&intermediate)
		if err != nil {
			log.Fatalf("cannot write into %v", oname)
		}
		//os.Rename(ofile.Name(), oname)
		//fmt.Fprintf(ofile, "%+v \n", kva[i].Key, values)
		i = j
		ofile.Close()
	}

	// call back

}

func dealReduce(filenameList []string, taskReply task) {
	// shuffle
	// all Filename:
	fmt.Printf("task %d, dealing reduce %+v \n", taskReply.Id, taskReply)
	key := ""
	values := []string{}

	for i := 0; i < len(filenameList); i++ {
		filename := filenameList[i]
		oname := filename + taskReply.Filepath
		ifile, _ := os.Open(oname)
		// read, for each key
		defer ifile.Close()

		scanner := bufio.NewScanner(ifile)

		// Read and process each line from the file.
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, " ")
			if len(parts) == 2 {
				key = parts[0]
				values = append(values, parts[1])
			}
		}
	}

	output := mrReduce(key, values)
	oname := "mr-out-" + taskReply.Filepath
	ofile, _ := os.Create(oname)
	fmt.Fprintf(ofile, "%v %v\n", key, output)
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

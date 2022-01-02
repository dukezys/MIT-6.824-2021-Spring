package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//var mapfc func(string, string) []KeyValue
//var reducefc func(string, []string) string

//
// main/mrworker.go calls this function.
//

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func handleMapTask(mapf func(string, string) []KeyValue, taskStatus *TaskStatus) {
	task := &taskStatus.Task
	filename := task.Filename[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	//call real mapf
	kva := mapf(filename, string(content))

	//write result into real files
	//var filePtr *os.File
	//for _, kv := range kva {
	//	key := kv.Key
	//	partition := ihash(key) % task.reducer
	//	interfile := "./mr-" + strconv.Itoa(partition) + ".json"
	//	if !PathExist(interfile) {
	//		filePtr, _ = os.Create(interfile)
	//	} else {
	//		filePtr, _ = os.OpenFile(interfile, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	//	}
	//	encoder := json.NewEncoder(filePtr)
	//	err := encoder.Encode(kv)
	//	if err != nil {
	//		fmt.Println("error in write file in map")
	//	}
	//	filePtr.Close()
	//	task.Result[interfile] = true
	//}

	//temp file
	prefix := "mr-" + strconv.Itoa(task.TaskId) + "-"
	interfiles := make([]*os.File, task.Reducer)
	jsonEncoders := make([]*json.Encoder, task.Reducer)
	for i := 0; i < task.Reducer; i++ {
		interfiles[i], _ = ioutil.TempFile(".", prefix)
		jsonEncoders[i] = json.NewEncoder(interfiles[i])
	}
	for _, kv := range kva {
		key := kv.Key
		partition := ihash(key) % task.Reducer
		encoder := jsonEncoders[partition]
		err := encoder.Encode(&kv)
		if err != nil {
			fmt.Println("Temp file encode failed")
		}
	}
	for index, tempFile := range interfiles {
		realPath := prefix + strconv.Itoa(index)
		currPath := filepath.Join(tempFile.Name())
		os.Rename(currPath, realPath)
		task.Result[realPath] = true
	}

	task.Completed = true
	taskStatus.State = FINISHED
	CallTaskCompleted(taskStatus)
}

func handleReduceTask(reducef func(string, []string) string, taskStatus *TaskStatus) {
	task := &taskStatus.Task
	var kva []KeyValue
	for _, filepath := range task.Filename {
		filePtr, err := os.Open(filepath)
		if err != nil {
			fmt.Println("error when open file in reduce task")
		}
		dec := json.NewDecoder(filePtr)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		filePtr.Close()
	}
	sort.Sort(ByKey(kva))

	oname := "./mr-out-" + strconv.Itoa(task.TaskId)
	ofile, _ := ioutil.TempFile(".", "mr-*")

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	os.Rename(filepath.Join(ofile.Name()), oname)
	ofile.Close()

	task.Completed = true
	taskStatus.State = FINISHED
	CallTaskCompleted(taskStatus)
}

func PathExist(_path string) bool {
	_, err := os.Stat(_path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply := CallTaskHandler()
		ts := reply.TaskStatus
		if ts.State == FINISHED {
			return
		}
		if ts.State == WAITING {
			time.Sleep(time.Duration(1) * time.Second)
			continue
		}
		fmt.Printf("CLINET: get %v task %v\n", ts.Task.TaskType, ts.Task.TaskId)
		if ts.Task.TaskType == "map" {
			handleMapTask(mapf, &ts)
			continue
		}
		handleReduceTask(reducef, &ts)
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {
//	// declare an argument structure.
//	args := ExampleArgs{}
//	// fill in the argument(s).
//	args.X = 99
//	// declare a reply structure.
//	reply := ExampleReply{}
//	// send the RPC request, wait for the reply.
//	call("Coordinator.Example", &args, &reply)
//	// reply.Y should be 100.
//	fmt.Printf("reply.Y %v\n", reply.Y)
//}
func CallTaskHandler() TaskReply {
	// declare an argument structure.
	args := TaskArgs{}
	// fill in the argument(s).
	// declare a reply structure.
	reply := TaskReply{}
	// send the RPC request, wait for the reply.
	call("Coordinator.AllocateTask", &args, &reply)
	return reply
	// reply.Y should be 100.
	//fmt.Printf("reply.Y %v\n", reply.filenames)
}

func CallTaskCompleted(taskStatus *TaskStatus) {
	reply := TaskReply{}
	args := TaskReply{
		TaskStatus: *taskStatus,
	}
	if taskStatus.OverTime() {
		return
	}
	call("Coordinator.TaskCompleteHandler", &args, &reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns trerue.
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

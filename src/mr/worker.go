package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := CallGetTask()
		if reply.Done {
			// fmt.Println("All jobs completed successfully. Exiting.")
			break
		}
		performTaskArgs := PerformTaskArgs{}
		performTaskReply := PerformTaskReply{}
		if reply.MapTask != nil {
			mapTaskResponse := performMapTask(mapf, reply.MapTask)
			performTaskArgs.MapTaskResponse = mapTaskResponse
		} else if reply.ReduceTask != nil {
			reduceTaskResponse := performReduceTask(reducef, reply.ReduceTask)
			performTaskArgs.ReduceTaskResponse = reduceTaskResponse
		} else {
			continue
		}
		call("Coordinator.PerformTaskCb", &performTaskArgs, &performTaskReply)
	}
}

func CallGetTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Coordinator.GetTask", &args, &reply)
	return reply
}

func performMapTask(mapf func(string, string) []KeyValue, task *MapTask) *MapTaskResponse {
	filename := task.InputFile
	nReducer := task.NReducer
	intermediateFiles := make([]string, nReducer)

	resp := &MapTaskResponse{
		InputFile:         filename,
		IntermediateFiles: intermediateFiles,
		Error:             true,
	}

	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Cannot open file %v", filename)
		return resp
	}
	content, err := io.ReadAll(file)
	if err != nil {
		fmt.Printf("Cannot read file %v", filename)
		return resp
	}
	file.Close()

	kva := mapf(filename, string(content))
	partitionedKva := make([][]KeyValue, nReducer)
	for _, kv := range kva {
		partitionKey := ihash(kv.Key) % nReducer
		partitionedKva[partitionKey] = append(partitionedKva[partitionKey], kv)
	}

	for i := 0; i < nReducer; i++ {
		intermediateFilename := fmt.Sprintf("mr-%d-%d", task.MapTaskNumber, i)
		intermediateFiles[i] = intermediateFilename
		iFile, err := os.Create(intermediateFilename)
		if err != nil {
			fmt.Printf("Cannot create file %v", intermediateFilename)
			return resp
		}

		enc := json.NewEncoder(iFile)
		if err := enc.Encode(partitionedKva[i]); err != nil {
			fmt.Printf("Error encoding JSON: %v", err)
			return resp
		}

		iFile.Close()
	}

	resp.IntermediateFiles = intermediateFiles
	resp.Error = false
	return resp
}

func performReduceTask(reducef func(string, []string) string, task *ReduceTask) *ReduceTaskResponse {
	resp := &ReduceTaskResponse{
		ReduceTaskNumber: task.ReduceTaskNumber,
		Error:            true,
	}
	files := task.IntermediateFiles

	intermediate := []KeyValue{}

	for _, filename := range files {
		// kv, err := ioutil.

		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("Cannot open file %v", filename)
			return resp
		}
		defer file.Close()

		var kva []KeyValue
		dec := json.NewDecoder(file)
		if err := dec.Decode(&kva); err != nil {
			fmt.Printf("Error decoding JSON: %v", err)
			return resp
		}

		intermediate = append(intermediate, kva...)

	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.ReduceTaskNumber)
	tempFile, err := os.CreateTemp(".", oname)
	if err != nil {
		fmt.Printf("Cannot create temporary file %v", oname)
		return resp
	}
	defer tempFile.Close()

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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(tempFile.Name(), oname)

	resp.Error = false
	return resp
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

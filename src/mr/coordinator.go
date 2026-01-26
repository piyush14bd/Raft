package mr

import (
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	Pending TaskStatus = iota
	InProgress
	Completed
)

const MaxTimeout int = 10

type Task struct {
	startTime int64
	status    TaskStatus
}

type Coordinator struct {
	// Your definitions here.
	mapTaskNumber     int
	mapTasks          map[string]Task
	reduceTasks       map[int]Task
	intermediateFiles map[int][]string
	nReduce           int
	mu                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	mapTask, allMapTasksComplete := c.getMapTask()
	reply.MapTask = mapTask
	if allMapTasksComplete {
		reply.ReduceTask = c.getReduceTask()
	}
	reply.Done = c.Done()
	return nil
}

// func (c *Coordinator) mapTasksCompleted() bool {
// 	for _, value := range c.mapTasks {
// 		if value.status != Completed {
// 			return false
// 		}
// 	}
// 	return true
// }

func (c *Coordinator) getMapTask() (*MapTask, bool) {
	var mapTask *MapTask = nil
	var allMapTasksComplete = true

	for key, value := range c.mapTasks {
		if value.status != Completed {
			allMapTasksComplete = false
		}

		if value.status == Pending {
			mapTask = &MapTask{
				InputFile:     key,
				MapTaskNumber: c.mapTaskNumber,
				NReducer:      c.nReduce,
			}
			c.mapTaskNumber++
			c.mapTasks[key] = Task{startTime: time.Now().Unix(), status: InProgress}
			break
		}
	}
	return mapTask, allMapTasksComplete
}

func (c *Coordinator) getReduceTask() *ReduceTask {
	var reduceTask *ReduceTask = nil
	for key, value := range c.reduceTasks {
		if value.status == Pending {
			reduceTask = &ReduceTask{
				IntermediateFiles: c.intermediateFiles[key],
				ReduceTaskNumber:  key,
			}
			c.reduceTasks[key] = Task{startTime: time.Now().Unix(), status: InProgress}
			break
		}
	}
	return reduceTask
}

func (c *Coordinator) PerformTaskCb(args *PerformTaskArgs, reply *PerformTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	mapTaskResponse := args.MapTaskResponse
	reduceTaskResponse := args.ReduceTaskResponse

	if mapTaskResponse != nil {
		if mapTaskResponse.Error {
			c.mapTasks[mapTaskResponse.InputFile] = Task{startTime: math.MinInt64, status: Pending}
		} else {
			c.mapTasks[mapTaskResponse.InputFile] = Task{startTime: math.MinInt64, status: Completed}
			for i := 0; i < c.nReduce; i++ {
				c.intermediateFiles[i] = append(c.intermediateFiles[i], mapTaskResponse.IntermediateFiles[i])
			}
		}
	}
	if reduceTaskResponse != nil {
		if reduceTaskResponse.Error {
			c.reduceTasks[reduceTaskResponse.ReduceTaskNumber] = Task{startTime: math.MinInt64, status: Pending}
		} else {
			c.reduceTasks[reduceTaskResponse.ReduceTaskNumber] = Task{startTime: math.MinInt64, status: Completed}
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _, value := range c.mapTasks {
		if value.status != Completed {
			return false
		}
	}

	for _, value := range c.reduceTasks {
		if value.status != Completed {
			return false
		}
	}

	return true
}

func (c *Coordinator) CheckForStaleTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, value := range c.mapTasks {
		currentTime := time.Now().Unix()
		if value.status == InProgress && currentTime > (value.startTime+10) {
			c.mapTasks[key] = Task{startTime: math.MinInt64, status: Pending}
		}
	}

	for key, value := range c.reduceTasks {
		currentTime := time.Now().Unix()
		if value.status == InProgress && currentTime > (value.startTime+10) {
			c.reduceTasks[key] = Task{startTime: math.MinInt64, status: Pending}
		}
	}
}

func (c *Coordinator) StartTicker() {
	ticker := time.NewTicker(1 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				if c.Done() {
					return
				}
				c.CheckForStaleTasks()
			}
		}
	}()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mapTaskNumber = 0
	c.mapTasks = make(map[string]Task)
	for _, file := range files {
		c.mapTasks[file] = Task{startTime: math.MinInt64, status: Pending}
	}

	c.reduceTasks = make(map[int]Task)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{startTime: math.MinInt64, status: Pending}
	}

	c.intermediateFiles = make(map[int][]string)

	c.nReduce = nReduce

	c.StartTicker()

	c.server()
	return &c
}

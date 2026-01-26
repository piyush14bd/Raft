package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

type MapTask struct {
	InputFile     string
	MapTaskNumber int
	NReducer      int
}

type ReduceTask struct {
	IntermediateFiles []string
	ReduceTaskNumber  int
}

type GetTaskArgs struct {
}

type GetTaskReply struct {
	MapTask    *MapTask
	ReduceTask *ReduceTask
	Done       bool
}

type MapTaskResponse struct {
	InputFile         string
	IntermediateFiles []string
	Error             bool
}

type ReduceTaskResponse struct {
	ReduceTaskNumber int
	Error            bool
}

type PerformTaskArgs struct {
	MapTaskResponse    *MapTaskResponse
	ReduceTaskResponse *ReduceTaskResponse
}

type PerformTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

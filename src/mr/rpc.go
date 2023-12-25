package mr

import (
	"os"
	"strconv"
	"time"
)

type Task int

const (
	Map Task = iota
	Reduce
	Hold
	Exit
)

type State int

const (
	Assigned State = iota
	Unassigned
	Completed
	Pending
)

type MapReduceTask struct {
	Tasktype    Task
	State       State
	StartTime   time.Time
	Index       int
	InputFiles  []string
	OutputFiles []string
}
type CoordinatorReply struct {
	// 1. the task to be assigned
	Task         MapReduceTask
	TaskNumber   int
	ReduceTaskNo int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

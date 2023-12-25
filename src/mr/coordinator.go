package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	inputFiles        []string
	nReduce           int
	mapTasks          []MapReduceTask
	reduceTasks       []MapReduceTask
	mapDone           int
	reduceDone        int
	allMapComplete    bool
	allReduceComplete bool
	mutex             sync.Mutex
}

func (c *Coordinator) SendJobCompleteNotification(arg *CoordinatorReply, reply *CoordinatorReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// We will mark the task as complete
	if arg.Task.Tasktype == Map {
		// fmt.Println("Corrdinator.go notify complete  after map task arg task vlaue is ",arg.Task)
		c.mapTasks[arg.TaskNumber] = arg.Task
	} else if arg.Task.Tasktype == Reduce {
		// fmt.Println("Coordinator.go notify complete after completing reducer tasks", arg.Task)
		c.reduceTasks[arg.TaskNumber] = arg.Task
	}

	return nil
}

func (c *Coordinator) SendJobRequest(args *CoordinatorReply, reply *CoordinatorReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Check if all the map tasks are done
	if c.mapDone < len(c.inputFiles) {
		reply.TaskNumber = c.mapDone
		reply.Task.State = Assigned
		c.mapTasks[c.mapDone].StartTime = time.Now()
		reply.Task = c.mapTasks[c.mapDone]
		reply.ReduceTaskNo = c.nReduce

		c.mapDone++
		return nil
	}

	// Before starting with reduce we need to ensure that the all map tasks are completed,if not reassign map tasks if the worker is stalled
	if !c.allMapComplete {
		for index, mapTask := range c.mapTasks {
			if mapTask.State != Completed {
				if time.Since(mapTask.StartTime) > 10*time.Second {
					reply.Task = c.mapTasks[index]
					reply.TaskNumber = index
					c.mapTasks[index].StartTime = time.Now()
					reply.Task.State = Assigned
					reply.ReduceTaskNo = c.nReduce
					return nil
				} else {
					reply.Task.Tasktype = Hold
					return nil
				}
			}
		}
		c.allMapComplete = true
	}

	// Check if all reduce tasks are done
	if c.reduceDone < c.nReduce {
		reply.TaskNumber = c.reduceDone
		reply.Task.State = Assigned
		c.reduceTasks[c.reduceDone].StartTime = time.Now()
		reply.Task = c.reduceTasks[c.reduceDone]
		reply.ReduceTaskNo = c.nReduce

		c.reduceDone++
		return nil
	}

	// Check if all the reduce tasks are completed if not ressign reduce task if any worker is stalled
	if !c.allReduceComplete {
		for index, reduceTask := range c.reduceTasks {
		if reduceTask.State != Completed {
				if time.Since(reduceTask.StartTime) > 10*time.Second {
					reply.TaskNumber = index
					reply.ReduceTaskNo = c.nReduce
					c.reduceTasks[index].StartTime = time.Now()
					reply.Task = c.reduceTasks[index]
					reply.Task.State = Assigned
					return nil
				} else {
					reply.Task.Tasktype = Hold
					return nil
				}
			}
		}
		c.allReduceComplete = true
	}

	reply.Task.State = State(Exit)
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
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.allMapComplete && c.allReduceComplete
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFiles:        files,
		nReduce:           nReduce,
		mapTasks:          make([]MapReduceTask, len(files)),
		reduceTasks:       make([]MapReduceTask, nReduce),
		mapDone:           0,
		reduceDone:        0,
		mutex:             sync.Mutex{},
		allMapComplete:    false,
		allReduceComplete: false,
	}

	// Start Map tasks
	for i := range c.mapTasks {
		c.mapTasks[i] = MapReduceTask{
			Tasktype:    Map,
			State:       Unassigned,
			StartTime:   time.Now(),
			Index:       i,
			InputFiles:  []string{files[i]},
			OutputFiles: nil,
		}
	}

	// Start Reduce task
	for i := range c.reduceTasks {
		c.reduceTasks[i] = MapReduceTask{
			Tasktype:    Reduce,
			State:       Unassigned,
			StartTime:   time.Now(),
			Index:       i,
			InputFiles:  generateInputFiles(i, len(files)),
			OutputFiles: []string{fmt.Sprintf("mr-out-%d", i)},
		}
	}
	c.server()
	return &c
}

func generateInputFiles(i int, file int) []string {
	var inputFiles []string
	for j := 0; j < file; j++ {
		inputFiles = append(inputFiles, fmt.Sprintf("mr-%d-%d", j, i))
	}
	return inputFiles
}

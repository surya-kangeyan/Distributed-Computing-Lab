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
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		funcArgs := CoordinatorReply{}
		funcReply := CoordinatorReply{}

		// send Job Request to coordinator to fetch task
		result := call("Coordinator.SendJobRequest", &funcArgs, &funcReply)
		if !result {
			// Error
			break
		}
		switch funcReply.Task.Tasktype {
		case Map:
			// execute map function on the partitions
			executeMap(&funcReply, mapf)
		case Reduce:
			// execute Reduce function on the intermediate files
			executeReduce(&funcReply, reducef)
		case Hold:
			// wait for coordiantor commands
			time.Sleep(1 * time.Second)
		case Exit:
			os.Exit(0)
			// stop execution
		}
	}

}

func executeMap(reply *CoordinatorReply, mapf func(string, string) []KeyValue) {
	// fmt.Println("worker.go doMap called ")
	// Open and read the input file associated with this map task.

	file, err := os.Open(reply.Task.InputFiles[0])
	if err != nil {
		log.Fatalf("cannot open %v", reply.Task.InputFiles[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Task.InputFiles[0])
	}
	file.Close()
	// Apply the map function to the input content and obtain KeyValue pairs.

	keyValueTuple := mapf(reply.Task.InputFiles[0], string(content))

	// Create an array of slices to hold intermediate KeyValue pairs
	intermFile := make([][]KeyValue, reply.ReduceTaskNo)
	// fmt.Println("worker.go intermediate type")
	// fmt.Println(keyValueTuple)
	// fmt.Println("NReduce value is %d ", reply.ReduceTaskNo)
	// fmt.Println("Intermediate arrays is ")

	// Distribute KeyValue pairs to reduce tasks based on the hash of their keys.

	for _, kv := range keyValueTuple {
		r := ihash(kv.Key) % reply.ReduceTaskNo
		// fmt.Println("The current key value to be hashed is %s ---> %d ", kv.Key, r)
		intermFile[r] = append(intermFile[r], kv)
	}
	// fmt.Println(intermFile)

	// Write intermediate KeyValue pairs to temporary files named "mr-<map_task_index>-<reduce_task_index>".

	for r, keyValTuple := range intermFile {
		oname := fmt.Sprintf("mr-%d-%d", reply.Task.Index, r)
		ofile, _ := ioutil.TempFile("", oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range keyValTuple {
			enc.Encode(&kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}

	// // ================================================== Test code written by suryakangeyan to inspect the contents of the temprory intermediate files
	// const permDir = "/Users/suryakangeyan/HomeWork/Dsystems/src"

	// for _, kva := range intermediate {
	// 	// Updated file naming convention
	// 	// oname := fmt.Sprintf("mr-%d-%d.json", reply.Task.Index, r) // Assuming JSON format

	// 	// Create a path for the permanent file
	// 	// opath := permDir + oname

	// 	// Create or open the file
	// 	ofile, err := os.Create("testFile.txt")
	// 	if err != nil {
	// 		// handle error
	// 		fmt.Println("Error creating file:", err)
	// 		return
	// 	}

	// 	// Create a JSON encoder and encode key values
	// 	enc := json.NewEncoder(ofile)
	// 	for _, kv := range kva {
	// 		err := enc.Encode(&kv)
	// 		if err != nil {
	// 			// handle error
	// 			fmt.Println("Error encoding JSON to file:", err)
	// 			return
	// 		}
	// 	}

	// 	// Close the file after writing
	// 	ofile.Close()
	// }
	// Update server state of the task, by calling the RPC SendJobCompleteNotification
	reply.Task.State = Completed
	replyEx := CoordinatorReply{}
	call("Coordinator.SendJobCompleteNotification", &reply, &replyEx)
}

func executeReduce(reply *CoordinatorReply, reducef func(string, []string) string) {
	// Load intermFile files
	intermFile := []KeyValue{}
	for k := 0; k < len(reply.Task.InputFiles); k++ {
		file, err := os.Open(reply.Task.InputFiles[k])
		if err != nil {
			log.Fatalf("cannot open %v", reply.Task.InputFiles[k])
		}
		dec := json.NewDecoder(file)
		// fmt.Println("Worker.go reduce() intermediate file")
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermFile = append(intermFile, kv)
			// fmt.Println("Intermediate file ")
			// fmt.Println(intermFile)
		}
		file.Close()
	}

	// Sort the intermediate KeyValue pairs by key so that is is easy to prepare for grouping.
	sort.Slice(intermFile, func(i, j int) bool {
		return intermFile[i].Key < intermFile[j].Key
	})
	// fmt.Println("Sorting the values of the intermediate files")
	// fmt.Println(intermFile)

	// const permDir = "/Users/suryakangeyan/HomeWork/Dsystems/src"
	// 	onamee := fmt.Sprintf("mr-%d-%d.txt", reply.Task.Index, reply.Task.Index+aaa) // Assuming JSON format

	// 	// Create a path for the permanent file
	// 	opath := permDir + onamee

	// 	// Create or open the file
	// 	ofilee, err := os.Create(opath)
	// 	if err != nil {
	// 		// handle error
	// 		fmt.Println("Error creating file:", err)
	// 		return
	// 	}

	// 	// Create a JSON encoder and encode key values
	// 	// enc := json.NewEncoder(ofilee)
	// 	ie := 0
	// for ie < len(intermFile) {
	// 	j := ie + 1
	// 	for j < len(intermFile) && intermFile[j].Key == intermFile[ie].Key {
	// 		j++
	// 	}
	// 	values := []string{}
	// 	for k := ie; k < j; k++ {
	// 		values = append(values, intermFile[k].Value)
	// 		fmt.Println("worker.go the calculated values ",intermFile[k].Key, values)
	// 	}
	// 	// fmt.Println("worker.go the calculated values ", values)
	// 	output := reducef(intermFile[ie].Key, values)
	// 	fmt.Println("worker.go reduce function output ", intermFile[ie].Key, output )
	// 	fmt.Fprintf(ofilee, "%v %v\n", intermFile[ie].Key, output)
	// 	ie = j
	// }

	// 	// Close the file after writing
	// 	ofilee.Close()

	//  Create a temporary output file to store the final results.
	tempName := fmt.Sprintf("mr-out-%d", reply.Task.Index)
	tempFile, _ := ioutil.TempFile("", tempName)

	// Apply the reduce function to grouped values with the same key.
	i := 0
	for i < len(intermFile) {
		j := i + 1
		for j < len(intermFile) && intermFile[j].Key == intermFile[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermFile[k].Value)
			// fmt.Println("worker.go the calculated values ",intermFile[k].Key, values)
		}
		// fmt.Println("worker.go the calculated values ", values)
		output := reducef(intermFile[i].Key, values)
		// fmt.Println("worker.go reduce function output ", intermFile[i].Key, output )
		fmt.Fprintf(tempFile, "%v %v\n", intermFile[i].Key, output)
		i = j
	}

	// Close output file
	tempFile.Close()

	// Rename output file
	os.Rename(tempFile.Name(), tempName)

	// Update task status
	reply.Task.State = Completed
	finalReply := CoordinatorReply{}
	call("Coordinator.SendJobCompleteNotification", &reply, &finalReply)
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

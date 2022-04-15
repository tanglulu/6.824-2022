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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
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
        logfileName := fmt.Sprintf("logfile-worker-%v", os.Getpid())
	file, _ := os.OpenFile(logfileName, os.O_CREATE|os.O_RDWR, 0644)
	log.SetOutput(file)
	for {
		reply := RpcRequestReply{}
		arg := RpcRequestArgs{WorkerId: os.Getpid()}
		ok := call("Coordinator.GetTask", &arg, &reply)

		if !ok || reply.Status == COORDINATOR_NONE {
			log.Println("no tasks, sleep 1s")
			time.Sleep(1 * time.Second)
                        continue
		}

		if reply.Status == COORDINATOR_DONE {
			log.Println("all tasks have done, exit worker")
			break
		}

		// reply.Status == COORDINATOR_TASK
		if reply.TaskType == TASK_MAP {
			file, err := os.Open(reply.IntputFiles[0])
			if err != nil {
				log.Fatalf("cannot open %v", reply.IntputFiles[0])
			}
			content, err := ioutil.ReadAll(file)
			file.Close()

			if err != nil {
				log.Fatalf("cannot read %v", reply.IntputFiles[0])
			}
			kva := mapf(reply.IntputFiles[0], string(content))

			intermediate := make([][]KeyValue, reply.Num, reply.Num)
			for _, kv := range kva {
				i := ihash(kv.Key) % reply.Num
				intermediate[i] = append(intermediate[i], kv)
			}

			for i, kvs := range intermediate {
				filename := GetTaskOutputFilename(reply.TaskType, reply.TaskId, i)

				intermediateFile, _ := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
				enc := json.NewEncoder(intermediateFile)

				for _, kv := range kvs {
					enc.Encode(&kv)
				}
				intermediateFile.Sync()
				intermediateFile.Close()
				log.Printf("write result to file %v\n", filename)
			}
		} else {
			var kvs []KeyValue

                        log.Println(reply.IntputFiles)
			for _, filename := range reply.IntputFiles {
				file, _ := os.Open(filename)
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kvs = append(kvs, kv)
				}
				file.Close()

			}

			sort.Sort(ByKey(kvs))

			ofile, _ := os.OpenFile(GetTaskOutputFilename(TASK_REDUCE, reply.TaskId, 0), os.O_CREATE|os.O_RDWR, 0644)
			i := 0
			for i < len(kvs) {
				j := i + 1
				for j < len(kvs) && kvs[j].Key == kvs[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kvs[k].Value)
				}
				output := reducef(kvs[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

				i = j
			}
			ofile.Sync()
			ofile.Close()
		}

		log.Printf("task %v done!", reply.TaskId)

		responseArg := RpcResponseArgs{}
		responseReply := RpcResponseReply{}

		responseArg.WorkerId = os.Getpid()
		responseArg.Status = TASK_STATUS_DONE
		responseArg.TaskId = reply.TaskId
		call("Coordinator.TaskDone", &responseArg, &responseReply)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
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

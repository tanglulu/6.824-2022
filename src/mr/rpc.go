package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

const COORDINATOR_TASK = 0
const COORDINATOR_NONE = 1
const COORDINATOR_DONE = 2

type RpcRequestArgs struct {
	WorkerId int
}

type RpcRequestReply struct {
	TaskId      int
	TaskType    int8
	IntputFiles []string
	Status      int8
	Num         int
}

type RpcResponseArgs struct {
	WorkerId int
	TaskId   int
	Status   int8
}

type RpcResponseReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

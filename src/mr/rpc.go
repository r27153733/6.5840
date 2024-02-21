package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type SubmitMapArgs struct {
	ID          int
	HashKeyFile []string
}
type SubmitMapReply struct {
	Err error
}
type SubmitReduceArgs struct {
	HashKey  int
	FilePath string
}
type SubmitReduceReply struct {
	Err error
}
type GetJobReply struct {
	MapSource    *MapSource
	ReduceSource *ReduceSource
}

const (
	RPCFunSubmitMapJob    = "Coordinator.RPCSubmitMap"
	RPCFunSubmitReduceJob = "Coordinator.RPCSubmitReduce"
	RPCFunGetJob          = "Coordinator.RPCGetJob"
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

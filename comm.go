package gomapreduce

/*
Strucs for RPC communication between Map Reduce Node
*/

const (
	OK = "OK"
	ErrNoKey = "ErrNoKey"
)
type Err string

type Args interface {}
type Reply interface {}


type AssignMapTaskArgs struct {
	Job MapWorkerJob
}

type AssignMapTaskReply struct {
	OK bool
}

type MapTaskCompleteArgs struct {
	Job MapWorkerJob
}

type MapTaskCompleteReply struct {
	OK bool
}


type TestRPCArgs struct {
	Mapper Mapper
	Number int
}

type TestRPCReply struct {
	Err Err
}



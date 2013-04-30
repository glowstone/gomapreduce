package gomapreduce

/*
Strucs for RPC communication between MapReduceNode instances
*/

const (
	OK = "OK"
	ErrNoKey = "ErrNoKey"
)
type Err string

type Args interface {}
type Reply interface {}


type AssignTaskArgs struct {
	Job MapWorkerJob
}

type AssignTaskReply struct {
	OK bool
}

type TaskCompleteArgs struct {
	Job MapWorkerJob
}

type TaskCompleteReply struct {
	OK bool
}


type TestRPCArgs struct {
	Mapper Mapper
	Number int
}

type TestRPCReply struct {
	Err Err
}



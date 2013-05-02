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


// Master nodes assigns Tasks
///////////////////////////////////////////////////////////////////////////////

type AssignTaskArgs struct {
	Task Task         // MapTask or ReduceTask
}

type AssignTaskReply struct {
	OK bool
}



type TaskCompleteArgs struct {
	Task_id string
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



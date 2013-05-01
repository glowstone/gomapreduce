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
	Name string       // Task type (either 'map' or 'reduce')
	Task Task         // MapTask or ReduceTask
	Assigner int      // index of node assigning the task, maybe call Master?
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



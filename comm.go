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


type AssignTaskArgs struct {

}

type AssignTaskReply struct {

}

type TaskCompleteArgs struct {

}

type TaskCompleteReply struct {

}


type TestRPCArgs struct {
	Number int
}

type TestRPCReply struct {
	Err Err
}




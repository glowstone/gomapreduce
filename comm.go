package gomapreduce

/*
Strucs for RPC communication between Map Reduce Node
*/

import "fmt"

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



// Compilable Mapper

type ExampleMapper struct {
	Id int
}

func (self ExampleMapper) get_id() int {
	return self.Id
}

func (self ExampleMapper) Map_action() {
	fmt.Println("Performing example Mapper action")
}




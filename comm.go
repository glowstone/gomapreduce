package gomapreduce

/*
Strucs for RPC communication between MapReduceNode instances
*/

import (
	"time"
)

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


/* Ping stuff */

// nodes should ping each other this often, so that each node knows which other nodes are alive
const PingInterval = time.Millisecond * 100

// A node will declare another node dead if it hasn't heard from it in this many PingIntervals
const DeadPings = 5

type PingArgs struct {
  Me string     // "host:port"
  // Viewnum was included here for the Viewmaster, not sure if we need anything else for this
}

type PingReply struct {
  // In viewmaster, the current View was returned but I don't think we need that
}

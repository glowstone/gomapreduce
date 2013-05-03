package gomapreduce

/*
Structs for RPC communication between MapReduceNode instances
*/

import (
	"net/rpc"
	"time"
	"fmt"
)

const (
	OK = "OK"
	ErrNoKey = "ErrNoKey"
)
type Err string

/* Master nodes assigns Tasks */

type AssignTaskArgs struct {
	Task Task    // MapTask or ReduceTask
}

type AssignTaskReply struct {
	OK bool      // Acknowledgement
}


/* Workers nodes notify master that Task completed */

type TaskCompleteArgs struct {
	JobId string
	TaskId string
}

type TaskCompleteReply struct {
	OK bool      // Acknowledgement    
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


/*
Sends an RPC to the rpcname handler on server srv with arguments args, waits for 
the reply, and leaves the reply in reply. The reply argument should be a pointer
to a reply structure.
The return value is true if the server responded, and false if call() was not able 
to contact the server. in particular, the reply's contents are only valid if call() 
returned true.
You should assume that call() will time out and return an error after a while if it 
doesn't get a reply from the server.
Specify netMode as 'unix' or 'tcp'.
*/
func call(srv string, netMode string, rpcname string, args interface{}, reply interface{}) bool {
  var c *rpc.Client
  var errx error
  if netMode == "unix" {
    c, errx = rpc.Dial("unix", srv)
  } else if netMode == "tcp" {
    c, errx = rpc.Dial("tcp", srv)
  } else {
    panic("netMode must be either 'unix' or 'tcp'.")
  }
	if errx != nil {
		fmt.Printf("Error: %s\n", errx)
		return false
	}
	defer c.Close()
		
	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Printf("Error: %s\n", err)
	return false
}  

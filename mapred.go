/*
Package mapreduce implements a MapReduce library.
*/

package gomapreduce

import (
	"net"          // interface for network I/O
	"net/rpc"
	"log"
	"os"
	"syscall"
	"sync"
	"fmt"
	"math/rand"
  "time"
  "encoding/gob"
)

type MapReduce struct {
	mu sync.Mutex
	l net.Listener
	dead bool
	unreliable bool
	rpcCount int
	me int                // index into nodes
	nodes []string        // MapReduceNode port names
	node_count int
  net_mode string       // "unix" or "tcp"

}

func randNumber() int {
  return rand.Int()
}


func (self *MapReduce) Start(mapper Mapper) {
	fmt.Println("Start MapReduce", mapper)
  self.broadcast_testrpc(mapper)
}

/*
 *
 */
func (self *MapReduce) tick() {
  fmt.Println("Tick")
}

func (self *MapReduce) broadcast_testrpc(maptask Mapper) {
  fmt.Println(self.nodes, maptask)
  for index, node := range self.nodes {
    if index == self.me {
      continue
    }
    args := &TestRPCArgs{}         // declare and init zero valued struct
    args.Number = rand.Int()
    //task := ExampleMapper{}
    args.Mapper = maptask
    var reply TestRPCReply
    ok := self.call(node, "MapReduce.TestRPC", args, &reply)
    if ok {
      fmt.Println("Successfully sent")
      fmt.Println(reply)
    } else {
      fmt.Println("Sent but not received")
      fmt.Println(reply)
    }
  }
  return
}


// Handle test RPC RPC calls.
func (self *MapReduce) TestRPC(args *TestRPCArgs, reply *TestRPCReply) error {
  fmt.Println("Received TestRPC", args.Number)
  args.Mapper.Map_action()
  fmt.Printf("Task id: %d\n", args.Mapper.get_id())
  reply.Err = OK
  return nil
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func (self *MapReduce) call(srv string, rpcname string, args interface{}, reply interface{}) bool {
  fmt.Println("Sending to", srv)
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }
  return false
}  


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (self *MapReduce) Kill() {
  self.dead = true
  if self.l != nil {
    self.l.Close()
  }
}


// Create an MapReduceNode instance
// The ports of all the nodes (including this one) are in nodes[], 
// this node's port is nodes[me]

func Make(nodes []string, me int, rpcs *rpc.Server, mode string) *MapReduce {
  // Initialize a MapReduceNode
  mr := &MapReduce{}
  mr.nodes = nodes    
  mr.me = me
  mr.net_mode = mode
  // Initialization code
  mr.node_count = len(nodes)

  if rpcs != nil {
    rpcs.Register(mr)      // caller created RPC Server
  } else {
    rpcs = rpc.NewServer() // creates a new RPC server
    rpcs.Register(mr)      // Register exported methods of MapReduceNode with RPC Server         
    gob.Register(TestRPCArgs{})
    gob.Register(TestRPCReply{})
    gob.Register(ExampleMapper{})

    // Prepare node to receive connections

    if mode == "tcp" {
      fmt.Println("Making in TCP mode")
      listener, error := net.Listen("tcp", ":8080");

      if error != nil {
        log.Fatal("listen error: ", error);
      }
      mr.l = listener      // Set MapReduceNode listener
      go func() {
        for mr.dead == false {
          conn, err := mr.l.Accept()
          if err == nil && mr.dead == false {
            if mr.unreliable && (rand.Int63() % 1000) < 100 {
              // discard the request.
              conn.Close()
            } else if mr.unreliable && (rand.Int63() % 1000) < 200 {
              // process the request but force discard of reply.
              c1 := conn.(*net.UnixConn)
              f, _ := c1.File()
              err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
              if err != nil {
                fmt.Printf("shutdown: %v\n", err)
              }
              mr.rpcCount++
              go rpcs.ServeConn(conn)
            } else {
              mr.rpcCount++
              go rpcs.ServeConn(conn)
            }
          } else if err == nil {
            conn.Close()
          }
          if err != nil && mr.dead == false {
            fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
          }
        }
      }()

    } else {
      fmt.Println("Making in Unix mode")
      // mode assumed to be "unix"

      os.Remove(nodes[me]) // only needed for "unix"
      listener, error := net.Listen("unix", nodes[me]);
      if error != nil {
        log.Fatal("listen error: ", error);
      }
      mr.l = listener      // Set MapReduceNode listener

      

      // please do not change any of the following code,
      // or do anything to subvert it.
      
      // create a thread to accept RPC connections
      go func() {
        for mr.dead == false {
          conn, err := mr.l.Accept()
          if err == nil && mr.dead == false {
            if mr.unreliable && (rand.Int63() % 1000) < 100 {
              // discard the request.
              conn.Close()
            } else if mr.unreliable && (rand.Int63() % 1000) < 200 {
              // process the request but force discard of reply.
              c1 := conn.(*net.UnixConn)
              f, _ := c1.File()
              err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
              if err != nil {
                fmt.Printf("shutdown: %v\n", err)
              }
              mr.rpcCount++
              go rpcs.ServeConn(conn)
            } else {
              mr.rpcCount++
              go rpcs.ServeConn(conn)
            }
          } else if err == nil {
            conn.Close()
          }
          if err != nil && mr.dead == false {
            fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
          }
        }
      }()
    }

  }

  go func() {
    for mr.dead == false {
      mr.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return mr
}



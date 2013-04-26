package mapreduce

import (
	"net"
	"net/rpc"
	"log"
	"os"
	"syscall"
	"sync"
	"fmt"
	"math/rand"
  "time"
)

type MapReduce struct {
	mu sync.Mutex
	l net.Listener
	dead bool
	unreliable bool
	rpcCount int
	me int                // index into nodes
	nodes []string        // peer MapReduce nodes
	node_count int

}


func (self *MapReduce) Start() {
	fmt.Println("Start MapReduce")
}

/*
 *
 */
func (self *MapReduce) tick() {
  fmt.Println("Tick")

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

func Make(nodes []string, me int, rpcs *rpc.Server) *MapReduce {

  mr := &MapReduce{}
  mr.nodes = nodes
  mr.me = me
  // Initialization code
  mr.node_count = len(nodes)
 
  
  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(mr)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(mr)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(nodes[me]) // only needed for "unix"
    l, e := net.Listen("unix", nodes[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    mr.l = l
    
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

  go func() {
    for mr.dead == false {
      mr.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()


  return mr
}



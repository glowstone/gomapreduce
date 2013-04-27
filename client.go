package gomapreduce

/* 
 * Provides applications with MapReduce library stubs which wrap the MapReduce 
 * services exposed by MapReduceServer instances. Clerk stubs generate 
 * necesary RPC calls to MapReduce service.
 */

import (
 	"net/rpc"
	//"time"
  "math"
	"math/rand"
  "fmt"
)


func Sqrt(x float64) float64 {
  return math.Sqrt(x)
}

/**
 * 
 */
type Clerk struct {
  servers []string
  id int                           // unique clerk_id serves as a client identifier.
  request_id_generator func() int  // returns unique request id (among requests by this Clerk).
}

/*
 *
 */
func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  ck.id = rand.Int()
  ck.request_id_generator = make_id_generator()
  return ck
}


/*
  make_id_generator returns a function which will generate unique id's based on an enclosed base_id
  and incrementing the base_id by one each time the id_generator is called.
  The base_id starts at 0 so the first returned id is 1. This was done since it would be possible to 
  confuse an id of 0 with a default zero-valued int field (also 0).
*/
func make_id_generator() (func() int) {
  base_id := -1
  return func() int {
    base_id += 1
    return base_id
  }
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
func call(srv string, rpcname string, args interface{}, 
	reply interface{}) bool {
  fmt.Println("Sending to", srv)
  c, errx := rpc.Dial("tcp", srv)
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
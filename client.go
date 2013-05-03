package gomapreduce

/* 
 * Provides applications with MapReduce library stubs which wrap the MapReduce 
 * services exposed by MapReduceServer instances. Clerk stubs generate 
 * necesary RPC calls to MapReduce service.
 */

import (
	"math/rand"
)


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
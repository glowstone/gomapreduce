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
  "math"
  "time"
  "encoding/gob"
  "github.com/jacobsa/aws/s3"
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

  instances map[int] MapReduceInstance  // Maps instanceNumber -> Instance
  bucket s3.Bucket

}

type ConfigurationParams struct {
  InputFolder string    // The folder within the "mapreduce_testing1" s3 bucket
  OutputFolder string   // The folder to write output to (within "mapreduce_testing1")
  // TODO Should contain map and reduce functions, chunk sizes etc.
}

type MapReduceInstance struct {
  instanceNumber int
  finished bool
  master string
}

type MapWorkerJob struct {
  Key string    // The key where the data for the job can be found
  Worker string   // empty string if no assigned worker
  Completed bool
  // Map of intermediate keys to the location of the values for those keys (or some way for reducers to locate that data)
  IntermediateKeyLocation map[string]string
}


func (self *MapReduce) Start() {
	fmt.Println("Start MapReduce")
  self.broadcast_testrpc()
  master := self.nodes[rand.Intn(len(self.nodes))]  // Or do we want to always choose master to be the called node? Then the client can choose who they want to be the master
  fmt.Printf("Master: %s\n", master)
  self.StartMapReduce(0, ConfigurationParams{"small_test", ""})  // TODO should be RPC call to master instead
}

// The method used by the master node to start the entire mapreduce operation
func (self *MapReduce) StartMapReduce(sequenceNumber int, params ConfigurationParams) {
  instance := MapReduceInstance{sequenceNumber, false, self.nodes[self.me]}
  fmt.Printf("Master(%d): new instance: %s\n", self.me, instance)

  // Get the list of jobs that will need to be performed by workers
  jobs := self.getMapJobs(params.InputFolder)

  // While there are still unfinished worker jobs, assign new ones and wait for them to finish
  self.assignMapJobs(jobs)
}

// Gets all the keys that need to be processed by map workers for this instance of mapreduce, and constructs a 
// MapWorkerJob for each of them. Returns the list of jobs.
func (self *MapReduce) getMapJobs(inputFolder string) []MapWorkerJob {
  jobs := []MapWorkerJob{}

  keys := FilterKeysByPrefix(self.bucket, inputFolder + "/")   // Prefix needs to end with the slash so we don't get e.g. both test/ and test1/
  for _, key := range keys {
    jobs = append(jobs, MapWorkerJob{key, "", false, map[string]string{}})
  }

  return jobs
}

// Method used by the master. Assigns jobs to workers until all jobs are complete.
func (self *MapReduce) assignMapJobs(jobs []MapWorkerJob) {
  workers := append(self.nodes[:self.me], self.nodes[self.me:]...)  // The workers available for map tasks (everyone but me)
  fmt.Printf("Map Workers: %s\n", workers)

  numUnfinished := getNumberUnfinished(jobs)    // The number of jobs that are not complete
  var job MapWorkerJob

  for numUnfinished > 0 {   // While there are jobs left to complete
    fmt.Printf("Number unfinished: %d\n", numUnfinished)
    jobIndex := getUnassignedJob(jobs)  // Get the index of one of the unassigned jobs

    if jobIndex != -1 {     // A value of -1 means there are no unassigned jobs
      job = jobs[jobIndex]
      worker := workers[rand.Intn(len(workers))]    // TODO should only use an idle worker
      jobs[jobIndex].Worker = worker  // Assign the worker for the job
      args := AssignMapTaskArgs{job}
      reply := &AssignMapTaskReply{}

      self.call(worker, "MapReduce.StartMapJob", args, reply)   // TODO this should be asynchronous RPC, check for err etc.
      jobs[jobIndex].Completed = true     // TODO job should only be set to complete when the worker sends a "job complete" RPC
    }

    numUnfinished = getNumberUnfinished(jobs)
    // TODO should sleep for some amount of time before looping again?
  }
}

// A method used by a map worker. The worker will fetch the data associated with the key for the job it's assigned, and
// then run the map function on that data. The worker stores the intermediate key/value pairs in memory and tells the
// master where those values are stored so that reduce workers can get them when needed.
func (self *MapReduce) StartMapJob(args *AssignMapTaskArgs, reply *AssignMapTaskReply) error{
  fmt.Printf("Worker %d starting Map(%s)\n", self.me, args.Job.Key)
  mapData, _ := self.bucket.GetObject(args.Job.Key)
  fmt.Printf("Worker %d got map data: %s\n", self.me, string(mapData[:int(math.Min(30, float64(len(mapData))))]))
  // TODO Run the map function on the data (asynchronously)
  // TODO Write the intermediate keys/values to somewhere (in memory for now)
  // TODO Set job.intermediateKeyLocation to the place that the value was written so reducers can find it

  args.Job.Completed = true
  reply.OK = true

  return nil
}

// Iterates though jobs and counts the number that are unfinished.
func getNumberUnfinished(jobs []MapWorkerJob) int{
  unfinished := 0
  for _, job := range jobs {
    if !job.Completed {
      unfinished++
    }
  }

  return unfinished
}

// Gets a random unassigned job from the list of jobs and returns it
func getUnassignedJob(jobs []MapWorkerJob) int {
  for i, job := range jobs {
    if job.Worker == "" {
      return i
    }
  }

  return -1  // TODO check this
}

/*
 *
 */
func (self *MapReduce) tick() {
  // fmt.Println("Tick")
}

func (self *MapReduce) broadcast_testrpc() {
  fmt.Println(self.nodes)
  for index, node := range self.nodes {
    if index == self.me {
      continue
    }
    args := &TestRPCArgs{}         // declare and init zero valued struct
    args.Number = 5
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
  fmt.Println("Received TestRPC")
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
  mr.bucket = GetBucket()
  mr.instances = map[int]MapReduceInstance{}

  if rpcs != nil {
    rpcs.Register(mr)      // caller created RPC Server
  } else {
    rpcs = rpc.NewServer() // creates a new RPC server
    rpcs.Register(mr)      // Register exported methods of MapReduceNode with RPC Server         
    gob.Register(TestRPCArgs{})
    gob.Register(TestRPCReply{})

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



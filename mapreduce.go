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
  "runtime"
)

type MapReduceNode struct {
	mu sync.Mutex         // singleton mutex for node
	l net.Listener        // RPC network listener
	dead bool             // used for testing dead nodes
	unreliable bool       // used for testing unreliable nodes
	rpcCount int          // maintain count of RPC calls

	me int                // index into nodes
	nodes []string        // MapReduceNode port names
	netMode string        // "unix" or "tcp"

	// State for master role
  jm JobManager          // Manages Jobs the node is responsible for as a master.
  tm TaskManager         // Manages Tasks the node is responsible for as a master.
  sm StatsManager        // Manages Task and Job stats, like how long they take to run

	// State for worker roles
  emittedStorage EmittedStorage    // Storage system for emitted intermediate KVPairs
  emittedReader EmittedReader      // Interface for reading KVPairs across the Map Reduce cluster.

  // Last ping times for each other node
  lastPingTimes map[string]time.Time
  // State ("alive" or "dead") of each other node
  nodeStates map[string]string

  // Temporary (until jobmanager is built)
  jobDone map[string]bool
}


/*
Client would like to start a Job instance which is composed of Task 
instances (MapTasks or Reduce Tasks). Client passes a JobConfig instance along
with his implemented Mapper, Reducer, InputAccessor, and OutputAccessor.
Spawns a master_role thread to perform the requested Job by breaking it into 
tasks that are allocated to workers. Returns the int job_id assigned to the 
started Job.
Any configuration settings not for a particular job should be read from the 
environment.

Aside: Currently, this is called from a client which has a local MapReduceNode running
at it, but a wrapper that allows start to be called remotely via RPC could be 
created. We don't currently have any scenarios where the client is not also a 
member of the network but it is totally possible.
*/
func (self *MapReduceNode) Start(job_config JobConfig, mapper Mapper, 
  reducer Reducer, inputer Inputer, outputer Outputer) string {

  job_id := generate_uuid()       // Job identifier created internally, unlike in Paxos
  job := makeJob(job_id, mapper, reducer, inputer, outputer)
  self.jm.addJob(job, "starting")
  self.jobDone[job_id] = false    // TODO temporary, remove this once the jobManager exists
  self.sm.addJob(job_id)          // Add the job to the stats manager

  debug(fmt.Sprintf("(svr:%d) Start: job_id: %s, job: %v", self.me, job_id, job))

  // Spawn a thread to act as the master
	go self.masterRole(job, job_config)

  return job_id
}

func (self *MapReduceNode) Status(jobId string) bool{
  //TODO
  // debug(fmt.Sprintf("Called Status"))
  return self.jobDone[jobId]
}


/*
Performs the requested Job by breaking it into tasks based on the JobConfig,
allocating the Tasks to workers, and monitors progress on the Job until it is 
complete.
The method used by the master node to start the entire mapreduce operation
*/
func (self *MapReduceNode) masterRole(job Job, config JobConfig) {
	debug(fmt.Sprintf("(svr:%d) master_role: job", self.me))
  jobId := job.getId()
  // MapTasks
  // Split input data into M components. Right now, input is prechunked so do nothing.
  maptasks := self.makeMapTasks(job, config)    // Create M MapTasks
  self.tm.addBulkMapTasks(jobId, maptasks)      // Add tasks to TaskManager
  self.assignTasks(jobId)                       // Assign unassigned MapTasks to workers
  self.awaitTasks(jobId, "assigned", "all")     // Wait for MapTasks to be completed

  // temporary debugging
  fmt.Printf("Unassigned: %s\n", self.tm.listTasks(jobId, "unassigned", "all"))
  fmt.Printf("Assigned: %s\n", self.tm.listTasks(jobId, "assigned", "all"))
  fmt.Printf("Completed: %s\n", self.tm.listTasks(jobId, "completed", "all"))

  self.awaitTasks(jobId, "completed", "all")

  // temporary debugging
  fmt.Printf("Unassigned: %s\n", self.tm.listTasks(jobId, "unassigned", "all"))
  fmt.Printf("Assigned: %s\n", self.tm.listTasks(jobId, "assigned", "all"))
  fmt.Printf("Completed: %s\n", self.tm.listTasks(jobId, "completed", "all"))

  done := false
  for !done {   // Loop until all MapTasks are done
    fmt.Printf("Unassigned: %s\n", self.tm.listTasks(jobId, "unassigned", "all"))
    fmt.Printf("Assigned: %s\n", self.tm.listTasks(jobId, "assigned", "all"))
    fmt.Printf("Completed: %s\n", self.tm.listTasks(jobId, "completed", "all"))
    time.Sleep(2000 * time.Millisecond) 
    numUnfinished := self.tm.countTasks(jobId, "unassigned", "all") + self.tm.countTasks(jobId, "assigned", "all")
    done = (numUnfinished == 0)     // If there are no unfinished tasks, then we're done with this phase
  }

  // ReduceTasks (+ failed MapTasks )
  reduceTasks := self.makeReduceTasks(job, config)
  self.tm.addBulkReduceTasks(job.getId(), reduceTasks)
  self.assignTasks(jobId)

  done = false
  for !done {     // Loop until all tasks are done
    // TODO reassign map tasks whose workers failed
    fmt.Printf("Unassigned: %s\n", self.tm.listTasks(jobId, "unassigned", "all"))
    fmt.Printf("Assigned: %s\n", self.tm.listTasks(jobId, "assigned", "all"))
    fmt.Printf("Completed: %s\n", self.tm.listTasks(jobId, "completed", "all"))
    time.Sleep(2000*time.Millisecond)
    numUnfinished := self.tm.countTasks(jobId, "unassigned", "all") + self.tm.countTasks(jobId, "assigned", "all")
    done = (numUnfinished == 0)     // If there are no unfinished tasks, then we're done with this phase
  }
  fmt.Println("\n\nDONE!\n")

  self.jobDone[jobId] = true        // TODO temporary, remove once jobManager exists
  self.sm.jobComplete(jobId)        // Mark the job as complete in the statsManager
  fmt.Printf("Job %s took %v\n", jobId, self.sm.jobTime(jobId))   // Time the job
  //self.awaitTasks("all")                          // Wait for MapTasks and ReduceTasks to be completed

  // // Cleanup
  // self.tm.jobDone(job.getId())
}

/*
Executes the MapTask or ReduceTask
*/
func (self *MapReduceNode) workerRole(task Task) {
  if task.getKind() == "map" {
    mapTask := task.(MapTask)
    // Emitter created per MapTask; encapsulates jobId and taskId pairs are emitted to.
    emitter := makeSimpleEmitter(task.getJobId(), task.getJobConfig(), &self.emittedStorage)
    mapTask.execute(emitter)
  } else if task.getKind() == "reduce" {
    reduceTask := task.(ReduceTask)
    reduceTask.execute(self.emittedReader)
  } else {
    panic("worker tried to execute invalid kind of Task.")
  }
  return
}

func (self *MapReduceNode) tick() {
  //fmt.Println("Tick")
  self.sendPings()
	self.checkForDisconnectedNodes()
}

// Exported RPC functions (internal to mapreduce service)
///////////////////////////////////////////////////////////////////////////////

/*
Accepts notification of of Task Completion by a worker and uses the Task Manager to
change the status of the TaskState for the completed Task.
Does NOT check that worker was actually assigned the task or do other forms of 
validation. MapReduceNodes assumed to be trustworthy.
*/
func (self *MapReduceNode) TaskCompleted(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
  // TODO: Handle duplicate reqests - TaskManager may change task state from complete to
  // assigned and a stray late packet should not switch it back.
  debug(fmt.Sprintf("(svr:%d) Received TaskCompleted: %s", self.me, args.TaskId))
  self.tm.setTaskStatus(args.JobId, args.TaskId, "completed")

  // Collect stats information
  self.sm.taskComplete(args.JobId, args.TaskId)
  time := self.sm.taskTime(args.JobId, args.TaskId)
  fmt.Printf("Task took %v\n", time)

  
  reply.OK = true            // acknowledge receipt of the notification
  return nil
}

/*
Accepts a request to perform a MapTask or ReduceTask. Immediately returns an 
acknowledgement that the Task has been accepted and spawns a workRole thread to 
execute the Task.
*/
func (self *MapReduceNode) ReceiveTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
  debug(fmt.Sprintf("(svr:%d) ReceiveTask: %v", self.me, args))

  // Spawn a worker to execute the Task
  go self.workerRole(args.Task)

	reply.OK = true
	return nil
}

/*

*/
func (self *MapReduceNode) Get(args *GetEmittedArgs, reply *GetEmittedReply) error {
    debug(fmt.Sprintf("(svr:%d) Get: %v", self.me, args))
    //TODO - think carefully about locking, duplicate request handling, ensuring all 
    //intermediates done being generated.
    slicePairs := self.emittedStorage.getEmitted(args.JobId, args.PartitionNumber)
    // fmt.Println("Emitted pairs:", slicePairs)
    if len(slicePairs) == 0 {
      reply.Err = ErrNoKey
    } else {
      reply.KVPairs = slicePairs
      reply.Err = OK
    }
    return nil
}


// Helpers
///////////////////////////////////////////////////////////////////////////////

/*
Gets all the keys that need to be mapped via MapTasks for the job and constructs 
MapTask instances. Returns a slice of MapTasks.
*/
func (self *MapReduceNode) makeMapTasks(job Job, config JobConfig) []MapTask {
  var mapTasks []MapTask

  // Assumes the Job input is prechunked
	for _, key := range job.inputer.ListKeys() {
    taskId := generate_uuid()
    maptask := makeMapTask(taskId, key, job.getId(), config, job.mapper, job.inputer, self.nodes[self.me], self.netMode)
    mapTasks = append(mapTasks, maptask)
	}
	return mapTasks
}

/*
Creates R ReduceTasks assigned as reduceGroups 0,1,...(R-1). Each ReduceTask with
a partition number 'partitionNumber'is responsible for executing the 'reduce' 
method on for all intermediate values where hash(key)%R equals the 'partitionNumber'.
Returns a slice of ReduceTasks.
*/
func (self *MapReduceNode) makeReduceTasks(job Job, config JobConfig) []ReduceTask {
  var reduceTasks  []ReduceTask

  for partitionNumber :=0 ; partitionNumber < config.R ; partitionNumber ++ {
    taskId := generate_uuid()
    // TODO: pass the EmittedReader wrapper instead of self.nodes
    reduceTask := makeReduceTask(taskId, partitionNumber, job.getId(), config, job.reducer, job.outputer, self.nodes[self.me], self.netMode)
    reduceTasks = append(reduceTasks, reduceTask)
  }
  return reduceTasks
}

/*
Synchronous function used by the master thread to assign Tasks to workers. 
Returns when all Tasks for the given jobId in the TaskManager have been assigned.
*/
func (self *MapReduceNode) assignTasks(jobId string) {
  var worker string             // MapReducerNode port
  var task Task
 
  num_unassigned := self.tm.countTasks(jobId, "unassigned", "all")
  for num_unassigned > 0 {
    debug(fmt.Sprintf("(svr:%d) Unassigned: %v", self.me, num_unassigned))
    taskIds := self.tm.listTasks(jobId, "unassigned", "all")

    // Assign unassigned Tasks
    for _, taskId := range taskIds {
      taskState := self.tm.getTaskStateCopy(jobId, taskId)
      task = taskState.task
      args := AssignTaskArgs{Task: task}
      var reply AssignTaskReply
      worker = self.nodes[taskState.workerIndex]

      ok := call(worker, self.netMode, "MapReduceNode.ReceiveTask", args, &reply)
      if ok && reply.OK {
        // Worker accepted the Task assignment
        self.tm.setTaskStatus(jobId, task.getId(), "assigned")
        self.sm.addTask(jobId, task)
      }
    }

    num_unassigned = self.tm.countTasks(jobId, "unassigned", "all")
    time.Sleep(100 * time.Millisecond) 
  }
}

/*
Synchronous function checks that all Tasks of the given JobId and kind ('map', 'reduce',
'all') have a status matching the given status string.
!Note: Reads TaskStates for a Job incrementally, not all atomically. Thus, although
in one call all 'x' kind Tasks may have reached a 'y' status, this may change in the 
future and there may not have ever been an instant where all Tasks had 'y' status.
*/
func (self *MapReduceNode) awaitTasks(jobId string, status string, kind string) {
  var done bool

  taskIds := self.tm.listTasks(jobId, "all", kind)   // Consider only the specified kind of Tasks
  fmt.Println(taskIds)
  for !done {
    done = true

    for _, taskId := range taskIds {
      taskState := self.tm.getTaskStateCopy(jobId, taskId)
      fmt.Println(taskState)
      if taskState.status != status {
        done = false           // Continue await if any Task has not reached desired status
      }
    }
    time.Sleep(100 * time.Millisecond) 
  }
  return
}

// Helper method for a node to send a ping to all of its peers
func (self *MapReduceNode) sendPings() {
	args := &PingArgs{self.nodes[self.me]}
	for _, node := range self.nodes {
		if node != self.nodes[self.me]{
			reply := &PingReply{}
			go call(node, self.netMode, "MapReduceNode.HandlePing", args, reply)   // TODO is it ok to do this as a go func? It would hang forever if it wasn't a go func and the other node was dead
		}
	}
}

// Ping handler, called via RPC when a node wants to ping this node
func (self *MapReduceNode) HandlePing(args *PingArgs, reply *PingReply) error {
	// fmt.Printf("Node %d receiving ping from Node %s\n", self.me, args.Me[len(args.Me)-1:])

	self.lastPingTimes[args.Me] = time.Now()

	return nil
}

// checks to see when each of the other nodes last pinged. If it was too long ago, mark them as dead
func (self *MapReduceNode) checkForDisconnectedNodes() {
	for node, lastPingTime := range self.lastPingTimes {
		timeDifference := time.Now().Sub(lastPingTime)
		if timeDifference > DeadPings * PingInterval {
			fmt.Printf("Node %d marking node %s as dead\n", self.me, node)
			self.nodeStates[node] = "dead"
		}
	}
}


// Handle test RPC RPC calls.
// func (self *MapReduceNode) TestRPC(args *TestRPCArgs, reply *TestRPCReply) error {
//   fmt.Println("Received TestRPC", args.Number)
//   result := args.Mapper.Map("This is a sample string sample string is is")       // perform work on a random input
//   fmt.Println(result)
//   //fmt.Printf("Task id: %d\n", args.Mapper.get_id())
//   reply.Err = OK
//   return nil
// }


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (self *MapReduceNode) Kill() {
	self.dead = true
	if self.l != nil {
		self.l.Close()
	}
}


// Create an MapReduceNode instance
// The ports of all the nodes (including this one) are in nodes[], 
// this node's port is nodes[me]

func MakeMapReduceNode(nodes []string, me int, rpcs *rpc.Server, mode string) *MapReduceNode {
  runtime.GOMAXPROCS(4)
  // Initialize a MapReduceNode
  mr := &MapReduceNode{}
  mr.nodes = nodes    
  mr.me = me
  mr.netMode = mode
  // Nodes acting as masters manage Jobs and Tasks
  mr.jm = makeJobManager()
  mr.tm = makeTaskManager(len(nodes))
  mr.sm = makeStatsManager()

  // Each node stores emitted intermediate pairs for multiple jobs.
  mr.emittedStorage = makeEmittedStorage()

  /*
  EmittedReader created per node to serve as an interface for reading 
  emitted intermediate values known across the cluster
  */
  mr.emittedReader = makeSimpleEmittedReader(nodes, mode)


  // Initialize last ping times for each node
  mr.nodeStates = map[string]string{}
  mr.lastPingTimes = map[string]time.Time{}
  for _, node := range mr.nodes {
  	if node == mr.nodes[mr.me] { 	// Skip ourself
  		continue
  	}
  	mr.lastPingTimes[node] = time.Now()
  	mr.nodeStates[node] = "alive"
  }

  // TODO remove me
  mr.jobDone = map[string]bool{}

  if rpcs != nil {
    rpcs.Register(mr)      // caller created RPC Server
  } else {
    rpcs = rpc.NewServer() // creates a new RPC server
    rpcs.Register(mr)      // Register exported methods of MapReduceNode with RPC Server         
    
    // Comm Structs
    gob.Register(PingArgs{})
    gob.Register(PingReply{})
    gob.Register(AssignTaskArgs{})
    gob.Register(AssignTaskReply{})
    gob.Register(TaskCompleteArgs{})
    gob.Register(TaskCompleteReply{})
    gob.Register(GetEmittedArgs{})
    gob.Register(GetEmittedReply{})

    // System Transferred Structs
    gob.Register(MapTask{})
    gob.Register(ReduceTask{})
    gob.Register(KVPair{})
    gob.Register(JobConfig{})

    // Client specified objects
    gob.Register(DemoMapper{})
    gob.Register(DemoReducer{})
    gob.Register(S3Inputer{})
    gob.Register(S3Outputer{})

    // Prepare node to receive connections

    if mode == "tcp" {
      debug(fmt.Sprintln("Making TCP mode MapReduceNode"))
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
      // mode assumed to be "unix"
      debug(fmt.Sprintln("Making unix mode MapReduceNode"))

      os.Remove(nodes[me]) // only needed for "unix"
      listener, error := net.Listen("unix", nodes[me]);
      if error != nil {
        log.Fatal("listen error: ", error);
      }
      mr.l = listener      // Set MapReduceNode listener
      
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

  // Spawn a thread to call mr.tick() every 250ms
  go func() {
    for mr.dead == false {
      mr.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return mr
}



package gomapreduce

/*
Task interface, MapTask and ReduceTask structs
*/

import (
	"fmt"
	"time"
)

type Task interface {
	getKind() string
	getId() string
	getJobId() string
	getMaster() string
	execute(Emitter)
	completed()
}


// Implements the Task interface
type MapTask struct {
	Id string                     // Task unique id (string for greater possibilities).
	Key string                    // Key to call the Mapper with.
	JobId string                  // Identifies the Job this Task corresponds to.
	Mapper Mapper                 // Implementation of Mapper interface.
	Inputer InputAccessor         // Allows worker to read its chunk of the input.
	Master string                 // Port name of the master node assigning the task.
	NetMode string                // 'unix' or 'tcp'
}

// MapTask Constructor
func makeMapTask(id string, key string, jobId string, mapper Mapper, 
	inputer InputAccessor, master string, netMode string) MapTask {

	return MapTask{Id: id, Key: key, JobId: jobId, Mapper: mapper, Inputer: inputer,
						Master: master, NetMode: netMode}
}

// Get the kind of Task
func (self MapTask) getKind() string {
	return "map"
}

// Get MapTask Id
func (self MapTask) getId() string {
	return self.Id
}

// Get Job Id
func (self MapTask) getJobId() string {
	return self.JobId
}

// Get the master node index
func (self MapTask) getMaster() string {
	return self.Master
}

/*
Executes the MapTask. Accepts an Emitter which can safely be used by the client Map
method to Emit key/value pairs.
*/ 
func (self MapTask) execute(emitter Emitter) {
	key := self.Key                       // Key associated with MapTask
	value := self.Inputer.GetValue(key)   // Read input value corresponding to key
	self.Mapper.Map(key, value, emitter)
	self.completed()
}

// Notify master that Job completed
func (self MapTask) completed() {
	var notified bool
	for !notified {
		args := TaskCompleteArgs{JobId: self.getJobId(), TaskId: self.getId()}
		var reply TaskCompleteReply
		debug(fmt.Sprintf("Sending TaskComplete: %s", self.Id))
		ok := call(self.Master, self.NetMode, "MapReduceNode.TaskCompleted", args, &reply)
		if ok && reply.OK {
			notified = true
		}
		time.Sleep(100 * time.Millisecond) 
	}
}




// Implements the Task interface
type ReduceTask struct {
	Id string                    // Task unqiue id (string for unlimited possibilities).
	Key interface{}              // Key to call the Reducer with.
	JobId string             
	Reducer Reducer              // Implementation of Reducer interface.
	// Intermediate
	// Outputer OutputerAccessor
	Master string                 // Port name of the master node assigning the task.
	NetMode string                // 'unix' or 'tcp'
	Nodes []string
}

// ReduceTask Constructor
func makeReduceTask(id string, key string, jobId string, reducer Reducer, 
	master string, netMode string, nodes []string) ReduceTask {
	return ReduceTask{Id: id, Key: key, JobId: jobId, Reducer: reducer, Master: master, NetMode: netMode, Nodes: nodes}
}

// Get the kind of Task
func (self ReduceTask) getKind() string {
	return "reduce"
}

// Get ReduceTask Id
func (self ReduceTask) getId() string {
	return self.Id
}

// Get Job Id
func (self ReduceTask) getJobId() string {
	return self.JobId
}

// Get the master node index
func (self ReduceTask) getMaster() string {
	return self.Master
}

// Execute the ReduceTask
func (self ReduceTask) execute(emitter Emitter) {
	fmt.Printf("Executing reduce task\n")
	values := make([]KVPair, 0)

	for _, node := range self.Nodes { 		// For each node, get the intermediate KVPairs that hash to your partition
		fmt.Printf("Get(%s, %s) from node %s\n", self.JobId, self.Key, node)
		args := &GetEmittedArgs{JobId: self.JobId, PartitionNumber: self.Key.(string)}

		var reply GetEmittedReply
		ok := call(node, self.NetMode, "MapReduceNode.Get", args, &reply)
		
		for !ok { 		// TODO make sure this doesn't loop forever
			time.Sleep(50 * time.Millisecond)
		}

		values = append(values, reply.KVPairs...)
	}

	fmt.Printf("All values: %v\n", values)

	self.Reducer.Reduce(values)
}

// 
func (self ReduceTask) completed() {
	var notified bool
	for !notified {
		args := TaskCompleteArgs{JobId: self.getJobId(), TaskId: self.getId()}
		var reply TaskCompleteReply
		debug(fmt.Sprintf("Sending TaskComplete: %s", self.Id))
		ok := call(self.Master, self.NetMode, "MapReduceNode.TaskCompleted", args, &reply)
		if ok && reply.OK {
			notified = true
		}
		time.Sleep(100 * time.Millisecond) 
	}
}


package gomapreduce

/*
Task interface, MapTask and ReduceTask structs
*/

import (
	"fmt"
	"time"
)

type Task interface {
	getId() string
	getKind() string
	getJobId() string
	getJobConfig() JobConfig
	getMaster() string
	execute(Emitter)
	completed()
}


// Implements the Task interface
type MapTask struct {
	Id string                     // Task unique id (string for greater possibilities).
	Key string                    // Key to call the Mapper with.
	JobId string                  // Identifies the Job this Task corresponds to.
	JobConfig JobConfig           // JobConfig for the Job this Task corresponds to.
	Mapper Mapper                 // Implementation of Mapper interface.
	Inputer InputAccessor         // Allows worker to read its chunk of the input.
	Master string                 // Port name of the master node assigning the task.
	NetMode string                // 'unix' or 'tcp'
}

// MapTask Constructor
func makeMapTask(id string, key string, jobId string, jobConfig JobConfig, 
	mapper Mapper, inputer InputAccessor, master string, netMode string) MapTask {

	return MapTask{Id: id, Key: key, JobId: jobId, JobConfig: jobConfig, 
		Mapper: mapper, Inputer: inputer, Master: master, NetMode: netMode}
}

// Get MapTask Id
func (self MapTask) getId() string {
	return self.Id
}

// Get the kind of Task
func (self MapTask) getKind() string {
	return "map"
}

// Get Job Id
func (self MapTask) getJobId() string {
	return self.JobId
}

// Get Job Config
func (self MapTask) getJobConfig() JobConfig {
	return self.JobConfig
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
	self.completed()                      // Try to notify Master that Task completed.
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
	PartitionNumber int          // Partition Number in range 0, 1, .. JobConfig.R.
	JobId string                 // Identifies the Job this Task corresponds to.
	JobConfig JobConfig          // JobConfig for the Job this Task corresponds to.  
	Reducer Reducer              // Implementation of Reducer interface.
	// Intermediate
	// Outputer OutputerAccessor
	Master string                 // Port name of the master node assigning the task.
	NetMode string                // 'unix' or 'tcp'
	Nodes []string
}

// ReduceTask Constructor
func makeReduceTask(id string, partitionNumber int, jobId string, jobConfig JobConfig,
	reducer Reducer, master string, netMode string, nodes []string) ReduceTask {

	return ReduceTask{Id: id, PartitionNumber: partitionNumber, JobId: jobId, 
		JobConfig: jobConfig, Reducer: reducer, Master: master, NetMode: netMode, 
		Nodes: nodes}
}

// Get ReduceTask Id
func (self ReduceTask) getId() string {
	return self.Id
}

// Get the kind of Task
func (self ReduceTask) getKind() string {
	return "reduce"
}

// Get Job Id
func (self ReduceTask) getJobId() string {
	return self.JobId
}

// Get Job Config
func (self ReduceTask) getJobConfig() JobConfig {
	return self.JobConfig
}

// Get the master node index
func (self ReduceTask) getMaster() string {
	return self.Master
}

// Execute the ReduceTask
// TODO: remove emitter and replace with outputer. Change mapreduce.go code accordingly.
func (self ReduceTask) execute(emitter Emitter) {
	fmt.Printf("Executing reduce task\n")
	allvalues := make([]KVPair, 0)
	// allvalues is a temporary name

	// Migrate to emittedReader
	/////////////////////////////////////////
	for _, node := range self.Nodes { 		// For each node, get the intermediate KVPairs that hash to your partition
		fmt.Printf("Get(%s, %s) from node %s\n", self.JobId, self.PartitionNumber, node)
		args := &GetEmittedArgs{JobId: self.JobId, PartitionNumber: self.PartitionNumber}

		var reply GetEmittedReply
		ok := call(node, self.NetMode, "MapReduceNode.Get", args, &reply)
		
		for !ok { 		// TODO make sure this doesn't loop forever
			time.Sleep(50 * time.Millisecond)
		}

		allvalues = append(allvalues, reply.KVPairs...)
	}

	fmt.Printf("All values: %v\n", allvalues)
	/////////////////////////////

	
	// Sort and Group by Intermediate Keys.
	uniqueKeys := make(map[string]int)   // Serves as mathematical Set
	for _, pair := range allvalues {
		uniqueKeys[pair.Key] = 1
	}
	fmt.Println("unique keys", uniqueKeys)
	for key, _ := range uniqueKeys {
		fmt.Println(key)
		// Collect values from KVPairs with a particular Key
		values := make([]interface{}, 0)
		for _, pair := range allvalues {
			values = append(values, pair.Value)
		}
		fmt.Println(values)
		// Call Reduce on groups of KVPairs with the same key.
		self.Reducer.Reduce(key, values)
	}
	fmt.Println(uniqueKeys)
	//self.Reducer.Reduce(values)
	//self.completed()
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


package gomapreduce

/*
Task interface, MapTask and ReduceTask structs
*/

import (
	"fmt"      // temporary
)

type Task interface {
	getKind() string
	getId() string
	getMaster() string
	execute()
	completed()
}


// Implements the Task interface
type MapTask struct {
	Id string                     // Task unique id (string for greater possibilities).
	Key string                    // Key to call the Mapper with.
	Mapper Mapper                 // Implementation of Mapper interface.
	Inputer InputAccessor         // Allows worker to read its chunk of the input.
	Emitter IntermediateAccessor  // Allows worker to emit intermediate pairs.
	Master string                 // Port name of the master node assigning the task.
	NetMode string                // 'unix' or 'tcp'
}

// MapTask Constructor
func makeMapTask(id string, key string, mapper Mapper, inputer InputAccessor, 
	emitter IntermediateAccessor, master string, netMode string) MapTask {

	return MapTask{Id: id, Key: key, Mapper: mapper, Inputer: inputer,
				  	Emitter: emitter, Master: master, NetMode: netMode}
}

// Get MapTask Id
func (self MapTask) getId() string {
	return self.Id
}

// Get the master node index
func (self MapTask) getMaster() string {
	return self.Master
}

// Get the kind of Task
func (self MapTask) getKind() string {
	return "map"
}

// Execute the MapTask
func (self MapTask) execute() {
	key := self.Key                       // Key associated with MapTask
	value := self.Inputer.GetValue(key)   // Read input value corresponding to key
	self.Mapper.Map(key, value, self.Emitter)
	self.completed()
}

// Notify master that Job completed
func (self MapTask) completed() {
	fmt.Println("Notify master that task is completed", self.Master, self.NetMode)
}




// Implements the Task interface
type ReduceTask struct {
	Id string                    // Task unqiue id (string for unlimited possibilities).
	Key interface{}              // Key to call the Reducer with.
	Reducer Reducer              // Implementation of Reducer interface.
	// Intermediate
	// Outputer OutputerAccessor
	Master string                 // Port name of the master node assigning the task.
	NetMode string                // 'unix' or 'tcp'
}

// ReduceTask Constructor
func makeReduceTask(id string, key string, reducer Reducer, master string, 
	netMode string) ReduceTask {
	return ReduceTask{Id: id, Key: key, Reducer: reducer, Master: master, NetMode: netMode}
}

// Get ReduceTask Id
func (self ReduceTask) getId() string {
	return self.Id
}

// Get the master node index
func (self ReduceTask) getMaster() string {
	return self.Master
}

// Get the kind of Task
func (self ReduceTask) getKind() string {
	return "reduce"
}

// Execute the ReduceTask
func (self ReduceTask) execute() {
	//TODO
}

// Notify master that Job completed
func (self ReduceTask) completed() {
	// TODO
}


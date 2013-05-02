package gomapreduce

/*
Task interface, MapTask and ReduceTask structs
*/

type Task interface {
	getKind() string
	getId() string
	getMaster() int
	execute()
}


// Implements the Task interface
type MapTask struct {
	Id string              // Task unique id (string for greater possibilities).
	Master int             // Index of master node assigning the Task.
	Key string             // Key to call the Mapper with.
	Mapper Mapper          // Implementation of Mapper interface.
	Inputer InputAccessor  // Allows worker to read its chunk of the input
	IntermediateAccessor IntermediateAccessor
}

// MapTask Constructor
func makeMapTask(id string, master int, key string, mapper Mapper, 
	inputer InputAccessor, intermediateAccessor IntermediateAccessor) MapTask {

	return MapTask{Id: id, Master: master, Key: key, Mapper: mapper, Inputer: inputer,
				   IntermediateAccessor: intermediateAccessor}
}

// Get MapTask Id
func (self MapTask) getId() string {
	return self.Id
}

// Get the master node index
func (self MapTask) getMaster() int {
	return self.Master
}

// Get the kind of Task
func (self MapTask) getKind() string {
	return "map"
}

// Execute the MapTask
func (self MapTask) execute() {
	key := self.Key
	value := self.Inputer.GetValue(key)
	self.Mapper.Map(key, value, self.IntermediateAccessor)
}



// Implements the Task interface
type ReduceTask struct {
	Id string           // Task unqiue id (string for unlimited possibilities).
	Master int          // Index of master node assigning the Task.
	Key interface{}     // Key to call the Reducer with.
	Reducer Reducer     // Implementation of Reducer interface.
}

// ReduceTask Constructor
func makeReduceTask(id string, key string, reducer Reducer) ReduceTask {
	return ReduceTask{Id: id, Key: key, Reducer: reducer}
}

// Get ReduceTask Id
func (self ReduceTask) getId() string {
	return self.Id
}

// Get the master node index
func (self ReduceTask) getMaster() int {
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


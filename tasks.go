package gomapreduce

/*
Task interface, MapTask and ReduceTask structs
*/

type Task interface {
	getId() string
	getKind() string
	getMaster() int
	//execute()
}


// Implements the Task interface
type MapTask struct {
	Id string           // Task unique id (string for greater possibilities).
	Master int          // Index of master node assigning the Task.
	Key interface{}         // Key to call the Mapper with.
	Mapper Mapper       // Implementation of Mapper interface.
}

// MapTask Constructor
func makeMapTask(id string, key string, mapper Mapper) MapTask {
	return MapTask{Id: id, Key: key, Mapper: mapper}
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


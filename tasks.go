package gomapreduce

/*
Task interface, MapTask and ReduceTask structs
*/

type Task interface {
	getId() string
}


// Implements the Task interface
type MapTask struct {
	Id string           // String unique id for unlimited possibilities.
	Key string          // Input key.
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



// Implements the Task interface
type ReduceTask struct {
	Id string              // String unqiue id for unlimited possibilities.
	Key string          // Intermediate key.
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
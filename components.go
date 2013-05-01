package gomapreduce



// State associated with an individual MapReduce job requested by a client.
type Job struct{
	job_id string       // String unique id for unlimited possibilities.
	finished bool       // Whether the instance is finished
	master int          // The node acting as master for the instance
	status string       // "starting", "working", "done"
	mapper Mapper       // mapper to be used for this Job
	reducer Reducer     // reducer to be used for this Job
	inputAccessor InputAccessor
	outputAccessor OutputAccessor
}



func (self *Job) get_id() string {
	return self.job_id
}

func (self *Job) is_done() bool {
	return self.status == "done"
}




type Task interface {
	get_id() int
}

type MapTask struct {
	Id string           // String unique id for unlimited possibilities.
	Key string          // Input key.
	Mapper Mapper       // Implementation of Mapper interface.
}

func makeMapTask(id string, key string, mapper Mapper) MapTask {
	return MapTask{Id: id, Key: key, Mapper: mapper}
}


type ReduceTask struct {
	Id string              // String unqiue id for unlimited possibilities.
	Key string          // Intermediate key.
	Reducer Reducer     // Implementation of Reducer interface.
}

func makeReduceTask(id string, key string, reducer Reducer) ReduceTask {
	return ReduceTask{Id: id, Key: key, Reducer: reducer}
}



// Tuple for looking up intermediate results
type MediateTuple struct {
	Job_num string
	Key string
}


type Pair struct {
	Key interface{}
	Value interface{}
}

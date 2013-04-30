package gomapreduce



// State associated with an individual MapReduce job requested by a client.
type Job struct{
	job_id string       // String unique id for unlimited possibilities.
	finished bool       // Whether the instance is finished
	master int          // The node acting as master for the instance
	status string       // "starting", "working", "done"
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
	id string           // String unique id for unlimited possibilities.  
	mapper Mapper       // Implementation of Mapper interface.
}


type ReduceTask struct {
	id int              // String unqiue id for unlimited possibilities.
	reducer Reducer     // Implementation of Reducer interface.
}

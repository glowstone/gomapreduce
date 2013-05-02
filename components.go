package gomapreduce



// State associated with an individual MapReduce job requested by a client.
type Job struct{
	job_id string       // String unique id for unlimited possibilities.
	status string       // "starting", "working", "done"
	master int          // Index of node acting as the Job master
	mapper Mapper       // mapper to be used for this Job
	reducer Reducer     // reducer to be used for this Job
	inputAccessor InputAccessor
	outputAccessor OutputAccessor
}

func makeJob(job_id string, status string, master int, mapper Mapper, 
	reducer Reducer, inputer InputAccessor, outputer OutputAccessor) Job {
	return Job{job_id: job_id,    
             status: status,
             master: master,
             mapper: mapper,
             reducer: reducer,
             inputAccessor: inputer,
             outputAccessor: outputer,
            }
}

func (self *Job) getId() string {
	return self.job_id
}

func (self *Job) isDone() bool {
	return self.status == "done"
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

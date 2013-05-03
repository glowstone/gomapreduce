package gomapreduce



// State associated with an individual MapReduce job requested by a client.
type Job struct{
	jobId string       // String unique id for unlimited possibilities.
	status string       // "starting", "working", "done"
	master int          // Index of node acting as the Job master
	mapper Mapper       // mapper to be used for this Job
	reducer Reducer     // reducer to be used for this Job
	inputer InputAccessor
	intermediateAccessor IntermediateAccessor
	outputer OutputAccessor
}

func makeJob(jobId string, status string, master int, mapper Mapper, 
	reducer Reducer, inputer InputAccessor, intermediateAccessor IntermediateAccessor, outputer OutputAccessor) Job {
	return Job{jobId: jobId,    
             status: status,
             master: master,
             mapper: mapper,
             reducer: reducer,
             inputer: inputer,
             intermediateAccessor: intermediateAccessor,
             outputer: outputer,
            }
}

func (self *Job) getId() string {
	return self.jobId
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

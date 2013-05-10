package gomapreduce



// State associated with an individual MapReduce job requested by a client.
type Job struct{
	jobId string               // String unique id for unlimited possibilities.
	mapper Mapper              // mapper to be used for this Job
	reducer Reducer            // reducer to be used for this Job
	inputer InputAccessor
	outputer OutputAccessor
}

func makeJob(jobId string, mapper Mapper, reducer Reducer, inputer InputAccessor, 
	outputer OutputAccessor) Job {
	return Job{jobId: jobId,    
             mapper: mapper,
             reducer: reducer,
             inputer: inputer,
             outputer: outputer,
            }
}

func (self *Job) getId() string {
	return self.jobId
}

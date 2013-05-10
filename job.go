package gomapreduce



// An individual MapReduce job requested by a client application.
type Job struct{
	jobId string               // String unique id for unlimited possibilities.
	mapper Mapper              // mapper to be used for this Job
	reducer Reducer            // reducer to be used for this Job
	inputer Inputer            // Input data reader
	outputer Outputer          // Output data writer
}

func makeJob(jobId string, mapper Mapper, reducer Reducer, inputer Inputer, 
	outputer Outputer) Job {
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

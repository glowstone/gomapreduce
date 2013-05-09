package gomapreduce

/*
The emitter provides a thin wrapper around a MapReduceNode's emittedStorage,
giving Mapper Map methods access to an Emit method but preventing other accesses
to storage which is internal to the MapReduceNode.
*/

import (

)

/*
An Emitter allows Mappers the ability to Emit (i.e. write) intermediate key/value 
pairs to be written to the executing MapReduceNode's internal emittedStorage.
*/
type Emitter interface {
	Emit(key string, value interface{})
}

/* Simple Emitter implementation */

type SimpleEmitter struct {
	jobId string             // Job identifier the Emitter should emit KVPairs to.
	jobConfig JobConfig      // Job Config passed to emittedStorage to hash intermediate keys modulo R
	emittedStorage *EmittedStorage      // Pointer to an emittedStorage instance
}

// SimpleEmitter Constructor
func makeSimpleEmitter(jobId string, jobConfig JobConfig, 
	emittedStorage *EmittedStorage) SimpleEmitter {

	se := SimpleEmitter{jobId: jobId, 
		jobConfig: jobConfig,
		emittedStorage: emittedStorage}
	return se
}

func (self SimpleEmitter) Emit(key string, value interface{}) {
	kvpair := KVPair{Key: key, Value: value}
	self.emittedStorage.putEmitted(self.jobId, self.jobConfig, kvpair)
}
	


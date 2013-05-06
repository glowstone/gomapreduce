package gomapreduce

// Emitted Intermediate Store for the Node

import (
	"sync"
)

// Representation of a Key Value Pair
type KVPair struct {
	Key string
	Value interface{}
}



type EmittedStore struct {
	mu sync.Mutex                           // Singleton mutex for storage system
	storage map[string]map[string][]KVPair  // Maps jobID -> taskId -> []KVPair (slice)
}

// EmittedStore Constructor
func makeEmittedStore() EmittedStore {
	es := EmittedStore{}
	return es
}

/*
Adds an individual emitted intermediate KVPair corresponding to a particular jobId 
and taskId.
*/
func (self *EmittedStore) putEmitted(jobId string, taskId string, pair KVPair) {
	//TODO - locking for safe writes
	if _, present := self.storage[jobId]; !present {
		self.storage[jobId] = make(map[string][]KVPair)
	}
	if _, present := self.storage[jobId][taskId]; !present {
		self.storage[jobId][taskId] = make([]KVPair,0)
	}
	slicePairs := self.storage[jobId][taskId]
	slicePairs = append(slicePairs, pair)
}

/*
Retrieves all the emitted intermediate KVPairs corresponding to a particular jobId and 
taskId
*/
func (self *EmittedStore) getEmitted(jobId string, taskId string) []KVPair {
	// TODO locking for safe reads
	if _, present := self.storage[jobId]; present {
		if _, present := self.storage[jobId][taskId]; present {
			return self.storage[jobId][taskId]
		}
		return make([]KVPair,0)
	}
	return make([]KVPair,0)
}
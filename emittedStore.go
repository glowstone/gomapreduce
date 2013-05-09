package gomapreduce

// Emitted Intermediate Store for the Node

import (
	"sync"
	"hash/adler32"
)

// Representation of a Key Value Pair
type KVPair struct {
	Key string
	Value interface{}
}



type EmittedStorage struct {
	mu sync.Mutex                           // Singleton mutex for storage system
	storage map[string]map[int][]KVPair     // Maps jobID -> PartitionNumber -> []KVPair (slice)
}

// EmittedStore Constructor
func makeEmittedStorage() EmittedStorage {
	es := EmittedStorage{}
	es.storage = make(map[string]map[int][]KVPair)	// Maps jobId -> partitionNumber(hashed intermediate key) -> slice of KVPairs
	return es
}

/*
Adds an individual emitted intermediate KVPair corresponding to a particular jobId 
and taskId. A jobCofnig must be passed because the hash-mod operation performed
to partition KVPairs into groups varies per Job.
*/
func (self *EmittedStorage) putEmitted(jobId string, jobConfig JobConfig, pair KVPair) {
	self.mu.Lock()
	defer self.mu.Unlock()

	// jobConfig.R now available
	partitionNumber := int(adler32.Checksum([]byte(pair.Key)) % uint32(2))		// TODO Mod R

	if _, present := self.storage[jobId]; !present {
		// Create string -> []KVPair map for the jobId
		self.storage[jobId] = make(map[int][]KVPair)
	}
	if _, present := self.storage[jobId][partitionNumber]; !present {
		// Create []KVPair slice for the hashedKey
		self.storage[jobId][partitionNumber] = make([]KVPair,0)
	}
	slicePairs := self.storage[jobId][partitionNumber]
	slicePairs = append(slicePairs, pair)
	self.storage[jobId][partitionNumber] = slicePairs
}

/*
Retrieves all the emitted intermediate KVPairs corresponding to a particular jobId 
and partitionNumber. Returns a []KVPair slice or, if no entry corresponds to the 
given jobId, partitionNumber pair, an empty slice is returned.
*/
func (self *EmittedStorage) getEmitted(jobId string, partitionNumber int) []KVPair {
	self.mu.Lock()
	defer self.mu.Unlock()

	if _, present := self.storage[jobId]; present {
		if _, present := self.storage[jobId][partitionNumber]; present {
			return self.storage[jobId][partitionNumber]
		}
		return make([]KVPair,0)
	}
	return make([]KVPair,0)
}
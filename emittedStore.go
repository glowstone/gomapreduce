package gomapreduce

// Emitted Intermediate Store for the Node

import (
	"sync"
	"hash/adler32"
	"strconv"
)

// Representation of a Key Value Pair
type KVPair struct {
	Key string
	Value interface{}
}



type EmittedStorage struct {
	mu sync.Mutex                           // Singleton mutex for storage system
	storage map[string]map[string][]KVPair  // Maps jobID ->  -> []KVPair (slice)
}

// EmittedStore Constructor
func makeEmittedStorage() EmittedStorage {
	es := EmittedStorage{}
	es.storage = make(map[string]map[string][]KVPair)	// Maps jobId -> partitionNumber(hashed intermediate key) -> slice of KVPairs
	return es
}

/*
Adds an individual emitted intermediate KVPair corresponding to a particular jobId 
and taskId.
*/
func (self *EmittedStorage) putEmitted(jobId string, pair KVPair) {
	self.mu.Lock()
	defer self.mu.Unlock()

	partitionNumber := strconv.Itoa(int(adler32.Checksum([]byte(pair.Key)) % uint32(2))) 		// TODO Mod R

	if _, present := self.storage[jobId]; !present {
		// Create string -> []KVPair map for the jobId
		self.storage[jobId] = make(map[string][]KVPair)
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
Retrieves all the emitted intermediate KVPairs corresponding to a particular jobId and 
taskId
*/
func (self *EmittedStorage) getEmitted(jobId string, partitionNumber string) []KVPair {
	// TODO locking for safe reads
	if _, present := self.storage[jobId]; present {
		if _, present := self.storage[jobId][partitionNumber]; present {
			return self.storage[jobId][partitionNumber]
		}
		return make([]KVPair,0)
	}
	return make([]KVPair,0)
}
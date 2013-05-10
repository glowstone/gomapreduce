package gomapreduce

/*
A structure enclosing the functionality of contacting known MapReduceNodes to 
retrieve emitted intermediate pairs with a particular jobId and partitionNumber.
*/

import (
	"fmt"        // temporary
	"time"
)

/* 
An EmittedReader reads all intermediate key/values pairs known to the cluster
of MapReduceNodes and having a given jobId and partitionNumber.
*/
type EmittedReader interface {
	ReadEmitted(jobId string, partitionNumber int) []KVPair	
}

type SimpleEmittedReader struct {
	nodes []string             // Should become LiveTable (need better name)
	netMode string             // 'unix' or 'tcp'
}

// SimpleEmittedReader Constructor
func makeSimpleEmittedReader(nodes []string, netMode string) SimpleEmittedReader {
	er := SimpleEmittedReader{nodes: nodes, netMode: netMode}
	return er
} 

/*
Note: If not all nodes can be contacted.... TODO
*/
func (self SimpleEmittedReader) ReadEmitted(jobId string, partitionNumber int) []KVPair {
	kvpairs := make([]KVPair, 0)

	for _, node := range self.nodes { 		// For each node, get the intermediate KVPairs that hash to your partition
		debug(fmt.Sprintf("Get(%s, %s) from node %s\n", jobId, partitionNumber, node))
		args := &GetEmittedArgs{JobId: jobId, PartitionNumber: partitionNumber}

		var reply GetEmittedReply
		ok := call(node, self.netMode, "MapReduceNode.Get", args, &reply)
		
		for !ok { 		// TODO make sure this doesn't loop forever
			time.Sleep(50 * time.Millisecond)
		}

		kvpairs = append(kvpairs, reply.KVPairs...)
	}

	// fmt.Printf("All values: %v\n", kvpairs)
	return kvpairs
}

// TODO: Wizardry may be needed to make this tolerant of MapTask failures and
// down servers

package gomapreduce

/*
The Mapper interface is typically implemented by clients of the mapreduce 
service to define the 'map' operation used in a Map Reduce job.
http://research.google.com/archive/mapreduce.html
*/

import "fmt"         // temporary

type Mapper interface {
	// 'map' is keyword and 'Map' may lead to confusion
	Map_action(interface{}) interface{}
}



// Demo Mapper implementation
type DemoMapper struct {
	Id int
}

// Perform the demo map operation as part of the demo MapReduce job.
func (self DemoMapper) Map_action(interface{}) interface{} {
	fmt.Println("Performing DemoMapper action")
	return 5
}
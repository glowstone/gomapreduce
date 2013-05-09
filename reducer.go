package gomapreduce

/*
The Reducer interface is typically implemented by clients of the mapreduce 
service to define the 'reduce' operation used in a Map Reduce job.
http://research.google.com/archive/mapreduce.html
*/

import "fmt"         // temporary

type Reducer interface {
	Reduce(interface{}) interface{}
}


/*
Demo Reducer implementation used for performing demo MapReduce jobs.
*/
type DemoReducer struct {

}

/*
Accepts a string key and returns the total int number of occurances of that key
over all intermediate key/value pairs with the given key.
*/
func (self DemoReducer) Reduce(data interface{}) interface{} {
	//key := data.(string)                  // type assertion

	// TODO fetch all intermediate data via an intermediateAccessor implementation
	//intermediate := list
	//map[string]int{is:3, is:4, is:1, is:1}

	fmt.Println("DemoReducer doing reduce")

	wordCounts := make(map[string]int)
	dataSlice := data.([]KVPair)
	for _, pair := range dataSlice {
		// fmt.Printf("Pair: %v\n", pair)
		key := pair.Key
		value := pair.Value
		wordCounts[key] += value.(int)
	}
	fmt.Printf("\nOUTPUT!!!: %v\n\n", wordCounts)
	return nil
}
package gomapreduce

/*
The Reducer interface is typically implemented by clients of the mapreduce 
service to define the 'reduce' operation used in a Map Reduce job.
http://research.google.com/archive/mapreduce.html
*/

import (
	"fmt"      // temporary
)

type Reducer interface {
	Reduce(interface{}) interface{}
}


/*
Demo Reducer impelementation. Aggregates the counts of particular words. The Reduce
method is executed in a ReduceTask, which is responsible for aggregating the word
counts of all words (words used as intermediate keys) which hash to the same value
modulo R. 
*/
type DemoReducer struct {

}

/*
Accepts a string key and returns the total int number of occurances of that key
over all intermediate key/value pairs with the given key.
*/
func (self DemoReducer) Reduce(data interface{}) interface{} {
	
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
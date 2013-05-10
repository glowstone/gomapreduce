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
	Reduce(key string, values []interface{}, outputer OutputAccessor)          // TODO, add Otuputer
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
Accepts a word key and a list of the different counts of occurances of that word
in the text chunks handled by different MapTasks. Uses the given Outputer to
output the resulting total as 'word: total count'
*/
func (self DemoReducer) Reduce(key string, data []interface{}, outputer OutputAccessor) {
	total := 0
	for _, count := range data {
		total += count.(int)
	}
	result := fmt.Sprintf("%s: %d", key, total)
	debug(result)
	outputer.Output(key, total)
}
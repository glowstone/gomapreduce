package gomapreduce

/*
The Mapper interface is typically implemented by clients of the mapreduce 
service to define the 'map' operation used in a Map Reduce job.
http://research.google.com/archive/mapreduce.html
*/

import "fmt"         // temporary
import "strings"

type Mapper interface {
	Map(key string, value interface{}, intermediateAccessor IntermediateAccessor)
}


// Demo Mapper implementation. Returns the int number of words in a given
// string.
type DemoMapper struct {
	
}

// Perform the demo map operation as part of the demo MapReduce job.
func (self DemoMapper) Map(key string, value interface{}, intermediateAccessor IntermediateAccessor) {
	text := value.(string)                 // type assertion
	intermediate := make(map[string]int)   // intermediate data

	words := strings.Split(text, " ")
	// fmt.Println(words)
	for _, word := range words {
		if _, present := intermediate[word]; present {
			intermediate[word] += 1
		} else {
			intermediate[word] = 1
		}
	}
	fmt.Println("DemoMapper doing map")   // temporary
	fmt.Println("Mapper got: %s\n", intermediate)

	for key, value := range intermediate {
		intermediateAccessor.Emit(key, value)
	}
	// return intermediate                   // map[string]int, intermediate key -> value
}
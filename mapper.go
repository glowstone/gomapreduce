package gomapreduce

/*
The Mapper interface is typically implemented by clients of the mapreduce 
service to define the 'map' operation used in a Map Reduce job.
http://research.google.com/archive/mapreduce.html
*/

import "fmt"         // temporary
import "strings"

type Mapper interface {
	Map(interface{}) interface{}
}


// Demo Mapper implementation. Returns the int number of words in a given
// string.
type DemoMapper struct {
	
}

// Perform the demo map operation as part of the demo MapReduce job.
func (self DemoMapper) Map(data interface{}) interface{} {
	text := data.(string)                 // type assertion
	intermediate := make(map[string]int)  // intermediate data

	words := strings.Split(text, " ")
	fmt.Println(words)
	for _, word := range words {
		if _, present := intermediate[word]; present {
			intermediate[word] += 1
		} else {
			intermediate[word] = 1
		}
	}
	fmt.Println("DemoMapper doing map")   // temporary
	return intermediate                   // map[string]int, intermediate key -> value
}
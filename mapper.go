package gomapreduce

/*
The Mapper interface is typically implemented by clients of the mapreduce 
service to define the 'map' operation used in a Map Reduce job.
http://research.google.com/archive/mapreduce.html
*/

import (
	"strings"
)

type Mapper interface {
	/*
	Accepts string key and data value interface{} to be used in the 'map' 
	operation. An emitter is provided and should be used to emit generated 
	intermediate pairs.
	*/
	Map(key string, value interface{}, emitter Emitter)
}

/*
Demo Mapper implementation. Used to count the number of occurances of different
 words.
*/
type DemoMapper struct {}

/*
Counts the number of occurances of each word in the given text value and emits
intermediate pairs <word, word(count)> after processing the text.
*/
func (self DemoMapper) Map(key string, value interface{}, emitter Emitter) {
	text := value.(string)                // type assertion
	wordCounts := make(map[string]int)

	words := strings.Fields(text)
	for _, word := range words {
		if _, present := wordCounts[word]; present {
			wordCounts[word] += 1
		} else {
			wordCounts[word] = 1
		}
	}

	// Mapper can emit (key, value) Pairs as soon as they are ready
	for key, value := range wordCounts {
		emitter.Emit(key, value)
	}
}
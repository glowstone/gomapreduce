package gomapreduce

/*
Specification of the Ouputer interface
*/

import (
	"strconv"
)

type Outputer interface {
	Output(key string, value interface{})  // Outputs value to a location 'key' on a filesystem. 
}



// S3Outputer implementation of the Outputer interface

type S3Outputer struct {
	OutputFolder string
}

// S3Outputer Constructor
func MakeS3Outputer(outputFolder string) S3Outputer {
	return S3Outputer{OutputFolder: outputFolder}
}


func (self S3Outputer) Output(key string, value interface{}) {
	bucket := GetBucket()
	key = self.OutputFolder + "/" + key
	valueString := strconv.Itoa(value.(int))
	bucket.StoreObject(key, []byte(valueString))
}


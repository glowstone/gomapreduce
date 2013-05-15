package gomapreduce

/*
Specification of the Inputer interface
*/

import (
	"strings"
)

type Inputer interface {
	ListKeys() []string 			// A method to list all of the keys (i.e. all of the map jobs).
	GetValue(key string) string 	// A method to get the value for one of the keys.
}



// S3Inputer implementation of the Inputer interface

type S3Inputer struct { 	// Satisfies the InputAccessor interface
	Folder string
}

// S3 Inputer Constructor
func MakeS3Inputer (inputFolder string) S3Inputer {	
	if !strings.HasSuffix(inputFolder, "/") { 	// Make sure that the folder has a trailing slash
		inputFolder += "/"
	}
	accessor := S3Inputer{Folder: inputFolder}
	return accessor
}

func (self S3Inputer) ListKeys() []string {
	bucket := GetBucket()
	keys := FilterKeysByPrefix(bucket, self.Folder)
	return keys
}

func (self S3Inputer) GetValue(key string) string {
	bucket := GetBucket()
	value := GetObject(bucket, key)
	return string(value)
}
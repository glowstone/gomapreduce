package gomapreduce


import (
	"fmt"
	"strings"
	"github.com/jacobsa/aws/s3"
	"encoding/gob"
)


type InputAccessor interface {
	ListKeys() []string 			// A method to list all of the keys (i.e. all of the map jobs)
	GetValue(key string) string 	// A method to get the value for one of the keys
}

type S3Accessor struct { 	// Satisfies the InputAccessor interface
	Bucket s3.Bucket
	Folder string
}

func (self S3Accessor) ListKeys() []string {
	keys := FilterKeysByPrefix(self.Bucket, self.Folder)
	return keys
}

func (self S3Accessor) GetValue(key string) string {
	value := GetObject(self.Bucket, key)
	return string(value)
}

// Creates an S3Accessor
func makeS3Accessor (inputFolder string) S3Accessor {	
	//gob.Register(s3.bucket{})
	if !strings.HasSuffix(inputFolder, "/") { 	// Make sure that the folder has a trailing slash
		inputFolder += "/"
	}
	fmt.Printf("Folder: %s\n", inputFolder)
	bucket := GetBucket()
	accessor := S3Accessor{Bucket: bucket, Folder: inputFolder}

	return accessor
}



// perhaps these go in a separate file? Not sure how big they become
type OutputAccessor interface {
	//TODO
}

type S3Outputer struct {

}

type IntermediateAccessor interface{
	//TODO
}

package gomapreduce


import (
	"fmt"
	"strings"
	"github.com/jacobsa/aws/s3"
)


type InputAccessor interface {
	listKeys() []string 			// A method to list all of the keys (i.e. all of the map jobs)
	getValue(key string) string 	// A method to get the value for one of the keys
}

type S3Accessor struct { 	// Satisfies the InputAccessor interface
	bucket s3.Bucket
	folder string
}

func (self S3Accessor) listKeys() []string {
	keys := FilterKeysByPrefix(self.bucket, self.folder)
	return keys
}

func (self S3Accessor) getValue(key string) string {
	value := GetObject(self.bucket, key)
	return string(value)
}

// Creates an S3Accessor
func makeS3Accessor (inputFolder string) S3Accessor {
	if !strings.HasSuffix(inputFolder, "/") { 	// Make sure that the folder has a trailing slash
		inputFolder += "/"
	}
	fmt.Printf("Folder: %s\n", inputFolder)
	bucket := GetBucket()
	accessor := S3Accessor{bucket: bucket, folder: inputFolder}

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

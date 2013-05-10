package gomapreduce


import (
	"fmt"
	"strings"
	"strconv"
)


type InputAccessor interface {
	ListKeys() []string 			// A method to list all of the keys (i.e. all of the map jobs)
	GetValue(key string) string 	// A method to get the value for one of the keys
}

type S3Accessor struct { 	// Satisfies the InputAccessor interface
	Folder string
}

func (self S3Accessor) ListKeys() []string {
	bucket := GetBucket()
	keys := FilterKeysByPrefix(bucket, self.Folder)
	return keys
}

func (self S3Accessor) GetValue(key string) string {
	bucket := GetBucket()
	value := GetObject(bucket, key)
	return string(value)
}

// Creates an S3Accessor
func MakeS3Accessor (inputFolder string) S3Accessor {	
	if !strings.HasSuffix(inputFolder, "/") { 	// Make sure that the folder has a trailing slash
		inputFolder += "/"
	}
	fmt.Printf("Folder: %s\n", inputFolder)
	accessor := S3Accessor{Folder: inputFolder}

	return accessor
}



// perhaps these go in a separate file? Not sure how big they become
type OutputAccessor interface {
	//TODO
	Output(string, interface{})
}

type S3Outputer struct {
	OutputFolder string
}

func (self S3Outputer) Output(key string, value interface{}) {
	bucket := GetBucket()
	key = self.OutputFolder + "/" + key
	valueString := strconv.Itoa(value.(int))
	bucket.StoreObject(key, []byte(valueString))
}

func MakeS3Outputer(outputFolder string) S3Outputer {
	return S3Outputer{OutputFolder: outputFolder}
}
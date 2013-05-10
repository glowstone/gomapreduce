package gomapreduce


import (
	"strconv"
	"strings"
	"fmt"
)

type Inputer interface {
	ListKeys() []string 			// A method to list all of the keys (i.e. all of the map jobs)
	GetValue(key string) string 	// A method to get the value for one of the keys
}

type S3Inputer struct { 	// Satisfies the InputAccessor interface
	Folder string
}

// S3 Inputer Constructor
func MakeS3Inputer (inputFolder string) S3Inputer {	
	if !strings.HasSuffix(inputFolder, "/") { 	// Make sure that the folder has a trailing slash
		inputFolder += "/"
	}
	fmt.Printf("Folder: %s\n", inputFolder)
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



// perhaps these go in a separate file? Not sure how big they become
type Outputer interface {
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
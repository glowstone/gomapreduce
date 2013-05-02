package gomapreduce


import (
	"fmt"
	"strings"
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
func makeS3Accessor (inputFolder string) S3Accessor {	
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
}

type S3Outputer struct {

}

type IntermediateAccessor interface{
	//TODO
	Emit(key string, value interface{})
	ReadIntermediateValues(key string) []interface{} 	// There might be multiple values associated with an intermediate key, even on this one node
}

type SimpleIntermediateAccessor struct {

}

func (self SimpleIntermediateAccessor) Emit(key string, value interface{}) {
	fmt.Printf("Emit(%s, %d)\n", key, value)
}

func (self SimpleIntermediateAccessor) ReadIntermediateValues(key string) []interface{} {
	fmt.Println("Writing intermediate pair")
	return nil
}
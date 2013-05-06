package gomapreduce


import (
	"fmt"
	"strings"
	"hash/adler32"
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
}

type S3Outputer struct {

}

func MakeS3Outputer() S3Outputer {
	return S3Outputer{}
}


type IntermediateAccessor interface{
	Emit(key string, value interface{})
	ReadIntermediateValues(key string) []interface{} 	// There might be multiple values associated with an intermediate key, even on this one node
}

type SimpleIntermediateAccessor struct {
	EmittedStore *EmittedStore
}

func MakeSimpleIntermediateAccessor() SimpleIntermediateAccessor {
	return SimpleIntermediateAccessor{makeEmittedStore()}
}


func (self SimpleIntermediateAccessor) Emit(key string, value interface{}) {
	fmt.Printf("Emit(%s, %d)\n", key, value)

	partitionNumber := strconv.Itoa(int(adler32.Checksum([]byte(key)) % uint32(2))) 		// TODO Mod R

	JobId := 0		// TODO pass this in

	pair := IntermediatePair{key, value}

	_, present := self.EmittedStore.Storage[JobId]		// If the internal map doesn't exist yet, make one
	if !present {
		m := make(map[string][]IntermediatePair)
		self.EmittedStore.Storage[JobId] = m
	}

	values, present := self.EmittedStore.Storage[JobId][partitionNumber]
	if present { 		// If the slice of values exists, append to it
		values = append(values, pair)
		fmt.Printf("Values: %v\n", values)
	} else { 			// Otherwise create the slice and append the value to it
		values = make([]IntermediatePair, 0)
		values = append(values, pair)
		fmt.Printf("(%s, %v)\n", partitionNumber, values)
	}
	self.EmittedStore.Storage[JobId][partitionNumber] = values
}

func (self SimpleIntermediateAccessor) ReadIntermediateValues(key string) []interface{} {
	fmt.Printf("Reading intermediate values for key %s\n", key)
	fmt.Printf("Values: %s\n", self.EmittedStore.Storage[0][key]) 	// TODO JobId
	return nil
}

// Struct for storing emitted values (intermediate key/value pairs)
type EmittedStore struct {
	Storage map[int]map[string][]IntermediatePair 	// Maps jobId -> intermediate key -> Intermediate Pairs
}

func makeEmittedStore() *EmittedStore {
	m := make(map[int]map[string][]IntermediatePair)
	return &EmittedStore{m}
}

type IntermediatePair struct {
	Key string
	Value interface{}
}
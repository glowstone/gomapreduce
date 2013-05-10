package gomapreduce


import (
	"fmt"
	"strings"
	"strconv"
)








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
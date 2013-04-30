package gomapreduce

import (
	"strings"
	"fmt"
	"github.com/jacobsa/aws"
	"github.com/jacobsa/aws/s3"
	"os"
	"bufio"
	"bytes"
)

// Fetches the bucket used for this project. Hardcoded access keys and bucket name for now for simplicity.
func GetBucket() s3.Bucket{
	bucketName := "mapreduce_testing1"
	key := aws.AccessKey{"AKIAJ6XE2BOMK4K4WPTA", "IoAnEqiwKCr8yQfuFsKj1Tsoj4y3AxnJRQYPH23v"}
	region := s3.RegionUsStandard

	bucket, err := s3.OpenBucket(bucketName, region, key)	// Get the Bucket object
	if err != nil {
		fmt.Printf("Error! %s\n", err)
	}

	return bucket
}

// Fetches an object from bucket with the given key. Just a wrapper to handle errors
func GetObject(bucket s3.Bucket, key string) []byte {
	data, err := bucket.GetObject(key) 		// Get data from a key in the bucket

	if err != nil {
		fmt.Printf("Error! %s\n", err)
	}

	return data
}

// Filters the bucket's keys to include only keys that have the given prefix.
func FilterKeysByPrefix(bucket s3.Bucket, prefix string) []string {
	// use prefix as the "marker" parameter to s3, so only keys after prefix (alphabetically) will be returned
	keys, err := bucket.ListKeys(prefix)

	if err != nil {
		fmt.Printf("Error in filterKeys: %s\n", err)
	}

	filteredKeys := make([]string, 0)
	// Iterate through the keys, keeping the ones that start with prefix
	for i := range keys {
		key := keys[i]
		if strings.HasPrefix(keys[i], prefix) {
			filteredKeys = append(filteredKeys, key)
		}
	}
	return filteredKeys
}

// Read data from the file at filepath, break its contents into chunks of size chunkSize, and write them to 
// the specified directory in the given bucket
func SplitFileIntoChunks(filepath string, bucket s3.Bucket, directory string, chunkSize int) bool{
    file, err := os.Open(filepath)

    if err != nil {
        return false
    }

    defer file.Close()

    reader := bufio.NewReader(file)				// The file buffer to read from
    buffer := bytes.NewBuffer(make([]byte, 0))	// The temporary buffer to write each chunk to
    done := false
    chunkNumber := 0

    for !done {
    	fmt.Printf("Chunk #%d\n", chunkNumber)
    	chunkNumber++

    	for buffer.Len() < chunkSize {	// While the buffer is smaller than our chunk size
	        part, _, err := reader.ReadLine() 	// Read the next line of the file

	        if err != nil {	// There's nothing left to read from the file
	            done = true
	            break
	        }

	        buffer.Write(part)
	    }

	    fmt.Printf("Buffer length: %d\n", buffer.Len())
	    key := fmt.Sprintf("%s/%d", directory, chunkNumber) 	// Create the key for this chunk ("directory/chunkNumber")
	    bucket.StoreObject(key, buffer.Bytes())			// Write the buffer data to that key
    	buffer.Reset()
    }

	return true
}

// func main() {
// 	bucket := GetBucket()
// 	fmt.Printf("Bucket: %s\n", bucket)

// 	// SplitFileIntoChunks("../../final_project/texts/output.txt", bucket, "small_test3", 100000)

// 	// jobs := []WorkerJob{}

// 	keys := FilterKeysByPrefix(bucket, "small_test3/")	// Print out all the keys/chunks in the small_test/ directory
// 	for _, key := range keys {
// 		fmt.Println(key)
// 		// jobs = append(jobs, WorkerJob{key, "", false, map[string]string{}})
// 	}

// }
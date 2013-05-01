package gomapreduce

import (
	"testing"
	"runtime"
	// "math/rand"
	"os"
	"strconv"
	"time"
	"fmt"
	"encoding/gob"
)


func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "px-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

func cleanup(pxa []*MapReduceNode) {
	for i := 0; i < len(pxa); i++ {
		if pxa[i] != nil {
			pxa[i].Kill()
		}
	}
}


func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)      // sets max number of CPUs used simultaneously


	gob.Register(DemoMapper{})

	const nnodes = 5
	var pxh []string = make([]string, nnodes)         // Create empty slice of host strings
	var pxa []*MapReduceNode = make([]*MapReduceNode, nnodes) // Create empty slice of MapReduce instances
	defer cleanup(pxa)

		for i := 0; i < nnodes; i++ {
			pxh[i] = port("basic", i)
		}
		for i := 0; i < nnodes; i++ {
			pxa[i] = Make(pxh, i, nil, "unix")
		}
		fmt.Println(pxa)
		fmt.Println(pxh)

		mapper := DemoMapper{}
		reducer := DemoReducer{}
		config := JobConfig{InputFolder: "small_test", 
							 OutputFolder: "",
							 M: 2,
							 R: 2,
							}
		job_id := pxa[0].Start(mapper, reducer, config)
		debug(fmt.Sprintf("job_id: %s", job_id))

		time.Sleep(2000 * time.Millisecond)

		

	fmt.Printf("Passed...\n")
}

// Test that writing to and reading from s3 works
// func TestS3(t *testing.T) {
//   bucket := GetBucket()

//   file, _ := os.Create("test.txt")        // Create a new testing file
//   file.Write([]byte("a\nb\nc\nd\ne\n"))   // And write some text to it
//   file.Close()
	
//   defer os.Remove("test.txt")

//   SplitFileIntoChunks("test.txt", bucket, "testing", 1) // Split the file into chunks of 1 byte each and write them to s3

//   value := GetObject(bucket, "testing/1")     // Now fetch the value for the first chunk that was just written

//   if value[0] != []byte("a")[0] {   // And ensure that the first character of that chunk is "a"
//     fmt.Printf("%s != a\n", value, )
//   }

//   // Clean up the objects we just wrote
//   bucket.DeleteObject("testing/1")
//   bucket.DeleteObject("testing/2")
//   bucket.DeleteObject("testing/3")
//   bucket.DeleteObject("testing/4")
//   bucket.DeleteObject("testing/5")
//   bucket.DeleteObject("testing/6")
//   bucket.DeleteObject("testing")

//   fmt.Printf("Passed...\n")
// }
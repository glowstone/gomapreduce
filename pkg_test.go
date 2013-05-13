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

// func TestBootstrap(t *testing.T) {
// 	// Bootstrap
// 	bucket := GetBucket()
// 	fmt.Println(bucket)
// 	SplitFileIntoChunks("output.txt", bucket, "alice_in_wonderland", 100000) // Split the file into chunks of 1 byte each and write them to s3
// 	fmt.Println("Bootstrapped!")
// }


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
		pxa[i] = MakeMapReduceNode(pxh, i, nil, "unix")
	}
	fmt.Println(pxa)
	fmt.Println(pxh)

	mapper := DemoMapper{}
	reducer := DemoReducer{}
	config := MakeJobConfig("small_test", "small_test_output", 2, 2, true, "")
	inputer := MakeS3Inputer("small_test")
	outputer := MakeS3Outputer("small_test_output")
	job_id := pxa[0].Start(config, mapper, reducer, inputer, outputer)
	debug(fmt.Sprintf("job_id: %s", job_id))


	for !pxa[0].Status(job_id) { 		// While the job isn't done, keep waiting
		time.Sleep(500*time.Millisecond)
	}
	// time.Sleep(60000 * time.Millisecond)
		
	fmt.Printf("Passed...\n")
}

// func TestGetEmitted(t *testing.T) {
// 	runtime.GOMAXPROCS(4)      // sets max number of CPUs used simultaneously

// 	fmt.Printf("Test: Getting Emitted Intermediate KVPairs ... \n")

// 	const nnodes = 5
// 	var pxh []string = make([]string, nnodes)         // Create empty slice of host strings
// 	var pxa []*MapReduceNode = make([]*MapReduceNode, nnodes) // Create empty slice of MapReduce instances
// 	defer cleanup(pxa)

// 	for i := 0; i < nnodes; i++ {
// 		pxh[i] = port("basic", i)
// 	}
// 	for i := 0; i < nnodes; i++ {
// 		pxa[i] = MakeMapReduceNode(pxh, i, nil, "unix")
// 	}
// 	fmt.Println(pxa)
// 	fmt.Println(pxh)

// 	// hack

// 	args := &GetEmittedArgs{JobId: job_id, TaskId: "766576575"}
// 	var reply GetEmittedReply
// 	pxa[0].Get(args, &reply)

// 	if reply.Err != ErrNoKey {
// 		t.Fatalf("invalid response when trying to read nonexistent emittedStore entry.")
// 	}
		
// 	fmt.Printf("  ... Passed\n")
// }

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

func TestPing(t *testing.T) {
	const nnodes = 2
	var pxh []string = make([]string, nnodes)         // Create empty slice of host strings
	var pxa []*MapReduceNode = make([]*MapReduceNode, nnodes) // Create empty slice of MapReduce instances
	defer cleanup(pxa)

	for i := 0; i < nnodes; i++ {
		pxh[i] = port("basic", i)
	}
	for i := 0; i < nnodes; i++ {
		pxa[i] = MakeMapReduceNode(pxh, i, nil, "unix")
	}


    pxa[1].dead = true
    time.Sleep(1500*time.Millisecond)

    fmt.Println("... Test over. Make sure that node 0 marked node 1 as dead!")
}
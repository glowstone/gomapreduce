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
// 	SplitFileIntoChunks("output.txt", bucket, "small_test", 100000) // Split the file into chunks of 1 byte each and write them to s3
// 	fmt.Println("Bootstrapped!")
// }


func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)      // sets max number of CPUs used simultaneously
	gob.Register(DemoMapper{})
	gob.Register(DemoReducer{})
	gob.Register(S3Inputer{})
    gob.Register(S3Outputer{})

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

	mapper := makeDemoMapper()
	reducer := makeDemoReducer()
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

	// Mark node 1 as dead, then wait a while, and make sure node 0 marked it as dead
    pxa[1].dead = true
    time.Sleep(1500*time.Millisecond)

    // Now when we try to get the index for a live node, it should return 0 instead of 1
    index := pxa[0].tm.getWorkerIndex(pxa[0].nodes, pxa[0].nodeStates)
    if index != 0 {
    	t.Fatalf("Should assign node 0, but instead trying to assign node %d\n", index)
    }

    fmt.Println("... Test over. Make sure that node 0 marked node 1 as dead!")
}


func TestJobManager(t *testing.T) {

	fmt.Printf("Test: JobManager Basics ...\n")
	var jm JobManager
	var testJob, storedJob Job
	var jobId, storedStatus string          // TODO add status and iteration
	var error error

	jm = makeJobManager()
	jobId = generateUUID()
	testJob = makeJob(jobId, makeDemoMapper(), makeDemoReducer(), MakeS3Inputer("test"), MakeS3Outputer("test"))
	
	fmt.Println(jm, testJob)
	
	// Test adding a Job
	error = jm.addJob(testJob, "starting")
	storedJob = jm.storage[jobId].job
	storedStatus = jm.storage[jobId].status
	if error != nil || storedJob != testJob || storedStatus != "starting" {
		t.Fatalf("Failed to addJob")
	}

	// Test that adding a Job with the same jobId fails
	error = jm.addJob(testJob, "starting")
	if error == nil {
		t.Fatalf("addJob allows Job with a jonId matching an existing JobState to be added.")
	}

	// Test that adding a Job with invalid status fails
	testJob = makeJob(generateUUID(), makeDemoMapper(), makeDemoReducer(), MakeS3Inputer("test"), MakeS3Outputer("test"))
	error = jm.addJob(testJob, "invalid")
	if error == nil {
		t.Fatalf("addJob allows a Job to be added with an invalid status")
	}

	// Test removing a Job
	jm.removeJob(jobId)
	if jm.storage[jobId] != nil {
		fmt.Println(jm.storage[jobId])
		t.Fatalf("failed to removeJob")
	}

	jobId = generateUUID()
	testJob = makeJob(jobId, makeDemoMapper(), makeDemoReducer(), MakeS3Inputer("test"), MakeS3Outputer("test"))
	jm.addJob(testJob, "starting")

	// Test getting Job
	storedJob, _ = jm.getJob(jobId)
	if storedJob != testJob {
		t.Fatalf("getJob failed to return expected Job")
	}

	// Test getting status
	storedStatus, _ = jm.getStatus(jobId)
	if storedStatus != "starting" {
		t.Fatalf("getStatus failed to return expected status")
	}

	// Test setting status
	jm.setStatus(jobId, "working")
	storedStatus, _ = jm.getStatus(jobId)
	if storedStatus != "working" {
		t.Fatalf("setStatus fails to set the correct status")
	}
	jm.setStatus(jobId, "completed")
	storedStatus, _ = jm.getStatus(jobId)
	if storedStatus != "completed" {
		t.Fatalf("setStatus fails to set the correct status")
	}

	// Test isCompleted
	if !jm.isCompleted(jobId) {
		t.Fatalf("isCompleted returns false for a Job that should be completed")
	}
	jm.setStatus(jobId, "starting")
	if jm.isCompleted(jobId) {
		t.Fatalf("isCompleted returns true for a Job that is not completed")
	}

	// Test that setting invalid status fails
	error = jm.setStatus(jobId, "invalid")
	if error == nil {
		t.Fatalf("setStatus with invalid status should return a non-nil error")
	}
}
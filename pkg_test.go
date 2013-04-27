package gomapreduce

import (
	"testing"
	"runtime"
	// "math/rand"
	"os"
	"strconv"
	"time"
	"fmt"
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

func cleanup(pxa []*MapReduce) {
  for i := 0; i < len(pxa); i++ {
    if pxa[i] != nil {
      pxa[i].Kill()
    }
  }
}


func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)      // sets max number of CPUs used simultaneously

	const nnodes = 5
	var pxh []string = make([]string, nnodes)         // Create empty slice of host strings
	var pxa []*MapReduce = make([]*MapReduce, nnodes) // Create empty slice of MapReduce instances
 	defer cleanup(pxa)

  	for i := 0; i < nnodes; i++ {
    	pxh[i] = port("basic", i)
  	}
  	for i := 0; i < nnodes; i++ {
    	pxa[i] = Make(pxh, i, nil, "unix")
  	}
  	fmt.Println(pxa)
  	fmt.Println(pxh)
    time.Sleep(5000 * time.Millisecond)

    pxa[0].Start()

	fmt.Printf("Passed...\n")
}
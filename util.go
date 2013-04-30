package gomapreduce

import (
	"fmt"
	"strconv"
	"math/rand"
)

var verbose = true           // TODO: make this an environment variable

func debug(debug_message string) {
  if verbose {
    fmt.Println(debug_message)
  }
}

/*
TODO: Does not actually return a UUID according to standards. Just a random
int which suffices for now.
http://tools.ietf.org/html/rfc4122
*/
func generate_uuid() string {
  return strconv.Itoa(rand.Int())
}
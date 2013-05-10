package gomapreduce

// Interface for reading the input for Map Reduce Jobs from different file stores.

type Inputer interface {
	ListKeys() []string 			// A method to list all of the keys (i.e. all of the map jobs)
	GetValue(key string) string 	// A method to get the value for one of the keys
}
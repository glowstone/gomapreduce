package gomapreduce


type Task interface {
	get_id() int
}



type Reducer interface {
	get_id() int
	Reduce_action() interface{}
}
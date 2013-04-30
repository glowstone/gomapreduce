package gomapreduce


type Task interface {
	get_id() int
}

type Mapper interface {
	get_id() int
	// 'map' is keyword and 'Map' may lead to confusion
	Map_action()
}

type Reducer interface {
	get_id() int
	Reduce_action() interface{}
}
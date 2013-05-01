package gomapreduce

// Task Management System

type TaskManager struct {
	// purposefully not using map within a map
	task_states map[string]map[string]TaskState    // job_id, task_id -> TaskState

}

// Add a task
func (self *TaskManager) add_task(job_id string, task Task) {

}


func makeTaskManager() TaskManager{
	tm := TaskManager{}
	tm.task_states = make(map[string]map[string]TaskState)
	return tm
}
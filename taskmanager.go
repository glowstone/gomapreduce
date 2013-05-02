package gomapreduce

// Task Management System

import (
	"math/rand"
	"fmt"
)

// Representation of Tasks maintained at the master node
type TaskState struct {
	task Task         // The MapTask or ReduceTask being managed
	workerIndex int   // Index of the node assigned as the Task
	status string     // "unassigned", "assigned", "completed"
}

// TaskState Constructor
func makeTaskState(task Task, workerIndex int, status string) *TaskState {
	ts := &TaskState{task: task, workerIndex: workerIndex, status: status}
	return ts
}



type TaskManager struct {
	nworkers int                              // Number of worker nodes
	storage map[string]map[string]*TaskState  // Maps job_id -> task_id -> *TaskState
}

// TaskManager Constructor
func makeTaskManager(nworkers int) TaskManager{
	tm := TaskManager{nworkers: nworkers}
	tm.storage = make(map[string]map[string]*TaskState)
	return tm
}

func (self *TaskManager) getTaskState(jobId string, taskId string) *TaskState {
	if _, present := self.storage[jobId]; present {
		if _, present := self.storage[jobId][taskId]; present {
			return self.storage[jobId][taskId]
		}
		return &TaskState{}
	}
	return &TaskState{}
}

func (self *TaskManager) setTaskStatus(jobId string, taskId string, status string) {
	taskState := self.getTaskState(jobId, taskId)

	switch status {
		case "unassigned":
			debug(fmt.Sprint("Marked unassigned"))
			taskState.status = "unassigned"
		case "assigned":
			debug(fmt.Sprintln("Mark assigned"))
			taskState = self.getTaskState(jobId, taskId)
			taskState.status = "assigned"
		case "complete":
			debug(fmt.Sprintln("Mark complete"))
			taskState = self.getTaskState(jobId, taskId)
			taskState.status = "completed"
		default:
			panic("tried to set invalid TaskState status")
	}
}



/*
Internal method for adding an individual Task to be managed by storing
a TaskState object
*/
func (self *TaskManager) addTask(jobId string, task Task) {
	if _, present := self.storage[jobId]; !present {
		self.storage[jobId] = make(map[string]*TaskState)
	}
	// Initially, Tasks assigned to a random worker and are not done
	startState := makeTaskState(task, rand.Intn(self.nworkers), "unassigned")
	self.storage[jobId][task.getId()] = startState
}

/*
Accepts a slice of MapTasks and adds them to collection of Tasks being managed.
*/
func (self *TaskManager) addBulkMapTasks(jobId string, tasks []MapTask) {
	for _, task := range tasks {
		self.addTask(jobId, task)
	}
	fmt.Println(self.storage)
	return
}

/*
Accepts a slice of ReduceTasks and adds them to the collection of Tasks being managed.
*/
func (self *TaskManager) addBulkReduceTasks(jobId string, tasks []ReduceTask) {
	for _, task := range tasks {
		self.addTask(jobId, task)
	}
	return
}

/*
Returns a list of the unfinshed Task Ids for a particular Job by iterating over 
the TaskStates for the Job and cheking whether it is finished.
*/
func (self *TaskManager) listUnassignedTasks(jobId string) []string {
	var taskIds []string
	for _, taskState := range self.storage[jobId] {
		if taskState.status == "unassigned" {
			taskIds = append(taskIds, taskState.task.getId())
		}
	}
	return taskIds
}


/*
Iterates though the Tasks for a particular Job and returns the number of Tasks
not completed.
*/
func (self *TaskManager) getNumberUnassigned(jobId string) int {
	return len(self.listUnassignedTasks(jobId))
}








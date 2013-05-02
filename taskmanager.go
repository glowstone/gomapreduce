package gomapreduce

// Task Management System

import (
	"math/rand"
	"fmt"
)

// Representation of Tasks maintained at the master node
type TaskState struct {
	task Task       // The MapTask or ReduceTask being managed
	worker int      // Index of the node assigned as the Task
	finished bool   // Whether the task is finished or not
}

// TaskState Constructor
func makeTaskState(task Task, worker int, finished bool) TaskState {
	ts := TaskState{task: task, worker: worker, finished: finished}
	return ts
}



type TaskManager struct {
	nworkers int                             // Number of worker nodes
	storage map[string]map[string]TaskState  // Maps job_id -> task_id -> TaskState
}

// TaskManager Constructor
func makeTaskManager(nworkers int) TaskManager{
	tm := TaskManager{nworkers: nworkers}
	tm.storage = make(map[string]map[string]TaskState)
	return tm
}

/*
Internal method for adding an individual Task to be managed by storing
a TaskState object
*/
func (self *TaskManager) addTask(jobId string, task Task) {
	if _, present := self.storage[jobId]; !present {
		self.storage[jobId] = make(map[string]TaskState)
	}
	// Initially, Tasks assigned to a random worker and are not done
	startState := makeTaskState(task, rand.Intn(self.nworkers), false)
	self.storage[jobId][task.getId()] = startState
}


// Boo, Go does not support generics

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
Returns a list of the unfinshed Tasks for a particular Job by iterating over 
the TaskStates and checking whether done is true.
*/
func (self *TaskManager) listUnfinishedTasks(jobId string) []Task {
	var remainingTasks []Task
	for _, taskState := range self.storage[jobId] {
		if !taskState.finished {
			remainingTasks = append(remainingTasks, taskState.task)
		}
	}
	return remainingTasks
}


/*
Iterates though the Tasks for a particular Job and returns the number of Tasks
not completed.
*/
func (self *TaskManager) getNumberUnfinished(jobId string) int {
	unfinished := 0
	for _, taskState := range self.storage[jobId] {
		if !taskState.finished {
			unfinished++
		}
	}
	return unfinished
}








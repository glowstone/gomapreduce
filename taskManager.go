package gomapreduce

// Task Management System

import (
	"math/rand"
	"sync"
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
	mu sync.Mutex                             // Singleton mutex for manager
	nworkers int                              // Number of worker nodes
	storage map[string]map[string]*TaskState  // Maps jobId -> taskId -> *TaskState
}

// TaskManager Constructor
func makeTaskManager(nworkers int) TaskManager{
	tm := TaskManager{nworkers: nworkers}
	tm.storage = make(map[string]map[string]*TaskState)
	return tm
}

/*
Returns a copy of the TaskState object corresponding to the given jobId and
taskId. Mutating the copy does not affect TaskState stored in the TaskManager
*/
func (self *TaskManager) getTaskStateCopy(jobId string, taskId string) TaskState {
	if _, present := self.storage[jobId]; present {
		if _, present := self.storage[jobId][taskId]; present {
			return *self.storage[jobId][taskId]
		}
		return TaskState{}
	}
	return TaskState{}
}

/*
Returns the TaskState object corresponding to the given jobId and taskId.
Callers that mutate the TaskState are responsible for obtaining a lock.
*/
func (self *TaskManager) getTaskState(jobId string, taskId string) *TaskState {
	if _, present := self.storage[jobId]; present {
		if _, present := self.storage[jobId][taskId]; present {
			return self.storage[jobId][taskId]
		}
		return &TaskState{}
	}
	return &TaskState{}
}



/*
Accepts a jobId and taskId and sets the corresponding TaskState status to the
given status. Valid statuses are 'unassigned', 'assigned', and 'completed'
*/
func (self *TaskManager) setTaskStatus(jobId string, taskId string, status string) {
	self.mu.Lock()
	defer self.mu.Unlock()

	taskState := self.getTaskState(jobId, taskId)
	switch status {
		case "unassigned":
			taskState.status = "unassigned"
		case "assigned":
			taskState.status = "assigned"
		case "completed":
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
	startState := makeTaskState(task, -1, "unassigned")
	self.storage[jobId][task.getId()] = startState
}

/*
Accepts a slice of MapTasks and adds them to collection of Tasks being managed.
*/
func (self *TaskManager) addBulkMapTasks(jobId string, tasks []MapTask) {
	for _, task := range tasks {
		self.addTask(jobId, task)
	}
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
Returns a list of the Task Ids for a particular Job, which have are known to 
have the given status (in TaskState) and be of the given kind (in Task itself).
The given status may be 'unassigned', 'assigned', 'completed', or 'all' and 
the given type may be 'map', 'reduce', or 'all'.
*/
func (self *TaskManager) listTasks(jobId string, status string, kind string) []string {
	var taskIds []string
	for _, taskState := range self.storage[jobId] {
		if taskState.status == "all" {
			if kind == "all" {
				taskIds = append(taskIds, taskState.task.getId())
			} else {
				if taskState.task.getKind() == kind {
					taskIds = append(taskIds, taskState.task.getId())
				}
			}	
		} else {
			if taskState.status == status {
				taskIds = append(taskIds, taskState.task.getId())
			}
		}
	}
	return taskIds
}


/*
Iterates though the Tasks for a particular Job and returns the number of Tasks
not completed.
*/
func (self *TaskManager) countTasks(jobId string, status string, kind string) int {
	return len(self.listTasks(jobId, status, kind))
}



// The way these methods work needs to be reorganized, representation of liveness 
// should be an instance parameter with nice methods and autoupdate itself.

func (self *TaskManager) reassignDeadTasks(nodes []string, nodeStates map[string]string) {
	var workerIndex int
	for _, taskMap := range self.storage {
		for _, task := range taskMap {
			workerIndex = task.workerIndex
			if workerIndex > -1 {
				// Horrible failure at error hanlding
				if nodeStates[nodes[workerIndex]] == "dead" {
					self.getWorkerIndex(nodes, nodeStates)
					// newIndex := above line
					// TODO set new workerIndex atomically with locking
					// TODO invoke assigner. Consider where that should live.
				}
			}
		}
	}

}


// TODO
// Calls for determining percentage complete and other stats

// Returns the worker index for an eligible worker, i.e. not busy and not dead
// nodes is a slice of node names, nodeStates maps worker index -> state (alive or dead)
func (self *TaskManager) getWorkerIndex(nodes []string, nodeStates map[string]string) int {
	index := rand.Intn(self.nworkers)
	alive := nodeStates[nodes[index]] != "dead"

	for !alive {
		index = rand.Intn(self.nworkers)
		alive = nodeStates[nodes[index]] != "dead"
	}
	return index
}






package gomapreduce

// Job Management System

import (
	"sync"
	"errors"
)

// Representation fo Jobs maintained at the master node
type JobState struct {
	job Job                // The Job being managed
	status string          // "starting", "working", "completed" 
}

// JobState Constructor
func makeJobState(job Job, status string) *JobState {
	// passed status must be one of the options
	for _, option := range []string{"starting", "working", "completed"} {
		if option == status {
			js := &JobState{job: job, status: status}
			return js
		}
	}
	panic("tried to set invalid JobState status")	
}



type JobManager struct {
	mu sync.Mutex                    // Singleton mutex for manager
	storage map[string]*JobState     // Maps string job_id -> *JobState
}

// JobManager Constructor
func makeJobManager() JobManager {
	jm := JobManager{}
	jm.storage = make(map[string]*JobState)
	return jm
}

/*
Creates an internal JobState representation containing the passed Job and status
and stores it in the JobManager internal storage keyed by jobId. Returns an error
if a Job with the same jobId exists and has not been removed.
*/
func (self *JobManager) addJob(job Job, status string) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	jobId := job.getId()
	jobState := makeJobState(job, status)
	if _, present := self.storage[jobId]; present {
		return errors.New("a Job with the same jobId already exists.")
	}
	self.storage[jobId] = jobState
	return nil
}


/*
Internal method for obtaining a pointer to the JobState(mutable) associated with a 
jobId. Caller responsible for obtaining a lock. If no JobState is associated with
the given jobId, 
*/
func (self *JobManager) getJobState(jobId string) (*JobState, error)  {
	if _, present := self.storage[jobId]; !present {
		return &JobState{}, errors.New("no JobState with jobId")
	}
	return self.storage[jobId], nil
}




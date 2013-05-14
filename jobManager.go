package gomapreduce

// Job Management System

import (
	"sync"
	"errors"
)

var JobStatuses = [3]string{"starting", "working", "completed"}

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
Returns a copy of the Job associated with the given jobId string. Returns an empty
Job and an error if no Job is found with the given jobId.
*/
func (self *JobManager) getJob(jobId string) (Job, error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	jobState, error := self.getJobState(jobId)
	if error == nil {
		return jobState.job, nil
	}
	return Job{}, errors.New("no JobState with given jobId")
}

/*
Removes the JobState associated with the given jobId from the JobManager storage.
If there is no entry corresponding to the jobId, no action is performed.
*/
func (self *JobManager) removeJob(jobId string) {
	self.mu.Lock()
	defer self.mu.Unlock()

	delete(self.storage, jobId)
}


/*
INTERNAL method for obtaining a pointer to the JobState(mutable) associated with a 
jobId. Caller responsible for obtaining a lock. If no JobState is associated with
the given jobId, a pointer to an empty JobState and an error are returned.
*/
func (self *JobManager) getJobState(jobId string) (*JobState, error)  {
	if _, present := self.storage[jobId]; !present {
		return &JobState{}, errors.New("no JobState with jobId")
	}
	return self.storage[jobId], nil
}



func (self *JobManager) getStatus(jobId string) (string, error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	jobState, error := self.getJobState(jobId)
	if error == nil {
		return jobState.status, nil
	}
	return "", errors.New("no JobState with given jobId")
}

func (self *JobManager) isCompleted(jobId string) bool {
	self.mu.Lock()
	defer self.mu.Unlock()

	jobState, error := self.getJobState(jobId)
	if error == nil && jobState.status == "completed" {
		return true
	}
	return false
}

func (self *JobManager) setStatus(jobId string) (string, error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	jobState, error := self.getJobState(jobId)
	if error == nil {
		return jobState.status, nil
	}
	return "", errors.New("no JobState with given jobId")
}





// jobStatus and JobManager Helpers 

func validJobStatus(status string) bool {
	for _, validStatus := range JobStatuses {
		if status == validStatus {
			return true
		} 
	}
	return false
}






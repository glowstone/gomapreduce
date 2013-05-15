package gomapreduce

// Job Management System

import (
	"sync"
	"errors"
)

// GO will not allow an array const
var JOB_STATUSES = [3]string{"starting", "working", "completed"}

// Representation fo Jobs maintained at the master node
type JobState struct {
	job Job                // The Job being managed
	status string          // "starting", "working", "completed" 
}

// JobState Constructor
func makeJobState(job Job, status string) (*JobState, error) {
	if validStatus(status) {
		js := &JobState{job: job, status: status}
		return js, nil
	}
	return &JobState{}, errors.New("invalid JobState status")
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
and stores it as JobState in internal storage keyed by jobId. If a Job with the 
same jobId is already stored or the JobState cannot be created (invalid 
status) the returned error will be non-nil.
*/
func (self *JobManager) addJob(job Job, status string) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	jobId := job.getId()
	if _, present := self.storage[jobId]; present {
		return errors.New("a Job with the same jobId already exists.")
	}
	jobState, error := makeJobState(job, status)
	if error != nil {
		return errors.New(error.Error())
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

/*
Returns a copy of the Status associated with a given jobId string. If no Job is
found with the provided jobId, an empty string and a non-nil error are returned.
*/
func (self *JobManager) getStatus(jobId string) (string, error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	jobState, error := self.getJobState(jobId)
	if error == nil {
		return jobState.status, nil
	}
	return "", errors.New("no JobState with given jobId")
}

/*
If a Job with the given jobId is found and the given status is valid, the status
of the Job is set to the new status. Otherwise, a non-nil error is returned.
*/
func (self *JobManager) setStatus(jobId string, status string) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	jobState, error := self.getJobState(jobId)
	if error == nil {
		if validStatus(status) {
			jobState.status = status
			return nil
		}
		return errors.New("invalid JobState status")
	}
	return errors.New(error.Error())
}

/*
Returns true only if there is a Job stored in the Job Manager with the given jobId
and the status of that Job is "comepleted".
*/
func (self *JobManager) isCompleted(jobId string) bool {
	status, _ := self.getStatus(jobId)
	if status == "completed" {
		return true
	}
	return false
}

/*
Returns a copy of the JobConfig for the Job associated with the given jobId string. 
Returns an empty JobConfig and an error if no Job is found with the given jobId.
*/
func (self *JobManager) getConfig(jobId string) (JobConfig, error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	jobState, error := self.getJobState(jobId)
	if error == nil {
		return jobState.job.getConfig(), nil
	}
	return JobConfig{}, errors.New("no JobState with given jobId")
}


// jobStatus and JobManager Helpers
///////////////////////////////////////////////////////////////////////////////

func validStatus(status string) bool {
	for _, validStatus := range JOB_STATUSES {
		if status == validStatus {
			return true
		} 
	}
	return false
}






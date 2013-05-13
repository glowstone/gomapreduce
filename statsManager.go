package gomapreduce

// Statistics manager, like how long it takes jobs/tasks to run etc.

import (
	"fmt"
	"time"
	"sync"
)

// Keeps track of stats relevant to a job, and the tasks that make up that job
type JobStats struct {
	jobId string
	startTime time.Time
	finishTime time.Time
	tasks map[string]TaskStats	// Map taskId -> TaskStats
}

// Keeps track of stats relevant to a task
type TaskStats struct {
	task Task
	startTime time.Time
	finishTime time.Time
}

type StatsManager struct {
	mu sync.Mutex                             // Singleton mutex for manager
	storage map[string]JobStats  // Maps jobId -> JobStats
}

// StatsManager Constructor
func makeStatsManager() StatsManager{
	sm := StatsManager{}
	sm.storage = make(map[string]JobStats)
	return sm
}

// Adds a job with id jobId if it doesn't exist
func (self StatsManager) addJob(jobId string) {
	_, present := self.storage[jobId]
	if !present {
		self.storage[jobId] = JobStats{jobId: jobId, startTime: time.Now(), tasks: make(map[string]TaskStats)}
	}
}

// Marks job with id jobId as complete, setting its finishTime
func (self StatsManager) jobComplete(jobId string) {
	stats, present := self.storage[jobId]
	if present {
		stats.finishTime = time.Now()
		self.storage[jobId] = stats
	} else {
		fmt.Printf("Job %s doesn't exist!\n", jobId)
	}
}

// Gets the amount of time that a job took to run
func (self StatsManager) jobTime(jobId string) time.Duration {
	diff := time.Duration(0)
	stats, present := self.storage[jobId]
	if present {
		finish := stats.finishTime
		diff = finish.Sub(stats.startTime)
	} else {
		fmt.Printf("Job %s doesn't exist!\n", jobId)
	}
	return diff
}
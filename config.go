package gomapreduce

/*
Struct for representing per-Job configuration settings
*/
type JobConfig struct {
	InputFolder string    // The folder within the "mapreduce_testing1" s3 bucket
	OutputFolder string   // The folder to write output to (within "mapreduce_testing1")
	// TODO Should contain map and reduce functions, chunk sizes etc.
	M int                 // Number of MapTasks
	R int                 // Number of ReduceTasks
}



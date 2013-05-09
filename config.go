package gomapreduce

/*
Struct for representing per-Job configuration settings
*/
type JobConfig struct {
	InputFolder string    // The folder within the "mapreduce_testing1" s3 bucket
	OutputFolder string   // The folder to write output to (within "mapreduce_testing1")
	M int                 // Number of MapTasks
	R int                 // Number of ReduceTasks
	Prechunked bool       // Whether the file has already been broken into M chunks
	InputFile string      // File name of the entire input (not chunked)
}

// JobConfig constructor
func MakeJobConfig(inputFolder string, outputFolder string, m int, r int,
	prechunked bool, inputFile string) JobConfig {

	return JobConfig{InputFolder: inputFolder,
					OutputFolder: outputFolder,
					M: m,
					R: r,
					Prechunked: prechunked,
					InputFile: inputFile,
				}
	}





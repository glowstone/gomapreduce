package gomapreduce

/*
Struct for representing per-Job configuration settings
*/
type JobConfig struct {
	inputFolder string    // The folder within the "mapreduce_testing1" s3 bucket
	outputFolder string   // The folder to write output to (within "mapreduce_testing1")
	m int                 // Number of MapTasks
	r int                 // Number of ReduceTasks
	prechunked bool       // Whether the file has already been broken into M chunks
	inputFile string      // File name of the entire input (not chunked)
}

// JobConfig constructor
func MakeJobConfig(inputFolder string, outputFolder string, m int, r int,
	prechunked bool, inputFile string) JobConfig {

	return JobConfig{inputFolder: inputFolder,
					outputFolder: outputFolder,
					m: m,
					r: r,
					prechunked: prechunked,
					inputFile: inputFile,
				}
	}





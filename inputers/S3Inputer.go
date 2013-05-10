package gomapreduce

type S3Inputer struct { 	// Satisfies the InputAccessor interface
	Folder string
}

// S3 Inputer Constructor
func MakeS3Inputer (inputFolder string) S3Inputer {	
	if !strings.HasSuffix(inputFolder, "/") { 	// Make sure that the folder has a trailing slash
		inputFolder += "/"
	}
	fmt.Printf("Folder: %s\n", inputFolder)
	accessor := S3Inputer{Folder: inputFolder}

	return accessor
}

func (self S3Inputer) ListKeys() []string {
	bucket := GetBucket()
	keys := FilterKeysByPrefix(bucket, self.Folder)
	return keys
}

func (self S3Inputer) GetValue(key string) string {
	bucket := GetBucket()
	value := GetObject(bucket, key)
	return string(value)
}


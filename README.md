

## Not Ready for Public Use!

# gomapreduce v0.0.5

## Quick start

	(Install)[http://golang.org/doc/install] Go (or use this Gist (script)[https://gist.github.com/dghubble/5510615] and (setup your Go workspace)[http://golang.org/doc/code.html] if you haven't already.

	cd go-workspace/src
	git clone git@github.com:glowstone/gomapreduce.git
	go install gomapreduce
	go test       // ensure the tests pass

	Consult the (gomrclient)[https://github.com/glowstone/gomrclient], an example application which uses the gomapreduce library to run simple word frequency counting jobs.


### Environment Variables

If you choose to use the S3Inputer to read in input data or the S3Outputer to write output data you should define the S3_BUCKET_NAME, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY environment variables. Future Inputers and Oututers are also likely to make use of environment variables. You can set these variables manually, but we recommend (virtualenvwrapper)[http://virtualenvwrapper.readthedocs.org/en/latest/]

After you've made a virtual environment, place the following in your venv/bin/postactivate script:

	export S3_BUCKET_NAME=gomapreduce
	export AWS_ACCESS_KEY_ID=xxx
	export AWS_SECRET_ACCESS_KEY=yyy

and place the following in your venv/bin/postdeactivate script.

	unset S3_BUCKET_NAME
	unset AWS_ACCESS_KEY_ID
	unset AWS_SECRET_ACCESS_KEY

## Versioning

## Features

## Limitations


## Contributing


## Authors

**Dalton Hubble**
**Thomas Garver**


## Copyright and License


# Overview

* Note: Implementation not current with the description.

Provides a library called 'mapreduce.go' implementing the 'mapreduce' service which can perform 'Jobs' which are composed of Tasks (MapTasks and ReduceTasks)

The persistent nodes which perform Map reduce servers are called MapReduceNodes. Any MapReduceNode can serve as a master, mapper worker, or reducer worker

At times they play a worker role and at other times they play a master role.

The mapreduce package includes the following interfaces which are implemented by clients wishing to use the library:

+ Mapper
+ Reducer
+ InputAccessor
+ OutputAccessor
+ DataSplitter?     // default is at any line break

Typically, clients only need implement the Mapper and Reducer interfaces as reasonable builtin implementations for the InputData, OutputData, and DataChunker interfaces are available.

An intermediateAccessor interface and a default implementation is also part of the library; the default implementation just uses the MapReduceNode's local volatile RAM memory.

Clients must also initialize an instance of the JobConfig as well to set per-Job configuration settings.

### Exposed Client API

+ MakeMapReduceNode(nodes []string, me int, rpcs *rpc.Server, net_mode string) (*MapReduceNode)
+ Start(mapper, reducer, job_config) (job_id int)
+ Status(job_id) (is_done bool, stats Stats)


### Notes/Limitations/Uncertainties

Node requesting a Job be performed is part of the network of nodes like in Paxos. 

Workers process one task at a time rather than maintaining a queue of tasks to be performed. The master is responsible for continuing to try to push out the tasks.



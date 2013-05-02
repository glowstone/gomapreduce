

# Quickstart

	# set the GOPATH to your go workspace
	export GOPATH=$HOME/go-workspace
	cd $HOME/go-workspace/src
	git clone git@github.com:dghubble/gomapreduce.git gomapreduce

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


Notes/Limitations/Uncertainties

Workers process one task at a time rather than maintaining a queue of tasks to be performed. The master is responsible for continuing to try to push out the tasks.



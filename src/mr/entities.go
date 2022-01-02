package mr

import (
	"container/list"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	fileList []string
	reducer  int
	finished bool

	mapTaskRunningQ list.List
	mapTaskWaitingQ list.List

	reduceTaskWaitingQ list.List
	reduceTaskRunningQ list.List
	//result of map task
	interFiles map[string]bool
	//result of reduce task
	finalResult []string
	reducestart bool
}

type Task struct {
	TaskId   int
	TaskType string

	Filename []string
	Result   map[string]bool

	Reducer   int
	Completed bool
}

const (
	RUNNING  = 1
	FINISHED = 2
	WAITING  = 3
)

type TaskStatus struct {
	Task      Task
	State     int
	BeginTime time.Time
}

func (ts *TaskStatus) StartTask() {
	ts.BeginTime = time.Now()
	ts.State = RUNNING
}

func (ts *TaskStatus) OverTime() bool {
	return time.Now().Sub(ts.BeginTime) > time.Second*10
}

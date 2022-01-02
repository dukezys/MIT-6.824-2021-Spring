package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

var mutex sync.Mutex

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

func (c *Coordinator) AllocateTask(args TaskArgs, reply *TaskReply) error {
	mutex.Lock()
	defer mutex.Unlock()
	//Allocate Map Task
	mapWaitingFirst := c.mapTaskWaitingQ.Front()
	if mapWaitingFirst != nil {
		fmt.Printf("COORDINATOR: remove map task %v from waiting queue\n", mapWaitingFirst.Value.(Task).TaskId)
		c.mapTaskWaitingQ.Remove(c.mapTaskWaitingQ.Front())

		task := mapWaitingFirst.Value.(Task)
		ts := TaskStatus{Task: task}
		ts.StartTask()
		c.mapTaskRunningQ.PushBack(ts)
		reply.TaskStatus = ts
		return nil
	}
	//Map queue is 0, allocate reduce task
	if c.mapTaskRunningQ.Len() != 0 || c.mapTaskWaitingQ.Len() != 0 {
		reply.TaskStatus.State = WAITING
		return nil
	}
	//init reduce queue
	if !c.reducestart {
		for n := 0; n < c.reducer; n++ {
			var rf []string
			for k, _ := range c.interFiles {
				sa := strings.Split(k, "-")
				sr := []rune(sa[len(sa)-1])[0]
				if (int(sr) - '0') == n {
					rf = append(rf, k)
				}
			}
			c.reduceTaskWaitingQ.PushBack(Task{TaskId: n, TaskType: "reduce", Filename: rf, Completed: false, Result: map[string]bool{}})
		}
		c.reducestart = true
	}
	reduceWaitingFirst := c.reduceTaskWaitingQ.Front()
	if reduceWaitingFirst != nil {
		c.reduceTaskWaitingQ.Remove(c.reduceTaskWaitingQ.Front())
		task := reduceWaitingFirst.Value.(Task)
		ts := TaskStatus{Task: task}
		ts.StartTask()
		c.reduceTaskRunningQ.PushBack(ts)
		reply.TaskStatus = ts
		return nil
	}
	if c.mapTaskRunningQ.Len() > 0 || c.reduceTaskRunningQ.Len() > 0 {
		reply.TaskStatus.State = WAITING
		return nil
	}

	reply.TaskStatus.State = FINISHED
	c.finished = true
	return nil
}

func (c *Coordinator) TaskCompleteHandler(taskArgs *TaskArgs, taskReply *TaskReply) error {
	mutex.Lock()
	defer mutex.Unlock()
	fmt.Printf("COORDINATOR: receive %v task %v complete\n", taskArgs.TaskStatus.Task.TaskType, taskArgs.TaskStatus.Task.TaskId)
	task := taskArgs.TaskStatus.Task
	//map
	if task.TaskType == "map" {
		if task.Completed {
			//add inter file
			for k, _ := range task.Result {
				c.interFiles[k] = true
			}
			//remove from queue
			var ele *list.Element
			for ele = c.mapTaskRunningQ.Front(); ele != nil && ele.Value.(TaskStatus).Task.TaskId != task.TaskId; ele = ele.Next() {
			}
			if ele != nil {
				fmt.Printf("COORDINATOR: COMPLETED: remove map task %v from running queue\n", task.TaskId)
				c.mapTaskRunningQ.Remove(ele)
			}
		}
		return nil
	}
	//reduce
	if task.Completed {
		//add inter file
		for k, _ := range task.Result {
			c.finalResult = append(c.finalResult, k)
		}
		//remove from queue
		var ele *list.Element
		for ele = c.reduceTaskRunningQ.Front(); ele != nil && ele.Value.(TaskStatus).Task.TaskId != task.TaskId; ele = ele.Next() {
		}
		if ele != nil {
			fmt.Printf("COORDINATOR: COMPLETED: remove reduce task %v from running queue\n", task.TaskId)
			c.reduceTaskRunningQ.Remove(ele)
		}
	}
	return nil
}

func (c *Coordinator) timeController() {
	for {
		mutex.Lock()
		//fmt.Println("=========RUNNING TIMECONTROLLER============")
		mOverTime := c.getTimeOutTasks(&c.mapTaskRunningQ)
		if len(mOverTime) > 0 {
			for _, v := range mOverTime {
				fmt.Printf("COORDINATOR: TIMEOUT: push back map task %v\n", v.TaskId)
				c.mapTaskWaitingQ.PushBack(v)
			}
		}
		rOverTime := c.getTimeOutTasks(&c.reduceTaskRunningQ)
		if len(rOverTime) > 0 {
			for _, v := range rOverTime {
				fmt.Printf("COORDINATOR: TIMEOUT: push back reduce task %v\n", v.TaskId)
				c.reduceTaskWaitingQ.PushBack(v)
			}
		}
		mutex.Unlock()
		time.Sleep(time.Duration(time.Second * 10))
	}
}

func (c *Coordinator) getTimeOutTasks(list *list.List) (tasks []Task) {
	for ele := list.Front(); ele != nil; ele = ele.Next() {
		ts := ele.Value.(TaskStatus)
		if ts.OverTime() {
			tasks = append(tasks, ts.Task)
			fmt.Printf("COORDINATOR: TIMEOUT: remove %v task %v from runningQ\n", ts.Task.TaskType, ts.Task.TaskId)
			list.Remove(ele)
		}
	}
	return tasks
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	if c.finished {
		fmt.Println("==DONE METHOD==")
	}
	// Your code here.
	return c.finished
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//generate Coordinator
	c.fileList = files
	c.reducer = nReduce
	c.reducestart = false

	//generate init queue
	i := 1
	for _, path := range files {
		c.mapTaskWaitingQ.PushBack(Task{
			TaskId:    i,
			TaskType:  "map",
			Reducer:   c.reducer,
			Filename:  []string{path},
			Completed: false,
			Result:    map[string]bool{},
		})
		i++
	}
	c.interFiles = map[string]bool{}
	go c.timeController()

	c.server()
	return &c
}

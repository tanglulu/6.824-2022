package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const STAGE_INIT int8 = 0
const STAGE_MAP int8 = 1
const STAGE_REDUCE int8 = 2
const STAGE_DONE int8 = 3

const TASK_MAP int8 = 0
const TASK_REDUCE int8 = 1
const TASK_STATUS_UNSTARTED = 0
const TASK_STATUS_PROCESSING = 1
const TASK_STATUS_DONE = 2

type Task struct {
	id         int
	taskType   int8
	inputFiles []string
	status     int8
	startTime  float64
	timeout    float64
}

type Coordinator struct {
	stage    int8
	tasks    []Task
	taskDone int

	taskLock sync.Mutex
	nReduce  int
	nMap     int
}

func GetTaskOutputFilename(taskType int8, taskId int, hash int) string {
	if taskType == TASK_MAP {
		return fmt.Sprintf("intermediate-%v-%v.txt", taskId, hash)
	} else {
		return fmt.Sprintf("mr-out-%v", taskId)
	}
}

func (c *Coordinator) GetTask(arg *RpcRequestArgs, reply *RpcRequestReply) error {
	log.Println("receive GetTask call")
	c.taskLock.Lock()
	for taskId, _ := range c.tasks {
		if c.tasks[taskId].status == TASK_STATUS_UNSTARTED {
			c.tasks[taskId].status = TASK_STATUS_PROCESSING
			c.tasks[taskId].startTime = float64(time.Now().Unix())

			c.taskLock.Unlock()

			reply.TaskId = c.tasks[taskId].id
			reply.TaskType = c.tasks[taskId].taskType
			reply.IntputFiles = c.tasks[taskId].inputFiles
			reply.Status = COORDINATOR_TASK
			reply.Num = c.nReduce

			log.Printf("Task %v distributed to Worker %v\n", taskId, arg.WorkerId)
			return nil
		}
	}
	log.Println("There is no tasks to distribute")

	if c.stage == STAGE_DONE {
		reply.Status = COORDINATOR_DONE
	} else {
		reply.Status = COORDINATOR_NONE
	}
	c.taskLock.Unlock()

	return nil
}

func (c *Coordinator) TaskDone(args *RpcResponseArgs, reply *RpcResponseReply) error {
	taskId := args.TaskId

	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	if args.Status == TASK_STATUS_DONE {
		c.taskDone++
		c.tasks[taskId].status = TASK_STATUS_DONE
		log.Printf("task %v done!", taskId)
	}

	if c.taskDone == len(c.tasks) {
		switch c.stage {
		case STAGE_MAP:
			c.stage = STAGE_REDUCE
			c.taskDone = 0
			c.tasks = make([]Task, c.nReduce, c.nReduce)

			for i := 0; i < c.nReduce; i++ {
			        inputFiles := make([]string, c.nReduce, c.nReduce)
				for j := 0; j < c.nMap; j++ {
					inputFiles[j] = GetTaskOutputFilename(TASK_MAP, j, i)
				}
				c.tasks[i] = Task{
					id:         i,
					taskType:   TASK_REDUCE,
					timeout:    10,
					inputFiles: inputFiles,
					status:     TASK_STATUS_UNSTARTED,
				}

			}
			log.Printf("have %v reduce tasks\n", len(c.tasks))
		case STAGE_REDUCE:
			c.stage = STAGE_DONE
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) check_timeout() {
	for {
		c.taskLock.Lock()
		if c.stage == STAGE_INIT {
			time.Sleep(5 * time.Second)
		}

		if c.stage == STAGE_DONE {
			c.taskLock.Unlock()
			break
		}

		log.Println("start check timeout...")
		for i, task := range c.tasks {
			if task.status == TASK_STATUS_PROCESSING {
				if time.Now().Unix()-int64(task.startTime) >= int64(task.timeout) {
					log.Printf("task %v has timeout", i)
					task.status = TASK_STATUS_UNSTARTED
				}
			}
		}
		c.taskLock.Unlock()
		log.Println("check timeout done!")
		time.Sleep(5 * time.Second)
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	c.taskLock.Lock()
	if c.stage == STAGE_DONE {
		ret = true
	}
	c.taskLock.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	file, _ := os.OpenFile("logfile", os.O_CREATE|os.O_RDWR, 0644)
	log.SetOutput(file)
	log.Println("start configure Coordinator...")

	c := Coordinator{}
	c.stage = STAGE_INIT
	c.nReduce = nReduce
	c.nMap = len(files)
	c.taskDone = 0

	// generate map tasks
	c.tasks = make([]Task, len(files), len(files))
	for i, file := range files {
		c.tasks[i] = Task{
			id:         i,
			taskType:   TASK_MAP,
			inputFiles: []string{file},
			timeout:    10,
			status:     TASK_STATUS_UNSTARTED,
		}

	}

	c.stage = STAGE_MAP
	log.Println("Coordinator started!")
	go c.check_timeout()

	c.server()

	return &c
}

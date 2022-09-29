package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	taskId     int // 当前taskId, map阶段为mapId, reduce阶段为reduceId
	startTime  time.Time
	taskStatus TaskStatus
	fileName   string
	jobType    JobType
}

type Coordinator struct {
	// Your definitions here.
	nMap         int          // 输入文件数量
	nReduce      int          // 参数传入
	files        []string     // 要处理的文件,map阶段通过reply传递给worker
	currentPhase CurrentPhase // 判断当前处于 map阶段 or reduce阶段 or all down阶段
	tasks        []Task

	mu sync.Mutex
}

/*
	worker请求Job的RPC, 参数中的reply携带Job所需信息
*/
func (c *Coordinator) Request(args *HeartbeatArgs, reply *HeartbeatReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.currentPhase {
	case AllDonePhase:
		reply.JobType = CompleteJob
	case MapPhase:
		c.selectTask(reply, MapPhase)
	case ReducePhase:
		c.selectTask(reply, ReducePhase)
	}
	// log.Printf("Coordinator: assign a task %v to worker", reply)
	return nil
}

func (c *Coordinator) selectTask(reply *HeartbeatReply, phase CurrentPhase) {
	hasNewJob := false
	var taskLen int
	if phase == MapPhase {
		taskLen = len(c.files)
	} else if phase == ReducePhase {
		taskLen = c.nReduce
	}
	for i := 0; i < taskLen; i++ {
		task := &c.tasks[i]
		if task.taskStatus == Idle {
			hasNewJob = true
			task.taskStatus = Processing
			task.startTime = time.Now()

			reply.Id = task.taskId
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce
			reply.JobType = task.jobType
			// reduce阶段，file直接在worker里根据mr-x-x的格式得到
			reply.Filename = task.fileName
			break
		} else if task.taskStatus == Processing {
			// task[i]超时未完成，需要把它交给其他worker处理
			if time.Now().Sub(task.startTime) > time.Second*10 {
				hasNewJob = true

				reply.Id = task.taskId
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				reply.JobType = task.jobType
				// reduce阶段，file直接在worker里根据mr-x-x的格式得到
				reply.Filename = task.fileName

				task.startTime = time.Now()
				// log.Printf("Coordinator: a worker timeout, assign this task %v to another worker", reply)
				break
			}
		} else if task.taskStatus == Finished {
			continue
		} else {
			fmt.Errorf("unexcepted branch")
		}
	}

	if !hasNewJob {
		reply.JobType = WaitJob
	}
	return

}

func (c *Coordinator) Report(args *JobFinishArgs, reply *JobFinishReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentPhase == AllDonePhase {
		return nil
	}
	if args.JobPhase == c.currentPhase {
		//log.Printf("Coordinator: Worker has completed a %v task %v \n", args.JobPhase, args)
		c.tasks[args.Id].taskStatus = Finished
		if c.IsAllTaskFinish() {
			if c.currentPhase == MapPhase {
				//log.Printf("Coordinator: MapPhase finish, start init ReducePhase")
				c.ReducePhaseInit()
				return nil
			} else if c.currentPhase == ReducePhase {
				//log.Printf("Coordinator: ReducePhase finish, nothing need to do!")
				c.currentPhase = AllDonePhase
				return nil
			} else {
				fmt.Errorf("report: unexcepted branch")
			}
		}

	}
	return nil
}

func (c *Coordinator) IsAllTaskFinish() bool {
	allFinish := true
	for i := 0; i < len(c.tasks); i++ {
		if c.tasks[i].taskStatus != Finished {
			allFinish = false
			break
		}
	}
	return allFinish
}

func (c *Coordinator) ReducePhaseInit() {
	c.currentPhase = ReducePhase
	c.tasks = make([]Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.tasks[i].taskId = i
		c.tasks[i].taskStatus = Idle
		c.tasks[i].jobType = ReduceJob
	}
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

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentPhase == AllDonePhase
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mu:      sync.Mutex{},
		files:   files,
		nMap:    len(files),
		nReduce: nReduce,
	}
	c.currentPhase = MapPhase
	c.tasks = make([]Task, c.nMap)
	for index, file := range c.files {
		c.tasks[index] = Task{
			fileName:   file,
			taskId:     index,
			taskStatus: Idle,
		}
	}

	c.server()
	return &c
}

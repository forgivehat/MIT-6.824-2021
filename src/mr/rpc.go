package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)
import "strconv"

type TaskStatus int

const (
	Idle TaskStatus = iota
	Processing
	Finished
)

type JobType int

const (
	MapJob JobType = iota
	ReduceJob
	WaitJob
	CompleteJob
)

func (job JobType) String() string {
	switch job {
	case MapJob:
		return "MapJob"
	case ReduceJob:
		return "ReduceJob"
	case WaitJob:
		return "WaitJob"
	case CompleteJob:
		return "CompleteJob"
	}
	panic(fmt.Sprintf("unexpected jobType %d", job))
}

type CurrentPhase int

const (
	MapPhase CurrentPhase = iota
	ReducePhase
	AllDonePhase
)

func (phase CurrentPhase) String() string {
	switch phase {
	case MapPhase:
		return "MapPhase"
	case ReducePhase:
		return "ReducePhase"
	case AllDonePhase:
		return "CompletePhase"
	}
	panic(fmt.Sprintf("unexpected SchedulePhase %d", phase))
}

type HeartbeatArgs struct {
}

type HeartbeatReply struct {
	Filename string // map阶段使用
	JobType  JobType
	Id       int // map阶段传mapId, reduce阶段传reduceId
	NReduce  int // map阶段使用
	NMap     int // reduce阶段使用
}

func (response HeartbeatReply) String() string {
	switch response.JobType {
	case MapJob:
		return fmt.Sprintf("{JobType:%v,FileName:%v,Id:%v,NReduce:%v}", response.JobType, response.Filename, response.Id, response.NReduce)
	case ReduceJob:
		return fmt.Sprintf("{JobType:%v,Id:%v,NMap:%v,NReduce:%v}", response.JobType, response.Id, response.NMap, response.NReduce)
	case WaitJob, CompleteJob:
		return fmt.Sprintf("{JobType:%v}", response.JobType)
	}
	panic(fmt.Sprintf("unexpected JobType %d", response.JobType))
}

type JobFinishArgs struct {
	Id       int
	JobPhase CurrentPhase
}

func (request JobFinishArgs) String() string {
	return fmt.Sprintf("{Id:%v,JobPhase:%v}", request.Id, request.JobPhase)
}

type JobFinishReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

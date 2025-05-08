package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// worker发送请求给master要一个任务

type GetTaskReq struct {
}

type GetTaskResp struct {
	Task Task
}
type Task struct {
	TaskId    int
	TaskType  TaskType   // 0,1,2 分别对应未知，map，reduce
	Status    TaskStatus // start process finish 三个状态
	ReduceNum int
	fileName  []string //这玩意来固定我的map任务大小
}

type TaskType int

const (
	UnKnown TaskType = iota
	MapTask
	ReduceTask
)

type TaskStatus int

const (
	Working TaskStatus = iota //此阶段在工作
	Waiting                   //此阶段任务等待执行
	Done                      //此阶段已经做完
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

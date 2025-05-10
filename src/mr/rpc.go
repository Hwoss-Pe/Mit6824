package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

// worker发送请求给master要一个任务

type GetTaskReq struct {
}

type GetTaskResp struct {
	Task Task
}
type Task struct {
	TaskId        int
	TaskType      TaskType   // 0,1,2 分别对应未知，map，reduce
	Status        TaskStatus // start process finish 三个状态
	ReduceNum     int
	FileName      string    //这玩意来固定我的map任务大小
	MapIndex      int       // 存储映射索引
	ReduceIndex   int       // 存储映射索引
	StartTime     time.Time // 任务开始执行的时间
	SkipFiles     []string  // 需要跳过的记录ID列表
	IsIncremental bool      // 是否为增量任务
	CompletedMaps []int     // 已完成的Map任务ID列表
}

type ReportTaskReq struct {
	TaskId   int
	TaskType TaskType
}

type ReportTaskResp struct {
}
type TaskType int

const ( // 这个是记录某个任务的本身
	WaitingTask TaskType = iota //表示切换状态的时候，从map切换reduce还有在跑的
	MapTask
	IncrementalReduceTask //
	ReduceTask
	ExitTask //所有任务完成了
)

type TaskStatus int

const ( //这个是对某个任务的记录过程
	Working TaskStatus = iota //此阶段在工作
	Waiting                   //此阶段任务等待执行
	Done                      //此阶段已经做完
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

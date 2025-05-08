package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	TaskId         int                   // 作为自增任务id
	MapChan        chan *Task            //map任务队列
	ReduceChan     chan *Task            //reduce任务队列
	ReduceNum      int                   //reduce数量
	files          []string              //传入文件数组
	DistPhase      Phase                 //目前框架中处于什么阶段
	TaskMetaHolder map[int]*TaskMetaInfo //主节点可以掌握所有任务，taskId对应

	lock sync.RWMutex
}
type TaskMetaInfo struct {
	state   TaskStatus // 任务的状态
	TaskAdr *Task      // 只需要存储对应的任务指针
}
type Phase int

const (
	MapPhase Phase = iota //此阶段在分发MapTask
	ReducePhase
	AllDone //任务分配完成
)

func (c *Coordinator) GetTask(req *GetTaskReq, reply *GetTaskResp) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	for {
		switch c.DistPhase {
		case MapPhase:
			if len(c.MapChan) > 0 {
				task := <-c.MapChan
				// 记录任务信息
				c.TaskMetaHolder[task.TaskId] = &TaskMetaInfo{
					state:   Waiting,
					TaskAdr: task,
				}
				reply.Task = *task
				return nil
			} else if c.CheckPhaseFinish() {
				// 如果Map阶段任务都完成，切换到Reduce阶段
				c.DistPhase = ReducePhase
				continue
			} else {
				// 没有可用任务，等待
				reply.Task = Task{
					TaskType: UnKnown,
				}
				return nil
			}
		case ReducePhase:
			if len(c.ReduceChan) > 0 {
				task := <-c.ReduceChan
				reply.Task = *task
				// 记录任务信息
				c.TaskMetaHolder[task.TaskId] = &TaskMetaInfo{
					state:   Waiting,
					TaskAdr: task,
				}
				return nil
			} else if c.CheckPhaseFinish() {
				// 所有Reduce任务完成，标记作业完成
				c.DistPhase = AllDone
				return nil
			} else {
				reply.Task = Task{
					TaskType: UnKnown,
				}
				return nil
			}
		case AllDone:
			// 所有任务都已完成
			reply.Task = Task{
				TaskType: UnKnown,
			}
			return nil
		}
	}
}

// 获取全局id的方法，这里用自增id
func (c *Coordinator) generateId() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	res := c.TaskId
	c.TaskId++
	return res
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// MakeCoordinator 文件多少其实也决定了多少个map
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		files:          files,
		ReduceNum:      nReduce,
		DistPhase:      MapPhase,
		MapChan:        make(chan *Task, len(files)),
		ReduceChan:     make(chan *Task, nReduce),
		lock:           sync.RWMutex{},
		TaskMetaHolder: make(map[int]*TaskMetaInfo, len(files)+nReduce), // 任务的总数应该是files + Reducer的数量
	}
	//初始化map任务
	// TODO
	c.makeMapTasks(files)
	c.server()
	return &c
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) CheckPhaseFinish() bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	//检查是否所有阶段任务结束，那就是对记录表进行遍历，然后获取需要的任务类型判断其状态
	switch c.DistPhase {
	case MapPhase:
		{
			for _, info := range c.TaskMetaHolder {
				if info.TaskAdr.TaskType == MapTask {
					if info.state != Done {
						return false
					}
				}
			}
			return len(c.MapChan) == 0
		}
	case ReducePhase:
		{
			for _, info := range c.TaskMetaHolder {
				if info.TaskAdr.TaskType == ReduceTask {
					if info.state != Done {
						return false
					}
				}
			}
			return len(c.ReduceChan) == 0
		}
	default:
		return true
	}
}

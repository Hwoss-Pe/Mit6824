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
	TaskId     int        // 作为自增任务id
	MapChan    chan *Task //map任务队列
	ReduceChan chan *Task //reduce任务队列
	ReduceNum  int        //reduce数量
	files      []string   //传入文件数组
	DistPhase  Phase      //目前框架中处于什么阶段
	TaskMetaHolder  map[int]*TaskMetaInfo //主节点可以掌握所有任务，taskId对应

	lock sync.RWMutex
}
type TaskMetaInfo struct {
	state      TaskStatus   // 任务的状态
	TaskAdr   *Task // 只需要存储对应的任务指针
}
type Phase int

const (
	MapPhase Phase = iota //此阶段在分发MapTask
	ReducePhase
	AllDone //任务分配完成
)

func (c *Coordinator) GetTask(req *GetTaskReq, reply *GetTaskResp) error {
	//我要给你map还是reduce任务呢？
	currentPhase := c.DistPhase
	if currentPhase == MapPhase {

	}else if currentPhase == ReducePhase {

	} else{

	}

	reply.Task = Task{
		TaskId: c.generateId(),
	TaskType: }
	return nil
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

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.文件多少其实也决定了多少个map
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		files:             files,
		ReduceNum:        nReduce,
		DistPhase:         MapPhase,
		MapChan:    make(chan *Task, len(files)),
		ReduceChan: make(chan *Task, nReduce),
		lock: sync.RWMutex{},
		TaskMetaHolder:make(map[int]*TaskMetaInfo, len(files)+nReduce), // 任务的总数应该是files + Reducer的数量
		}
	}
	c.makeMapTasks(files)

	c.server()

	c.server()
	return &c
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

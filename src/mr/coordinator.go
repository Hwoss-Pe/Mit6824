package mr

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	TaskId         int                   // 作为自增任务id
	MapChan        chan *Task            //map任务队列
	ReduceChan     chan *Task            //reduce任务队列
	ReduceNum      int                   //reduce数量
	files          []string              //传入文件数组
	DistPhase      Phase                 //目前框架中处于什么阶段
	TaskMetaHolder map[int]*TaskMetaInfo //主节点可以掌握所有任务，taskId对应
	mapIndexes     map[int]int           // TaskId -> 在Map任务中的索引位置
	reduceIndexes  map[int]int           // TaskId -> 在Reduce任务中的索引位置
	lock           sync.RWMutex
	done           bool                   // 标记所有任务是否完成
	skipFiles      map[string]*SkipRecord // 文件名 -> 跳过记录信息
	skipLock       sync.RWMutex           // 保护skipFiles的锁
}
type TaskMetaInfo struct {
	state     TaskStatus // 任务的状态
	TaskAdr   *Task      // 只需要存储对应的任务指针
	StartTime time.Time  // 任务开始执行的时间
}
type Phase int

const (
	MapPhase Phase = iota //此阶段在分发MapTask
	ReducePhase
	AllDone //任务分配完成
)

// 用于持久化的状态结构体
type CoordinatorState struct {
	TaskId     int                    // 当前任务ID
	DistPhase  Phase                  // 分布式处理阶段
	TaskStates map[int]TaskState      // 任务状态映射
	Done       bool                   // 是否完成
	SkipFiles  map[string]*SkipRecord // 需要跳过的文件
}

// 持久化的任务状态
type TaskState struct {
	Type        TaskType   // 任务类型
	State       TaskStatus // 任务状态
	FileName    string     // 任务对应的文件名
	MapIndex    int        // Map任务索引
	ReduceIndex int        // Reduce任务索引
}

// 启动监听处理端口
func (c *Coordinator) startLastGaspServer() {
	addr := net.UDPAddr{
		Port: LAST_GASP_PORT,
		IP:   net.ParseIP("0.0.0.0")}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		LogError("启动Last Gasp服务器失败: %v", err)
		return
	}
	LogInfo("Last Gasp服务器已启动，监听端口: %d", LAST_GASP_PORT)

	go func() {
		defer conn.Close()

		buf := make([]byte, 1024)
		for {
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				continue
			}

			// 解析消息
			var msg LastGaspMessage
			err = json.Unmarshal(buf[:n], &msg)
			if err != nil {
				continue
			}

			// 处理崩溃报告
			c.handleCrashReport(msg)
		}
	}()
}

// 处理崩溃报告
func (c *Coordinator) handleCrashReport(msg LastGaspMessage) {
	c.skipLock.Lock()
	defer c.skipLock.Unlock()
	LogInfo("收到崩溃报告: Worker=%d, Task=%d, File=%s, Line=%d",
		msg.WorkerId, msg.TaskId, msg.FileName, msg.LineNum)
	// 检查文件是否已在跳过记录中
	record, exists := c.skipFiles[msg.FileName]

	if !exists {
		// 新建跳过记录
		record = &SkipRecord{
			FileName:    msg.FileName,
			CrashCount:  1,
			FirstCrash:  msg.Timestamp,
			LatestCrash: msg.Timestamp,
		}
		c.skipFiles[msg.FileName] = record
	} else {
		// 更新现有记录
		record.CrashCount++
		record.LatestCrash = msg.Timestamp

		// 检查是否达到跳过阈值
		if record.CrashCount >= MAX_CRASHES {
			LogInfo("文件 %s 已达到跳过阈值 (%d次崩溃), 将在后续任务中跳过",
				msg.FileName, record.CrashCount)
		}
	}
}
func (c *Coordinator) ReportTaskCompleted(req *ReportTaskReq, resp *ReportTaskResp) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// 根据任务类型更新相应任务的状态
	if taskInfo, exists := c.TaskMetaHolder[req.TaskId]; exists {
		// 检查任务类型是否匹配
		if taskInfo.TaskAdr.TaskType == req.TaskType {
			// 只有当任务处于进行中状态时才更新
			if taskInfo.state == Working {
				taskInfo.state = Done
				if req.TaskType == MapTask {
					LogInfo("Map任务 %d 已完成", req.TaskId)
				} else if req.TaskType == ReduceTask {
					LogInfo("Reduce任务 %d 已完成", req.TaskId)

					// 检查是否所有Reduce任务都已完成
					if c.DistPhase == ReducePhase {
						allDone := true
						for _, info := range c.TaskMetaHolder {
							if info.TaskAdr.TaskType == ReduceTask && info.state != Done {
								allDone = false
								break
							}
						}

						// 如果所有Reduce任务都完成了，更新阶段状态
						if allDone {
							LogInfo("所有Reduce任务已完成，切换到AllDone阶段")
							c.DistPhase = AllDone
							c.done = true
						}
					}
				}
			}
		}
	}

	return nil
}
func (c *Coordinator) GetTask(req *GetTaskReq, reply *GetTaskResp) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	//  统计任务状态
	mapCount := 0
	mapDone := 0
	reduceCount := 0
	reduceDone := 0
	for _, info := range c.TaskMetaHolder {
		if info.TaskAdr.TaskType == MapTask {
			mapCount++
			if info.state == Done {
				mapDone++
			}
		} else if info.TaskAdr.TaskType == ReduceTask {
			reduceCount++
			if info.state == Done {
				reduceDone++
			}
		}
	}

	// 检查是否需要切换阶段
	if c.DistPhase == MapPhase && mapCount > 0 && mapCount == mapDone && len(c.MapChan) == 0 {
		LogInfo("所有Map任务都已完成，切换到Reduce阶段")
		c.DistPhase = ReducePhase
		c.makeReduceTasks(c.ReduceNum)
	}

	// 在分配任务之前，添加跳过文件列表
	c.skipLock.RLock()
	var skipList []string
	for fileName, record := range c.skipFiles {
		if record.CrashCount >= MAX_CRASHES {
			skipList = append(skipList, fileName)
		}
	}
	c.skipLock.RUnlock()

	// 根据当前阶段分配任务
	switch c.DistPhase {
	case MapPhase:
		// 处理Map阶段的任务分配
		if len(c.MapChan) > 0 {
			task := <-c.MapChan
			// 设置任务开始时间
			startTime := time.Now()
			task.StartTime = startTime
			// 添加跳过文件列表
			task.SkipFiles = skipList
			// 记录任务信息
			c.TaskMetaHolder[task.TaskId] = &TaskMetaInfo{
				state:     Working,
				TaskAdr:   task,
				StartTime: startTime,
			}
			reply.Task = *task
			LogDebug("分配Map任务: %d", task.TaskId)
			return nil
		} else {
			// 没有可用任务，等待
			reply.Task = Task{
				TaskType: WaitingTask,
			}
			return nil
		}
	case ReducePhase:
		// 优先从ReduceChan获取任务
		if len(c.ReduceChan) > 0 {
			task := <-c.ReduceChan

			// 设置任务开始时间
			startTime := time.Now()
			task.StartTime = startTime
			// 添加跳过文件列表
			task.SkipFiles = skipList
			// 更新任务状态
			if info, exists := c.TaskMetaHolder[task.TaskId]; exists {
				info.state = Working
				info.StartTime = startTime
			}

			reply.Task = *task
			LogDebug("分配Reduce任务: ID=%d, ReduceIndex=%d", task.TaskId, task.ReduceIndex)
			return nil
		}
		// 备用方案：直接从TaskMetaHolder查找未分配的Reduce任务
		for id, info := range c.TaskMetaHolder {
			if info.TaskAdr.TaskType == ReduceTask && info.state == Waiting {
				// 找到一个等待中的Reduce任务
				startTime := time.Now()
				info.state = Working
				info.StartTime = startTime
				info.TaskAdr.StartTime = startTime
				reply.Task = *info.TaskAdr
				LogDebug("备用方案: 分配Reduce任务: ID=%d, ReduceIndex=%d", id, info.TaskAdr.ReduceIndex)
				return nil
			}
		}
		// 检查是否所有Reduce任务都已完成
		allDone := true
		for _, info := range c.TaskMetaHolder {
			if info.TaskAdr.TaskType == ReduceTask && info.state != Done {
				allDone = false
				break
			}
		}
		if allDone && reduceCount > 0 {
			// 所有Reduce任务都完成了
			LogInfo("所有Reduce任务已完成，切换到AllDone阶段")
			c.DistPhase = AllDone
			c.done = true // 设置完成标志

			// 分配退出任务
			reply.Task = Task{
				TaskType: ExitTask,
			}
			return nil
		} else {
			// 可能有正在进行中的Reduce任务，返回等待任务
			reply.Task = Task{
				TaskType: WaitingTask,
			}
			return nil
		}
	case AllDone:
		// 所有任务都已完成，返回退出任务
		reply.Task = Task{
			TaskType: ExitTask,
		}
		return nil
	default:
		reply.Task = Task{
			TaskType: WaitingTask,
		}
		return nil
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
	err := rpc.Register(c)
	if err != nil {
		return
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()

	// 尝试删除旧的套接字文件，如果文件不存在也没关系
	err = os.Remove(sockname)
	if err != nil && !os.IsNotExist(err) {
		return
	}

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			log.Fatal("serve error:", err)
		}
	}()
}

// Done 直接判断记录表是不是全部完成了，这个会每个一秒被调用
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	// 使用done标志来检查是否完成
	if c.done {
		LogInfo("所有任务已完成，程序将退出")
		return true
	}

	// 实际检查任务完成情况
	if c.DistPhase != AllDone {
		return false
	}

	// 检查是否有任务仍在进行中
	for taskId, info := range c.TaskMetaHolder {
		if info.state != Done {
			LogDebug("Done检查: 任务 %d 未完成", taskId)
			return false
		}
	}

	// 所有检查都通过，设置done标志
	c.done = true
	return true
}

// MakeCoordinator 文件多少其实也决定了多少个map
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 创建基础的Coordinator结构
	c := Coordinator{
		files:          files,
		ReduceNum:      nReduce,
		MapChan:        make(chan *Task, len(files)),
		ReduceChan:     make(chan *Task, nReduce),
		lock:           sync.RWMutex{},
		TaskMetaHolder: make(map[int]*TaskMetaInfo),
		mapIndexes:     make(map[int]int),
		reduceIndexes:  make(map[int]int),
		done:           false,
	}

	// 尝试从持久化状态恢复
	stateLoaded := c.loadState()

	if !stateLoaded {
		// 如果没有找到状态文件或加载失败，初始化新的MapReduce作业
		LogInfo("初始化新的Coordinator: %d个输入文件, %d个Reduce任务", len(files), nReduce)
		c.DistPhase = MapPhase

		//初始化map任务
		c.makeMapTasks(files)
	} else {
		LogInfo("从持久化状态恢复Coordinator成功")
	}

	// 启动超时检查机制
	c.startTimeoutChecker()
	//启动监听崩溃记录服务器
	c.startLastGaspServer()
	// 启动状态持久化机制
	c.startPersistenceRoutine()

	// 启动RPC服务器
	c.server()
	return &c
}

// makeMapTasks  初始化map任务，文件路径参数
func (c *Coordinator) makeMapTasks(files []string) {
	LogInfo("初始化 %d 个Map任务", len(files))
	for i, file := range files {
		task := &Task{
			TaskId:    c.generateId(),
			TaskType:  MapTask,
			Status:    Waiting,
			ReduceNum: c.ReduceNum,
			FileName:  file,
			MapIndex:  i}
		c.mapIndexes[task.TaskId] = i // 存储映射关系
		c.TaskMetaHolder[task.TaskId] = &TaskMetaInfo{state: Waiting, TaskAdr: task}
		c.MapChan <- task
	}
}

// makeReduceTasks 初始化reduce任务,也就是创建n个reduce任务
func (c *Coordinator) makeReduceTasks(nReduce int) {
	LogInfo("初始化 %d 个Reduce任务", nReduce)

	// 使用defer捕获任何可能的panic
	defer func() {
		if r := recover(); r != nil {
			LogError("Reduce任务创建过程中发生错误: %v", r)
		}
	}()

	for i := 0; i < nReduce; i++ {
		taskId := len(c.files) + i // 使用固定偏移避免与Map任务ID冲突

		// 创建任务对象
		task := &Task{
			TaskId:      taskId,
			TaskType:    ReduceTask,
			Status:      Waiting,
			ReduceNum:   c.ReduceNum,
			ReduceIndex: i,
		}

		// 保存映射关系
		c.reduceIndexes[taskId] = i

		// 保存到TaskMetaHolder
		c.TaskMetaHolder[taskId] = &TaskMetaInfo{
			state:   Waiting,
			TaskAdr: task,
		}

		// 重要：将任务放入ReduceChan通道
		c.ReduceChan <- task
	}

	LogInfo("所有Reduce任务创建完成")
}

// 检查并处理超时任务
func (c *Coordinator) checkTaskTimeout() {
	c.lock.Lock()
	defer c.lock.Unlock()

	// 如果已经完成所有任务，不需要检查
	if c.done {
		return
	}

	now := time.Now()
	timeout := 10 * time.Second // 设置超时时间为10秒

	for id, info := range c.TaskMetaHolder {
		// 只检查状态为Working的任务
		if info.state == Working {
			// 检查任务是否超时
			if now.Sub(info.StartTime) > timeout {
				LogInfo("任务 %d 超时（%v），重新分配", id, now.Sub(info.StartTime))

				// 根据任务类型将其重新放回相应的队列
				if info.TaskAdr.TaskType == MapTask {
					// 重新放回Map任务队列
					info.state = Waiting
					c.MapChan <- info.TaskAdr
					LogInfo("Map任务 %d 重新放回队列", id)
				} else if info.TaskAdr.TaskType == ReduceTask {
					// 重新放回Reduce任务队列
					info.state = Waiting
					c.ReduceChan <- info.TaskAdr
					LogInfo("Reduce任务 %d 重新放回队列", id)
				}
			}
		}
	}
}

// 启动周期性检查任务超时的goroutine
func (c *Coordinator) startTimeoutChecker() {
	go func() {
		for !c.done {
			c.checkTaskTimeout()
			time.Sleep(3 * time.Second) // 每3秒检查一次
		}
	}()
}

// 定期保存状态的函数
func (c *Coordinator) persistState() {
	c.lock.Lock()
	defer c.lock.Unlock()

	state := CoordinatorState{
		TaskId:     c.TaskId,
		DistPhase:  c.DistPhase,
		TaskStates: make(map[int]TaskState),
		Done:       c.done,
	}
	c.skipLock.RLock()
	state.SkipFiles = c.skipFiles
	c.skipLock.RUnlock()

	// 保存任务状态
	for id, info := range c.TaskMetaHolder {
		state.TaskStates[id] = TaskState{
			Type:        info.TaskAdr.TaskType,
			State:       info.state,
			FileName:    info.TaskAdr.FileName,
			MapIndex:    info.TaskAdr.MapIndex,
			ReduceIndex: info.TaskAdr.ReduceIndex,
		}
	}

	// 序列化到文件
	data, err := json.Marshal(state)
	if err != nil {
		LogError("序列化状态失败: %v", err)
		return
	}

	// 使用临时文件+重命名确保原子性写入
	tmpFile, err := os.CreateTemp(".", "mr-state-tmp-*")
	if err != nil {
		LogError("创建临时状态文件失败: %v", err)
		return
	}

	_, err = tmpFile.Write(data)
	if err != nil {
		LogError("写入状态失败: %v", err)
		tmpFile.Close()
		return
	}

	tmpFile.Close()
	err = os.Rename(tmpFile.Name(), "mr-coordinator-state.json")
	if err != nil {
		LogError("重命名状态文件失败: %v", err)
	}
}

// 从持久化状态恢复
func (c *Coordinator) loadState() bool {
	data, err := os.ReadFile("mr-coordinator-state.json")
	if err != nil {
		LogInfo("没有找到状态文件或读取失败: %v", err)
		return false
	}

	var state CoordinatorState
	err = json.Unmarshal(data, &state)
	if err != nil {
		LogError("解析状态文件失败: %v", err)
		return false
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// 恢复基本状态
	c.TaskId = state.TaskId
	c.DistPhase = state.DistPhase
	c.done = state.Done

	// 恢复任务状态
	for id, taskState := range state.TaskStates {
		task := &Task{
			TaskId:      id,
			TaskType:    taskState.Type,
			Status:      Waiting, // 重启后所有任务重新设为等待状态
			ReduceNum:   c.ReduceNum,
			FileName:    taskState.FileName,
			MapIndex:    taskState.MapIndex,
			ReduceIndex: taskState.ReduceIndex,
		}

		// 将任务加入对应队列
		if taskState.State != Done {
			if task.TaskType == MapTask {
				c.MapChan <- task
			} else if task.TaskType == ReduceTask {
				c.ReduceChan <- task
			}
		}

		// 记录任务元数据
		var state TaskStatus
		if taskState.State == Done {
			state = Done
		} else {
			state = Waiting
		}
		c.TaskMetaHolder[id] = &TaskMetaInfo{
			state:   state,
			TaskAdr: task,
		}

		// 更新索引映射
		if task.TaskType == MapTask {
			c.mapIndexes[id] = task.MapIndex
		} else if task.TaskType == ReduceTask {
			c.reduceIndexes[id] = task.ReduceIndex
		}

	}
	// 恢复跳过文件信息
	c.skipLock.Lock()
	if state.SkipFiles != nil {
		c.skipFiles = state.SkipFiles
		LogInfo("恢复了 %d 个需要跳过的文件记录", len(c.skipFiles))
	}
	c.skipLock.Unlock()
	LogInfo("成功从状态文件恢复: 当前阶段=%v, 任务总数=%d", c.DistPhase, len(c.TaskMetaHolder))
	return true
}

// 启动状态持久化后台协程
func (c *Coordinator) startPersistenceRoutine() {
	go func() {
		for !c.done {
			c.persistState()
			time.Sleep(5 * time.Second) // 每5秒保存一次状态
		}
		// 最后一次保存状态
		c.persistState()
	}()
}

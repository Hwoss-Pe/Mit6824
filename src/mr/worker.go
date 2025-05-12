package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"
)

// 全局变量存储当前处理的记录信息
var (
	currentTaskId   int
	currentTaskType TaskType
	currentFileName string // 当前处理的文件名
	currentLineNum  int    // 当前处理的行号
)

// setupSignalHandler 信号处理器
func setupSignalHandler() {
	c := make(chan os.Signal, 1)
	//指定的系统调用的信号比如段错误和无效内存地址都转发到对应的通道里面
	//这实际上就是越界和控制针模拟程序出问题了，对于不能解析数据导致json错误的，思路是在解析的时候err打印日志就行了。
	//如果严格要求解析没问题，这块可能需要增加一个错误记录机制
	signal.Notify(c, syscall.SIGSEGV, syscall.SIGBUS)

	go func() {
		<-c
		//发送Last Gasp消息
		if currentFileName != "" {
			SendLastGaspMessage(currentTaskId, currentTaskType, currentFileName, currentLineNum)
		}
		// 退出进程
		os.Exit(1)
	}()
}

// SendLastGaspMessage 往udp里面发送崩溃记录
func SendLastGaspMessage(taskId int, taskType TaskType, fileName string, lineNum int) {
	// 创建Last Gasp消息
	msg := LastGaspMessage{
		WorkerId:  os.Getpid(),
		TaskId:    taskId,
		TaskType:  taskType,
		FileName:  fileName,
		LineNum:   lineNum,
		Timestamp: time.Now().Unix(),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	conn, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", LAST_GASP_PORT))
	if err != nil {
		return
	}
	defer conn.Close()

	// 发送消息
	conn.Write(data)
}

type ByKey []KeyValue

func (b ByKey) Len() int {
	return len(b)
}

func (b ByKey) Less(i, j int) bool {
	return b[i].Key < b[j].Key
}

func (b ByKey) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker 启动后就会调用这个函数，那么就要他去不断的获取任务，直到那边说我不用干了
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	setupSignalHandler()

	for {
		task := GetTask()

		switch task.TaskType {
		case MapTask:
			task.Status = Working
			LogInfo("执行Map任务: %d, 文件: %s", task.TaskId, task.FileName)
			currentTaskId = task.TaskId
			currentTaskType = MapTask
			handleMapTask(task, mapf)
			currentFileName = ""
			currentLineNum = 0
			ReportTaskDone(task.TaskId, MapTask)

		case IncrementalReduceTask:
			task.Status = Working
			LogInfo("执行增量Reduce任务: %d, ReduceIndex: %d", task.TaskId, task.ReduceIndex)
			currentTaskId = task.TaskId
			currentTaskType = IncrementalReduceTask
			handleIncrementalReduceTask(task, reducef)
			currentFileName = ""
			currentLineNum = 0
			ReportTaskDone(task.TaskId, IncrementalReduceTask)

		case ReduceTask:
			task.Status = Working
			LogInfo("执行最终Reduce任务: %d, ReduceIndex: %d", task.TaskId, task.ReduceIndex)
			currentTaskId = task.TaskId
			currentTaskType = ReduceTask
			handleFinalReduceTask(task, reducef)
			currentFileName = ""
			currentLineNum = 0
			ReportTaskDone(task.TaskId, ReduceTask)

		case WaitingTask:
			time.Sleep(time.Second)

		case ExitTask:
			LogInfo("所有任务都已完成，worker退出")
			return

		default:
			LogError("信号未知，直接退出")
			return
		}
	}
}

// 处理最终Reduce任务
func handleFinalReduceTask(task Task, reducef func(string, []string) string) {
	reduceIdx := task.ReduceIndex
	LogDebug("最终Reduce任务[%d]开始处理", task.TaskId)

	// 1. 读取所有增量后完成的Map任务生成的中间文件
	intermediate := []KeyValue{}

	// 如果有增量阶段，只读取增量后完成的Map任务文件
	if task.CompletedMaps != nil && len(task.CompletedMaps) > 0 {
		for _, mapIdx := range task.CompletedMaps {
			filename := fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
			file, err := os.Open(filename)
			if err != nil {
				if !os.IsNotExist(err) {
					LogError("打开文件 %s 失败: %v", filename, err)
				}
				continue
			}

			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					if err != io.EOF {
						LogError("解码文件 %s 出错: %v", filename, err)
					}
					break
				}
				intermediate = append(intermediate, kv)
			}

			file.Close()
		}
	} else {
		// 没有增量阶段，读取所有可能的中间文件
		for i := 0; i < 100; i++ { //这个100应该是多少个map的数据，设计缺陷懒得改
			filename := fmt.Sprintf("mr-%d-%d", i, reduceIdx)
			file, err := os.Open(filename)
			if err != nil {
				if !os.IsNotExist(err) {
					LogError("打开文件 %s 失败: %v", filename, err)
				}
				continue
			}

			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					if err != io.EOF {
						LogError("解码文件 %s 出错: %v", filename, err)
					}
					break
				}
				intermediate = append(intermediate, kv)
			}

			file.Close()
		}
	}

	// 2. 读取增量结果文件
	incrRawName := fmt.Sprintf("mr-incr-raw-%d", reduceIdx)
	rawKVs := make(map[string][]string)

	if file, err := os.Open(incrRawName); err == nil {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			//这里的数据已经是key和value一行一个了
			parts := strings.SplitN(line, "\t", 2)
			if len(parts) == 2 { //注意这个values有可能是1，1 他就一起被序列化了["1", "1"] 转为 JSON字符串 "[\"1\",\"1\"]"
				key := parts[0]
				var values []string
				json.Unmarshal([]byte(parts[1]), &values)
				rawKVs[key] = values
			}
		}
		file.Close()
	}

	// 3. 创建最终输出文件
	oname := fmt.Sprintf("mr-out-%d", reduceIdx)
	tempFile, err := ioutil.TempFile("", "reduce-*")
	if err != nil {
		LogError("无法创建临时文件: %v", err)
		return
	}

	// 4. 合并增量结果和新数据
	newKVs := make(map[string][]string)

	// 收集新数据中的键值对
	for _, kv := range intermediate {
		newKVs[kv.Key] = append(newKVs[kv.Key], kv.Value)
	}

	// 合并所有键
	allKeys := make(map[string]bool)
	for k := range rawKVs {
		allKeys[k] = true
	}
	for k := range newKVs {
		allKeys[k] = true
	}

	// 处理每个键
	for key := range allKeys {
		var values []string

		// 合并增量结果中的值
		if incrValues, exists := rawKVs[key]; exists {
			values = append(values, incrValues...) //这里保存
		}

		// 合并新数据中的值
		if newValues, exists := newKVs[key]; exists {
			values = append(values, newValues...)
		}
		//这里做的就是吧不同的比如1，1。和1，1变成1，1，1，1
		// 应用Reduce函数
		if len(values) > 0 {
			output := reducef(key, values)
			fmt.Fprintf(tempFile, "%v %v\n", key, output)
		}
	}

	// 关闭并重命名文件
	tempFile.Close()
	os.Rename(tempFile.Name(), oname)

	LogInfo("最终Reduce任务[%d]完成", task.TaskId)
}

// 处理增量Reduce任务
func handleIncrementalReduceTask(task Task, reducef func(string, []string) string) {
	reduceIdx := task.ReduceIndex
	LogDebug("增量Reduce任务[%d]开始处理", task.TaskId)

	intermediate := []KeyValue{}
	fileCount := 0

	// 只读取已完成Map任务的中间文件
	for _, mapIdx := range task.CompletedMaps {
		filename := fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
		file, err := os.Open(filename)
		if err != nil {
			if !os.IsNotExist(err) {
				LogError("打开文件 %s 失败: %v", filename, err)
			}
			continue
		}

		fileCount++

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err != io.EOF {
					LogError("解码文件 %s 出错: %v", filename, err)
				}
				break
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	if len(intermediate) == 0 {
		LogInfo("增量Reduce任务[%d]没有找到数据", task.TaskId)
		return
	}

	// 按key排序
	sort.Sort(ByKey(intermediate))

	// 创建增量结果文件
	incrName := fmt.Sprintf("mr-incr-%d", reduceIdx)
	incrRawName := fmt.Sprintf("mr-incr-raw-%d", reduceIdx)

	tempFile, err := ioutil.TempFile("", "incr-reduce-*")
	if err != nil {
		LogError("无法创建临时文件: %v", err)
		return
	}

	rawTempFile, err := ioutil.TempFile("", "incr-raw-*")
	if err != nil {
		LogError("无法创建原始值临时文件: %v", err)
		tempFile.Close()
		return
	}

	// 处理数据，保存结果和原始值
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		key := intermediate[i].Key
		currentFileName = fmt.Sprintf("incr-reduce-%d-key-%s", task.ReduceIndex, key)
		currentLineNum = i

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// 保存原始值列表到raw文件
		rawValues, _ := json.Marshal(values)
		fmt.Fprintf(rawTempFile, "%s\t%s\n", key, string(rawValues))

		// 应用Reduce函数并保存结果
		output := ""
		func() {
			defer func() {
				if r := recover(); r != nil {
					LogError("增量Reduce处理key '%s' 时崩溃: %v", key, r)
				}
			}()

			output = reducef(key, values)
		}()

		if output != "" {
			fmt.Fprintf(tempFile, "%v %v\n", key, output)
		}

		i = j
	}

	// 关闭并重命名文件
	tempFile.Close()
	rawTempFile.Close()

	os.Rename(tempFile.Name(), incrName)
	os.Rename(rawTempFile.Name(), incrRawName)

	LogInfo("增量Reduce任务[%d]完成", task.TaskId)
}

// ReportTaskDone 需要告诉他我处理完了，然后更改那边的状态表
func ReportTaskDone(taskId int, taskType TaskType) {
	args := ReportTaskReq{
		TaskId:   taskId,
		TaskType: taskType,
	}
	reply := ReportTaskResp{}
	success := call("Coordinator.ReportTaskCompleted", &args, &reply)
	if !success {
		LogError("报告任务完成失败: TaskId=%d, TaskType=%v", taskId, taskType)
	}
}

func handleReduceTask(task Task, reducef func(string, []string) string) {
	// 读取所有中间文件，排序处理，生成结果
	reduceFileId := task.ReduceIndex
	LogDebug("Reduce任务[%d]开始处理", task.TaskId)

	intermediate := []KeyValue{}
	fileCount := 0

	// 尝试读取所有可能的中间文件
	for i := 0; i < 100; i++ { // 设计缺陷，这里应该是map乘reduce
		filename := fmt.Sprintf("mr-%d-%d", i, reduceFileId)
		file, err := os.Open(filename)
		if err != nil {
			if !os.IsNotExist(err) {
				LogError("打开文件 %s 失败: %v", filename, err)
			}
			continue // 尝试下一个文件
		}

		fileCount++

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err != io.EOF {
					LogError("解码文件 %s 出错: %v", filename, err)
				}
				break
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	if len(intermediate) == 0 {
		LogInfo("Reduce任务[%d]没有找到数据，创建空输出文件", task.TaskId)
		oname := fmt.Sprintf("mr-out-%d", reduceFileId)
		emptyFile, _ := os.Create(oname)
		if emptyFile != nil {
			emptyFile.Close()
		}
		return
	}

	// 按key排序
	sort.Sort(ByKey(intermediate))

	// 创建输出文件
	oname := fmt.Sprintf("mr-out-%d", reduceFileId)
	tempFile, err := ioutil.TempFile("", "reduce-*")
	if err != nil {
		LogError("无法创建临时文件: %v", err)
		return
	}

	//排序后把相同的进行聚合
	i := 0
	outputCount := 0

	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		key := intermediate[i].Key
		currentFileName = fmt.Sprintf("reduce-%d-key-%s", task.ReduceIndex, key)
		// 使用键值对在数组中的索引位置作为"行号"
		currentLineNum = i

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		func() {
			defer func() {
				if r := recover(); r != nil {
					LogError("Reduce处理key '%s' 时崩溃: %v", key, r)
					// 信号处理器将发送Last Gasp消息
				}
			}()

			// 调用用户的reduce函数
			output = reducef(key, values)
		}()
		// 清除当前记录信息
		currentFileName = ""
		currentLineNum = 0

		// 如果没有崩溃，输出结果
		if output != "" {
			fmt.Fprintf(tempFile, "%v %v\n", key, output)
			outputCount++
		}

		i = j
	}

	tempFile.Close()

	// 原子式重命名
	err = os.Rename(tempFile.Name(), oname)
	if err != nil {
		LogError("重命名输出文件失败: %v", err)
		return
	}

	LogInfo("Reduce任务[%d]完成，输出了 %d 个结果", task.TaskId, outputCount)
}

func handleMapTask(task Task, mapf func(string, string) []KeyValue) {
	// 检查文件是否在跳过列表中
	if task.SkipFiles != nil {
		for _, skipFile := range task.SkipFiles {
			if skipFile == task.FileName {
				LogInfo("跳过已知有问题的文件: %s", task.FileName)
				return // 跳过此文件的处理
			}
		}
	}
	//读取文件，执行map，输出文件，
	file, err := os.Open(task.FileName)
	if err != nil {
		LogError("无法打开文件: %s", task.FileName)
		return
	}
	defer file.Close()

	// 设置当前文件名
	currentFileName = task.FileName
	// 使用Scanner跟踪行号
	scanner := bufio.NewScanner(file)
	currentLineNum = 0
	var contentBuilder strings.Builder
	for scanner.Scan() {
		currentLineNum++
		contentBuilder.WriteString(scanner.Text())
		contentBuilder.WriteString("\n")
	}
	// 重置行号计数器，因为我们将处理整个文件
	currentLineNum = 0
	contentStr := contentBuilder.String()
	// 使用defer/recover处理可能的panic
	var values []KeyValue
	//只会捕获reduce造成的panic，防御编程
	func() {
		defer func() {
			if r := recover(); r != nil {
				LogError("处理文件 %s 时崩溃: %v",
					currentFileName, r)
				// 信号处理器会发送Last Gasp消息，包含当前行号
			}
		}()

		// 调用Map函数处理整个文件
		values = mapf(task.FileName, contentStr)
	}()

	// 清除文件名和行号
	currentFileName = ""
	currentLineNum = 0

	// 如果发生panic，values可能为nil
	if values == nil {
		return
	}

	//创建中间数据，对于一个文件，有多少reduce就会有多少对应的中间文件
	intermediates := make([]*os.File, task.ReduceNum)
	//转json方便在不同程序运行
	encoders := make([]*json.Encoder, task.ReduceNum)

	for i := 0; i < task.ReduceNum; i++ {
		oname := fmt.Sprintf("mr-%d-%d", task.MapIndex, i) //比如mr-2-3标识某个map任务的第几段
		create, err := os.Create(oname)
		if err != nil {
			LogError("创建中间文件失败: %s, %v", oname, err)
			return
		}
		intermediates[i] = create
		encoders[i] = json.NewEncoder(create)
	}

	//将map返回的数据进行分区hash
	for _, kv := range values {
		//把相同的键值对都放进一个中间文件
		reduceIndex := ihash(kv.Key) % task.ReduceNum
		err := encoders[reduceIndex].Encode(&kv)
		if err != nil {
			LogError("编码失败 %v", err)
			return
		}
	}

	// 关闭所有中间文件
	for i := 0; i < task.ReduceNum; i++ {
		intermediates[i].Close()
	}

	LogInfo("Map任务[%d]完成，处理了 %d 个键值对", task.TaskId, len(values))
}

func GetTask() Task {
	req := GetTaskReq{}
	resp := GetTaskResp{}
	ok := call("Coordinator.GetTask", &req, &resp)
	if !ok {
		LogError("获取任务失败，可能是Coordinator已停止或网络问题")
	}
	return resp.Task
}

// 辅助函数: 检查字符串是否在字符串切片中
func contains(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return true
		}
	}
	return false
}

// LogSkippedRecord  打印跳过记录的日志
func LogSkippedRecord(recordId string) {
	LogInfo("跳过已知有问题的记录: %s", recordId)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

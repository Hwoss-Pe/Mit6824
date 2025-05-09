package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task := GetTask()
		//这个task就是主节点帮我处理好的了
		switch task.TaskType {
		case MapTask:
			{
				task.Status = Working
				LogInfo("执行Map任务: %d, 文件: %s", task.TaskId, task.FileName)
				handleMapTask(task, mapf)
				ReportTaskDone(task.TaskId, MapTask)
			}
		case ReduceTask:
			{
				task.Status = Working
				LogInfo("执行Reduce任务: %d, ReduceIndex: %d", task.TaskId, task.ReduceIndex)
				handleReduceTask(task, reducef)
				ReportTaskDone(task.TaskId, ReduceTask)
			}
		case WaitingTask:
			time.Sleep(time.Second)
		case ExitTask:
			{
				LogInfo("所有任务都已完成，worker退出")
				return
			}
		default:
			{
				LogError("信号未知，直接退出")
				return
			}
		}
	}
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
	for i := 0; i < 100; i++ { // 设置上限以避免无限循环
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

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		// 输出到文件
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		outputCount++

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
	//读取文件，执行map，输出文件，
	file, err := os.Open(task.FileName)
	if err != nil {
		LogError("无法打开文件: %s", task.FileName)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		LogError("无法读取 %v", task.FileName)
		return
	}
	file.Close()
	values := mapf(task.FileName, string(content))

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

package main

//
// 简单的顺序执行版 MapReduce
//
// 运行命令: go run mrsequential.go wc.so pg*.txt
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"plugin"
	"sort"

	"6.5840/mr"
)

// 用于按键排序
type ByKey []mr.KeyValue

// 用于按键排序的接口实现
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "用法: mrsequential xxx.so 输入文件...\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	//
	// 读取每个输入文件,
	// 将其传递给 Map 函数,
	// 收集 Map 的中间输出结果
	//
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("无法打开文件 %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("无法读取文件 %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// 与真正的 MapReduce 的一个重大区别是所有
	// 中间数据都存储在一个地方 intermediate[] 中，
	// 而不是被分区到 NxM 个桶中
	//

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// 对 intermediate[] 中的每个不同键调用 Reduce 函数，
	// 并将结果打印到 mr-out-0 文件中
	//
	i := 0
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

		// 这是 Reduce 输出的每一行的正确格式
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// 从插件文件加载应用程序的 Map 和 Reduce 函数
// 例如从 ../mrapps/wc.so 文件加载
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("无法加载插件 %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("在 %v 中找不到 Map 函数", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("在 %v 中找不到 Reduce 函数", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

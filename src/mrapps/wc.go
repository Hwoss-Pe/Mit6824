package main

//
// MapReduce 框架的单词计数应用插件
// 一个用于 MapReduce 的词频统计应用
//
// 构建命令: go build -buildmode=plugin wc.go
//

import (
	"strconv"
	"strings"
	"unicode"

	"6.5840/mr"
)

// Map 函数为每个输入文件调用一次
// 第一个参数是输入文件的名称，第二个参数是文件的完整内容
// 您应该忽略输入文件名，只关注内容参数
// 返回值是键/值对的切片
func Map(filename string, contents string) []mr.KeyValue {
	// 用于检测单词分隔符的函数
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// 将内容分割成单词数组
	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

// Reduce 函数为 Map 任务生成的每个键调用一次
// 参数包含一个键和由任何 Map 任务为该键创建的所有值的列表
func Reduce(key string, values []string) string {
	// 返回这个单词出现的次数
	return strconv.Itoa(len(values))
}

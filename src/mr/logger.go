package mr

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

// 日志级别
const (
	LogLevelNone  = 0 // 不输出任何日志
	LogLevelError = 1 // 只输出错误
	LogLevelInfo  = 2 // 输出信息和错误
	LogLevelDebug = 3 // 输出所有信息，包括调试信息
)

var logLevel = LogLevelNone // 默认不输出日志

// 初始化日志级别，从环境变量读取
func init() {
	// 设置日志前缀
	log.SetPrefix("[MR] ")

	levelStr := os.Getenv("MR_LOG_LEVEL")
	if levelStr != "" {
		level, err := strconv.Atoi(levelStr)
		if err == nil && level >= LogLevelNone && level <= LogLevelDebug {
			logLevel = level
			//log.Printf("日志级别设置为: %d", logLevel)
		} else {
			log.Printf("无效的日志级别: %s, 使用默认级别: %d", levelStr, logLevel)
		}
	} else {
		log.Printf("未设置环境变量MR_LOG_LEVEL，使用默认级别: %d", logLevel)
	}
}

// LogError 输出错误信息
func LogError(format string, v ...interface{}) {
	if logLevel >= LogLevelError {
		log.Printf("[ERROR] "+format, v...)
	}
}

// LogInfo 输出普通信息
func LogInfo(format string, v ...interface{}) {
	if logLevel >= LogLevelInfo {
		log.Printf("[INFO] "+format, v...)
	}
}

// LogDebug 输出调试信息
func LogDebug(format string, v ...interface{}) {
	if logLevel >= LogLevelDebug {
		log.Printf("[DEBUG] "+format, v...)
	}
}

// LogPanic 记录严重错误并立即终止程序
func LogPanic(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	log.Printf("[PANIC] %s", msg)
	panic(msg)
}

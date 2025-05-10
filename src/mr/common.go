package mr

// LastGaspMessage 奔溃信息结构
type LastGaspMessage struct {
	WorkerId  int      // worker ID
	TaskId    int      // 任务ID
	TaskType  TaskType // 任务类型
	FileName  string   // 导致崩溃的文件名
	LineNum   int      // 导致崩溃的行号（如果可用）
	Timestamp int64    // 崩溃时间戳
}

// 崩溃记录信息
type SkipRecord struct {
	FileName    string // 记录ID
	CrashCount  int    // 崩溃次数，超过两次就跑路
	FirstCrash  int64  // 首次崩溃时间
	LatestCrash int64  // 最近崩溃时间
}

// LAST_GASP_PORT - UDP端口用于Last Gasp消息
const LAST_GASP_PORT = 8888

// MAX_CRASHES - 标记跳过前允许的最大崩溃次数
const MAX_CRASHES = 2

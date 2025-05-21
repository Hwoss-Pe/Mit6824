package raft

// 文件 raftapi/raft.go 定义了 raft 必须向服务器（或测试器）暴露的接口，
// 但请查看下面每个函数的注释以获取更多详细信息。
//
// Make() 创建一个实现 raft 接口的新 raft 节点。

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// 常量定义
const (
	// 心跳间隔，毫秒
	HEARTBEAT_INTERVAL = 100
	// 选举超时最小值，毫秒
	MIN_ELECTION_TIMEOUT = 400
	// 选举超时最大值，毫秒
	MAX_ELECTION_TIMEOUT = 600
)

// Raft 节点
type Raft struct {
	mu        sync.Mutex          // 锁，用于保护对该节点状态的共享访问
	peers     []*labrpc.ClientEnd // 所有节点的 RPC 端点
	persister *tester.Persister   // 用于保存该节点持久化状态的对象
	me        int                 // 该节点在 peers[] 中的索引
	dead      int32               // 由 Kill() 设置

	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int
	//领导者才有的字段
	nextIndex  []int
	matchIndex []int //每一个服务器已经复制到该服务器的最大索引号(初始化为0，单调递增)

	state         string // 服务器状态："follower"、"candidate"或"leader"
	lastHeartbeat time.Time
	votesReceived int // 选举中收到的投票数

	applyCh chan raftapi.ApplyMsg //3b中用于向应用发送已经提交的命令
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// GetState 返回当前任期和该服务器是否认为自己是领导者
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == "leader"

	return term, isleader
}

// 将 Raft 的持久化状态保存到稳定存储中，
// 在崩溃和重启后可以从中恢复。
// 参见论文的图 2，了解哪些状态应该持久化。
// 在实现快照之前，你应该将 nil 作为
// persister.Save() 的第二个参数。
// 在实现快照后，传递当前快照
// (如果还没有快照，则传递 nil)。
func (rf *Raft) persist() {
	// 你的代码在这里 (3C)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		fmt.Printf("服务器 %d: 编码currentTerm失败: %v\n", rf.me, err)
		return
	}
	if err := e.Encode(rf.votedFor); err != nil {
		fmt.Printf("服务器 %d: 编码votedFor失败: %v\n", rf.me, err)
		return
	}
	if err := e.Encode(rf.logs); err != nil {
		fmt.Printf("服务器 %d: 编码logs失败: %v\n", rf.me, err)
		return
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

	fmt.Printf("服务器 %d: 持久化状态 - 任期=%d, 投票=%d, 日志长度=%d\n",
		rf.me, rf.currentTerm, rf.votedFor, len(rf.logs))
}

// 恢复之前持久化的状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // 没有任何状态进行引导？
		return
	}
	// 你的代码在这里 (3C)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if err := d.Decode(&currentTerm); err != nil {
		fmt.Printf("服务器 %d: 解码currentTerm失败: %v\n", rf.me, err)
		return
	}
	if err := d.Decode(&votedFor); err != nil {
		fmt.Printf("服务器 %d: 解码votedFor失败: %v\n", rf.me, err)
		return
	}
	if err := d.Decode(&logs); err != nil {
		fmt.Printf("服务器 %d: 解码logs失败: %v\n", rf.me, err)
		return
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs

	fmt.Printf("服务器 %d: 从持久化恢复状态 - 任期=%d, 投票=%d, 日志长度=%d\n",
		rf.me, rf.currentTerm, rf.votedFor, len(rf.logs))
}

// Raft 持久化日志的字节数
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// 服务表示它已创建包含所有信息直到并包括索引的快照。
// 这意味着服务不再需要（包括）该索引的日志。
// Raft 现在应尽可能地裁剪其日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 你的代码在这里 (3D)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC 处理候选投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 初始化回复
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	fmt.Printf("服务器 %d: 收到来自 %d 的RequestVote, 候选人任期=%d, 我的任期=%d, 我的投票=%d\n",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)

	// 规则1: 如果请求的任期小于当前任期，拒绝投票
	if args.Term < rf.currentTerm {
		fmt.Printf("服务器 %d: 拒绝给 %d 投票 (更低的任期 %d < %d)\n",
			rf.me, args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	// 如果请求的任期更高，转为follower
	if args.Term > rf.currentTerm {
		fmt.Printf("服务器 %d: 由于收到更高任期的RequestVote %d > %d，转为跟随者\n",
			rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.votedFor = -1
		rf.persist()
	}

	// 更新回复中的任期
	reply.Term = rf.currentTerm

	// 规则2: 如果votedFor为空或已经投票给候选人，且候选人的日志至少与接收者的一样新，则投票
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 获取本地最后一条日志的信息
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := 0
		if lastLogIndex >= 0 {
			lastLogTerm = rf.logs[lastLogIndex].Term
		}

		// 严格按照Raft论文5.4.1节实现日志比较:
		// 1. 如果最后日志任期不同，则比较任期
		// 2. 如果最后日志任期相同，则比较日志长度
		// 投票给"更新"的日志（任期高的，或任期相同但更长的）
		logIsUpToDate := false

		if args.LastLogTerm > lastLogTerm {
			// 候选人的最后日志任期更高
			logIsUpToDate = true
			fmt.Printf("服务器 %d: 候选人 %d 的日志任期更高 (%d > %d)\n",
				rf.me, args.CandidateId, args.LastLogTerm, lastLogTerm)
		} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
			// 任期相同，但候选人的日志长度更长或相等
			logIsUpToDate = true
			fmt.Printf("服务器 %d: 候选人 %d 的日志任期相同但长度更长或相等 (%d >= %d)\n",
				rf.me, args.CandidateId, args.LastLogIndex, lastLogIndex)
		} else {
			fmt.Printf("服务器 %d: 候选人 %d 的日志较旧 (任期:%d vs %d, 长度:%d vs %d)\n",
				rf.me, args.CandidateId, args.LastLogTerm, lastLogTerm, args.LastLogIndex, lastLogIndex)
		}

		fmt.Printf("服务器 %d: 给 %d 的日志比较 - 我的最后任期=%d, 候选人最后任期=%d, 日志OK=%v\n",
			rf.me, args.CandidateId, lastLogTerm, args.LastLogTerm, logIsUpToDate)

		if logIsUpToDate {
			// 投票给候选人
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()

			// 重置选举超时计时器
			rf.lastHeartbeat = time.Now()

			fmt.Printf("服务器 %d: 在任期 %d 中投票给 %d\n", rf.me, rf.currentTerm, args.CandidateId)
		} else {
			fmt.Printf("服务器 %d: 由于日志较旧拒绝给 %d 投票\n", rf.me, args.CandidateId)
		}
	} else {
		fmt.Printf("服务器 %d: 已经投票给 %d, 不能再投票给 %d\n",
			rf.me, rf.votedFor, args.CandidateId)
	}
}

// 示例代码，向服务器发送 RequestVote RPC
// server 是目标服务器在 rf.peers[] 中的索引
// 期望 args 中的 RPC 参数
// 用 RPC 回复填充 *reply，因此调用者应该
// 传递 &reply
// 传递给 Call() 的 args 和 reply 的类型必须与
// 处理函数中声明的参数类型相同（包括它们是否是指针）
//
// labrpc 包模拟一个不可靠的网络，其中服务器
// 可能无法访问，请求和回复可能会丢失
// Call() 发送请求并等待回复。如果回复在
// 超时间隔内到达，Call() 返回 true；否则
// Call() 返回 false。因此 Call() 可能在一段时间内不返回。
// 如果服务器死亡、无法连接的活跃服务器、
// 丢失的请求或丢失的回复，都可能导致 false 返回
//
// Call() 保证返回（可能在延迟后），*除非* 服务器端的
// 处理函数不返回。因此，不需要
// 在 Call() 周围实现自己的超时。
//
// 查看 ../labrpc/labrpc.go 中的注释以获取更多详细信息
//
// 如果你在使 RPC 工作时遇到麻烦，请检查是否已
// 将通过 RPC 传递的结构体中的所有字段名大写，并且
// 调用者使用 & 传递回复结构体的地址，而不是
// 结构体本身。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Printf("服务器 %d: 实际发送RequestVote RPC给服务器 %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	fmt.Printf("服务器 %d: 发送给服务器 %d 的RequestVote RPC返回 %v\n", rf.me, server, ok)
	return ok
}

// 使用 Raft 的服务（例如 k/v 服务器）希望开始
// 对将附加到 Raft 日志的下一个命令达成一致。如果这个
// 服务器不是领导者，则返回 false。否则开始
// 协议并立即返回。不能保证这个
// 命令将被提交到 Raft 日志，因为领导者
// 可能会失败或输掉选举。即使 Raft 实例已被终止，
// 此函数也应优雅地返回。
//
// 第一个返回值是如果命令被提交，它将出现的索引。
// 第二个返回值是当前任期。
// 第三个返回值为 true，表示该服务器认为自己是领导者。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//写操作必须走主节点
	if rf.state != "leader" {
		return -1, -1, false
	}

	// 添加新日志条目
	// 注意：logs[0]是哨兵条目，所以新日志的实际索引是len(rf.logs)
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Command: command})
	index := len(rf.logs) - 1 // 新日志的索引
	term := rf.currentTerm

	// 更新matchIndex以考虑新的日志
	rf.matchIndex[rf.me] = index

	fmt.Printf("领导者 %d: 在索引 %d 添加新日志条目，命令 %v\n", rf.me, index, command)

	rf.persist()

	// 在返回前触发异步复制，但确保在锁释放后进行
	go rf.startAppendEntries()

	return index, term, true
}

// sendLogEntries 向指定服务器发送AppendEntries RPC
func (rf *Raft) sendLogEntries(server int) {
	// ... existing code ...
}

// 向所有其他服务器发送AppendEntries请求
func (rf *Raft) startAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果不是leader，直接返回
	if rf.state != "leader" {
		return
	}

	// 记录当前状态
	currentTerm := rf.currentTerm
	me := rf.me

	// 对每个服务器，启动goroutine处理日志同步
	for i := range rf.peers {
		if i == me {
			continue
		}

		// 立即启动一个goroutine处理日志同步
		go func(server int, term int) {
			rf.mu.Lock()

			// 确保仍然是leader且任期未变
			if rf.state != "leader" || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}

			// 获取要发送的日志条目的前一个索引和任期
			prevLogIndex := rf.nextIndex[server] - 1

			// 如果prevLogIndex超出了日志范围，修正为日志的最后索引
			if prevLogIndex >= len(rf.logs) {
				prevLogIndex = len(rf.logs) - 1
			}

			prevLogTerm := rf.logs[prevLogIndex].Term

			// 只发送从nextIndex开始的日志条目，而不是整个日志
			var entries []LogEntry
			if rf.nextIndex[server] < len(rf.logs) {
				// 限制每次发送的日志条目数量，避免单个RPC过大
				maxEntries := 100 // 限制每次发送的条目数
				endIdx := rf.nextIndex[server] + maxEntries
				if endIdx > len(rf.logs) {
					endIdx = len(rf.logs)
				}
				entries = make([]LogEntry, endIdx-rf.nextIndex[server])
				copy(entries, rf.logs[rf.nextIndex[server]:endIdx])

				fmt.Printf("领导者 %d: 向服务器 %d 发送 %d 条日志条目，从索引%d开始\n",
					me, server, len(entries), rf.nextIndex[server])
			} else {
				// 没有新的日志条目要发送，发送空的心跳
				entries = make([]LogEntry, 0)
				fmt.Printf("领导者 %d: 向服务器 %d 发送心跳\n", me, server)
			}

			// 准备RPC参数
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     me,
				Entries:      entries,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}

			rf.mu.Unlock()

			// 发送RPC
			reply := AppendEntriesReply{}
			ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

			if !ok {
				return
			}

			// 处理响应
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 如果任期已变或不再是leader，忽略响应
			if rf.currentTerm != term || rf.state != "leader" {
				return
			}

			// 如果对方返回更高任期，转为follower
			if reply.Term > rf.currentTerm {
				fmt.Printf("领导者 %d: 发现更高任期 %d > %d，转为跟随者\n",
					rf.me, reply.Term, rf.currentTerm)
				rf.currentTerm = reply.Term
				rf.state = "follower"
				rf.votedFor = -1
				rf.persist()
				return
			}

			// 如果追加成功，更新matchIndex和nextIndex
			if reply.Success {
				// 更新matchIndex和nextIndex
				newMatchIndex := prevLogIndex + len(args.Entries)
				if newMatchIndex > rf.matchIndex[server] {
					rf.matchIndex[server] = newMatchIndex
					rf.nextIndex[server] = newMatchIndex + 1

					fmt.Printf("领导者 %d: 追加成功，更新服务器 %d 的matchIndex为 %d\n",
						rf.me, server, rf.matchIndex[server])

					// 检查是否可以提交更多日志条目
					rf.updateCommitIndex()
				}
			} else {
				// 如果追加失败，使用优化的冲突解决机制
				// 利用返回的ConflictIndex和ConflictTerm快速回退
				if reply.ConflictTerm != -1 {
					// 情况1: follower的日志冲突，查找leader日志中相同任期的最后一个条目
					conflictTermIndex := -1
					for i := len(rf.logs) - 1; i > 0; i-- {
						if rf.logs[i].Term == reply.ConflictTerm {
							conflictTermIndex = i
							break
						}
					}

					if conflictTermIndex != -1 {
						// 找到了相同任期的条目，设置nextIndex为该任期的下一个条目
						rf.nextIndex[server] = conflictTermIndex + 1
					} else {
						// 没有找到相同任期的条目，直接跳到冲突索引
						rf.nextIndex[server] = reply.ConflictIndex
					}
				} else {
					// 情况2: follower的日志太短
					rf.nextIndex[server] = reply.ConflictIndex
				}

				// 确保nextIndex至少为1
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}

				fmt.Printf("领导者 %d: 追加失败，将服务器 %d 的nextIndex调整为 %d\n",
					rf.me, server, rf.nextIndex[server])
			}
		}(i, currentTerm)
	}
}

// 返回两个整数中的最大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// 返回两个整数中的最小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// 测试器不会在每次测试后停止由 Raft 创建的 goroutine，
// 但它会调用 Kill() 方法。你的代码可以使用 killed() 来
// 检查是否调用了 Kill()。使用 atomic 避免了
// 对锁的需求。
//
// 问题是长时间运行的 goroutine 会使用内存并可能消耗
// CPU 时间，可能导致后续测试失败并生成
// 令人困惑的调试输出。任何有长时间运行循环的 goroutine
// 都应该调用 killed() 来检查是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// 如果需要，在这里添加你的代码
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker是一个goroutine，定期检查选举超时
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// 休眠一段时间，检查是否需要选举
		time.Sleep(50 * time.Millisecond)

		rf.mu.Lock()

		// 如果是leader，不需要检查超时
		if rf.state == "leader" {
			rf.mu.Unlock()
			continue
		}

		// 检查是否超时
		electionTimeout := time.Duration(
			rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT)+MIN_ELECTION_TIMEOUT) * time.Millisecond
		if time.Since(rf.lastHeartbeat) > electionTimeout {
			fmt.Printf("服务器 %d: 选举超时，开始新选举，当前任期=%d\n", rf.me, rf.currentTerm)
			go rf.startElection()
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	// 只有当服务器是follower或candidate时才开始选举
	if rf.state == "leader" {
		rf.mu.Unlock()
		return
	}

	// 转为candidate状态
	rf.state = "candidate"
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesReceived = 1 // 投票给自己
	currentTerm := rf.currentTerm

	// 准备参数
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.logs[lastLogIndex].Term
	}

	// 计算所需票数
	majority := len(rf.peers)/2 + 1

	// 持久化状态
	rf.persist()

	fmt.Printf("服务器 %d: 开始选举，任期=%d，日志长度=%d，最后日志任期=%d\n",
		rf.me, currentTerm, lastLogIndex+1, lastLogTerm)

	rf.mu.Unlock()

	// 向所有其他服务器发送RequestVote RPC
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int, term int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}

			// 发送RPC
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 如果任期已经改变或状态改变，忽略响应
				if rf.currentTerm != term || rf.state != "candidate" {
					return
				}

				// 如果发现更高的任期，转为follower
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = "follower"
					rf.votedFor = -1
					rf.persist()
					return
				}

				// 计算得票
				if reply.VoteGranted {
					rf.votesReceived++

					// 检查是否获得多数票
					if rf.votesReceived >= majority && rf.state == "candidate" {
						// 成为领导者
						rf.becomeLeader()
					}
				}
			}
		}(i, currentTerm)
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = "leader"
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// 初始化nextIndex为自己的日志长度
	for i := range rf.nextIndex {
		// 重要修改：初始化nextIndex为1而不是日志长度
		// 这样可以确保新leader能发送完整日志给所有followers
		rf.nextIndex[i] = 1
	}

	// 初始化matchIndex为0
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}

	// matchIndex[me]等于自己的日志长度-1
	rf.matchIndex[rf.me] = len(rf.logs) - 1

	fmt.Printf("服务器 %d: 成为领导者，任期=%d, 日志长度=%d\n",
		rf.me, rf.currentTerm, len(rf.logs))

	// 状态变更，持久化
	rf.persist()

	// 立即发送一轮心跳/日志复制
	go rf.startAppendEntries()

	// 开始定期发送心跳
	go rf.startHeartbeats()
}

func (rf *Raft) startHeartbeats() {
	for !rf.killed() {
		// 检查是否仍是领导者
		rf.mu.Lock()
		if rf.state != "leader" {
			rf.mu.Unlock()
			return
		}

		// 每隔一段时间发送心跳和强制同步日志
		rf.mu.Unlock()

		// 先发送强制日志同步
		rf.startAppendEntries()

		// 然后等待心跳间隔
		time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //之前处理的日志索引值
	PrevLogTerm  int //prevLogIndex日志的任期号
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool //如果跟随者包含有匹配的prevLogIndex和prevLogTerm，则返回true，这个就不是心跳

	// 为了优化冲突解决过程而添加的字段
	ConflictIndex int // 冲突的索引位置
	ConflictTerm  int // 冲突位置的任期
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 初始化回复
	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// 记录服务器ID和当前状态，便于调试
	serverId := rf.me

	// 打印当前日志状态以便调试
	fmt.Printf("服务器 %d: 当前日志状态: 长度=%d, commitIndex=%d, 最后任期=%d, 当前任期=%d\n",
		serverId, len(rf.logs), rf.commitIndex, rf.logs[len(rf.logs)-1].Term, rf.currentTerm)
	fmt.Printf("服务器 %d: 收到AppendEntries: prevLogIndex=%d, prevLogTerm=%d, 条目数=%d, leaderCommit=%d, 领导者任期=%d\n",
		serverId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, args.Term)

	// 规则1: 如果RPC的任期小于当前任期，拒绝
	if args.Term < rf.currentTerm {
		fmt.Printf("服务器 %d: 拒绝来自 %d 的AppendEntries(更低任期 %d < %d)\n",
			serverId, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	// 重置选举超时计时器，无论是否接受日志
	rf.lastHeartbeat = time.Now()

	// 如果收到更高任期，转为follower并更新任期
	if args.Term > rf.currentTerm {
		fmt.Printf("服务器 %d: 收到更高任期 %d > %d 的AppendEntries, 转为跟随者\n",
			serverId, args.Term, rf.currentTerm)

		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.votedFor = -1
		rf.persist()
	} else if rf.state == "candidate" && args.Term == rf.currentTerm {
		// 同任期收到AppendEntries，认可对方为领导者
		fmt.Printf("服务器 %d: 候选人收到同任期的AppendEntries, 转为跟随者\n", serverId)
		rf.state = "follower"
		rf.persist()
	}

	// 更新回复中的任期
	reply.Term = rf.currentTerm

	// 规则2: 如果日志不包含prevLogIndex位置任期为prevLogTerm的条目，则拒绝
	// 但提供有用的冲突信息，以帮助leader更快找到匹配点

	// 如果是全新同步（从索引0开始），直接接受
	if args.PrevLogIndex == 0 && len(args.Entries) > 0 {
		// 特殊情况：从头开始的完整日志同步，不需要检查prevLogTerm
		fmt.Printf("服务器 %d: 接收从头开始的完整日志同步\n", serverId)
		reply.Success = true
	} else {
		// 正常的一致性检查
		// 如果prevLogIndex超出了日志长度
		if args.PrevLogIndex >= len(rf.logs) {
			fmt.Printf("服务器 %d: 日志太短 (长度=%d, 需要的索引=%d)\n", serverId, len(rf.logs), args.PrevLogIndex)

			// 提供冲突信息以加速回退过程
			reply.ConflictIndex = len(rf.logs)
			reply.ConflictTerm = -1 // 表示日志长度不够
			return
		}

		// 如果prevLogIndex处的日志任期不匹配
		if args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			// 记录冲突信息以优化回退
			reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term

			// 找到该任期的第一个日志索引
			reply.ConflictIndex = 1 // 默认至少回退到索引1
			for i := args.PrevLogIndex; i >= 1; i-- {
				if rf.logs[i].Term != reply.ConflictTerm {
					reply.ConflictIndex = i + 1
					break
				}
			}

			fmt.Printf("服务器 %d: prevLogIndex=%d处的任期不匹配(本地=%d, 领导者=%d)，冲突索引=%d\n",
				serverId, args.PrevLogIndex, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm, reply.ConflictIndex)
			return
		}

		// 一致性检查通过
		reply.Success = true
	}

	// 处理日志条目
	if len(args.Entries) > 0 {
		fmt.Printf("服务器 %d: 接收到 %d 个日志条目，从索引 %d 开始\n",
			serverId, len(args.Entries), args.PrevLogIndex+1)
		// 将新条目添加到日志中
		needPersist := false

		// 特殊情况：如果是从头同步（prevLogIndex=0）且本地日志有差异，直接替换本地日志
		if args.PrevLogIndex == 0 {
			// 保留索引0处的哨兵条目
			if len(args.Entries)+1 != len(rf.logs) || !logsMatch(rf.logs[1:], args.Entries) {
				newLogs := make([]LogEntry, 1)
				newLogs[0] = rf.logs[0] // 保留哨兵条目
				newLogs = append(newLogs, args.Entries...)
				rf.logs = newLogs
				needPersist = true
				fmt.Printf("服务器 %d: 从头替换本地日志，新日志长度=%d\n", serverId, len(rf.logs))
			}
		} else {
			// 常规处理：逐个检查并添加日志条目
			for i, entry := range args.Entries {
				logIndex := args.PrevLogIndex + 1 + i

				// 如果日志长度不够，直接添加新条目
				if logIndex >= len(rf.logs) {
					rf.logs = append(rf.logs, entry)
					needPersist = true
					continue
				}

				// 如果已有的日志条目与新条目冲突（索引相同但任期不同）
				if rf.logs[logIndex].Term != entry.Term {
					// 删除冲突条目及其之后的所有条目
					rf.logs = rf.logs[:logIndex]
					// 追加新条目
					rf.logs = append(rf.logs, entry)
					needPersist = true
					continue
				}
				// 如果已有条目与新条目相同，无需操作
			}
		}

		// 只有在日志被修改时才持久化
		if needPersist {
			fmt.Printf("服务器 %d: 日志被修改，保存持久化状态，新日志长度=%d\n", serverId, len(rf.logs))
			rf.persist()
		}
	}

	// 更新commitIndex - 即使是心跳消息，也应该更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		// 更新commitIndex为leaderCommit和本地日志长度-1的较小值
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
		fmt.Printf("服务器 %d: 更新提交索引 %d -> %d (日志长度=%d, leaderCommit=%d)\n",
			serverId, oldCommitIndex, rf.commitIndex, len(rf.logs), args.LeaderCommit)

		// 如果更新了commitIndex，确保持久化
		if rf.commitIndex != oldCommitIndex {
			rf.persist()
		}
	}
}

// 辅助函数：检查两个日志数组是否匹配
func logsMatch(logs1 []LogEntry, logs2 []LogEntry) bool {
	if len(logs1) != len(logs2) {
		return false
	}
	for i := 0; i < len(logs1); i++ {
		if logs1[i].Term != logs2[i].Term {
			return false
		}
	}
	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	fmt.Printf("服务器 %d: 实际发送AppendEntries RPC给服务器 %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	fmt.Printf("服务器 %d: 发送给服务器 %d 的AppendEntries RPC返回 %v\n", rf.me, server, ok)
	return ok
}

func (rf *Raft) applyLogs() {
	for !rf.killed() {
		rf.mu.Lock()

		// 检查是否有日志需要应用
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			// 如果没有需要应用的日志，短暂休眠再检查
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// 有日志需要应用，逐个应用
		toApply := make([]raftapi.ApplyMsg, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex && i < len(rf.logs); i++ {
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			}
			toApply = append(toApply, msg)
			rf.lastApplied = i
		}

		rf.mu.Unlock()

		// 在没有持有锁的情况下发送消息
		for _, msg := range toApply {
			fmt.Printf("服务器 %d: 应用索引 %d 的命令 %v\n", rf.me, msg.CommandIndex, msg.Command)
			rf.applyCh <- msg
		}

		// 短暂休眠，避免CPU过度使用
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) updateCommitIndex() {
	// 只有领导者才能更新commitIndex (基于matchIndex)
	if rf.state != "leader" {
		return
	}

	// 记录旧的commitIndex，用于检测变化
	oldCommitIndex := rf.commitIndex

	// 计算多数派数量
	majority := len(rf.peers)/2 + 1

	// 严格按照Raft论文5.4.2节实现：
	// 只有当日志被复制到大多数服务器时才能提交
	// 且领导者只提交当前任期的日志

	// 从旧的commitIndex+1开始，检查每个索引是否可以提交
	for N := rf.commitIndex + 1; N < len(rf.logs); N++ {
		// 计算复制到多少服务器了
		replicationCount := 1 // 包括领导者自己
		for server := range rf.peers {
			if server != rf.me && rf.matchIndex[server] >= N {
				replicationCount++
			}
		}

		// 如果超过多数派复制了该日志
		if replicationCount >= majority {
			// 关键安全属性：只提交当前任期的日志条目
			// 这是Raft的核心安全规则，防止已经被提交的日志被覆盖
			if rf.logs[N].Term == rf.currentTerm {
				rf.commitIndex = N
				fmt.Printf("领导者 %d: 日志索引 %d 已复制到 %d 个节点，属于当前任期 %d，提交该日志\n",
					rf.me, N, replicationCount, rf.currentTerm)
			}
		} else {
			// 如果该索引未达到多数复制，后续更高索引肯定也未达到，可以停止循环
			break
		}
	}

	// 如果commitIndex有更新，立即通知跟随者
	if rf.commitIndex > oldCommitIndex {
		fmt.Printf("领导者 %d: 更新commitIndex: %d -> %d\n", rf.me, oldCommitIndex, rf.commitIndex)

		// 提交索引更新，持久化状态
		rf.persist()

		// 立即启动一轮AppendEntries，让所有follower尽快知道新的commitIndex
		go rf.startAppendEntries()
	}
}

// applyCh 是测试器或服务期望 Raft
// 发送 ApplyMsg 消息的通道。
// Make() 必须快速返回，因此它应该为任何
// 长时间运行的工作启动 goroutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// 初始化随机数生成器，使用服务器ID作为种子的一部分，确保不同服务器有不同的随机行为
	rand.Seed(time.Now().UnixNano() + int64(me))

	// 初始化Raft状态
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = "follower"
	rf.lastHeartbeat = time.Now() // 初始化心跳时间为当前时间

	// 创建日志数组，索引0处放置一个哨兵日志条目
	rf.logs = make([]LogEntry, 1)
	rf.logs[0] = LogEntry{Command: nil, Term: 0}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// 这些变量只有在成为领导者时才会初始化
	rf.nextIndex = nil
	rf.matchIndex = nil

	// 从持久存储中恢复状态（如果有）
	rf.readPersist(persister.ReadRaftState())

	// 确保日志至少有哨兵条目
	if len(rf.logs) == 0 {
		rf.logs = make([]LogEntry, 1)
		rf.logs[0] = LogEntry{Command: nil, Term: 0}
	}

	// 初始化时先持久化一次状态，如果没有从持久化状态恢复
	if persister.ReadRaftState() == nil || len(persister.ReadRaftState()) < 1 {
		rf.persist()
	}

	fmt.Printf("服务器 %d: 初始化完成，日志长度=%d, commitIndex=%d, 任期=%d\n",
		rf.me, len(rf.logs), rf.commitIndex, rf.currentTerm)

	// 启动goroutine处理选举和心跳
	go rf.ticker()

	// 启动goroutine应用日志
	go rf.applyLogs()

	return rf
}

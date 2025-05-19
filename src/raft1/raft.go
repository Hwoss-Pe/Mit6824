package raft

// 文件 raftapi/raft.go 定义了 raft 必须向服务器（或测试器）暴露的接口，
// 但请查看下面每个函数的注释以获取更多详细信息。
//
// Make() 创建一个实现 raft 接口的新 raft 节点。

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
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
	// 示例:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// 恢复之前持久化的状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // 没有任何状态进行引导？
		return
	}
	// 你的代码在这里 (3C)
	// 示例:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

	fmt.Printf("Server %d: Received RequestVote from %d, candidateTerm=%d, myTerm=%d, myVotedFor=%d\n",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)

	// 如果请求的任期小于当前任期，拒绝投票
	if args.Term < rf.currentTerm {
		fmt.Printf("Server %d: Rejecting vote for %d (lower term %d < %d)\n",
			rf.me, args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	// 如果请求的任期更高，转为follower
	if args.Term > rf.currentTerm {
		fmt.Printf("Server %d: Converting to follower due to RequestVote with higher term %d > %d\n",
			rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.votedFor = -1
		rf.persist()
	}

	// 更新回复中的任期
	reply.Term = rf.currentTerm

	// 检查是否已经投票
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 比较日志新旧
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := 0
		if lastLogIndex >= 0 {
			lastLogTerm = rf.logs[lastLogIndex].Term
		}

		// 检查日志的新旧比较
		logOk := false
		if args.LastLogTerm > lastLogTerm {
			logOk = true
		} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
			logOk = true
		}

		fmt.Printf("Server %d: Log comparison for %d - myLastTerm=%d, candidateLastTerm=%d, logOk=%v\n",
			rf.me, args.CandidateId, lastLogTerm, args.LastLogTerm, logOk)

		if logOk {
			// 投票给候选人
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()

			// 重置选举超时计时器
			rf.lastHeartbeat = time.Now()

			fmt.Printf("Server %d: Voting for %d for term %d\n", rf.me, args.CandidateId, rf.currentTerm)
		} else {
			fmt.Printf("Server %d: Rejecting vote for %d due to older log\n", rf.me, args.CandidateId)
		}
	} else {
		fmt.Printf("Server %d: Already voted for %d, can't vote for %d\n",
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
	fmt.Printf("Server %d: Actually sending RequestVote RPC to server %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	fmt.Printf("Server %d: RequestVote RPC to server %d returned %v\n", rf.me, server, ok)
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
	index := len(rf.logs)
	term := rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})

	rf.persist()

	// 立即开始向跟随者复制日志
	go rf.startAppendEntries()

	return index, term, true

}

func (rf *Raft) startAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != "leader" {
		return
	}

	// 触发向所有服务器发送日志
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendLogEntries(i)
		}
	}
}

func (rf *Raft) sendLogEntries(server int) {
	rf.mu.Lock()
	if rf.state != "leader" {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.logs[prevLogIndex].Term
	//如果档案记录的节点下一个日志小于目前主节点拥有的日志，那么应该发送后面一段该节点没有的
	entries := make([]LogEntry, 0)
	if rf.nextIndex[server] < len(rf.logs) {
		entries = append(entries, rf.logs[rf.nextIndex[server]:]...)
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		Entries:      entries,
		PrevLogIndex: prevLogIndex,
		LeaderCommit: rf.commitIndex,
		PrevLogTerm:  prevLogTerm}
	// 记录当前的任期，用于后续检查
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//每次操作都要对leader状态进行确认
		if reply.Term > currentTerm {
			rf.currentTerm = reply.Term
			rf.state = "follower"
			rf.votedFor = -1
			rf.persist()
			return
		}
		//如果写入成功，就更新nextIndex和matchIndex，不成功就降低nextIndex然后异步重试
		if reply.Success {
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.matchIndex[server] = prevLogIndex + len(entries)
			//这里需要检查是否有新的日志可以提交
			rf.updateCommitIndex()
		} else {
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
			}
			// 重试
			go rf.sendLogEntries(server)
		}

	}
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

func (rf *Raft) ticker() {
	// 初始化随机数生成器 - 使用唯一种子确保服务器有不同的随机行为
	rand.Seed(time.Now().UnixNano() + int64(rf.me*1000))

	for !rf.killed() {
		// 生成新的随机选举超时 (150-300ms范围)
		electionTimeout := time.Duration(300+rand.Intn(300)) * time.Millisecond

		// 获取当前状态，但不要持有锁太长时间
		rf.mu.Lock()
		state := rf.state
		timeSinceLastHB := time.Since(rf.lastHeartbeat)
		currentTerm := rf.currentTerm
		rf.mu.Unlock()

		// 如果是follower或candidate且超时，开始选举
		if state != "leader" && timeSinceLastHB > electionTimeout {
			// 开始新的选举
			rf.mu.Lock()
			// 再次检查状态，避免在获取锁的过程中发生变化
			if rf.state != "leader" && rf.currentTerm == currentTerm && time.Since(rf.lastHeartbeat) > electionTimeout {
				// 重置心跳时间
				rf.lastHeartbeat = time.Now()
				// 释放锁，然后启动选举
				rf.mu.Unlock()
				go rf.startElection()
			} else {
				rf.mu.Unlock()
			}
		}

		// 短暂休眠再检查
		time.Sleep(10 * time.Millisecond)
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
						rf.state = "leader"

						// 初始化领导者状态
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.peers {
							rf.nextIndex[i] = len(rf.logs)
							rf.matchIndex[i] = 0
						}

						// 立即发送心跳以建立权威
						go rf.startHeartbeats()
					}

				}
			}
		}(i, currentTerm)
	}
}

func (rf *Raft) startHeartbeats() {
	for !rf.killed() {
		// 检查是否仍是领导者
		rf.mu.Lock()
		if rf.state != "leader" {
			rf.mu.Unlock()
			return
		}

		// 获取当前任期和其他信息
		currentTerm := rf.currentTerm
		rf.mu.Unlock()

		// 向所有其他服务器发送心跳
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(server int, term int) {
				rf.mu.Lock()
				// 再次检查是否仍是领导者且任期未变
				if rf.state != "leader" || rf.currentTerm != term {
					rf.mu.Unlock()
					return
				}

				// 准备心跳参数
				prevLogIndex := rf.nextIndex[server] - 1
				prevLogTerm := 0
				if prevLogIndex >= 0 && prevLogIndex < len(rf.logs) {
					prevLogTerm = rf.logs[prevLogIndex].Term
				}

				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      []LogEntry{}, // 空条目表示心跳
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}

				// 发送心跳RPC
				if rf.sendAppendEntries(server, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// 检查是否仍是领导者且任期未变
					if rf.state != "leader" || rf.currentTerm != term {
						return
					}

					// 处理响应
					if reply.Term > rf.currentTerm {
						// 发现更高任期，转为follower
						rf.currentTerm = reply.Term
						rf.state = "follower"
						rf.votedFor = -1
						rf.persist()
					}
				}
			}(i, currentTerm)
		}

		// 心跳间隔 (应小于选举超时的一半，通常100ms)
		time.Sleep(100 * time.Millisecond)
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 初始化回复
	reply.Success = false
	reply.Term = rf.currentTerm

	// 如果RPC的任期小于当前任期，拒绝
	if args.Term < rf.currentTerm {
		return
	}

	// 收到与当前或更高任期的AppendEntries，认可领导者权威
	// 重置选举超时计时器
	rf.lastHeartbeat = time.Now()

	// 如果收到更高任期，转为follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.votedFor = -1
		rf.persist()
	} else if rf.state == "candidate" && args.Term == rf.currentTerm {
		// 同任期收到AppendEntries，认可对方为领导者
		rf.state = "follower"
	}

	// 更新Term
	reply.Term = rf.currentTerm

	// 处理心跳请求 (空Entries)
	if len(args.Entries) == 0 {
		reply.Success = true
		return
	}
	//日志一致性检查，保证跟随者最后一个日志至少大于preIndex并且任期得一样
	if args.PrevLogIndex >= len(rf.logs) {
		// 跟随者日志比prevLogIndex短，失败
		reply.Success = false
		return
	}
	if args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// prevLogIndex位置的任期不匹配，失败
		reply.Success = false
		return
	}
	reply.Success = true
	//追加日志，注意用截取的方式
	if len(args.Entries) > 0 {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
	}
	// 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1) //怎么理解
		// 应用新提交的日志到状态机
		go rf.applyLogs()
	}
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock() // 应用所有已提交但未应用的日志
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}

		// 解锁再发送，避免死锁
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.mu.Lock()
		rf.lastApplied = i
	}
}

func (rf *Raft) updateCommitIndex() {
	// 领导者查找可以提交的最高日志索引，保证大多数都有这个日志才能提交。
	// 注意：只考虑当前任期的日志条目
	for i := rf.commitIndex + 1; i < len(rf.logs); i++ {
		if rf.logs[i].Term == rf.currentTerm {
			// 计算已复制到大多数服务器的日志
			count := 1 // 领导者自己
			for peer := range rf.peers {
				if peer != rf.me && rf.matchIndex[peer] >= i {
					count++
				}
			}
			// 如果大多数服务器已复制，则提交
			if count > len(rf.peers)/2 {
				rf.commitIndex = i
			} else {
				// 如果这个索引未达到多数，后面的也不会达到
				break
			}
		}
	}
	// 应用新提交的日志
	if rf.lastApplied < rf.commitIndex {
		go rf.applyLogs()
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

	// 初始化Raft状态
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = "follower"
	rf.lastHeartbeat = time.Now() // 初始化心跳时间为当前时间
	rf.logs = make([]LogEntry, 1) // 创建带有哨兵值的日志数组
	rf.logs[0] = LogEntry{nil, 0} // 索引0的哨兵日志
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	// 这些变量只有在成为领导者时才会初始化
	rf.nextIndex = nil
	rf.matchIndex = nil

	// 从持久存储中恢复状态（如果有）
	rf.readPersist(persister.ReadRaftState())

	// 启动goroutine处理选举和心跳
	go rf.ticker()

	return rf
}

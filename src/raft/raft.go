package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// 三个角色
const (
	Follower = iota
	Candidate
	Leader
)

/*
每个日志都包含三部分的内容:
1, Index : 表示该日志条目在整个日志中的位置
2, Term  : 日志首次被领导者创建时的任期
3, Command : 应用于状态机的命令
*/
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//所有服务器需要持久化的
	currentTerm int        //当前已知最新任期
	votedFor    int        //投票给谁
	log         []LogEntry //日志

	//不需要持久化的信息
	state         int
	electionTime  time.Time // 记录重置选举的时间
	heartbeatTime time.Time //心跳时间
	timeout       time.Duration
	commitIndex   int
	lastApplied   int
	matchIndex    []int
	nextIndex     []int

	//用于通知apply携程应用
	conditional *sync.Cond
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	isleader = false
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term, voteFor int

	if d.Decode(&term) != nil || d.Decode(&voteFor) != nil {
		fmt.Println("readPersist error")
	}
	rf.currentTerm = term
	rf.votedFor = voteFor
	d.Decode(&rf.log)
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	//用于选举限制
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	IsOK        bool
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//填充reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	//任期小于自己的任期，直接返回
	if args.Term < rf.currentTerm {
		//fmt.Println("peer[", rf.me, "] 的任期为", rf.currentTerm, "peer[", args.CandidateId, "] 的任期为", args.Term, "不投票给他")
		DPrintf("RequestVote： RaftNode[%d], term: %d, 发现对方 RaftNode[%d], term: %d 任期比我小，不投票给他", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	//如果收到更大的任期，更新自己的任期，并且重置超时，状态变回follower
	if args.Term > rf.currentTerm {
		rf.toBeFollower(args.Term)
		rf.votedFor = -1
		rf.persist()
	}

	//可能可以投票给他
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {

		//进行选举限制检查
		if rf.log[rf.getLastLogIndex()].Term > args.LastLogTerm ||
			rf.log[rf.getLastLogIndex()].Term == args.LastLogTerm &&
				rf.getLastLogIndex() > args.LastLogIndex {
			//fmt.Println("peer[", rf.me, "] 未通过选举限制，不投票给", "peer[", args.CandidateId, "]")
			DPrintf("RequestVote： RaftNode[%d], term: %d, 发现对方 RaftNode[%d], term: %d 未通过选举限制，不投票给他", rf.me, rf.currentTerm, args.CandidateId, args.Term)
			return
		}

		//fmt.Println("peer[", rf.me, "] 通过选举限制，投票给", "peer[", args.CandidateId, "]")
		DPrintf("RaftNode[%d], term: %d, 投票给 RaftNode[%d], term: %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		rf.votedFor = args.CandidateId
		rf.state = Follower
		reply.VoteGranted = true
		//重置选举超时时间
		rf.electionTime = time.Now()
		rf.persist()
	} else {
		//fmt.Println("peer[", rf.me, "] 拒绝投票给", "peer[", args.CandidateId, "]")
		DPrintf("RaftNode[%d], term: %d, 拒绝投票给 RaftNode[%d], term: %d，因为已经投给比人了", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// 注意传递结构体&， 并且结构体内部成员变量为大写
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 追加日志args
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	LeaderCommit int //领导者已提交的最大的日志索引，用于follower提交
	Entries      []LogEntry

	//用于一致性检查
	PrevLogIndex int
	PrevLogTerm  int
}

// 追加日志reply
type AppendEntriesReply struct {
	Term    int
	Success bool

	//加速回溯日志
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	//看看是否需要apply
	defer func() {
		if rf.lastApplied < rf.commitIndex {
			rf.conditional.Broadcast()
		}
		rf.mu.Unlock()
	}()

	reply.Term = rf.currentTerm
	reply.Success = false

	//任期比我小
	if args.Term < rf.currentTerm {
		DPrintf("AppendEntries： RaftNode[%d], term: %d, 发现 leader RaftNode[%d], term: %d 的任期比我小", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("AppendEntries： RaftNode[%d], term: %d, 发现 leader RaftNode[%d], term: %d 的任期比我大", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.toBeFollower(args.Term)
		rf.persist()
	}

	//重置心跳时间
	rf.electionTime = time.Now()

	//一致性检查失败
	if rf.getLastLogIndex() < args.PrevLogIndex {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1

		if rf.log[rf.getLastLogIndex()].Term == args.PrevLogTerm {
			reply.ConflictTerm = 0
			if rf.log[rf.getLastLogIndex()].Term-1 > 0 {
				reply.ConflictTerm = rf.log[rf.getLastLogIndex()].Term - 1
			}
		}
		DPrintf("AppendEntries： RaftNode[%d], term: %d 的LastLogIndex:%d,  leader RaftNode[%d], term: %d 的PrevLogIndex: %d，不一致，设置reply.ConflictTerm=-1", rf.me, rf.currentTerm, rf.getLastLogIndex(), args.LeaderId, args.Term, args.PrevLogIndex)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		//倒着寻找到日志中为ConflictTerm的第一个日志
		firstConfictTermLogIndex := args.PrevLogIndex

		//i从 args.PrevLogIndex 开启寻找即可
		for i := args.PrevLogIndex; i >= 1; i-- {
			//找到最小index的日志为ConflictTerm的日志索引
			if rf.log[i].Term == reply.ConflictTerm {
				firstConfictTermLogIndex = i
				continue
			} else {
				break
			}
		}

		reply.ConflictIndex = firstConfictTermLogIndex
		DPrintf("RaftNode[%d], term: %d LastLogTerm:%d,  leader RaftNode[%d], term: %d PrevLogTerm: %d，不一致", rf.me, rf.currentTerm, rf.log[rf.getLastLogIndex()].Term, args.LeaderId, args.Term, args.PrevLogTerm)
		DPrintf("RaftNode[%d] 被leader :%d 追加日志失败：term: %d, votefor: %d, logs: %v", rf.me, args.LeaderId, rf.currentTerm, rf.votedFor, rf.log)
		DPrintf("RaftNode[%d] 返回给leader： %d 的conflictIndex: %d,  conflictTerm: %d", rf.me, args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
		return
	}

	//一致性检查成功
	reply.Success = true

	//需要处理重复的RPC请求
	//比较日志条目的任期，以确定是否能安全地追加日志
	//否则会导致重复应用命令
	index := args.PrevLogIndex
	log_num := len(rf.log)
	for i, entry := range args.Entries {
		index++

		//该位置存在日志
		if index < len(rf.log) {
			if rf.log[index].Term == entry.Term {
				continue
			}
			rf.log = rf.log[:index] //不一样的日志，那么截断该位置的日志
		}

		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()
	}
	DPrintf("RaftNode[%d] 被leade: %d 追加日志后：term: %d, votefor: %d, logs: %v", rf.me, args.LeaderId, rf.currentTerm, rf.votedFor, rf.log)
	DPrintf("RaftNode[%d], term: %d 原来的日志长度：%d, 现在追加后长度: %d", rf.me, rf.currentTerm, log_num, len(rf.log))
	if rf.commitIndex < args.LeaderCommit {
		lastLogIndex := rf.getLastLogIndex()
		if lastLogIndex < args.LeaderCommit {
			DPrintf("RaftNode[%d], term: %d 的 lastLogIndex < args.LeaderCommit ，lastLogIndex: %d, args.LeaderCommit: %d", rf.me, rf.currentTerm, lastLogIndex, args.LeaderCommit)
			rf.commitIndex = lastLogIndex
		} else {
			DPrintf("RaftNode[%d], term: %d 的 lastLogIndex >= args.LeaderCommit ，lastLogIndex: %d, args.LeaderCommit: %d", rf.me, rf.currentTerm, lastLogIndex, args.LeaderCommit)
			rf.commitIndex = args.LeaderCommit
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = rf.getLastLogIndex()
	term = rf.currentTerm
	if rf.state != Leader {
		return -1, -1, false
	} else {
		//追加日志
		logs := LogEntry{Index: len(rf.log), Command: command, Term: rf.currentTerm}
		rf.log = append(rf.log, logs)
		rf.persist()
		DPrintf("RaftNode[%d], term: %d, 收到客户端追加日志，当前日志长度为: %d", rf.me, rf.currentTerm, len(rf.log))
		DPrintf("node: %d 被客户端追加日志后：term: %d, votefor: %d, logs: %v", rf.me, rf.currentTerm, rf.votedFor, rf.log)
		index = rf.getLastLogIndex()
		rf.timeout = time.Duration(0) * time.Millisecond
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) toBeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
}

func (rf *Raft) toBeCandidate() {
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.electionTime = time.Now()
	DPrintf("RaftNode[%d] Follower -> Candidate", rf.me)
}

func (rf *Raft) toBeLeader() {
	//初始化nextIndex[]和matchIndex[]
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.state = Leader
	rf.timeout = time.Duration(0) * time.Millisecond
	DPrintf("RaftNode[%d], term: %d 成为leader，初始化next[i]的大小为: %d", rf.me, rf.currentTerm, rf.nextIndex[0])
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 获取最后一个日志的Index
func (rf *Raft) getLastLogIndex() int {
	if len(rf.log)-1 != rf.log[len(rf.log)-1].Index {
		DPrintf("len(rf.log)- 1 is %d, f.log[len(rf.log)-1].Index is %d. not equal", len(rf.log)-1, rf.log[len(rf.log)-1].Index)
	} else {
		//DPrintf("len(rf.log)- 1 is %d, f.log[len(rf.log)-1].Index is %d. Equal!!", len(rf.log)-1, rf.log[len(rf.log)-1].Index)
	}
	return rf.log[len(rf.log)-1].Index
}

// 获取最后一个日志的Term
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log)-1 != rf.log[len(rf.log)-1].Index {
		DPrintf("len(rf.log)- 1 is %d, f.log[len(rf.log)-1].Index is %d. not equal", len(rf.log)-1, rf.log[len(rf.log)-1].Index)
	} else {
		//DPrintf("len(rf.log)- 1 is %d, f.log[len(rf.log)-1].Index is %d. Equal!!", len(rf.log)-1, rf.log[len(rf.log)-1].Index)
	}
	return rf.log[rf.getLastLogIndex()].Term
}

// 获取日志长度
func (rf *Raft) getLogLength() int {
	return len(rf.log)
}

func (rf *Raft) constructRequestVoteArgs(args *RequestVoteArgs) {

	args.Term = rf.currentTerm
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	args.CandidateId = rf.me
}

func (rf *Raft) constructAppendEntriesArgs(args *AppendEntriesArgs, i int) {
	args.Term = rf.currentTerm
	args.Entries = nil
	//fmt.Println("rf.nextIndex[", i, "] is ", rf.nextIndex[i])
	DPrintf("RaftNode[%d], term: %d, 构造AppendEntries给RaftNode[%d]，其nextIndex[%d]为: %d", rf.me, rf.currentTerm, i, i, rf.nextIndex[i])
	args.Entries = append(args.Entries, rf.log[rf.nextIndex[i]:]...)
	args.LeaderCommit = rf.commitIndex
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[i] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		//周期性10ms检查一次时间差：
		time.Sleep(20 * time.Millisecond)

		go rf.electionTicker()
	}
}

func (rf *Raft) electionTicker() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A)
	// Check if a leader election should be started.
	timeout := time.Duration(800+rand.Int31n(400)) * time.Millisecond
	currentTime := time.Now()
	elapses := currentTime.Sub(rf.electionTime)
	//判断有无选举超时：选举超时的话
	if rf.state != Leader && elapses >= timeout {
		DPrintf("RaftNode[%d], term: %d,  开始进行选举", rf.me, rf.currentTerm)
		//变成candidate
		rf.toBeCandidate()
		rf.persist()
		//开始请求投票
		var args RequestVoteArgs

		type VoteResult struct {
			reply RequestVoteReply
			id    int
		}

		rf.constructRequestVoteArgs(&args)
		ch := make(chan VoteResult, len(rf.peers))
		peersNum := len(rf.peers)
		currentTerm := rf.currentTerm
		id := rf.me
		maxTerm := rf.currentTerm

		//发起rpc时间较长，可先释放掉锁
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(args *RequestVoteArgs, i int) {
					DPrintf("RaftNode[%d], term: %d,  发送RequestVote给 RaftNode[%d]", id, currentTerm, i)
					var response RequestVoteReply
					ok := rf.sendRequestVote(i, args, &response)
					if !ok {
						DPrintf("RaftNode[%d], term: %d,  发送给 RaftNode[%d] RequestVote失败1次", id, currentTerm, i)
						response.IsOK = false
						ch <- VoteResult{reply: response, id: i}
						return
					} else {
						DPrintf("RaftNode[%d], term: %d,  发送给 RaftNode[%d] RequestVote成功", id, currentTerm, i)
						response.IsOK = true
						ch <- VoteResult{reply: response, id: i}
						return
					}
				}(&args, i)
			}
		}

		//计算票数：计算票数时间比较长，所以不要拿着锁进行
		count := 1
		finish := 1
		for response := range ch {
			finish++
			//正常回复
			if response.reply.IsOK {
				if response.reply.VoteGranted == true {
					//fmt.Println("peer[", rf.me, "] 获得票数: ", count)
					count++
				}
				if response.reply.Term > maxTerm {
					maxTerm = response.reply.Term
				}
			}

			//已经超过半数 或者 不可能超过半数了，那么跳出计算投票阶段
			if count > len(rf.peers)/2 || finish == peersNum || (peersNum-finish+count) <= peersNum/2 {
				break
			}

			if response.reply.IsOK == false {
				//fmt.Println("peer[", id, "] to reply.IsOK == false")
				//continue   继续执行这个会导致finish == len(rf.peers)执行不到 跳不出去，造成阻塞
			}
		}

		//计算投票结束，统计票数：注意持锁进行，因为需要开始修改rf的状态
		rf.mu.Lock()

		//先检查上面发送rpc到计算投票阶段因为没有锁情况下，rf的状态有无发生改变
		if rf.currentTerm < maxTerm {
			rf.toBeFollower(maxTerm)
			rf.persist()
			DPrintf("RaftNode[%d], term: %d, 发现了更大的任期，退出竞选", rf.me, rf.currentTerm)
			return
		}
		if rf.state != Candidate {
			rf.toBeFollower(rf.currentTerm)
			DPrintf("RaftNode[%d], term: %d, 发现了更大的任期，退出竞选", rf.me, rf.currentTerm)
			return
		}

		if count > len(rf.peers)/2 {
			//成为了leader
			DPrintf("RaftNode[%d], term: %d, 成为了leader", rf.me, rf.currentTerm)
			rf.toBeLeader()
			go rf.leaderTicker()
			return
		}
	}
}

func (rf *Raft) leaderTicker() {
	for rf.killed() == false {
		//周期性10ms检查一次时间差：
		time.Sleep(10 * time.Millisecond)
		go func() {
			//fmt.Println("peer[", rf.me, "] 进入leaderTicker")
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//fmt.Println("peer[", rf.me, "] 进入leaderTicker后获取锁成功")

			//可能由于网络分区，所以必须检查自己还是不是leader
			if rf.state != Leader {
				//DPrintf("RaftNode[%d], term: %d, 发现自己不再是leader，而是 %d", rf.me, rf.currentTerm, rf.state)
				return
			}
			currentTime := time.Now()
			elapses := currentTime.Sub(rf.heartbeatTime)

			//心跳大于100ms了发送心跳包 :
			if elapses >= rf.timeout {
				rf.heartbeatTime = currentTime
				rf.timeout = time.Duration(50) * time.Millisecond
				//如果最新的日志不包含当前term，则 send no-op日志

				/*
					if rf.getLastLogTerm() != rf.currentTerm {
						//append one no-op log
						noOpLog := LogEntry{Index: rf.getLastLogIndex() + 1, Term: rf.currentTerm, Command: nil}
						rf.log = append(rf.log, noOpLog)
						//fmt.Println("peer[", rf.me, "] 构造no-op日志")
						DPrintf("RaftNode[%d], term: %d,  构造no-op日志", rf.me, rf.currentTerm)
					}
				*/

				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					//构造AppendEntriesArgs
					var args AppendEntriesArgs
					rf.constructAppendEntriesArgs(&args, i)

					go func(i int, args *AppendEntriesArgs) {
						//构造AppendEntriesArgs
						var reply AppendEntriesReply
						ok := rf.sendAppendEntries(i, args, &reply)
						if ok {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							//fmt.Println("peer[", i, "] 接受到sendAppendEntries")

							//RPC后重新判定状态：因为发送到结束该协程是无锁状态
							//检查是否已经不是leader
							if rf.state != Leader {
								DPrintf("RaftNode[%d], term: %d, 发送完sendAppendEntries发现自己不再是leader，而是%d", rf.me, rf.currentTerm, rf.state)
								return
							}
							//检查term有发生变化
							if rf.currentTerm != args.Term {
								return
							}
							if rf.votedFor != rf.me {
								return
							}

							//追加失败
							if !reply.Success {
								if reply.Term > rf.currentTerm {
									//变回follower
									DPrintf("RaftNode[%d], term: %d 追加给RaftNode[%d], term: %d 失败，因为对面任期更大", rf.me, rf.currentTerm, i, reply.Term)
									rf.toBeFollower(reply.Term)
									rf.persist()
									return
								} else {
									//日志一致性检查失败造成的
									/*
										if rf.nextIndex[i] > rf.matchIndex[i]+1 {
											DPrintf("RaftNode[%d], term: %d 收到追加失败：原因是一致性检查失败，递减nextIndex[%d]为：%d", rf.me, rf.currentTerm, i, rf.nextIndex[i]-1)
											rf.nextIndex[i]--
										}*/
									if reply.ConflictTerm == -1 {
										rf.nextIndex[i] = reply.ConflictIndex
										DPrintf("RaftNode[%d], term: %d 追加给RaftNode[%d], term: %d 失败, 原因是一致性检查失败，reply.ConflictTerm[]为-1,即对面日志比较短，conflictIndex为：%d", rf.me, rf.currentTerm, i, reply.Term, reply.ConflictIndex)
									} else {
										//此种情况说明： 对面的日志 >= 我们
										//寻找ConflictTerm该任期下的最后一个日志
										rf.nextIndex[i] = reply.ConflictIndex
										DPrintf("RaftNode[%d], term: %d 追加给RaftNode[%d], term: %d 失败, 原因是一致性检查失败，nextIndex[%d]初始化为: %d ", rf.me, rf.currentTerm, i, reply.Term, i, rf.nextIndex[i])
										//该情况下不可能找的到
										if reply.ConflictTerm > rf.currentTerm {
											DPrintf("RaftNode[%d], term: %d 追加给RaftNode[%d], term: %d 失败, 原因是一致性检查失败，reply.ConflictTerm: %d > rf.currentTerm : %d", rf.me, rf.currentTerm, i, reply.Term, reply.ConflictTerm, rf.currentTerm)
											return
										} else {
											DPrintf("RaftNode[%d], term: %d 追加给RaftNode[%d], term: %d 失败, 原因是一致性检查失败，reply.ConflictTerm: %d <= rf.currentTerm : %d", rf.me, rf.currentTerm, i, reply.Term, reply.ConflictTerm, rf.currentTerm)
											//正着找
											for j := 1; j <= rf.getLastLogIndex(); j++ {
												if rf.log[j].Term == reply.ConflictTerm {
													for j <= rf.getLastLogIndex() && rf.log[j].Term == reply.ConflictTerm {
														j++
													}
													rf.nextIndex[i] = j
												}
											}

											DPrintf("RaftNode[%d], term: %d 正着找node: %d 的ConflictTerm: %d 的最后一个该term的下一个位置: %d ", rf.me, rf.currentTerm, i, reply.ConflictTerm, rf.nextIndex[i])
										}
									}

								}
								DPrintf("RaftNode[%d], term: %d, 追加失败后的nextIndex[%d]: %d, matchIndex[%d]: %d", rf.me, rf.currentTerm, i, rf.nextIndex[i], i, rf.matchIndex[i])
							} else {
								//追加成功
								DPrintf("RaftNode[%d], term: %d, 追加成功给RaftNode[%d], term: %d,", rf.me, rf.currentTerm, i, reply.Term)
								rf.nextIndex[i] = len(args.Entries) + args.PrevLogIndex + 1
								rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
								rf.matchIndex[rf.me] = rf.getLastLogIndex() //注意更新自己的下标的match
								DPrintf("RaftNode[%d], term: %d, 追加成功后的nextIndex[%d]: %d, matchIndex[%d]: %d", rf.me, rf.currentTerm, i, rf.nextIndex[i], i, rf.matchIndex[i])
								//看看是否可以更新commitIndex
								//创建matchIndex的副本
								sortMatchIndex := append(rf.matchIndex[:0:0], rf.matchIndex...)
								//对其进行排序
								sort.Ints(sortMatchIndex)
								//取其中位数
								midIndex := sortMatchIndex[len(sortMatchIndex)/2]

								//延迟提交检查：rf.log[midIndex].Term == rf.currentTerm
								if midIndex > rf.commitIndex && rf.log[midIndex].Term == rf.currentTerm {
									DPrintf("RaftNode[%d], term: %d 延迟提交检查成功，midIndex: %d, commitIndex: %d, rf.log[midIndex].Term: %d", rf.me, rf.currentTerm, midIndex, rf.commitIndex, rf.log[midIndex].Term)
									rf.commitIndex = midIndex
								} else {
									DPrintf("RaftNode[%d], term: %d 延迟提交检查不成功，midIndex: %d, commitIndex: %d, rf.log[midIndex].Term: %d", rf.me, rf.currentTerm, midIndex, rf.commitIndex, rf.log[midIndex].Term)
								}
								//看看是否需要apply
								if rf.lastApplied < rf.commitIndex {
									rf.conditional.Broadcast()
								}
							}
						} else {
							//fmt.Println("peer[", i, "] 未接受到sendAppendEntries")
						}
					}(i, &args)
				}
			}
		}()

	}
}

func (rf *Raft) initializeRaft() {
	rf.currentTerm = 0
	rf.votedFor = -1 //初始化为-1

	entry := LogEntry{0, 0, nil}
	rf.log = append(rf.log, entry)

	rf.state = Follower
	rf.electionTime = time.Now()
	rf.timeout = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.conditional = sync.NewCond(&rf.mu)
	//fmt.Println("peer[", rf.me, "] Init Raft finish")
	//fmt.Println("peer[", rf.me, "] 初始化完后 log 大小:", len(rf.log))
	//fmt.Println("peer[", rf.me, "] 初始化完后 peer 大小:", len(rf.peers))
}

func (rf *Raft) applyTicker(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		func() {
			rf.mu.Lock()
			for rf.lastApplied >= rf.commitIndex {
				rf.conditional.Wait()
			}

			rf.lastApplied++
			apply := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.log[rf.lastApplied].Index}
			rf.mu.Unlock() //释放锁防止下面阻塞带着锁
			applyCh <- apply
			return
		}()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.initializeRaft()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("RaftNode[%d] 读取持久化的数据后：term: %d, votefor: %d, logs: %v", rf.me, rf.currentTerm, rf.votedFor, rf.log)

	// start ticker goroutine to start elections
	go rf.ticker()

	//定期检查有无需要apply的日志
	go rf.applyTicker(applyCh)

	return rf
}

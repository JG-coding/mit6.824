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
	"math"
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
	currentTerm       int        //当前已知最新任期
	votedFor          int        //投票给谁
	log               []LogEntry //日志
	LastIncludedIndex int        //快照所包含的最后一个日志条目的索引
	LastIncludeTerm   int        //快照所包含的最后一个日志条目的任期

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
	applyCh     chan ApplyMsg
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
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludeTerm)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.snapshot)
	DPrintf("RaftNode[%d], term: %d 调用persist(), command: %v", rf.me, rf.currentTerm, rf.log)
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
	var term, voteFor, lastIncludeIndex, lastIncludeTerm int

	if d.Decode(&term) != nil || d.Decode(&voteFor) != nil || d.Decode(&lastIncludeIndex) != nil || d.Decode(&lastIncludeTerm) != nil {
		fmt.Println("readPersist error")
	}

	rf.currentTerm = term
	rf.votedFor = voteFor
	rf.LastIncludedIndex = lastIncludeIndex
	rf.LastIncludeTerm = lastIncludeTerm

	//引入snapshot后,committed,lastApplied可修改成
	rf.commitIndex = rf.LastIncludedIndex
	rf.lastApplied = rf.LastIncludedIndex

	d.Decode(&rf.log)
	DPrintf("RaftNode[%d], term: %d 调用readPersist(), command: %v", rf.me, rf.currentTerm, rf.log)
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
		logs := LogEntry{Index: rf.getLastLogIndex() + 1, Command: command, Term: rf.currentTerm}
		rf.log = append(rf.log, logs)
		rf.persist()
		DPrintf("RaftNode[%d], term: %d, 收到客户端追加日志command : %v，当前日志长度为: %d", rf.me, rf.currentTerm, command, len(rf.log))
		DPrintf("RaftNode[%d] 被客户端追加日志后：term: %d, votefor: %d, logs: %v", rf.me, rf.currentTerm, rf.votedFor, rf.log)
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

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 获取最后一个日志的Index
func (rf *Raft) getLastLogIndex() int {
	//可能所有日志都被打快照了
	if rf.getLogLength() == 1 {
		return rf.LastIncludedIndex
	}
	return rf.log[len(rf.log)-1].Index
}

// 获取最后一个日志的Term
func (rf *Raft) getLastLogTerm() int {
	//可能所有日志都被打快照了
	if rf.getLogLength() == 1 {
		return rf.LastIncludeTerm
	}
	return rf.log[rf.getLogLength()-1].Term
}

// 获取日志长度
func (rf *Raft) getLogLength() int {
	return len(rf.log)
}

// 包括快照的所有日志长度
func (rf *Raft) getAbsoluteLogLength() int {
	return rf.LastIncludedIndex + len(rf.log)
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
	DPrintf("RaftNode[%d], term: %d, 构造AppendEntries给 RaftNode[%d]，其 nextIndex[%d]为: %d,其lastIncludeIndex为 %d", rf.me, rf.currentTerm, i, i, rf.nextIndex[i], rf.LastIncludedIndex)

	//snapshot   需要发送installSnapshot
	if rf.nextIndex[i] <= rf.LastIncludedIndex {
		var snapshotArgs InstallSnapshotArgs
		snapshotArgs.Term = rf.currentTerm
		snapshotArgs.Data = clone(rf.persister.ReadSnapshot())
		snapshotArgs.LastIncludeTerm = rf.LastIncludeTerm
		snapshotArgs.LastIncludedIndex = rf.LastIncludedIndex
		snapshotArgs.LeaderId = rf.me
		DPrintf("RaftNode[%d], term: %d call constructAppendEntriesArgs the args.data is %v to RaftNode[%d], rf.persister.ReadSnapshot() is %v", rf.me, rf.currentTerm, snapshotArgs.Data, i, rf.persister.ReadSnapshot())
		//发送rpc
		go rf.installSnapshotSender(i, snapshotArgs)

		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		args.Term = rf.currentTerm
		args.PrevLogIndex = rf.LastIncludedIndex
		args.PrevLogTerm = rf.LastIncludeTerm
		if rf.getLogLength() == 1 {
			//发空包---因为都通过快照了
			args.Entries = append(args.Entries, rf.log[(rf.getLogLength()):]...)
		} else {
			args.Entries = append(args.Entries, rf.log[rf.afterSnapshotIndex(rf.LastIncludedIndex+1):]...)
		}
	} else {
		args.Term = rf.currentTerm
		args.LeaderCommit = rf.commitIndex
		args.LeaderId = rf.me
		args.Entries = append(args.Entries, rf.log[rf.afterSnapshotIndex(rf.nextIndex[i]):]...)
		if rf.afterSnapshotIndex(rf.nextIndex[i]) > 1 {
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[rf.afterSnapshotIndex(args.PrevLogIndex)].Term
		} else {
			args.PrevLogIndex = rf.LastIncludedIndex
			args.PrevLogTerm = rf.LastIncludeTerm
		}
	}

}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		//周期性10ms检查一次时间差：
		time.Sleep(20 * time.Millisecond)

		go rf.electionTicker()
	}
}

func (rf *Raft) applyTicker() {
	for rf.killed() == false {
		func() {
			rf.mu.Lock()
			for rf.lastApplied >= rf.commitIndex {
				rf.conditional.Wait()
			}
			firstIndex := rf.afterSnapshotIndex(rf.lastApplied + 1)
			endIndex := rf.afterSnapshotIndex(rf.commitIndex + 1)
			commitIndex := rf.commitIndex
			lastApplied := rf.lastApplied
			entries := make([]LogEntry, commitIndex-lastApplied)
			copy(entries, rf.log[firstIndex:endIndex])
			DPrintf("RaftNode[%d], term: %d apply entry, the firstIndex is %d, the endIndex is %d", rf.me, rf.currentTerm, firstIndex, endIndex)
			rf.mu.Unlock()
			for _, entry := range entries {
				apply := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index}
				rf.applyCh <- apply
				DPrintf("RaftNode[%d], term: %d apply entry: %v", rf.me, rf.currentTerm, apply)
			}

			//全部发送完后检查 commitIndex(发送前的大小) 与 当前rf.lastApplied大小的关系
			//防止：该raft被调用了InstallSnapshot后rf.lastApplied变的更大，而造成当前更小的commitIndex覆盖掉了
			rf.mu.Lock()
			rf.lastApplied = int(math.Max(float64(commitIndex), float64(rf.lastApplied)))
			rf.mu.Unlock()
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
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.initializeRaft()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("RaftNode[%d] 读取持久化的数据后：term: %d, votefor: %d, logs: %v", rf.me, rf.currentTerm, rf.votedFor, rf.log)

	// start ticker goroutine to start elections
	go rf.ticker()

	//定期检查有无需要apply的日志
	go rf.applyTicker()

	return rf
}

func (rf *Raft) initializeRaft() {
	rf.currentTerm = 0
	rf.votedFor = -1 //初始化为-1
	rf.state = Follower
	rf.electionTime = time.Now()
	rf.timeout = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.LastIncludedIndex = 0
	rf.LastIncludeTerm = 0
	rf.conditional = sync.NewCond(&rf.mu)

	ZeroEntry := LogEntry{Index: 0, Command: nil, Term: 0}
	rf.log = append(rf.log, ZeroEntry)
}

func (rf *Raft) toBeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
}

func (rf *Raft) toBeCandidate() {
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.electionTime = time.Now() //开始一次选举重置超时时间
	DPrintf("RaftNode[%d], term: %d Follower -> Candidate", rf.me, rf.currentTerm)
}

func (rf *Raft) toBeLeader() {
	//初始化nextIndex[]和matchIndex[]
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.state = Leader
	rf.timeout = time.Duration(0) * time.Millisecond
	DPrintf("RaftNode[%d], term: %d 成为leader，初始化next[i]的大小为: %d", rf.me, rf.currentTerm, rf.nextIndex[0])
}

func (rf *Raft) afterSnapshotIndex(index int) int {
	if index == 0 {
		return 0
	}
	if index-rf.LastIncludedIndex < 0 {
		DPrintf("RaftNode[%d], term: %d : error: index : %d < LastIncludedIndex : %d", rf.me, rf.currentTerm, index, rf.LastIncludedIndex)
		return -1
	}
	return index - rf.LastIncludedIndex
}

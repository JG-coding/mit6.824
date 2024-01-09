package raft

import (
	"6.5840/labgob"
	"bytes"
	"time"
)

type InstallSnapshotArgs struct {
	Term              int //leader任期
	LeaderId          int
	LastIncludedIndex int //快照所包含的最后一个日志条目的索引
	LastIncludeTerm   int //快照所包含的最后一个日志条目的任期
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	//对面任期更小，不处理该rpc
	if rf.currentTerm > args.Term {
		rf.mu.Unlock()
		return
	}

	//避免旧快照替换新快照
	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	// LastIncludedIndex > lastLogIndex
	if args.LastIncludedIndex > rf.getLastLogIndex() {
		rf.LastIncludedIndex = args.LastIncludedIndex
		rf.LastIncludeTerm = args.LastIncludeTerm
		rf.log = rf.log[0:1]
	} else {
		cutIndex := rf.afterSnapshotIndex(args.LastIncludedIndex)
		//剪裁日志
		rf.log = rf.log[cutIndex+1:]
		//让日志重新从下标1开始
		entry := LogEntry{0, 0, nil}
		rf.log = append([]LogEntry{entry}, rf.log...)
		rf.LastIncludedIndex = args.LastIncludedIndex
		rf.LastIncludeTerm = args.LastIncludeTerm
	}

	/*注意：这里必须有 rf.lastApplied < rf.rf.LastIncludedIndex 更新
	因为可能出现：rf.commitIndex == rf.LastIncludedIndex 但是rf.lastApplied < rf.LastIncludedIndex
	此时，会导致applyTicker中应用到空日志，从而出现bug
	*/
	if rf.commitIndex < rf.LastIncludedIndex || rf.lastApplied < rf.LastIncludedIndex {
		rf.commitIndex = rf.LastIncludedIndex
		rf.lastApplied = rf.LastIncludedIndex
	}

	//重置选举时间
	rf.heartbeatTime = time.Now()

	DPrintf("---------RaftNode[%d], term: %d call InstallSnapshot,the LastIncludedIndex: %d,the LastIncludeTerm: %d", rf.me, rf.currentTerm, rf.LastIncludedIndex, rf.LastIncludeTerm)
	DPrintf("---------RaftNode[%d], term: %d call InstallSnapshot,the log is : %v", rf.me, rf.currentTerm, rf.log)
	//做快照，序列化数据进行持久化
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludeTerm)
	e.Encode(rf.log)
	if args.Data == nil {
		DPrintf("RaftNode[%d], term: %d recive nil snapshot, the sender is %d", rf.me, rf.currentTerm, args.LeaderId)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, args.Data)
	//构造ApplyMsg发送
	var msg ApplyMsg
	msg.SnapshotValid = true
	msg.Snapshot = args.Data
	msg.SnapshotIndex = args.LastIncludedIndex
	msg.SnapshotTerm = args.LastIncludeTerm
	rf.mu.Unlock()
	rf.applyCh <- msg
}

func (rf *Raft) installSnapshotSender(i int, args InstallSnapshotArgs) {
	args_in := args
	DPrintf("RaftNode[%d], term: %d call installSnapshotSender the args.data is %v to RaftNode[%d]", rf.me, rf.currentTerm, args_in.Data, i)
	var reply InstallSnapshotReply
	ok := rf.sendInstallSnapshot(i, &args_in, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.toBeFollower(reply.Term)
			rf.votedFor = -1
		}
		DPrintf("RaftNode[%d], term: %d 发送InstallSnapshot RPC成功给 RaftNode[%d]", rf.me, rf.currentTerm, i)
	} else {
		DPrintf("RaftNode[%d], term: %d 发送InstallSnapshot RPC失败给 RaftNode[%d]", rf.me, rf.currentTerm, i)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintf("---------RaftNode[%d], term: %d call Snapshot() and the index is %d, the lastIncludeIndex is %d,the commitIndex is %d", rf.me, rf.currentTerm, index, rf.LastIncludedIndex, rf.commitIndex)
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//该 index 之前的已经做过快照了，不需要再做
	if index <= rf.LastIncludedIndex {
		return
	}

	//寻找到index处的日志的下标
	cutIndex := index - rf.LastIncludedIndex
	DPrintf("---------RaftNode[%d], term: %d call Snapshot() and calculate cutIndex = %d", rf.me, rf.currentTerm, cutIndex)

	//更新LastIncludedIndex, LastIncludeTerm
	rf.LastIncludedIndex = index
	rf.LastIncludeTerm = rf.log[cutIndex].Term

	//剪裁日志
	rf.log = rf.log[cutIndex+1:]

	//让日志重新从下标1开始
	entry := LogEntry{0, 0, nil}
	rf.log = append([]LogEntry{entry}, rf.log...)
	DPrintf("---------RaftNode[%d], term: %d call Snapshot(),the LastIncludedIndex: %d,the LastIncludeTerm: %d", rf.me, rf.currentTerm, rf.LastIncludedIndex, rf.LastIncludeTerm)
	DPrintf("---------RaftNode[%d], term: %d call Snapshot(),the log is : %v", rf.me, rf.currentTerm, rf.log)
	//做快照，序列化数据进行持久化
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludeTerm)
	e.Encode(rf.log)

	if rf.commitIndex < rf.LastIncludedIndex {
		rf.commitIndex = rf.LastIncludedIndex
		rf.lastApplied = rf.LastIncludedIndex
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
	DPrintf("---------RaftNode[%d], term: %d call Snapshot(),the snapshot is : %v", rf.me, rf.currentTerm, snapshot)
	DPrintf("---------RaftNode[%d], term: %d call Snapshot() and then call readSnapShot,the snapshot is : %v", rf.me, rf.currentTerm, rf.persister.ReadSnapshot())

}

package raft

import (
	"6.5840/labgob"
	"bytes"
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

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintf("---------RaftNode[%d], term: %d call Snapshot()-------", rf.me, rf.currentTerm)
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
	//做快照，序列化数据进行持久化
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludeTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

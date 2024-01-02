package raft

import (
	"math/rand"
	"time"
)

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
		if rf.getLastLogTerm() > args.LastLogTerm ||
			rf.getLastLogTerm() == args.LastLogTerm &&
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
		rf.electionTime = time.Now() //投票给别人，重置超时时间
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

		//防止之前任期的投票，让过期的任期的term成为了leader。 不增加这个会出现脑烈情况
		if rf.currentTerm > currentTerm {
			return
		}

		if rf.votedFor != rf.me {
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

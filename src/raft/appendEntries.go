package raft

import (
	"sort"
	"time"
)

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

func (rf *Raft) searchTheFirstIndex(term int) int {
	index := 0
	for i := 0; i < rf.getLogLength(); i++ {
		if rf.log[i].Term == term {
			index = i
			break
		}
	}
	return index
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
	rf.electionTime = time.Now() //收到合格的心跳包，重置选举时间

	//一致性检查失败
	if rf.getLastLogIndex() < args.PrevLogIndex {
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		reply.ConflictTerm = -1
		DPrintf("AppendEntries： RaftNode[%d], term: %d 的LastLogIndex:%d,  leader RaftNode[%d], term: %d 的PrevLogIndex: %d，不一致，设置reply.ConflictTerm=-1， reply.ConflictIndex: %d", rf.me, rf.currentTerm, rf.getLastLogIndex(), args.LeaderId, args.Term, args.PrevLogIndex, reply.ConflictIndex)
		return
	}
	if rf.log[rf.afterSnapshotIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[rf.afterSnapshotIndex(args.PrevLogIndex)].Term
		//倒着寻找到日志中为ConflictTerm的第一个日志
		firstConfictTermLogIndex := args.PrevLogIndex

		if reply.ConflictTerm == 0 {
			reply.ConflictIndex = 0
			return
		}

		//i从 args.PrevLogIndex 开启寻找即可
		for i := rf.afterSnapshotIndex(args.PrevLogIndex); i >= 1; i-- {
			//找到最小index的日志为ConflictTerm的日志索引
			if rf.log[i].Term == reply.ConflictTerm {
				firstConfictTermLogIndex = rf.log[i].Index
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
		if rf.afterSnapshotIndex(index) < rf.getLogLength() {
			if rf.log[rf.afterSnapshotIndex(index)].Term == entry.Term {
				continue
			}
			rf.log = rf.log[:rf.afterSnapshotIndex(index)] //不一样的日志，那么截断该位置的日志
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
				DPrintf("RaftNode[%d], term: %d, 认为自己是leader，发送心跳包", rf.me, rf.currentTerm)
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

					go func(i int, args AppendEntriesArgs) {
						//构造AppendEntriesArgs
						var reply AppendEntriesReply
						ok := rf.sendAppendEntries(i, &args, &reply)
						if !ok {
							DPrintf("RaftNode[%d], term: %d, 发送完sendAppendEntries失败给RaftNode[%d]", rf.me, rf.currentTerm, i)
						}
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
							if reply.Term > rf.currentTerm {
								//变回follower
								DPrintf("RaftNode[%d], term: %d 追加给RaftNode[%d], term: %d 失败，因为对面任期更大, 变回follower", rf.me, rf.currentTerm, i, reply.Term)
								rf.toBeFollower(reply.Term)
								rf.persist()
								return
							}
							//追加失败
							if !reply.Success {
								//追加失败
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
										for j := 0; j <= args.PrevLogIndex; j++ {
											index := rf.afterSnapshotIndex(j)
											if rf.log[index].Term == reply.ConflictTerm {
												for j <= args.PrevLogIndex && rf.log[index].Term == reply.ConflictTerm {
													j++
													index = rf.afterSnapshotIndex(j)
												}
												rf.nextIndex[i] = j
												break
											}
										}

										DPrintf("RaftNode[%d], term: %d 正着找node: %d 的ConflictTerm: %d 的最后一个该term的下一个位置: %d ", rf.me, rf.currentTerm, i, reply.ConflictTerm, rf.nextIndex[i])
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
								sortedMatchIndex := make([]int, 0)
								sortedMatchIndex = append(sortedMatchIndex, len(rf.log))
								for i := 0; i < len(rf.peers); i++ {
									if i == rf.me {
										continue
									}
									sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
								}
								//对其进行排序
								sort.Ints(sortedMatchIndex)
								//取其中位数
								midIndex := sortedMatchIndex[len(rf.peers)/2]

								//snapshot后：
								index := rf.afterSnapshotIndex(midIndex)

								//延迟提交检查：rf.log[midIndex].Term == rf.currentTerm
								if midIndex > rf.commitIndex && rf.log[index].Term == rf.currentTerm {
									DPrintf("RaftNode[%d], term: %d 延迟提交检查成功，midIndex: %d, commitIndex: %d, rf.log[midIndex].Term: %d", rf.me, rf.currentTerm, midIndex, rf.commitIndex, rf.log[index].Term)
									rf.commitIndex = midIndex
								} else {
									DPrintf("RaftNode[%d], term: %d 延迟提交检查不成功，midIndex: %d, commitIndex: %d, rf.log[midIndex].Term: %d", rf.me, rf.currentTerm, midIndex, rf.commitIndex, rf.log[index].Term)
								}
								//看看是否需要apply
								if rf.lastApplied < rf.commitIndex {
									rf.conditional.Broadcast()
								}
							}
						} else {
							//fmt.Println("peer[", i, "] 未接受到sendAppendEntries")
						}
					}(i, args)
				}
			}
		}()

	}
}

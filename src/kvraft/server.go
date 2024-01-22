package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

// 提交给raft存储在日志中的
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandType string
	Key         string
	Value       string
	CommandId   int
	ClientId    int64
}

// 融合 get,put,append的信息
type ApplyMsg struct {
	Err   Err
	Value string //保存get操作的结果, put,append时忽略该字段

	//网络分区可能造成自己已经不是leader，任期会发生改变
	Term int
}

type CommandInfo struct {
	ApplyMsg  ApplyMsg
	CommandId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Kv_db       kvDataBase
	applyMsg    map[int64]CommandInfo //用于检查某个客户端的某个应用有无已经被应用
	replyChMap  map[int]chan ApplyMsg //index的响应chan
	lastApplied int                   //用于防止快照回退
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	//1, 先判断该命令是否已经被命令---如果已经被应用，直接返回结果
	if commandInfo, ok := kv.applyMsg[args.ClientId]; ok {
		//已经被应用，则直接返回结果
		if commandInfo.CommandId == args.CommandId {
			reply.Err = commandInfo.ApplyMsg.Err
			reply.Value = commandInfo.ApplyMsg.Value
			DPrintf("server[%d]: 从client[%d]收到 Get RPC;args=[%v]，命令已经应用过，直接返回结果， ok = %v, reply = [%v]", kv.me, args.ClientId, args, ok, reply)
			kv.mu.Unlock()
			return
		}
	}

	var op Op
	op.Key = args.Key
	op.CommandId = args.CommandId
	op.ClientId = args.ClientId
	op.CommandType = GET

	index, term, isleader := kv.rf.Start(op)
	if isleader == false {
		//DPrintf("server[%d]: Get; bu I am not leader", kv.me)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		DPrintf("server[%d]: 从client[%d]收到 Get RPC;args=[%v]，发现raft不是leader,直接返回, reply = [%v]", kv.me, args.ClientId, args, reply)
		return
	}

	// 是leader，那么等待raft被应用
	replyChan := make(chan ApplyMsg, 1)
	kv.replyChMap[index] = replyChan

	kv.mu.Unlock()
	//4.等待应用后返回消息----注意检查收到的消息是否还是原来的index和term，如果不是，说明该raft已经不是leader
	select {
	case replyMsg := <-replyChan:

		//提交的日志与最开始追加的日志不一样，说明该raft已经由leader->follower
		if replyMsg.Term != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = replyMsg.Err
			reply.Value = replyMsg.Value
			DPrintf("server[%d]: 从client[%d]收到 Get RPC;args=[%v],得到应用后返回消息 reply = [%v]", kv.me, args.ClientId, args, reply)
		}

	case <-time.After(2000 * time.Millisecond):
		DPrintf("server[%d]: 处理请求超时: %v\n", kv.me, op)
		reply.Err = ErrTimeOut
	}
	//清除该chan
	go kv.CloseChan(index)
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	//1, 先判断该命令是否已经被命令---如果已经被应用，直接返回结果
	if commandInfo, ok := kv.applyMsg[args.ClientId]; ok {
		//已经被应用，则直接返回结果
		if commandInfo.CommandId == args.CommandId {
			reply.Err = commandInfo.ApplyMsg.Err
			kv.mu.Unlock()
			return
		}
	}

	var op Op
	op.Key = args.Key
	op.Value = args.Value
	op.CommandId = args.CommandId
	op.ClientId = args.ClientId
	op.CommandType = args.Op

	index, term, isleader := kv.rf.Start(op)
	if isleader == false {
		//DPrintf("server[%d]: PutAppend; bu i am not leader", kv.me)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	// 是leader，那么等待raft被应用
	replyChan := make(chan ApplyMsg, 1)
	kv.replyChMap[index] = replyChan

	kv.mu.Unlock()
	//4.等待应用后返回消息
	select {
	case replyMsg := <-replyChan:
		if replyMsg.Term != term {
			reply.Err = ErrWrongLeader
		} else {
			//当被通知时,返回结果
			reply.Err = replyMsg.Err
			DPrintf("server[%d]: 从client[%d]收到 PutAppend RPC;args=[%v],得到应用后返回消息 reply = [%v]", kv.me, args.ClientId, args, reply)
		}

	case <-time.After(2000 * time.Millisecond):
		DPrintf("server[%d]: 处理请求超时: %v\n", kv.me, op)
		reply.Err = ErrTimeOut
	}
	//清除该chan
	go kv.CloseChan(index)
	return
}

func (kv *KVServer) CloseChan(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.replyChMap[index]
	if !ok {
		return
	}
	close(ch)
	delete(kv.replyChMap, index)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.Kv_db = kvDataBase{make(map[string]string)}
	kv.replyChMap = make(map[int]chan ApplyMsg)
	kv.applyMsg = make(map[int64]CommandInfo)
	kv.lastApplied = 0
	//读取快照数据信息
	kv.readSnapshot(kv.rf.GetSnapshot())

	go kv.receiveRaftApply()

	return kv
}

func (kv *KVServer) receiveRaftApply() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			//合法命令
			if applyMsg.CommandValid {
				//可以在数据库中做相应的动作了
				DPrintf("server[%d] -> client[%d] receiveRaftApply, 已经收到raft发来的Commnd信息", kv.me, applyMsg.Command.(Op).ClientId)
				kv.ApplyCommand(applyMsg)
			} else if applyMsg.SnapshotValid {
				//被动接收到快照数据
				DPrintf("server[%d] receiveRaftApply, 已经收到raft发来snapshot信息", kv.me)
				kv.ApplySnapshot(applyMsg)
			} else {
				//不应该发生的情况
			}
		}
	}
}

// 需要操作数据库，所以得保证线程安全
func (kv *KVServer) ApplyCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := applyMsg.Command.(Op)
	var reply ApplyMsg

	//检查命令有无已经执行过-----防止重复操作数据库
	if commandContext, ok := kv.applyMsg[op.ClientId]; ok && commandContext.CommandId >= op.CommandId {
		reply = commandContext.ApplyMsg
		return
	}

	//Get方法
	if op.CommandType == GET {
		value, isExistKey := kv.Kv_db.get(op.Key)
		DPrintf("server[%d] -> client[%d] ApplyCommand, 方法为Get, key:%v, value:%v", kv.me, applyMsg.Command.(Op).ClientId, op.Key, op.Value)
		if !isExistKey {
			reply.Err = ErrNoKey
			reply.Value = value
		} else {
			reply.Value = value
		}
	} else if op.CommandType == PUT {
		kv.Kv_db.put(op.Key, op.Value)
		DPrintf("server[%d] -> client[%d] ApplyCommand, 方法为PUT, key:%v, value:%v", kv.me, applyMsg.Command.(Op).ClientId, op.Key, op.Value)
	} else if op.CommandType == APPEND {
		kv.Kv_db.append(op.Key, op.Value)
		DPrintf("server[%d] -> client[%d] ApplyCommand, 方法为APPEND, key:%v, value:%v", kv.me, applyMsg.Command.(Op).ClientId, op.Key, op.Value)
	}

	//发送数据到replyChMap
	if replyCh, ok := kv.replyChMap[applyMsg.CommandIndex]; ok {
		replyCh <- reply
		DPrintf("server[%d] -> client[%d] 发送已经应用数据到replyChMap, key:%v, value:%v", kv.me, applyMsg.Command.(Op).ClientId, op.Key, op.Value)
	}

	//更新client最新的command,防止重复提交command
	kv.applyMsg[op.ClientId] = CommandInfo{reply, op.CommandId}
	if kv.lastApplied < applyMsg.CommandIndex {
		kv.lastApplied = applyMsg.CommandIndex
	}
	//该应用成功后检查是否需要做快照
	if kv.needSnapshot() { // >=0.9
		SnapshotData := kv.createSnapshot()
		go kv.rf.Snapshot(applyMsg.CommandIndex, SnapshotData)
	}
}

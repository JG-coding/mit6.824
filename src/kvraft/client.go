package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId      int
	clientId      int64
	LastcommandId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.LastcommandId = 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs
	args.CommandId = ck.LastcommandId + 1
	args.Key = key
	args.ClientId = ck.clientId

	//寻找leader,向leader发送Get rpc
	serverId := ck.leaderId
	serverNum := len(ck.servers)
	DPrintf("client[%d]: 调用Get, key: %v", ck.clientId, key)
	for ; ; serverId = (serverId + 1) % serverNum {
		var reply GetReply
		//DPrintf("client[%d]: 开始发送Get RPC;args=[%v]到server[%d]", ck.clientId, args, serverId)
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

		//发送失败：可能是rpc超时，可能是该server不是leader,则发送给下一个服务器
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			DPrintf("client[%d]: 发送Get RPC;args=[%v]到server[%d]失败， ok = %v, reply = [%v]", ck.clientId, args, serverId, ok, reply)
			continue
		} else {
			//发送get rpc成功给正确的leader
			DPrintf("client[%d]: 发送Get RPC;args=[%v]到server[%d]成功， ok = %v, reply = [%v]", ck.clientId, args, serverId, ok, reply)

			//更新正确的leader和最新命令标识
			ck.leaderId = serverId
			ck.LastcommandId = args.CommandId

			//no key
			if reply.Err == ErrNoKey {
				DPrintf("client[%d]: 调用Get, key: %v, 结果key不存在", ck.clientId, key)
				return ""
			}

			DPrintf("client[%d]: 调用Get, key: %v, 得到value: %v", ck.clientId, key, reply.Value)
			return reply.Value
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs
	args.CommandId = ck.LastcommandId + 1
	args.ClientId = ck.clientId
	args.Key = key
	args.Value = value
	args.Op = op
	DPrintf("client[%d]: 调用PutAppend,op :%v, key: %v, value: %v", ck.clientId, op, key, value)
	//寻找leader,向leader发送PutAppend rpc
	serverId := ck.leaderId
	serverNum := len(ck.servers)
	for ; ; serverId = (serverId + 1) % serverNum {
		var reply PutAppendReply
		//DPrintf("client[%d]: 开始发送PutAppend RPC;args=[%v]到server[%d]", ck.clientId, args, serverId)
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)

		//发送失败：可能是rpc超时，可能是该server不是leader,则发送给下一个服务器
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			//DPrintf("client[%d]: 发送发送PutAppend RPC;args=[%v]到server[%d]失败， ok = %v, reply = [%v]", ck.clientId, args, serverId, ok, reply)
			continue
		} else {
			//更新正确的leader和最新命令标识
			DPrintf("client[%d]: 发送PutAppend RPC;args=[%v]到server[%d]成功， ok = %v, reply = [%v]", ck.clientId, args, serverId, ok, reply)
			ck.leaderId = serverId
			ck.LastcommandId = args.CommandId
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

package kvraft

import (
	"6.5840/labgob"
	"6.5840/raft"
	"bytes"
	"fmt"
)

// 判断是否需要进行snapshot(90%则进行)
func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}

	var proportion float32
	proportion = float32(kv.rf.GetPersistSize() / kv.maxraftstate)
	return proportion >= 0.9
}

// 创建快照数据
func (kv *KVServer) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Kv_db)
	e.Encode(kv.applyMsg)
	snapshotData := w.Bytes()

	return snapshotData
}

// 从snapshot中解码读取数据
func (kv *KVServer) readSnapshot(snapshotData []byte) {

	//错误处理
	if snapshotData == nil || len(snapshotData) < 1 {
		return
	}

	//解码
	var kvDb kvDataBase
	var applyMsg map[int64]CommandInfo
	r := bytes.NewBuffer(snapshotData)
	d := labgob.NewDecoder(r)
	if d.Decode(&kvDb) != nil || d.Decode(&applyMsg) != nil {
		fmt.Println("readSnapshot error")
	} else {
		kv.Kv_db = kvDb
		kv.applyMsg = applyMsg
	}
}

// 被到收到snapshot的服务端处理
func (kv *KVServer) ApplySnapshot(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.lastApplied >= applyMsg.SnapshotIndex {
		return
	}

	//读取快照信息来更新数据库
	kv.readSnapshot(applyMsg.Snapshot)
	kv.lastApplied = applyMsg.SnapshotIndex
}

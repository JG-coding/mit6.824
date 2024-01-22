package kvraft

// 只存储在内存，因此使用map数据结构保存
type kvDataBase struct {
	KvData map[string]string
}

// 封装该数据库的get, append, put操作
func (kv *kvDataBase) get(key string) (value string, ok bool) {

	//该key存在
	if value, ok := kv.KvData[key]; ok {
		DPrintf("database: get key: %v, value: %v", key, value)
		return value, ok
	}
	DPrintf("database: get key: %v, key no exit", key)
	return "", ok
}

func (kv *kvDataBase) put(key string, value string) {
	kv.KvData[key] = value
	DPrintf("database: put key: %v, value: %v", key, value)
}

func (kv *kvDataBase) append(key string, arg string) {
	//存在该key
	DPrintf("database: append key: %v, arg: %v", key, arg)
	if value, ok := kv.get(key); ok {
		newValue := value + arg //追加
		kv.put(key, newValue)
		DPrintf("database: append key: %v, value: %v", key, newValue)
	} else {
		//不存在
		kv.put(key, arg)
		DPrintf("database: append key: %v, value: %v", key, arg)
	}
}

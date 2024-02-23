package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	//mu sync.Mutex

	// Your definitions here.
	kvMap sync.Map
	IDMap sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if v, ok := kv.kvMap.Load(args.Key); ok {
		reply.Value = v.(string)
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if v, ok := kv.IDMap.LoadOrStore(args.ClientID, &OperateReturn{
		ID:  args.OperateID,
		ret: "",
	}); ok && v == args.OperateID {
		return
	}
	kv.kvMap.Store(args.Key, args.Value)
}

type OperateReturn struct {
	ID  string
	ret string
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if v, ok := kv.IDMap.Load(args.ClientID); ok && v.(*OperateReturn).ID == args.OperateID {
		reply.Value = v.(*OperateReturn).ret
		return
	}
	for {
		if v, ok := kv.kvMap.Load(args.Key); ok {
			sv := v.(string)
			if !kv.kvMap.CompareAndSwap(args.Key, sv, sv+args.Value) {
				continue
			}
			kv.IDMap.Store(args.ClientID, &OperateReturn{
				ID:  args.OperateID,
				ret: sv,
			})
			reply.Value = sv
			break
		} else {
			break
		}
	}

}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.

	return kv
}

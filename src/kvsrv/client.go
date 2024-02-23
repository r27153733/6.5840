package kvsrv

import (
	"6.5840/labrpc"
	"crypto/rand"
	"github.com/google/uuid"
	"math/big"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	id string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.id = uuid.NewString()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	res := GetReply{}
	for {
		ok := ck.server.Call("KVServer.Get", &GetArgs{Key: key}, &res)
		if ok {
			break
		}
	}
	//log.Println(ck.id, "get: ", key, "["+res.Value+"]")
	return res.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	ID := uuid.NewString()
	res := PutAppendReply{}
	if op == "Put" {
		for {
			ok := ck.server.Call("KVServer.Put", &PutAppendArgs{Key: key, Value: value, OperateID: ID, ClientID: ck.id}, &res)
			if ok {
				break
			}
		}
	} else if op == "Append" {
		for {
			ok := ck.server.Call("KVServer.Append", &PutAppendArgs{Key: key, Value: value, OperateID: ID, ClientID: ck.id}, &res)
			if ok {
				break
			}
		}
	}
	//log.Println(ck.id, op+": ", key, "["+value+"]", res.Value)
	return res.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

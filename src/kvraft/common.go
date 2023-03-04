package kvraft

import (
	"fmt"
	"log"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KvDB struct {
	KV map[string]string
}

func (db *KvDB) Put(key string, value string) Err {
	db.KV[key] = value
	return OK
}

func (db *KvDB) Append(key string, value string) Err {
	db.KV[key] += value
	return OK
}

func (db *KvDB) Get(key string) (string, Err) {
	value, ok := db.KV[key]
	if ok {
		return value, OK
	}
	return "", ErrNoKey
}

type Command struct {
	*CommandArgs
}

type Session struct {
	MaxAppliedCommandId int64
	LastReply           *CommandReply
}

type OperationType uint8

const (
	OpPut OperationType = iota
	OpAppend
	OpGet
)

func (op OperationType) String() string {
	switch op {
	case OpPut:
		return "OpPut"
	case OpAppend:
		return "OpAppend"
	case OpGet:
		return "OpGet"
	}
	panic(fmt.Sprintf("unexpected OperationType %d", op))
}

type Err uint8

const (
	OK Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrTimeout
)

func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeout:
		return "ErrTimeout"
	}
	panic(fmt.Sprintf("unexpected Err %d", err))
}

type CommandArgs struct {
	Key       string
	Value     string
	Op        OperationType
	ClientId  int64
	CommandId int64
}

func (args CommandArgs) String() string {
	return fmt.Sprintf("{Key:%v,Value:%v,Op:%v,ClientId:%v,CommandId:%v}", args.Key, args.Value, args.Op, args.ClientId, args.CommandId)
}

type CommandReply struct {
	Err   Err
	Value string
}

func (reply CommandReply) String() string {
	return fmt.Sprintf("{Err:%v,Value:%v}", reply.Err, reply.Value)
}

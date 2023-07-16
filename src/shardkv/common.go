package shardkv

import (
	"6.824/shardctrler"
	"fmt"
	"log"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Err uint

const (
	OK Err = iota
	ErrNoKey
	ErrWrongGroup
	ErrWrongLeader
	ErrTimeout
	ErrNotReady
	ErrOutDated
)

func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongGroup:
		return "ErrWrongGroup"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrOutDated:
		return "ErrOutDated"
	case ErrTimeout:
		return "ErrTimeout"
	case ErrNotReady:
		return "ErrNotReady"
	}
	panic(fmt.Sprintf("unexpected Err %d", err))
}

type ShardStatus uint8

const (
	Serving ShardStatus = iota
	Pulling
	BePulling
	NeedGC
)

func (status ShardStatus) String() string {
	switch status {
	case Serving:
		return "Serving"
	case Pulling:
		return "Pulling"
	case BePulling:
		return "BePulling"
	case NeedGC:
		return "GCing"
	}
	panic(fmt.Sprintf("unexpected ShardStatus %d", status))
}

type dbShard struct {
	Status ShardStatus
	KVs    map[string]string
}

func (db *dbShard) Put(key string, value string) Err {
	db.KVs[key] = value
	return OK
}

func (db *dbShard) Append(key, value string) Err {
	db.KVs[key] += value
	return OK
}

func (db *dbShard) Get(key string) (string, Err) {
	value, ok := db.KVs[key]
	if ok {
		return value, OK
	}
	return "", ErrNoKey
}

type Session struct {
	LastAppliedCommandId int64
	LastResponse         *OperationResponse
}

type CommandType uint8

const (
	kvCmd CommandType = iota
	ConfigurationCmd
	InsertShardsCmd
	DeleteShardsCmd
	EmptyLogCmd
)

func (cmd CommandType) String() string {
	switch cmd {
	case kvCmd:
		return fmt.Sprintf("kvCmd")
	case ConfigurationCmd:
		return fmt.Sprintf("ConfigurationCmd")
	case InsertShardsCmd:
		return fmt.Sprintf("InsertShardsCmd")
	case DeleteShardsCmd:
		return fmt.Sprintf("DeleteShardsCmd")
	case EmptyLogCmd:
		return fmt.Sprintf("EmptyLogCmd")
	}
	panic(fmt.Sprintf("unexpected CommandType %d", cmd))
}

type Command struct {
	CommandType CommandType
	Data        interface{}
}

func newOperationCommand(request *OperationRequest) Command {
	return Command{kvCmd, *request}
}

func newConfigurationCommand(config *shardctrler.Config) Command {
	return Command{ConfigurationCmd, *config}
}

func newInsertShardsCommand(response *ShardOperationResponse) Command {
	return Command{InsertShardsCmd, *response}
}

func newDeleteShardsCommand(request *ShardOperationRequest) Command {
	return Command{DeleteShardsCmd, *request}
}

func newEmptyLogCommand() Command {
	return Command{EmptyLogCmd, ""}
}

type OperationType uint8

const (
	OpGet OperationType = iota
	OpPut
	OpAppend
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
	panic(fmt.Sprintf("unexpected OperationOp %d", op))
}

type OperationRequest struct {
	Key       string
	Value     string
	Op        OperationType
	ClientId  int64
	CommandId int64
}

func (request OperationRequest) String() string {
	return fmt.Sprintf("Shard:%v,Key:%v,Value:%v,Op:%v,ClientId:%v,CommandId:%v}", key2shard(request.Key), request.Key, request.Value, request.Op, request.ClientId, request.CommandId)
}

type OperationResponse struct {
	Err   Err
	Value string
}

func (response OperationResponse) String() string {
	return fmt.Sprintf("{Err:%v,Value:%v}", response.Err, response.Value)
}

type ShardOperationRequest struct {
	ConfigNum int
	ShardIds  []int
}

func (request ShardOperationRequest) String() string {
	return fmt.Sprintf("{ConfigNum:%v,ShardIds:%v}", request.ConfigNum, request.ShardIds)
}

type ShardOperationResponse struct {
	Err            Err
	ConfigNum      int
	Shards         map[int]map[string]string
	ClientSessions map[int64]Session
}

func (response ShardOperationResponse) String() string {
	return fmt.Sprintf("{Err:%v,ConfigNum:%v,ShardIds:%v,lastSession:%v}", response.Err, response.ConfigNum, response.Shards, response.ClientSessions)
}

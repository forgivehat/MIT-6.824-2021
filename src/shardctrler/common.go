package shardctrler

import (
	"fmt"
	"log"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (cf Config) String() string {
	return fmt.Sprintf("{Num:%v,Shards:%v,Groups:%v}", cf.Num, cf.Shards, cf.Groups)
}

type Session struct {
	LastAppliedCommandId int64
	LastResponse         *OperationResponse
}

type Command struct {
	*CommandRequest
}

type Err uint8

const (
	OK Err = iota
	ErrWrongLeader
	ErrTimeout
)

func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeout:
		return "ErrTimeout"
	}
	panic(fmt.Sprintf("unexpected Err %d", err))
}

type OperationType int

const (
	OpJoin OperationType = iota
	OpLeave
	OpMove
	OpQuery
)

func (op OperationType) String() string {
	switch op {
	case OpJoin:
		return "OpJoin"
	case OpLeave:
		return "OpLeave"
	case OpMove:
		return "OpMove"
	case OpQuery:
		return "OpQuery"
	}
	panic(fmt.Sprintf("unexpected OperationType %d", op))
}

type CommandRequest struct {
	ClientId  int64
	CommandId int64
	Op        OperationType
	// for join
	Servers map[int][]string // new GID -> servers mappings
	// for leave
	GIDs []int
	// for move
	Shard int
	GID   int
	// for query
	Num int // desired config number
}

func (request CommandRequest) String() string {
	switch request.Op {
	case OpJoin:
		return fmt.Sprintf("{Op:%v, Servers:%v, ClientId:%v, CommandId:%v}", request.Op, request.Servers, request.ClientId, request.CommandId)
	case OpLeave:
		return fmt.Sprintf("{Op:%v, GIDs:%v, ClientId:%v, CommandId:%v}", request.Op, request.Servers, request.ClientId, request.CommandId)
	case OpMove:
		return fmt.Sprintf("{Op:%v, Shard:%v,GID:%v, ClientId:%v, CommandId:%v}", request.Op, request.Shard, request.GID, request.ClientId, request.CommandId)
	case OpQuery:
		return fmt.Sprintf("{Op:%v, Num:%v, ClientId:%v, CommandId:%v}", request.Op, request.Num, request.ClientId, request.CommandId)
	default:
		panic(fmt.Sprintf("unexpected CommandOp %d", request.Op))
	}
}

type OperationResponse struct {
	Err Err
	// for query
	Config Config
}

func (response *OperationResponse) String() string {
	return fmt.Sprintf("{Err:%v, Config:%v}", response.Err, response.Config)
}

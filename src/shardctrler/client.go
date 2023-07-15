package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	leaderId  int64
	clientId  int64
	commandId int64
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
	ck.clientId = nrand()
	ck.commandId = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	return ck.Command(&CommandRequest{Op: OpQuery, Num: num})
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.Command(&CommandRequest{Op: OpJoin, Servers: servers})
}

func (ck *Clerk) Leave(gids []int) {
	ck.Command(&CommandRequest{Op: OpLeave, GIDs: gids})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.Command(&CommandRequest{Op: OpMove, Shard: shard, GID: gid})
}

func (ck *Clerk) Command(request *CommandRequest) Config {
	request.ClientId, request.CommandId = ck.clientId, ck.commandId
	for {
		var response OperationResponse
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Command", request, &response) || response.Err == ErrWrongLeader || response.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.commandId++
		return response.Config
	}
}

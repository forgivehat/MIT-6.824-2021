package shardctrler

import (
	"6.824/raft"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	dead    int32
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num

	clientSessions map[int64]Session
	notifyCh       map[int]chan *OperationResponse
}

func (sc *ShardCtrler) Command(request *CommandRequest, response *OperationResponse) {
	//defer DPrintf("[Node %v] processes CommandRequest %v with OperationResponse %v", sc.me, request, response)
	sc.mu.RLock()
	if request.Op != OpQuery && sc.isDuplicatedRequest(request.ClientId, request.CommandId) {
		lastReply := sc.clientSessions[request.ClientId].LastResponse
		response.Config, response.Err = lastReply.Config, lastReply.Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()
	index, _, isLeader := sc.rf.Start(Command{request})
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	ch := sc.getNotifyCh(index)
	sc.mu.Unlock()
	select {
	case result := <-ch:
		response.Config, response.Err = result.Config, result.Err
	case <-time.After(500 * time.Millisecond):
		response.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		sc.removeNotifyCh(index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) getNotifyCh(index int) chan *OperationResponse {
	if _, ok := sc.notifyCh[index]; !ok {
		sc.notifyCh[index] = make(chan *OperationResponse, 1)
	}
	return sc.notifyCh[index]
}

func (sc *ShardCtrler) removeNotifyCh(index int) {
	delete(sc.notifyCh, index)
}

func (sc *ShardCtrler) isDuplicatedRequest(clientId int64, requestId int64) bool {
	lastSession, ok := sc.clientSessions[clientId]
	return ok && requestId <= lastSession.LastAppliedCommandId
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case applyMsg := <-sc.applyCh:
			//DPrintf("[Node %v] tries to apply message %v", sc.me, applyMsg)
			if applyMsg.CommandValid {
				var response *OperationResponse
				command := applyMsg.Command.(Command)
				sc.mu.Lock()
				if command.Op != OpQuery && sc.isDuplicatedRequest(command.ClientId, command.CommandId) {
					response = sc.clientSessions[command.ClientId].LastResponse
				} else {
					response = sc.executor(command)
					if command.Op != OpQuery {
						sc.clientSessions[command.ClientId] = Session{command.CommandId, response}
					}
				}
				if currentTerm, isLeader := sc.rf.GetState(); isLeader && currentTerm == applyMsg.CommandTerm {
					ch := sc.getNotifyCh(applyMsg.CommandIndex)
					ch <- response
				}
				sc.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected applyMsg %v", applyMsg))
			}
		}
	}
}

func (sc *ShardCtrler) executor(command Command) *OperationResponse {
	var err Err
	var conf Config
	switch command.Op {
	case OpJoin:
		err = sc.Join(command.Servers)
	case OpLeave:
		err = sc.Leave(command.GIDs)
	case OpMove:
		err = sc.Move(command.Shard, command.GID)
	case OpQuery:
		err, conf = sc.Query(command.Num)
	}
	//DPrintf("[Node %v] executor command {Op:%v, ClientId:%v,CommandId:%v}", sc.me, command.Op, command.ClientId, command.CommandId)
	return &OperationResponse{err, conf}
}

func (sc *ShardCtrler) Join(groups map[int][]string) Err {
	configLen := len(sc.configs)
	lastConf := sc.configs[configLen-1]

	newConf := Config{
		Num:    configLen,
		Shards: lastConf.Shards,
		Groups: deepCopy(lastConf.Groups),
	}
	for gid, servers := range groups {
		if _, ok := newConf.Groups[gid]; !ok {
			newConf.Groups[gid] = servers
		}
	}
	// 每个group对应了哪些shards，如Shards[0] = 1, Shards[1] = 4, Shards[2] = 1, ...
	// 则g2s[1] = {0, 2, ...}, g2s[4] = {1, ...}
	// 初始时，Shards[0] = Shards[1] = ... = 0
	// 所以此时g2s[0] = {0, 1, ..., 9}
	g2s := gid2ShardIDs(newConf)
	for {
		dst, src := GetMinGroup(g2s), GetMaxGroup(g2s)
		if src != 0 && len(g2s[src])-len(g2s[dst]) <= 1 {
			break
		}
		g2s[dst] = append(g2s[dst], g2s[src][0])
		g2s[src] = g2s[src][1:]
	}
	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConf.Shards = newShards
	sc.configs = append(sc.configs, newConf)
	return OK
}

func (sc *ShardCtrler) Leave(gids []int) Err {
	configLen := len(sc.configs)
	lastConf := sc.configs[configLen-1]
	newConf := Config{
		Num:    configLen,
		Shards: lastConf.Shards,
		Groups: deepCopy(lastConf.Groups),
	}
	g2s := gid2ShardIDs(newConf)
	unallocatedShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConf.Groups[gid]; ok {
			delete(newConf.Groups, gid)
		}
		if shards, ok := g2s[gid]; ok {
			unallocatedShards = append(unallocatedShards, shards...)
			delete(g2s, gid)
		}
	}
	var newShards [NShards]int
	if len(newConf.Groups) != 0 {
		for _, shard := range unallocatedShards {
			dst := GetMinGroup(g2s)
			g2s[dst] = append(g2s[dst], shard)
		}
		for gid, shards := range g2s {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConf.Shards = newShards
	sc.configs = append(sc.configs, newConf)
	return OK
}

func gid2ShardIDs(conf Config) map[int][]int {
	g2s := make(map[int][]int)
	for gid, _ := range conf.Groups {
		g2s[gid] = make([]int, 0)
	}
	// 初始时, Shards数组元素初始化为0, 即未被分配, 所以gid0代表了invalid gid
	for shardIdx, gid := range conf.Shards {
		g2s[gid] = append(g2s[gid], shardIdx)
	}
	return g2s
}

func GetMinGroup(g2s map[int][]int) int {
	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	index, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(g2s[gid]) < min {
			index, min = gid, len(g2s[gid])
		}
	}
	return index
}

func GetMaxGroup(s2g map[int][]int) int {
	// 如果shard属于gid0，即未分配状态，必须先将这些shards分配完
	if shards, ok := s2g[0]; ok && len(shards) > 0 {
		return 0
	}
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	index, max := -1, -1
	for _, gid := range keys {
		if len(s2g[gid]) > max {
			index, max = gid, len(s2g[gid])
		}
	}
	return index
}

func (sc *ShardCtrler) Move(shard int, GID int) Err {
	configLen := len(sc.configs)
	lastConf := sc.configs[configLen-1]
	newGroup := deepCopy(lastConf.Groups)
	newConf := Config{
		Num:    configLen,
		Shards: lastConf.Shards,
		Groups: newGroup,
	}
	newConf.Shards[shard] = GID
	sc.configs = append(sc.configs, newConf)
	return OK
}

func (sc *ShardCtrler) Query(num int) (Err, Config) {
	configLen := len(sc.configs)
	if num == -1 || num >= configLen {
		return OK, sc.configs[configLen-1]
	} else {
		return OK, sc.configs[num]
	}
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroup := make(map[int][]string)
	for k, v := range groups {
		newGroup[k] = v
	}
	return newGroup
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	applyCh := make(chan raft.ApplyMsg)
	labgob.Register(Command{})
	sc := &ShardCtrler{
		applyCh:        applyCh,
		me:             me,
		dead:           0,
		rf:             raft.Make(servers, me, persister, applyCh),
		configs:        make([]Config, 1),
		clientSessions: make(map[int64]Session),
		notifyCh:       make(map[int]chan *OperationResponse),
	}

	sc.configs[0].Groups = make(map[int][]string)
	sc.configs[0].Num = 0
	sc.configs[0].Shards = [NShards]int{}

	go sc.applier()

	DPrintf("[ShardCtrler %v] has started", sc.me)

	return sc
}

package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	maxRaftState int // snapshot if log grows this big
	dead         int32

	lastApplied int

	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config

	stateMachines  map[int]*dbShard
	clientSessions map[int64]Session
	notifyChs      map[int]chan *OperationResponse
	clerk          *shardctrler.Clerk
}

func (kv *ShardKV) Command(request *OperationRequest, response *OperationResponse) {
	kv.mu.RLock()
	if request.Op != OpGet && kv.isDuplicatedRequest(request.ClientId, request.CommandId) {
		lastResponse := kv.clientSessions[request.ClientId].LastResponse
		response.Err = lastResponse.Err
		response.Value = lastResponse.Value
		kv.mu.RUnlock()
		return
	}
	if !kv.canServe(key2shard(request.Key)) {
		response.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	kv.Executor(newOperationCommand(request), response)
}

func (kv *ShardKV) canServe(shardId int) bool {
	return kv.currentConfig.Shards[shardId] == kv.gid && kv.stateMachines[shardId].Status == Serving || kv.stateMachines[shardId].Status == NeedGC
}

func (kv *ShardKV) isDuplicatedRequest(clientId int64, commandId int64) bool {
	session, ok := kv.clientSessions[clientId]
	return ok && session.LastAppliedCommandId >= commandId
}

func (kv *ShardKV) Executor(command Command, response *OperationResponse) {
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	defer DPrintf("[Node %v][Group %v] processes Command %v with OperationResponse %v", kv.me, kv.gid, command, response)
	kv.mu.Lock()
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()
	select {
	case reply := <-ch:
		response.Value = reply.Value
		response.Err = reply.Err
	case <-time.After(500 * time.Millisecond):
		response.Err = ErrTimeout
	}
	go func() {
		kv.mu.Lock()
		kv.deleteNotifyCh(index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) getNotifyCh(raftLogIndex int) chan *OperationResponse {
	if _, ok := kv.notifyChs[raftLogIndex]; !ok {
		kv.notifyChs[raftLogIndex] = make(chan *OperationResponse, 1)
	}
	return kv.notifyChs[raftLogIndex]
}

func (kv *ShardKV) deleteNotifyCh(raftLogIndex int) {
	delete(kv.notifyChs, raftLogIndex)
}

func (kv *ShardKV) GetShardsData(request *ShardOperationRequest, response *ShardOperationResponse) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	defer DPrintf("[Node %v][Group %v] processes PullTaskRequest %v with response %v", kv.me, kv.gid, request, response)

	if request.ConfigNum > kv.currentConfig.Num {
		response.Err = ErrNotReady
		return
	}

	response.Shards = make(map[int]map[string]string)
	for _, shardId := range request.ShardIds {
		newShard := make(map[string]string)
		for k, v := range kv.stateMachines[shardId].KVs {
			newShard[k] = v
		}
		response.Shards[shardId] = newShard
	}

	response.ClientSessions = make(map[int64]Session)
	for id, s := range kv.clientSessions {
		response.ClientSessions[id] = Session{
			LastAppliedCommandId: s.LastAppliedCommandId,
			LastResponse:         &OperationResponse{s.LastResponse.Err, s.LastResponse.Value},
		}
	}
	response.ConfigNum, response.Err = kv.currentConfig.Num, OK
}

func (kv *ShardKV) DeleteShardsData(request *ShardOperationRequest, response *ShardOperationResponse) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	defer DPrintf("[Node %v][Group %v] processes GCTaskRequest %v with response %v", kv.me, kv.gid, request, response)
	kv.mu.RLock()
	if kv.currentConfig.Num > request.ConfigNum {
		DPrintf("[Node %v][Group %v]'s encounters duplicated shards deletion %v when currentConfig is %v", kv.me, kv.gid, request, kv.currentConfig)
		response.Err = OK
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	var operationResponse OperationResponse
	kv.Executor(newDeleteShardsCommand(request), &operationResponse)
	response.Err = operationResponse.Err
}

//the tester calls Kill() when a ShardKV instance won't
//be needed again. you are not required to do anything
//in Kill(), but it might be convenient to (for example)
//turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	DPrintf("[Node %v][Group %v] has been killed", kv.me, kv.gid)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			DPrintf("[Node %v][Group %v] tries to apply message %v", kv.me, kv.gid, message)
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					DPrintf("[Node %v][Group %v] discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.me, kv.gid, message, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex
				var response *OperationResponse
				command := message.Command.(Command)
				switch command.CommandType {
				case kvCmd:
					operation := command.Data.(OperationRequest)
					response = kv.applyKvStateMachine(&message, &operation)
				case ConfigurationCmd:
					nextConfig := command.Data.(shardctrler.Config)
					response = kv.applyConfiguration(&nextConfig)
				case InsertShardsCmd:
					shardsInfo := command.Data.(ShardOperationResponse)
					response = kv.applyInsertShards(&shardsInfo)
				case DeleteShardsCmd:
					shardsInfo := command.Data.(ShardOperationRequest)
					response = kv.applyDeleteShards(&shardsInfo)
				case EmptyLogCmd:
					response = kv.applyEmptyLog()
				}
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == message.CommandTerm {
					ch := kv.getNotifyCh(message.CommandIndex)
					ch <- response
				}
				if kv.needSnapshot() {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.restoreSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected message %v", message))
			}
		}
	}
}

func (kv *ShardKV) applyKvStateMachine(message *raft.ApplyMsg, request *OperationRequest) *OperationResponse {
	shardId := key2shard(request.Key)
	response := OperationResponse{}
	if kv.canServe(shardId) {
		if request.Op != OpGet && kv.isDuplicatedRequest(request.ClientId, request.CommandId) {
			DPrintf("[Node %v][Group %v] doesn't apply duplicated message %v to stateMachine because LastAppliedCommandId is %v for client %v", kv.me, kv.gid, message, kv.clientSessions[request.ClientId], request.ClientId)
			return kv.clientSessions[request.ClientId].LastResponse
		}
		switch request.Op {
		case OpGet:
			response.Value, response.Err = kv.stateMachines[shardId].Get(request.Key)
		case OpPut:
			response.Err = kv.stateMachines[shardId].Put(request.Key, request.Value)
		case OpAppend:
			response.Err = kv.stateMachines[shardId].Append(request.Key, request.Value)
		}
		if request.Op != OpGet {
			kv.clientSessions[request.ClientId] = Session{request.CommandId, &response}
		}
		return &response
	}
	return &OperationResponse{ErrWrongGroup, ""}
}

func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *OperationResponse {
	if nextConfig.Num == kv.currentConfig.Num+1 {
		DPrintf("[Node %v][Group %v] updates currentConfig from %v to %v", kv.me, kv.gid, kv.currentConfig, nextConfig)
		for i := 0; i < shardctrler.NShards; i++ {
			if nextConfig.Shards[i] == kv.gid && kv.currentConfig.Shards[i] != kv.gid {
				// need pulling other groups' dbShard to me
				gid := kv.currentConfig.Shards[i]
				if gid != 0 {
					kv.stateMachines[i].Status = Pulling
				}
			}
			// other groups need pulling dbShard from me
			if nextConfig.Shards[i] != kv.gid && kv.currentConfig.Shards[i] == kv.gid {
				gid := kv.currentConfig.Shards[i]
				if gid != 0 {
					kv.stateMachines[i].Status = BePulling
				}
			}
		}
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *nextConfig
		return &OperationResponse{OK, ""}
	}
	DPrintf("[Node %v][Group %v] rejects outdated config %v when currentConfig is %v", kv.me, kv.gid, nextConfig, kv.currentConfig)
	return &OperationResponse{ErrOutDated, ""}
}

func (kv *ShardKV) applyInsertShards(pulledShardsInfo *ShardOperationResponse) *OperationResponse {
	if pulledShardsInfo.ConfigNum == kv.currentConfig.Num {
		DPrintf("[Node %v][Group %v] accepts shards insertion %v when currentConfig is %v", kv.me, kv.gid, pulledShardsInfo, kv.currentConfig)
		for shardId, shardData := range pulledShardsInfo.Shards {
			if kv.stateMachines[shardId].Status == Pulling {
				for k, v := range shardData {
					kv.stateMachines[shardId].KVs[k] = v
				}
				// 已经从其他group拿到了完整的dbShard, 此时需要在当前Server上将这些dbShard的状态设置为NeedGC
				// 后台的gcAction协程就能检测到这些需要在远端Server上被清理的dbShard, 记录的lastConfig可以得到
				// 这些dbShard所属的groups, 随后通过rpc调用的方式通知远端group的Server(必须发送给leader)进行清理工作
				kv.stateMachines[shardId].Status = NeedGC
			} else {
				DPrintf("[Node %v][Group %v] encounters duplicated shards insertion %v when currentConfig is %v", kv.me, kv.gid, pulledShardsInfo, kv.currentConfig)
				break
			}
		}
		for clientId, session := range pulledShardsInfo.ClientSessions {
			if myClientSessions, ok := kv.clientSessions[clientId]; !ok ||
				myClientSessions.LastAppliedCommandId < session.LastAppliedCommandId {
				kv.clientSessions[clientId] = session
			}
		}
		return &OperationResponse{OK, ""}
	}
	DPrintf("[Node %v][Group %v] rejects outdated shards insertion %v when currentConfig is %v", kv.me, kv.gid, pulledShardsInfo, kv.currentConfig)
	return &OperationResponse{ErrOutDated, ""}
}

func (kv *ShardKV) applyDeleteShards(deletedShardsInfo *ShardOperationRequest) *OperationResponse {
	if deletedShardsInfo.ConfigNum == kv.currentConfig.Num {
		for _, shardId := range deletedShardsInfo.ShardIds {
			if kv.stateMachines[shardId].Status == NeedGC {
				kv.stateMachines[shardId].Status = Serving
			} else if kv.stateMachines[shardId].Status == BePulling {
				kv.stateMachines[shardId] = &dbShard{Serving, make(map[string]string)}
			} else {
				DPrintf("[Node %v][Group %v] encounters duplicated shards deletion %v when currentConfig is %v", kv.me, kv.gid, deletedShardsInfo, kv.currentConfig)
				break
			}
		}
		return &OperationResponse{OK, ""}
	}
	DPrintf("[Node %v][Group %v]'s encounters duplicated shards deletion %v when currentConfig is %v", kv.me, kv.gid, deletedShardsInfo, kv.currentConfig)
	return &OperationResponse{OK, ""}
}

func (kv *ShardKV) applyEmptyLog() *OperationResponse {
	return &OperationResponse{OK, ""}
}

func (kv *ShardKV) needSnapshot() bool {
	return kv.maxRaftState != -1 && kv.rf.GetRaftStateSize() >= kv.maxRaftState
}

func (kv *ShardKV) takeSnapshot(lastIncludedIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachines)
	e.Encode(kv.clientSessions)
	e.Encode(kv.currentConfig)
	e.Encode(kv.lastConfig)
	kv.rf.Snapshot(lastIncludedIndex, w.Bytes())
}

func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		for i := 0; i < shardctrler.NShards; i++ {
			if _, ok := kv.stateMachines[i]; !ok {
				kv.stateMachines[i] = &dbShard{Serving, map[string]string{}}
			}
		}
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachines map[int]*dbShard
	var clientSessions map[int64]Session
	var currentConfig shardctrler.Config
	var lastConfig shardctrler.Config
	if d.Decode(&stateMachines) != nil || d.Decode(&clientSessions) != nil || d.Decode(&currentConfig) != nil || d.Decode(&lastConfig) != nil {
		DPrintf("[Node %d][Group %d] snapshot failed", kv.me, kv.gid)
	}
	kv.stateMachines, kv.clientSessions, kv.currentConfig, kv.lastConfig = stateMachines, clientSessions, currentConfig, lastConfig
}

func (kv *ShardKV) configureAction() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			allShardsServing := true
			kv.mu.RLock()
			for _, shard := range kv.stateMachines {
				if shard.Status != Serving {
					allShardsServing = false
					break
				}
			}
			currentConfigNum := kv.currentConfig.Num
			kv.mu.RUnlock()
			if allShardsServing {
				nextConfig := kv.clerk.Query(currentConfigNum + 1)
				if nextConfig.Num == currentConfigNum+1 {
					DPrintf("[Node %v][Group %v] fetches latest configuration %v when currentConfigNum is %v", kv.me, kv.gid, nextConfig, currentConfigNum)
					kv.Executor(newConfigurationCommand(&nextConfig), &OperationResponse{})
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) migrationAction() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.RLock()
			gid2ShardIds := kv.getShardsIDsByStatus(Pulling)
			var wg sync.WaitGroup
			for gid, ShardIds := range gid2ShardIds {
				DPrintf("[Node %v][Group %v] starts a PullTask to get shards %v from group %v when config is %v", kv.me, kv.gid, ShardIds, gid, kv.currentConfig)
				wg.Add(1)
				go func(servers []string, configNum int, shardsId []int) {
					defer wg.Done()
					pullTaskRequest := ShardOperationRequest{configNum, shardsId}
					for _, server := range servers {
						var pullTaskResponse ShardOperationResponse
						srv := kv.makeEnd(server)
						if srv.Call("ShardKV.GetShardsData", &pullTaskRequest, &pullTaskResponse) && pullTaskResponse.Err == OK {
							DPrintf("[Node %v][Group %v] gets a PullTaskResponse %v and tries to commit it when currentConfigNum is %v", kv.me, kv.gid, pullTaskResponse, configNum)
							kv.Executor(newInsertShardsCommand(&pullTaskResponse), &OperationResponse{})
						}
					}
				}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, ShardIds)
			}
			kv.mu.RUnlock()
			wg.Wait()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) gcAction() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.RLock()
			gid2ShardIds := kv.getShardsIDsByStatus(NeedGC)
			var wg sync.WaitGroup
			for gid, ShardIds := range gid2ShardIds {
				DPrintf("[Node %v][Group %v] starts a GCTask to delete shards %v in group %v when config is %v", kv.me, kv.gid, ShardIds, gid, kv.currentConfig)
				wg.Add(1)
				go func(servers []string, configNum int, ShardIds []int) {
					defer wg.Done()
					gcTaskRequest := ShardOperationRequest{configNum, ShardIds}
					for _, server := range servers {
						var gcTaskResponse ShardOperationResponse
						srv := kv.makeEnd(server)
						if srv.Call("ShardKV.DeleteShardsData", &gcTaskRequest, &gcTaskResponse) && gcTaskResponse.Err == OK {
							DPrintf("[Node %v][Group %v] deletes shards %v in remote group successfully when currentConfigNum is %v", kv.me, kv.gid, ShardIds, configNum)
							kv.Executor(newDeleteShardsCommand(&gcTaskRequest), &OperationResponse{})
						}
					}
				}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, ShardIds)
			}
			kv.mu.RUnlock()
			wg.Wait()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) emptyLogAction() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			if !kv.rf.HasLogInCurrentTerm() {
				kv.Executor(newEmptyLogCommand(), &OperationResponse{})
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (kv *ShardKV) getShardsIDsByStatus(status ShardStatus) map[int][]int {
	gid2ShardsIds := make(map[int][]int)
	for i, shard := range kv.stateMachines {
		if shard.Status == status {
			gid := kv.lastConfig.Shards[i]
			if gid != 0 {
				if _, ok := gid2ShardsIds[gid]; !ok {
					gid2ShardsIds[gid] = make([]int, 0)
				}
				gid2ShardsIds[gid] = append(gid2ShardsIds[gid], i)
			}
		}
	}
	return gid2ShardsIds
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(OperationRequest{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardOperationRequest{})
	labgob.Register(ShardOperationResponse{})

	applyCh := make(chan raft.ApplyMsg)

	kv := &ShardKV{
		me:             me,
		dead:           0,
		rf:             raft.Make(servers, me, persister, applyCh),
		applyCh:        applyCh,
		makeEnd:        makeEnd,
		gid:            gid,
		clerk:          shardctrler.MakeClerk(ctrlers),
		lastApplied:    0,
		maxRaftState:   maxraftstate,
		lastConfig:     shardctrler.Config{Groups: map[int][]string{}},
		currentConfig:  shardctrler.Config{Groups: map[int][]string{}},
		stateMachines:  map[int]*dbShard{},
		clientSessions: map[int64]Session{},
		notifyChs:      map[int]chan *OperationResponse{},
	}
	kv.restoreSnapshot(persister.ReadSnapshot())
	go kv.applier()

	go kv.configureAction()
	go kv.migrationAction()
	go kv.gcAction()
	go kv.emptyLogAction()

	DPrintf("[Node %v][group %v] start", kv.me, kv.gid)
	return kv
}

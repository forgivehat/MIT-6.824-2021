package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type KVServer struct {
	mu      sync.RWMutex
	dead    int32
	rf      *raft.Raft
	me      int
	applyCh chan raft.ApplyMsg

	maxRaftState int
	lastApplied  int

	db             KvDB
	clientSessions map[int64]Session
	notifyChs      map[int]chan *CommandReply
}

func (kv *KVServer) Command(request *CommandArgs, response *CommandReply) {
	kv.mu.RLock()
	if request.Op != OpGet && kv.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastResponse := kv.clientSessions[request.ClientId].LastReply
		response.Value, response.Err = lastResponse.Value, lastResponse.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	index, _, isLeader := kv.rf.Start(Command{request})
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		response.Value, response.Err = result.Value, result.Err
	case <-time.After(500 * time.Millisecond):
		response.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removedNotifyCh(index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) isDuplicateRequest(clientId int64, requestId int64) bool {
	operationContext, ok := kv.clientSessions[clientId]
	return ok && requestId <= operationContext.MaxAppliedCommandId
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var response *CommandReply
				command := message.Command.(Command)
				if command.Op != OpGet && kv.isDuplicateRequest(command.ClientId, command.CommandId) {
					response = kv.clientSessions[command.ClientId].LastReply
				} else {
					response = kv.applyLogToStateMachine(command)
					if command.Op != OpGet {
						kv.clientSessions[command.ClientId] = Session{command.CommandId, response}
					}
				}

				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyCh(message.CommandIndex)
					ch <- response
				}
				if kv.needSnapshot() {
					// CommandIndex之前，包括CommandIndex的日志应该被丢弃
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid { //下层raft节点收到了leader发送的snapshot
				kv.mu.Lock()
				// CondInstallSnapshot的作用是判断下层raft节点在将applyMsg放入applyCh
				// 到server收到applyMsg期间有没有apply log
				//如果apply了新log, 则这个snapshot可能是过时的，不能应用到本地
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.restoreSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

func (kv *KVServer) needSnapshot() bool {
	return kv.maxRaftState != -1 && kv.rf.GetRaftStateSize() >= kv.maxRaftState
}

func (kv *KVServer) takeSnapshot(lastIncludedIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.clientSessions)
	kv.rf.Snapshot(lastIncludedIndex, w.Bytes())
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db KvDB
	var clientSessions map[int64]Session
	if d.Decode(&db) != nil || d.Decode(&clientSessions) != nil {
		DPrintf("[Server %v] restores snapshot failed", kv.me)
	}
	kv.db, kv.clientSessions = db, clientSessions
}

func (kv *KVServer) getNotifyCh(index int) chan *CommandReply {
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChs[index]
}

func (kv *KVServer) removedNotifyCh(index int) {
	delete(kv.notifyChs, index)
}

func (kv *KVServer) applyLogToStateMachine(command Command) *CommandReply {
	var value string
	var err Err
	switch command.Op {
	case OpPut:
		err = kv.db.Put(command.Key, command.Value)
	case OpAppend:
		err = kv.db.Append(command.Key, command.Value)
	case OpGet:
		value, err = kv.db.Get(command.Key)
	}
	return &CommandReply{err, value}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		maxRaftState:   maxraftstate,
		applyCh:        applyCh,
		dead:           0,
		lastApplied:    0,
		me:             me,
		rf:             raft.Make(servers, me, persister, applyCh),
		db:             KvDB{map[string]string{}},
		clientSessions: make(map[int64]Session),
		notifyChs:      make(map[int]chan *CommandReply),
	}
	kv.restoreSnapshot(persister.ReadSnapshot())
	go kv.applier()

	DPrintf("[Node %v] has started", kv.me)
	return kv
}

package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log Entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new Entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"sort"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State
	currentTerm int
	votedFor    int
	voteCnt     int
	logs        []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	replicatorCond []*sync.Cond
	applyCond      *sync.Cond
	applyCh        chan ApplyMsg

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	return data
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []Entry
	var currentTerm int
	var votedFor int
	if d.Decode(&logs) != nil || d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil {
		DPrintf("cannot read persist data")
	} else {
		rf.logs = logs
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastApplied = rf.logs[0].Index
		rf.commitIndex = rf.logs[0].Index
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("")
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}
	if lastIncludedIndex > rf.getLastLog().Index {
		rf.logs = make([]Entry, 1)
	} else {
		tmp := make([]Entry, len(rf.logs[lastIncludedIndex-rf.getFirstLog().Index:]))
		copy(tmp, rf.logs[lastIncludedIndex-rf.getFirstLog().Index:])
		rf.logs = tmp
		rf.logs[0].Command = nil
	}
	//
	rf.logs[0].Term = lastIncludedTerm
	rf.logs[0].Index = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("[Node %v]'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludedTerm, lastIncludedIndex)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index
	if snapshotIndex >= index {
		DPrintf("[Node %v] rejects snapshotting with snapshotIndex %v as current snapshotIndex is %v in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	newLogLen := len(rf.logs[index-rf.getFirstLog().Index:])
	tmp := make([]Entry, newLogLen)
	copy(tmp, rf.logs[index-rf.getFirstLog().Index:])
	rf.logs = tmp
	rf.logs[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("[Node %v]'s state is ", rf.me)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomElectionTimeout())

	if rf.commitIndex >= args.LastIncludedIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) genInstallSnapshotArgs() *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.getFirstLog().Index,
		LastIncludedTerm:  rf.getFirstLog().Term,
		Data:              rf.persister.ReadSnapshot(),
	}
}

func (rf *Raft) handleInstallSnapshotReply(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.state == Leader && rf.currentTerm == args.Term {
		if reply.Term > rf.currentTerm {
			rf.ChangeState(Follower)
			rf.reInitFollowTimer()
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.persist()
		} else {
			rf.matchIndex[peer] = args.LastIncludedIndex
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	args := rf.genRequestVoteArgs()
	DPrintf("[Node %v] starts election with RequestVoteArgs: %v", rf.me, args)
	rf.votedFor = rf.me
	rf.voteCnt = 1
	rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// 异步发起投票
		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) {
				// startElection在循环结束后返回，ticker协程正常释放锁，这里不会死锁
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.handleRequestVoteReply(peer, args, reply)
			}
		}(peer)
	}
}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// defer print，如果参数是值，如rf.state,调用这条语句那一刻就固定了，不会改变
	// 如果参数是指针，如reply, 值会变化
	defer DPrintf("[Node %v]'s state is {state %v,term %v,commitIndex %v,lastApplied %v,"+
		"firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied,
		rf.getFirstLog(), rf.getLastLog(), args, reply)
	// 显然是个过时的投票请求
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// 当前任期已经给其他候选者（或者给自己）投票了，不能再投票
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 一个Term只能给一个peer投票
	// 收到一个更高任期的RequestVoteArgs RPC，变为Follower, 采用这个新任期，清空votedFor, 从而重新获得投票权
	// 注意: 如果你当前不是follower, 需要重置选举计时器; 如果是, 不要重置选举计时器!
	// 因为它可能被其他候选者无限打断, 候选者总是在任期上占优！
	if args.Term > rf.currentTerm {
		state := rf.state
		rf.ChangeState(Follower)
		if state != Follower {
			rf.electionTimer.Reset(RandomElectionTimeout())
		}
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if args.LastLogTerm < rf.getLastLog().Term || (args.LastLogTerm == rf.getLastLog().Term && args.LastLogIndex < rf.getLastLog().Index) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(RandomElectionTimeout())
	reply.Term = rf.currentTerm
	reply.VoteGranted = true

}

// 候选者处理投票者的reply
func (rf *Raft) handleRequestVoteReply(peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("[Node %v] receives RequestVoteReply %v from [Node %v] after sending RequestVoteArgs %v in term %v", rf.me, args, peer, reply, rf.currentTerm)
	//一旦这个节点不再是candidate或者term增加了，后续传来的投票就过期了，丢弃即可
	if rf.state == Candidate && rf.currentTerm == args.Term {
		if reply.Term > rf.currentTerm {
			DPrintf("[Node %v] finds a new leader [Node %v] with term %v and steps down in term %v", rf.me, peer, reply.Term, rf.currentTerm)
			rf.ChangeState(Follower)
			rf.electionTimer.Reset(RandomElectionTimeout())
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.voteCnt = 0
			rf.persist()
			return
		}
		if reply.VoteGranted {
			rf.voteCnt += 1
			if rf.voteCnt > len(rf.peers)/2 {
				DPrintf("[Node %v] receives majority votes in term %v", rf.me, rf.currentTerm)
				rf.ChangeState(Leader)
				rf.reInitLeaderState()
				rf.voteCnt = 0 // 不知道有没有必要清0
				// 当选leader后立即发送心跳信号
				rf.BroadcastHeartbeat(true)
			}
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) BroadcastHeartbeat(isHeartbeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartbeat {
			go rf.doReplicate(peer)
		} else {
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) doReplicate(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	// 对于一个新当选的leader，rf.nextIndex[peer]初始化为rf.getLastLog().Index + 1
	// 此时preLogIndex就是leader最后一条log的index
	preLogIndex := rf.nextIndex[peer] - 1
	if preLogIndex >= rf.getFirstLog().Index {
		args := rf.genAppendEntriesArgs(preLogIndex)
		rf.mu.RUnlock()
		reply := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, args, reply) {
			rf.mu.Lock()
			rf.handleAppendEntriesReply(peer, args, reply)
			rf.mu.Unlock()
		}
	} else { // need snapshot
		args := rf.genInstallSnapshotArgs()
		rf.mu.RUnlock()
		reply := new(InstallSnapshotReply)
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.mu.Lock()
			rf.handleInstallSnapshotReply(peer, args, reply)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) genAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {
	firstIndex := rf.getFirstLog().Index
	entries := make([]Entry, len(rf.logs[prevLogIndex+1-firstIndex:]))
	// 深拷贝，snapshot时可能会把entry删掉，导致发送的RPC里entries引用不到
	copy(entries, rf.logs[prevLogIndex+1-firstIndex:])
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PreLogIndex:  prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex-firstIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	defer DPrintf("[Node %v]'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} "+
		"before processing AppendEntriesArgs %v and reply AppendEntriesReply %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.ChangeState(Follower)
	rf.reInitFollowTimer()

	if args.PreLogIndex < rf.getFirstLog().Index {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[Node %v] receives unexpected AppendEntriesRequest %v from [Node %v] because prevLogIndex %v < firstLogIndex %v", rf.me, args, args.LeaderId, args.PreLogIndex, rf.getFirstLog().Index)
		return
	}

	//	preLogIndex > rf.getLastLog().Index说明当前节点缺少日志
	//	rf.logs[preLogIndex - rf.getFirstLog().Index].Term != term说明存在日志冲突
	lastLogIndex := rf.getLastLog().Index
	firstLogIndex := rf.getFirstLog().Index
	if args.PreLogIndex > lastLogIndex || rf.logs[args.PreLogIndex-firstLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		if args.PreLogIndex > lastLogIndex {
			reply.ConflictIndex = lastLogIndex + 1
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = rf.logs[args.PreLogIndex-firstLogIndex].Term
			i := args.PreLogIndex - 1
			for i >= firstLogIndex && rf.logs[i-firstLogIndex].Term == reply.ConflictTerm { // 每次只判断一个任期内的日志
				i--
			}
			reply.ConflictIndex = i
		}
		return
	}

	for i, entry := range args.Entries {
		if entry.Index > lastLogIndex || rf.logs[entry.Index-firstLogIndex].Term != entry.Term {
			rf.logs = append(rf.logs[:entry.Index-firstLogIndex], args.Entries[i:]...)
			break
		}
	}
	rf.followerCommit(args.LeaderCommit)
	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) handleAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 确保当前仍是Leader状态并且Term没有改变的情况下才处理reply RPC
	if rf.state == Leader && rf.currentTerm == args.Term {
		rf.persist()
		if reply.Term > rf.currentTerm {
			rf.ChangeState(Follower)
			rf.reInitFollowTimer()
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			return
		}
		if reply.Success {
			rf.matchIndex[peer] = args.PreLogIndex + len(args.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.leaderCommit()
		} else {
			rf.nextIndex[peer] = reply.ConflictIndex
			if reply.ConflictTerm != -1 { // reply.ConflictTerm等于-1时表示Follower缺少日志
				firstLogIndex := rf.getFirstLog().Index
				// 快速定位到冲突任期里leader拥有的最后一条entry
				for i := args.PreLogIndex; i >= firstLogIndex; i-- {
					if rf.logs[i-firstLogIndex].Term == reply.ConflictTerm {
						rf.nextIndex[peer] = i + 1
						break
					}
				}
			}
		}
	}
	DPrintf("[Node %v]'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} "+
		"after handling AppendEntriesReply %v from [Node %v] for AppendEntriesArgs %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), reply, peer, args)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) followerCommit(leaderCommit int) {
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = Min(leaderCommit, rf.getLastLog().Index)
		rf.applyCond.Signal()
	}
}

func (rf *Raft) leaderCommit() {
	//根据matchIndex，判断出那些log已经被大多数peer记录了
	n := len(rf.matchIndex)
	tmp := make([]int, n)
	copy(tmp, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(tmp)))
	newCommitIndex := tmp[n/2]

	if newCommitIndex > rf.commitIndex {
		// leader只能提交当前任期下的日志
		if newCommitIndex <= rf.getLastLog().Index && rf.currentTerm == rf.logs[newCommitIndex-rf.getFirstLog().Index].Term {
			DPrintf("[Node %d] advance commitIndex from %d to %d with matchIndex %v in term %d",
				rf.me, rf.commitIndex, newCommitIndex, rf.matchIndex, rf.currentTerm)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		} else {
			DPrintf("[Node %d] can not advance commitIndex from %d because the term of newCommitIndex %d is not equal to currentTerm %d",
				rf.me, rf.commitIndex, newCommitIndex, rf.currentTerm)
		}
	}

}

func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) ChangeState(state State) {
	if rf.state == state {
		return
	}
	DPrintf("[Node %d] changes state from %v to %v in term %d", rf.me, rf.state, state, rf.currentTerm)
	rf.state = state
}

func (rf *Raft) reInitFollowTimer() {
	rf.heartbeatTimer.Stop()
	rf.electionTimer.Reset(RandomElectionTimeout())
}

func (rf *Raft) reInitLeaderState() {
	lastLog := rf.getLastLog()
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastLog.Index + 1
	}
	rf.heartbeatTimer.Reset(FixedHeartbeatTimeout())
	rf.electionTimer.Stop()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		lastLog := rf.getLastLog()
		newLog := Entry{lastLog.Index + 1, rf.currentTerm, command}
		rf.logs = append(rf.logs, newLog)
		rf.matchIndex[rf.me] = newLog.Index
		rf.nextIndex[rf.me] = newLog.Index + 1
		rf.persist()
		DPrintf("[Node %v] receives a new command[%v] to replicate in term %v", rf.me, newLog, rf.currentTerm)
		rf.BroadcastHeartbeat(false)
		return newLog.Index, newLog.Term, true
	}
	return -1, -1, false
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		lastApplied := rf.lastApplied
		firstLogIndex := rf.getFirstLog().Index
		commitIndex := rf.commitIndex
		applyEntries := make([]Entry, commitIndex-lastApplied)
		copy(applyEntries, rf.logs[lastApplied-firstLogIndex+1:commitIndex-firstLogIndex+1])
		rf.mu.Unlock()
		for _, entry := range applyEntries {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			rf.applyCh <- applyMsg
		}
		rf.mu.Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			// 只有leader才能发送心跳
			if rf.state == Leader {
				rf.BroadcastHeartbeat(true)
				// 发送心跳后重置心跳计时器
				rf.heartbeatTimer.Reset(FixedHeartbeatTimeout())
			}
			rf.mu.Unlock()
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			DPrintf("[Node %d] election elapsed, start election", rf.me)
			rf.ChangeState(Candidate)
			rf.currentTerm++
			rf.startElection()
			// startElection异步发起投票后返回，重置选举超时时间
			rf.electionTimer.Reset(RandomElectionTimeout())

			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.doReplicate(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		state:          Follower,
		currentTerm:    0,
		votedFor:       -1,
		voteCnt:        0,
		logs:           make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(FixedHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomElectionTimeout()),
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}

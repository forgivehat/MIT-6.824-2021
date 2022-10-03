package raft

import "fmt"

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (args RequestVoteArgs) String() string {
	return fmt.Sprintf("{Term:%v,CandidateId:%v,LastLogIndex:%v,LastLogTerm:%v}", args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (reply RequestVoteReply) String() string {
	return fmt.Sprintf("{Term:%v,VoteGranted:%v}", reply.Term, reply.VoteGranted)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,PreLogIndex:%v,PrevLogTerm:%v,LeaderCommit:%v,Entries:%v}", args.Term, args.LeaderId, args.PreLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (response AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term:%v,Success:%v}", response.Term, response.Success)
}

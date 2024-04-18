package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (id, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	// "log/slog"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/"
	"6.5840/labrpc"
)

type Role int

const (
	RoleLeader Role = iota
	RoleFollower
	RoleCandidate
	// NOTE(gukele): 类似两阶段选举的概念，为了解决搅屎棍term不断增长的问题，在成为candidate之前，会有一个pre_candidate的状态RoleCandidate，收到集群大多数节点回复后才会变成candidate
)

const (
	// 声明在函数外部，首字母小写则包内可见，大写则所有包可见
	heartBeatInterval            = 50 * time.Millisecond
	electionTimeoutBase          = 4 * heartBeatInterval
	electionTimeoutRandIncrement = 150

	RPCTimeout = 100 * time.Millisecond

	MaxRetries = 10; // 连续append entries rpc无回应时，认为follower crash，暂停日志发送，直到收到该日志心跳的reply才继续尝试日志添加
)

type LogEntry struct {
	Id_      int         // Log的固定索引号，在所有的服务器都是相同的，不会被改变.类似TCP的序列号，也是一个累计确认。(注意并不是在数组中的下标，只是可能刚开始是一样的，在快照后就不一样了，感觉改名字叫LogID更好)
	Term_    int         // Leader接收到该log时的任期
	Command_ interface{} // 空接口，类似于std::any吧，go语言中几乎所有数据结构最底层都是interface
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// TODO(gukele): 使用共享锁来优化
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	cur_term_  int        // 服务器知道的最近任期，当服务器启动时初始化为0，单调递增。
	voted_for_ int        // 当前任期中，该服务器给投过票的candidateId，如果没有则为null。持久化为了防止一个server在同一term投出多票，假如重启没有持久化这个，就可能投出多票
	logs_      []LogEntry // 日志条目；每一条包含了状态机指令以及该条目被leader收到时的任期号（第一条为哨兵，要么是0，要么就是当前快照最后一条日志）

	// Volatile state on all servers
	// 当一条日志(也代表之前的都完成了)被复制到大多数节点上时就是commit，然后leader会通知所有follower多少日志commit了,然后就可以apply。所以通常应该是[last_applied_, commit_id_]一个窗口一样
	commit_id_    int // 已知被提交(大多数复制)的最高日志条目索引号，一开始是0，单调递增！！,(leader根据大多数复制来设定,而follower是max(commit_id_, min(leader_commit_id, 本次成功添加的最后一个id))),
	last_applied_ int // 应用到状态机(应用到上层存储中的)的最高日志条目索引号，一开始为0，单调递增

	// Volatile state on leader
	next_id_  []int // 针对所有的服务器，内容是需要发送给每个服务器下一条日志条目索引号(初始化为leader的最高索引号+1)nextIndex为乐观估计，指代 leader 保留的对应 follower 的下一个需要传输的日志条目，
	match_id_ []int // 针对所有的服务器，内容是已经复制到每个服务器上的最高日志条目索引号，初始化为0，单调递增！！！

	role_     Role
	apply_ch_ chan ApplyMsg // 用来通知上层状态机执行cmd

	timeout_ time.Duration
	ticker_  *time.Ticker // 对于leader来说就是心跳计时器，那么对于follower来说就是选举计时器

	replicator_cv_ []*sync.Cond // 用于唤醒所有的异步日志发送线程
	applier_cv_    sync.Cond    // 用于唤醒applier应用日志到状态机
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var is_leader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.cur_term_
	is_leader = (rf.role_ == RoleLeader)

	return term, is_leader
}

func (rf *Raft) SetTerm(term int) {

	if rf.cur_term_ >= term {
		return
	}

	Debug(dTerm, "S%v Term update %v -> %v", rf.me, rf.cur_term_, term)
	rf.cur_term_ = term
	rf.voted_for_ = -1
	rf.persistState()
}

func (rf *Raft) SetCommitId(commit_id int) bool {
	if commit_id <= rf.commit_id_ {
		return false
	}

	old_commit_id := rf.commit_id_
	rf.commit_id_ = commit_id
	if rf.role_ == RoleLeader {
		Debug(dCommit, "S%v Updating LCI %v -> %v", rf.me, old_commit_id, rf.commit_id_)
	} else {
		Debug(dCommit, "S%v Updating CI:%v -> %v", rf.me, old_commit_id, rf.commit_id_)
	}
	return true
}

func (rf *Raft) GetRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.role_
}

func (rf *Raft) SetRoleToFollower() {
	rf.role_ = RoleFollower
}

func (rf *Raft) SetRoleAndTicker(role Role) {
	old_role := rf.role_
	rf.role_ = role
	switch role {
	case RoleLeader:
		if old_role != role {
			Debug(dLeader, "S%v Converting to leader at T:%v", rf.me, rf.cur_term_)

			// 变成leader后初始化一下next 和 match
			for peer := range rf.peers {
				if peer == rf.me {
					rf.next_id_[peer] = rf.GetLastLogId() + 1
					rf.match_id_[peer] = rf.GetLastLogId()
					continue
				}
				rf.next_id_[peer] = rf.GetLastLogId() + 1
				rf.match_id_[peer] = 0
			}

			rf.ResetHeartBeatInterval()

			// 马上尝试发送一波日志添加，来代替心跳，还能进行next id回退、match id更新、leader commit id更新等。
			for peer := range rf.peers {
				if peer != rf.me {
					rf.replicator_cv_[peer].Signal()
				}
			}
		}
	case RoleCandidate:
		Debug(dTerm, "S%v Converting to candidate, begin election T:%v LLT:%v LLI:%v", rf.me, rf.cur_term_, rf.GetLastLogTerm(), rf.GetLastLogId())
		// rf.ResetTimeout()
	case RoleFollower:
		if old_role != RoleFollower {
			Debug(dInfo, "S%v %v -> %v", rf.me, old_role.String(), role.String())
		}
		rf.ResetTimeout()
	}
}

func (rf *Raft) GetVotedFor() int {

	return rf.voted_for_
}

func (rf *Raft) SetVotedFor(voted_for int) {

	// make sure equal or voted_for_ == -1 ???
	if rf.voted_for_ != voted_for {
		rf.voted_for_ = voted_for
		Debug(dVote, "S%v Vote S%v", rf.me, voted_for)

		rf.persistState()
	}
}

func (rf *Raft) PeersSize() int {

	return len(rf.peers)
}

func (rf *Raft) LogBack() LogEntry {
	return rf.logs_[len(rf.logs_)-1]
}

// 超时时间
func (rf *Raft) ResetTimeout() {

	rf.timeout_ = GetRandomElectionTimeout()
	rf.ticker_.Reset(rf.timeout_)
	// log.Printf("We reset %v ticker %v \n", rf.me, rf.timeout_)
}

func (rf *Raft) ResetTimeoutWith(timeout time.Duration) {

	rf.timeout_ = timeout
	rf.ticker_.Reset(rf.timeout_)
}

// 心跳间隔时间
func (rf *Raft) ResetHeartBeatInterval() {

	rf.timeout_ = heartBeatInterval
	rf.ticker_.Reset(rf.timeout_)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log.
// if this server isn't the leader, returns false.
// otherwise start the agreement and return immediately.
//
// there is no guarantee that this command will ever be committed to the Raft log,
// since the leader may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the id that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 这个函数只是尝试在一个可能是leader的节点上去尝试一个command,具体后面是否会成功还要通过channel来知道Star
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	id := -1
	term := -1
	is_leader := false

	// Your code here (2B).
	rf.mu.Lock()
	// defer rf.mu.Unlock()

	if rf.role_ != RoleLeader {
		rf.mu.Unlock()
		return id, term, is_leader
	}

	log_entry := rf.AppendNewEntry(command)
	Debug(dClient, "S%v start a command:%v at T:%v LLI:%v", rf.me, command, rf.cur_term_, rf.GetLastLogId())
	rf.persistState()

	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {
			rf.replicator_cv_[i].Signal()
		}
	}

	id = log_entry.Id_
	term = log_entry.Term_
	is_leader = true

	return id, term, is_leader
}

func (rf *Raft) AppendNewEntry(command interface{}) LogEntry {
	log_entry := LogEntry{
		Term_:    rf.cur_term_,
		Id_:      rf.GetLastLogId() + 1,
		Command_: command,
	}

	rf.logs_ = append(rf.logs_, log_entry)
	rf.match_id_[rf.me] += 1
	rf.next_id_[rf.me] += 1

	if rf.next_id_[rf.me] != rf.match_id_[rf.me]+1 {
		Debug(dError, "Next id=%v is not match match id=%v", rf.next_id_[rf.me], rf.next_id_[rf.me])
	}

	return log_entry
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

func (rf *Raft) ticker() {
	Debug(dInfo, "S%v Start T:%v LLI:%v", rf.me, rf.cur_term_, rf.GetLastLogId())

	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.ticker_.C:
			rf.mu.Lock()
			if rf.role_ == RoleFollower || rf.role_ == RoleCandidate {
				// rf.ResetTicker()
				rf.mu.Unlock()
				rf.StartElection()
			} else if rf.role_ == RoleLeader {
				rf.mu.Unlock()

				rf.BroadcastHeartBeat()
			}
			// rf.mu.Unlock()
		}

	}
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	// rf := &Raft{}
	// rf.peers = peers
	// rf.persister = persister
	// rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		cur_term_:  0,
		voted_for_: -1,
		logs_:      make([]LogEntry, 1), // 哨兵

		commit_id_:    0,
		last_applied_: 0,

		match_id_: make([]int, len(peers)),
		next_id_:  make([]int, len(peers)),

		role_:     RoleFollower,
		apply_ch_: applyCh,

		timeout_: GetRandomElectionTimeout(),

		replicator_cv_: make([]*sync.Cond, len(peers)),
		applier_cv_:    *sync.NewCond(&sync.Mutex{}),
	}

	rf.ticker_ = time.NewTicker(rf.timeout_)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for peer := range rf.peers {
		rf.replicator_cv_[peer] = sync.NewCond(&sync.Mutex{})
		go rf.Replicator(peer)
	}

	go rf.applier()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

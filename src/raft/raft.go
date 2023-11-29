package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
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
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	RoleLeader = iota
	RoleFollower
	// NOTE(gukele): 类似两阶段选举的概念，为了解决搅屎棍term不断增长的问题，
	// 在成为candidate之前，会有一个pre_candidate的状态RoleCandidate，收到集群大多数节点回复后才会变成candidate
	RoleCandidate
)

type LogEntry struct {
	Term_    int         // Leader接收到该log时的任期
	Idx_     int         // Log的固定下标，在所有的服务器都是相同的，不会被改变.类似TCP的序列号，也是一个累计确认，
	Command_ interface{} // 空接口，类似于std::any吧，go语言中几乎所有数据结构最底层都是interface
}

const (
	// 声明在函数外部，首字母小写则包内可见，大写则所有包可见
	heartBeatTimeout             = 100 * time.Millisecond
	electionTimeoutBase          = 200 * time.Millisecond
	electionTimeoutRandIncrement = 150
)

func GetRandomElectionTimeout() time.Duration {
	return electionTimeoutBase + time.Duration(rand.Intn(electionTimeoutRandIncrement))*time.Millisecond
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	cur_term_  int        // 服务器知道的最近任期，当服务器启动时初始化为0，单调递增
	voted_for_ int        // 当前任期中，该服务器给投过票的candidateId，如果没有则为null
	logs_      []LogEntry // 日志条目；每一条包含了状态机指令以及该条目被leader收到时的任期号]

	// Volatile state on all servers
	// 当一条日志(也代表之前的都完成了)被复制到大多数节点上时就是commit，然后leader会通知所有follower多少日志commit了,然后就可以apply。所以通常应该是[last_applied_, commit_idx_]一个窗口一样
	commit_idx_   int // 已知被提交(大多数复制)的最高日志条目索引号，一开始是0，单调递增,(leader根据大多数复制来设定,而follower是min(leader_commit_idx, index of last new entry))
	last_applied_ int // 应用到状态机(应用到上层存储中的)的最高日志条目索引号，一开始为0，单调递增

	// Volatile state on leader
	next_idx_  []int // 针对所有的服务器，内容是需要发送给每个服务器下一条日志条目索引号(初始化为leader的最高索引号+1)nextIndex为乐观估计，指代 leader 保留的对应 follower 的下一个需要传输的日志条目，
	match_idx_ []int // 针对所有的服务器，内容是已经复制到每个服务器上的最高日志条目号，初始化为0，单调递增

	// TODO(gukele)：role和term有比较设置成原子变量吗，在发送日志添加和心跳的时候都判断是否是leader，避免中途role改变不再是leader后还继续发送心跳或日志添加
	role_     Role
	apply_ch_ chan ApplyMsg // 用来通知上层状态机执行cmd

	timeout_ time.Duration
	ticker_  *time.Ticker // 对于leader来说就是心跳计时器，那么对于follower来说就是选举计时器

	replicator_cv_ []*sync.Cond // 用于唤醒所有的异步日志发送线程
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

	rf.cur_term_ = term
	rf.persist()
}

func (rf *Raft) GetRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.role_
}

func (rf *Raft) SetRole(role Role) {
	if role == RoleLeader && rf.role_ != RoleLeader {
		rf.ResetHeartBeat()
	}
	rf.role_ = role
}

func (rf *Raft) GetVotedFor() int {

	return rf.voted_for_
}

func (rf *Raft) SetVotedFor(voted_for int) {

	// make sure equal or voted_for_ == -1 ???
	rf.voted_for_ = voted_for
	rf.persist()
}

func (rf *Raft) PeersSize() int {

	return len(rf.peers)
}

func (rf *Raft) LogBack() LogEntry {
	return rf.logs_[len(rf.logs_)-1]
}

func (rf *Raft) ResetTicker() {

	rf.timeout_ = GetRandomElectionTimeout()
	rf.ticker_.Reset(rf.timeout_)
	// log.Printf("We reset %v ticker %v \n", rf.me, rf.timeout_)
}

func (rf *Raft) ResetTickerWith(timeout time.Duration) {

	rf.timeout_ = timeout
	rf.ticker_.Reset(rf.timeout_)
}

func (rf *Raft) ResetHeartBeat() {

	rf.timeout_ = heartBeatTimeout
	rf.ticker_.Reset(rf.timeout_)
}

func (rf *Raft) GetLastTermAndIndex() (int, int) {

	return rf.GetLastLogTerm(), rf.GetLastLogIndex()
}

func (rf *Raft) GetLastLogIndex() int {

	var last_index = len(rf.logs_) - 1
	return last_index
}

func (rf *Raft) GetLastLogTerm() int {

	var last_term = 0

	if rf.GetLastLogIndex() >= 0 {
		last_term = rf.logs_[rf.GetLastLogIndex()].Term_
	}

	return last_term
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
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 这个函数只是尝试在一个可能是leader的节点上去尝试一个command,具体后面是否会成功还要通过channel来知道Star
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	// isLeader := true
	is_leader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role_ != RoleLeader {
		return index, term, is_leader
	}

	slog.Info("Leader start a command", "leader", rf.me, "command", command, "term", rf.cur_term_)
	log_entry := rf.AppendNewEntry(command)

	for i := range rf.peers {
		if i != rf.me {
			rf.replicator_cv_[i].Signal()
		}
	}

	index = log_entry.Idx_
	term = log_entry.Term_
	is_leader = true

	return index, term, is_leader
}

func (rf *Raft) AppendNewEntry(command interface{}) LogEntry {
	log_entry := LogEntry{
		Term_:    rf.cur_term_,
		Idx_:     len(rf.logs_),
		Command_: command,
	}

	rf.logs_ = append(rf.logs_, log_entry)
	rf.match_idx_[rf.me] += 1
	rf.next_idx_[rf.me] += 1

	if rf.next_idx_[rf.me] != rf.match_idx_[rf.me]+1 {
		// slog.Error("Next index is not match match_index", "next_idx", rf.next_idx_[rf.me], "match_idx", rf.match_idx_[rf.me])
		Debug(dLog, "Next index=%v is not match match index=%v", rf.next_idx_[rf.me], rf.next_idx_[rf.me])
	}
	rf.persist()

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
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.ticker_.C:

			// NOTE: go的switch相当于每个case自带break
			// switch rf.GetRole() {
			// case RoleFollower:
			// 	rf.mu.Lock()
			// 	rf.SetRole(RoleCandidate)
			// 	rf.mu.Unlock()
			// 	fallthrough
			// case RoleCandidate: // 此时应该是对应着选举失败？
			// 	rf.mu.Lock()
			// 	rf.ResetTicker()
			// 	rf.StartElection()
			// 	rf.mu.Unlock()
			// case RoleLeader:
			// 	rf.mu.Lock()
			// 	rf.ResetHeartBeat()
			// 	// log.Printf("%v periodically broadcast heart beat\n", rf.me)
			// 	rf.BroadcastHeartBeat()
			// 	rf.mu.Unlock()
			// }
			rf.mu.Lock()
			if rf.role_ == RoleFollower || rf.role_ == RoleCandidate {
				rf.SetRole(RoleCandidate)
				rf.ResetTicker()
				rf.mu.Unlock()
				rf.StartElection()
			} else if rf.role_ == RoleLeader {
				rf.ResetHeartBeat()
				rf.mu.Unlock()

				rf.BroadcastHeartBeat(false)
			}
			// rf.mu.Unlock()
		}

		// // pause for a random amount of time between 50 and 350
		// // milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
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

		commit_idx_:   0,
		last_applied_: 0,

		match_idx_: make([]int, len(peers)),
		next_idx_:  make([]int, len(peers)),

		role_:     RoleFollower,
		apply_ch_: applyCh,

		timeout_: GetRandomElectionTimeout(),

		replicator_cv_: make([]*sync.Cond, len(peers)),
	}

	rf.ticker_ = time.NewTicker(rf.timeout_)

	for i := range rf.peers {
		rf.next_idx_[i] = len(rf.logs_)
		rf.match_idx_[i] = 0
		if i != rf.me {
			rf.replicator_cv_[i] = sync.NewCond(&sync.Mutex{})
			go rf.Replicator(i)
		}
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

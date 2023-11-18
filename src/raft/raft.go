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
	Command_ interface{} //
}

const (
	// 声明在函数外部，首字母小写则包内可见，大写则所有包可见
	heartBeatTimeout    = 100 * time.Millisecond
	electionTimeoutBase = 200 * time.Millisecond
	electionTimeoutRandIncrement = 150
)

func GetRandomElectionTimeout() time.Duration {
	return electionTimeoutBase + time.Duration(rand.Intn(electionTimeoutRandIncrement)) * time.Millisecond
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

	// Persistent state
	cur_term_  int        // 服务器知道的最近任期，当服务器启动时初始化为0，单调递增
	voted_for_ int        // 当前任期中，该服务器给投过票的candidateId，如果没有则为null
	logs_      []LogEntry // 日志条目；每一条包含了状态机指令以及该条目被leader收到时的任期号]

	// Volatile state
	commit_idx_   int // 已知被提交的最高日志条目索引号，一开始是0，单调递增
	last_applied_ int // 应用到状态机的最高日志条目索引号，一开始为0，单调递增

	// Leader volatile state
	next_idx_  []int // 针对所有的服务器，内容是需要发送给每个服务器下一条日志条目索引号(初始化为leader的最高索引号+1)
	match_idx_ []int // 针对所有的服务器，内容是已知要复制到每个服务器上的最高日志条目号，初始化为0，单调递增

	role_    Role
	timeout_ time.Duration
	ticker_  *time.Ticker
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
}

func (rf *Raft) GetRole() Role {
	rf.mu.Lock()
	rf.mu.Unlock()

	return rf.role_
}

func (rf *Raft) SetRole(role Role) {

	rf.role_ = role
}

func (rf *Raft) GetVotedFor() int {

	return rf.voted_for_
}

func (rf *Raft) SetVotedFor(voted_for int) {

	// make sure equal or voted_for_ == -1 ???
	rf.voted_for_ = voted_for
}


func (rf *Raft) GetSize() int {

	return len(rf.peers)
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
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

			switch rf.GetRole() {
			case RoleFollower:
				rf.mu.Lock()
				rf.SetRole(RoleCandidate)
				rf.mu.Unlock()

			case RoleCandidate: // 此时应该是对应着选举失败？
				rf.mu.Lock()
				rf.ResetTicker()
				rf.StartElection()
				rf.mu.Unlock()
				fallthrough
			case RoleLeader:
				rf.mu.Lock()
				rf.ResetHeartBeat()
				// log.Printf("%v periodically broadcast heart beat\n", rf.me)
				rf.BroadcastHeartBeat()
				rf.mu.Unlock()
			}
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
		peers: peers,
		persister: persister,
		me: me,

		cur_term_: 0,
		voted_for_: -1,
		logs_: make([]LogEntry, 1),

		commit_idx_: 0,
		last_applied_: 0,

		match_idx_: make([]int, 0),
		next_idx_: make([]int, 0),

		role_: RoleFollower,
		timeout_: GetRandomElectionTimeout(),
	}

	rf.ticker_ = time.NewTicker(rf.timeout_)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

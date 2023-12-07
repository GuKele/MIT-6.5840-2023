package raft

import (
	"math/rand"
	"sort"
	"time"
)

const (
	// 声明在函数外部，首字母小写则包内可见，大写则所有包可见
	heartBeatTimeout             = 100 * time.Millisecond
	electionTimeoutBase          = 200 * time.Millisecond
	electionTimeoutRandIncrement = 150
)

func GetRandomElectionTimeout() time.Duration {
	return electionTimeoutBase + time.Duration(rand.Intn(electionTimeoutRandIncrement))*time.Millisecond
}

func (rf *Raft) GetLastTermAndId() (int, int) {

	return rf.GetLastLogTerm(), rf.GetLastLogId()
}

func (rf *Raft) GetLastLogId() int {
	return rf.LogBack().Id_
}

func (rf *Raft) GetLastLogTerm() int {
	return rf.LogBack().Term_
}

// 返回任期为term的第一条日志，如果有的话
func (rf *Raft) GetFirstLogIndexOfTerm(term int) (int, bool) {
	idx := sort.Search(len(rf.logs_), func(i int) bool {
		return rf.logs_[i].Term_ >= term
	})

	return idx, idx != len(rf.logs_) && rf.logs_[idx].Term_ == term
}

func (rf *Raft) GetLastLogIndexOfTerm(term int) (int, bool) {
	idx := sort.Search(len(rf.logs_), func(i int) bool {
		return rf.logs_[i].Term_ < term+1
	})
	return idx, idx != len(rf.logs_) && rf.logs_[idx].Term_ == term
}

func (rf *Raft) GetIndexOfLogId(log_id int) int {
	idx := sort.Search(len(rf.logs_), func(i int) bool {
		return rf.logs_[i].Id_ >= log_id
	})
	return idx
}

func (rf *Raft) GetLogOfLogId(log_id int) LogEntry {
	idx := rf.GetIndexOfLogId(log_id)
	if idx < len(rf.logs_) {
		return rf.logs_[idx]
	}
	return LogEntry{}
}

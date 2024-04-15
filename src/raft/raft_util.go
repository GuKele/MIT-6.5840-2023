package raft

import (
	"math/rand"
	"sort"
	"time"
)

const (
	// 声明在函数外部，首字母小写则包内可见，大写则所有包可见
	heartBeatInterval            = 50 * time.Millisecond
	electionTimeoutBase          = 4 * heartBeatInterval
	electionTimeoutRandIncrement = 150
)

func GetRandomElectionTimeout() time.Duration {
	return electionTimeoutBase + time.Duration(rand.Intn(electionTimeoutRandIncrement))*time.Millisecond
}

func (rf *Raft) GetLastTermAndId() (int, int) {

	return rf.GetLastLogTerm(), rf.GetLastLogId()
}

func (rf *Raft) GetLastLogId() int {
	return rf.logs_[len(rf.logs_)-1].Id_
}

func (rf *Raft) GetLastLogTerm() int {
	return rf.logs_[len(rf.logs_)-1].Term_
}

// 返回任期为term的第一条日志，如果有的话
func (rf *Raft) GetFirstLogOfTerm(term int) (LogEntry, bool) {
	log := LogEntry{}
	exist := false

	n := len(rf.logs_)
	idx := sort.Search(n, func(i int) bool {
		return rf.logs_[i].Term_ >= term
	})

	if idx < n && rf.logs_[idx].Term_ == term {
		log = rf.logs_[idx]
		exist = true
	}

	return log, exist
}

func (rf *Raft) GetLastLogAtTerm(term int) (LogEntry, bool) {
	log := LogEntry{}
	exist := false

	n := len(rf.logs_)
	left, right := 0, n-1
	for left < right {
		mid := left + (right-left+1)/2
		if rf.logs_[mid].Term_ <= term {
			left = mid
		} else {
			right = mid - 1
		}
	}

	if left < n && rf.logs_[left].Term_ == term {
		log = rf.logs_[left]
		exist = true
	}

	return log, exist
}

func (rf *Raft) GetIndexOfLogId(log_id int) (int, bool) {
	exist := true
	idx := log_id - rf.logs_[0].Id_

	if idx >= len(rf.logs_) || idx < 0 {
		idx = -1
		exist = false
	}
	return idx, exist
}

func (rf *Raft) GetLogOfLogId(log_id int) (LogEntry, int, bool) {
	log := LogEntry{}
	idx, exist := rf.GetIndexOfLogId(log_id)

	if exist {
		log = rf.logs_[idx]
	}

	return log, idx, exist
}

func Min(lhs int, rhs int) int {
	if lhs < rhs {
		return lhs
	}
	return rhs
}

func Max(lhs int, rhs int) int {
	if lhs > rhs {
		return lhs
	}
	return rhs
}

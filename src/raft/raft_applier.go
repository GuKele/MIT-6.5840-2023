package raft

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

	CommandId    int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotId    int
}

type Snapshot struct {
	Data         []byte
	SnapshotTerm int
	SnapshotId   int
}

func (rf *Raft) applier() {
	rf.applier_cv_.L.Lock()
	defer rf.applier_cv_.L.Unlock()

	for !rf.killed() {
		// 考虑到如果if，apply期间，有新的commit id了，然后notify了，但是此时并没有wait，这个notify就丢失了。所以使用for循环来代替。
		for data, ok := rf.needAppling(); ok && !rf.killed(); data, ok = rf.needAppling() {
			rf.apply(data)
		}
		rf.applier_cv_.Wait()
	}
}

// ok表示需要应用, data可能是日志也可能是快照
func (rf *Raft) needAppling() (interface{}, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	ok := false
	var data interface{} = nil
	if rf.last_applied_ < rf.commit_id_ {
		ok = true
		if rf.last_applied_ >= rf.logs_[0].Id_ {
			begin, _ := rf.GetIndexOfLogId(rf.last_applied_)
			end, _ := rf.GetIndexOfLogId(rf.commit_id_)
			data = rf.logs_[begin+1 : end+1]
		} else {
			data = Snapshot{
				Data:         rf.persister.ReadSnapshot(),
				SnapshotId:   rf.logs_[0].Id_,
				SnapshotTerm: rf.logs_[0].Term_,
			}
		}
	}

	return data, ok
}


// ApplyMsg是通知状态机,应用到状态机
func (rf *Raft) apply(data interface{}) {
	if logs, ok := data.([]LogEntry); ok { // 应用日志
		for _, log := range logs {
			rf.apply_ch_ <- ApplyMsg{
				CommandValid: true,
				Command:      log.Command_,
				CommandId:    log.Id_,
			}
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.last_applied_ != logs[0].Id_-1 {
			Debug(dError, "S%v try Apply LI:[%v, %v], but LA:%v CI:%v LLI:%v", logs[0].Id_, logs[len(logs)-1].Id_, rf.last_applied_, rf.commit_id_, rf.LogBack().Id_)
		}

		rf.last_applied_ = logs[len(logs)-1].Id_
		Debug(dInfo, "S%v Apply LI:[%v, %v]", rf.me, logs[0].Id_, logs[len(logs)-1].Id_)

	} else if snapshot, ok := data.(Snapshot); ok { // 应用快照
		rf.apply_ch_ <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot.Data,
			SnapshotTerm:  snapshot.SnapshotTerm,
			SnapshotId:    snapshot.SnapshotId,
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.last_applied_ = snapshot.SnapshotId
		Debug(dInfo, "S%v Apply SI:[0,%v]", rf.me, snapshot.SnapshotId)
	}
}

package raft

import (
	"bytes"
	// "log/slog"

	"6.5840/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	wbuf := new(bytes.Buffer)
	serializer := labgob.NewEncoder(wbuf)
	serializer.Encode(rf.cur_term_)
	serializer.Encode(rf.voted_for_)
	serializer.Encode(rf.logs_)
	raft_state := wbuf.Bytes()
	rf.persister.Save(raft_state, nil)
	Debug(dPersist, "S%v Persist T:%v VF:%v LLI:%v", rf.me, rf.cur_term_, rf.voted_for_, rf.GetLastLogId())
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
	rbuf := bytes.NewBuffer(data)
	deserializer := labgob.NewDecoder(rbuf)
	var cur_term int
	var voted_for int
	var logs []LogEntry
	// 反序列化成功则返回nil
	if deserializer.Decode(&cur_term) != nil || deserializer.Decode(&voted_for) != nil || deserializer.Decode(&logs) != nil {
		Debug(dError, "S%v Deserialization fail when read persist", rf.me)
	} else {
		rf.cur_term_ = cur_term
		rf.voted_for_ = voted_for
		rf.logs_ = logs
	}
	Debug(dPersist, "S%v Read persist T:%v VF:%v", rf.me, rf.cur_term_, rf.voted_for_)

}

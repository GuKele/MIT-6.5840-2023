package raft

import "time"

// the service says it has created a snapshot that has
// all info up to and including id. this means the
// service no longer needs the log through (and including)
// that id. Raft should now trim its log as much as possible.
// Your code here (2D).
// 生成一次快照，快照包含的最后一条日志的id，删除掉id及之前的日志，snapshot是已经压缩好的快照(下层状态机提供的)
func (rf *Raft) Snapshot(id int, snapshot []byte) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	idx, exist := rf.GetIndexOfLogId(id)
	if !exist { // 此时可能打比之前已有快照更小，apply的id居然大于已经有的id
		Debug(dWarn, "S%v Try snapshot Id:[0,%v], but now log between (%v, %v]", rf.me, id, rf.logs_[0].Id_, rf.LogBack().Id_)
		return
	}

	rf.logs_ = rf.logs_[idx:]                   // 0还是哨兵，此时是快照最后一条日志的id和term
	new_logs := make([]LogEntry, len(rf.logs_)) // 切片操作并不会复制，而是引用，也不会释放原始的数组，即使是没有引用的部分
	copy(new_logs, rf.logs_)
	rf.logs_ = new_logs
	rf.logs_[0].Command_ = nil

	Debug(dSnap, "S%v Snapshot SI:(0, %v], Persist T:%v VF:%v LLI:%v", rf.me, id, rf.cur_term_, rf.voted_for_, rf.GetLastLogId())

	rf.persistStateAndSnap(snapshot)
}

type InstallSnapshotArgs struct {
	Leader_term_ int
	Leader_id_   int

	Last_include_id_   int // 快照包含的最后一条日志的id
	Last_include_term_ int // 快照包含的最后一条日志的term

	Offset_ int  // 原论文中快照分成一个个chunk，这个字段为该chunk在快照中的字节偏移量
	Done_   bool // true if this is the last chunk

	Data_ []byte // chunk,在这里就是一个完整的快照。快照包括last include id和snapshot
}

type InstallSnapshotReply struct {
	Term_ int
}

func (rf *Raft) InstallSnapshotRPC(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term_ = rf.cur_term_

	if args.Leader_term_ < rf.cur_term_ {
		return
	}

	if args.Leader_term_ > rf.cur_term_ {
		rf.SetRoleAndTicker(RoleFollower)
		rf.SetTerm(args.Leader_term_)
	}

	// 本项目不要求快照分块
	// if args.Offset_ == 0 {
	// 	// 新建快照文件
	// }

	// if args.Done != true {
	// 	// 等待更多数据块
	// }

	// 与last apply或者commit id比较，这样可以避免更多的重复apply
	if args.Last_include_id_ < rf.logs_[0].Id_ {
		Debug(dWarn, "S%v <- S%v Not accept snapshot SI:(0, %v] but already SI:(0，%v]", rf.me, args.Leader_id_, args.Last_include_id_, rf.logs_[0].Id_)
		return
	} else if args.Last_include_id_ < rf.last_applied_ || args.Last_include_id_ < rf.commit_id_ {
		Debug(dWarn, "S%v <- S%v Not accept snapshot SI:(0, %v] but already SI:(0，%v] AI:%v CI:%v", rf.me, args.Leader_id_, args.Last_include_id_, rf.logs_[0].Id_, rf.last_applied_, rf.commit_id_)
		return
	}

	// 如果现有的日志条目索引和任期与快照最后一条一样，则保留其后的日志
	if idx, ok := rf.GetIndexOfLogId(args.Last_include_id_); ok && rf.logs_[idx].Term_ == args.Last_include_term_ {
		rf.logs_ = rf.logs_[idx:]                   // 0还是哨兵，此时是快照最后一条日志的id和term
		new_logs := make([]LogEntry, len(rf.logs_)) // 切片操作并不会复制，而是引用，也不会释放原始的数组，即使是没有引用的部分
		copy(new_logs, rf.logs_)
		rf.logs_ = new_logs
		rf.logs_[0].Command_ = nil

	} else { // 快照最后一条日志id 大于 server最后一条日志的id，或者server对应id的日志term对不上，都应该清空server的log
		rf.logs_ = make([]LogEntry, 0)
		sentry := LogEntry{
			Id_:      args.Last_include_id_,
			Term_:    args.Last_include_term_,
			Command_: nil,
		}
		rf.logs_ = append(rf.logs_, sentry)
	}

	rf.persistStateAndSnap(args.Data_)

	if rf.SetCommitId(args.Last_include_id_) {
		rf.applier_cv_.Signal()
	}

	Debug(dSnap, "S%v -> S%v Accept snapshot SI:(0,%v] CI:%v LLI:%v", rf.me, args.Leader_id_, args.Last_include_id_, rf.commit_id_, rf.GetLastLogId())
}

// 似乎是以前项目需要的接口?
// func (rf *Raft) CondInstallSnapshot() {
// }

func (rf *Raft) SendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if rf.killed() {
		return false
	}

	// ok := rf.peers[server].Call("Raft.InstallSnapshotRPC", args, reply)
	// return ok

	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	ch := make(chan bool, 1)
	go func() {
		if rf.peers[server].Call("Raft.InstallSnapshotRPC", args, reply) {
			ch <- true
		}
	}()

	select {
	case <-ch:
		return true
	case <-rpcTimer.C:
		return false
	}
}

// 返回值是rpc是否收到reply，即本次快照添加是否网络连通
func (rf *Raft) InstallSnapshotOneRoundWithLock(server int) bool {
	if rf.killed() {
		rf.mu.Unlock()
		return false
	}

	// rf.mu.Lock()
	if rf.role_ != RoleLeader {
		rf.mu.Unlock()
		return false
	}

	args := InstallSnapshotArgs{
		Leader_term_: rf.cur_term_,
		Leader_id_:   rf.me,

		Last_include_id_:   rf.logs_[0].Id_,
		Last_include_term_: rf.logs_[0].Term_,

		Offset_: 0,
		Done_:   true,

		Data_: rf.persister.ReadSnapshot(),
	}

	reply := InstallSnapshotReply{}

	Debug(dSnap, "S%v -> S%v Snapshot SI:(0, %v] LT:%v MI:%v", args.Leader_id_, server, args.Last_include_id_, args.Leader_term_, rf.match_id_[server])

	rf.mu.Unlock()

	if rf.SendInstallSnapshotRPC(server, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.cur_term_ != args.Leader_term_ || rf.role_ != RoleLeader {
			return true
		}

		if reply.Term_ > rf.cur_term_ {
			rf.SetTerm(reply.Term_)
			rf.SetRoleAndTicker(RoleFollower)
		} else {
			rf.match_id_[server] = Max(rf.match_id_[server], args.Last_include_id_)
			rf.next_id_[server] = Max(rf.next_id_[server], args.Last_include_id_+1)
			// rf.tryCommit()
		}
		return true
	}

	return false
}

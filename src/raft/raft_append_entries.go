package raft

import (
	// "log/slog"
	"sort"
)

type AppendEntriesArgs struct {
	Leader_Term_         int
	Leader_id_           int
	Leader_commit_index_ int // 大多数复制的index，用来让follower来判断是否可以apply到上层状态机

	Prev_log_index_ int // 本次发给该节点的log的前一条log的下标和term,用来让follower节点判断是否接受本次新发的日志（接受本次log要保证之前的log都接收到了）
	Prev_log_term_  int

	Entries_ []LogEntry // 空则表示是心跳

	Match_idx_ int // 心跳或者append log失败时,用来判断apply
	// TODO(gukele): 发送一条还是多条？如果是多条，出现极端情况下，一直跟follower的log不同步，代价太大了;如果是发送单条，出现了可以发送多种情况时，又浪费
	Immediately_ bool // for test
}

type AppendEntriesReply struct {
	Term_    int // 答复者的term,用来让leader做更新
	Success_ bool
}

func (rf *Raft) AppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term_ = rf.cur_term_
	reply.Success_ = false

	// 如果我是candidate，现在产生了leader的任期比自己大，那么自己应该变成follower吧
	if args.Leader_Term_ < rf.cur_term_ {
		if len(args.Entries_) == 0 {
			// slog.Info("不接受心跳, 因为server的term大", "leader", args.Leader_id_, "leader_term", args.Leader_Term_, "server", rf.me, "server_term", rf.cur_term_, "immediately", args.Immediately_)
			Debug(dLog, "S%v <- S%v Not accept heartbeat LT:%v < T:%v", rf.me, args.Leader_id_, args.Leader_Term_, rf.cur_term_)

		} else {
			// slog.Info("不接受日志, 因为server的term大", "leader", args.Leader_id_, "leader_term", args.Leader_Term_, "server", rf.me, "server_term", rf.cur_term_, "immediately", args.Immediately_)
			Debug(dLog, "S%v <- S%v Not accept log LT:%v < T:%v", rf.me, args.Leader_id_, args.Leader_Term_, rf.cur_term_)
		}

		reply.Success_ = false
		return
	}

	// if args.Leader_Term_ > rf.cur_term_ {
	// 	rf.SetTerm(args.Leader_Term_)
	// 	rf.SetRole(RoleFollower)
	//  rf.voted_for_ = args.Leader_id_
	//  rf.ResetTicker()
	// }

	// 如果是网络分区的旧leader,它一直心跳,也不知道有多少follower呢?
	// 但是也不会影响到客户端,因为永远也无法prepare + commit
	if args.Leader_Term_ >= rf.cur_term_ {
		// 只有leader可以发送append RPC
		rf.ResetTicker() // 只要有leader给自己发心跳也好，日志也罢，都更新一下过期时间，不管怎么样现在有一个合法的leader,自己要避免选举
		rf.SetTerm(args.Leader_Term_)
		rf.SetRole(RoleFollower) // rf此时可能是old leader或者candidate或者follower
		// rf.commit_idx_ = args.Leader_commit_index_

		if len(args.Entries_) == 0 { // 心跳
			reply.Success_ = true
			// BUG: 如果是心跳,不能保证此时更新[old_commit_index, new_commit_index]的log是和leader一致的, 如果此时是分区的旧leader刚连接上,可能会有大量的垃圾log.
			// 比如你之前断开连接但是被不停的start好几个日志，等你连上后变为follower，因为leader发了一个commit index，自己就应用了错误日志.
			// 除非这里也需要判断prev log index、term是否匹配，或者带上match？
			if len(rf.logs_)-1 == args.Prev_log_index_ && rf.logs_[args.Prev_log_index_].Term_ == args.Prev_log_term_ {
				rf.commit_idx_ = Min(args.Leader_commit_index_, rf.GetLastLogIndex())
			}
			// log.Printf("Follower %v accept heart beat from leader %v in term %v", rf.me, args.Leader_id_, args.Leader_Term_)
			Debug(dHeart, "S%v <- S%v Heartbeat", rf.me, args.Leader_id_)
		} else { // 日志
			// 如果日志条数对不上,或者prev log term对不上,说明prev log index是错误的
			if len(rf.logs_)-1 < args.Prev_log_index_ || rf.logs_[args.Prev_log_index_].Term_ != args.Prev_log_term_ {
				// slog.Debug("前边日志对不上,server不接受该日志", "server", rf.me)
				Debug(dLog, "S%v <- S%v Not accept log PLI:%v LLI:%v", rf.me, args.Leader_id_, args.Prev_log_index_, rf.GetLastLogIndex())

				reply.Success_ = false
			} else {
				reply.Success_ = true
				rf.logs_ = rf.logs_[:args.Prev_log_index_+1]
				rf.logs_ = append(rf.logs_, args.Entries_...)
				rf.persist()

				rf.commit_idx_ = Min(args.Leader_commit_index_, rf.GetLastLogIndex())

				// slog.Info("接受日志.", "server", rf.me, "prev log index", args.Prev_log_index_, "cur log index", rf.GetLastLogIndex())
				Debug(dLog, "S%v <- S%v Accept log LLI:%v", rf.me, args.Leader_Term_, rf.GetLastLogIndex())
			}
		}

		// apply的log有两个条件:
		//     1.自己本身已经添加的entries
		//     2.并且leader告诉我们大部分都已经复制了该log,完成了两阶段提交的第一阶段,该进入第二阶段了
		// 此时可以apply了
		rf.Apply()
	}
}

// ApplyMsg是通知状态机,应用到状态机
func (rf *Raft) Apply() {
	if rf.last_applied_ < rf.commit_idx_ {
		old_last_applied := rf.last_applied_

		for i := rf.last_applied_ + 1; i <= rf.commit_idx_; i += 1 {
			rf.apply_ch_ <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs_[i].Command_,
				CommandIndex: i,
			}
			rf.last_applied_ = i
		}

		Debug(dInfo, "S%v Apply LA:[%v, %v]", rf.me, old_last_applied + 1, rf.last_applied_)
	}
}

func Min(lhs int, rhs int) int {
	if lhs < rhs {
		return lhs
	}
	return rhs
}

/*
 * @brief
 */
func (rf *Raft) SendAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	// FIXME: 也不能无限发送吧
	// for retry := 0; !ok && retry < 1; retry += 1 {
	// 	if rf.killed() {
	// 		return false
	// 	}
	// 	ok = rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	// }

	return ok
}

/*
 * Broadcast heartbeat without lock!
 */
func (rf *Raft) BroadcastHeartBeat(immediately bool) {
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			if rf.role_ != RoleLeader {
				rf.mu.Unlock()
				return
			}

			// 发送心跳
			args := AppendEntriesArgs{
				Leader_Term_:         rf.cur_term_,
				Leader_id_:           rf.me,
				Leader_commit_index_: rf.commit_idx_,
				Prev_log_index_:      rf.next_idx_[server] - 1,
				Prev_log_term_:       rf.logs_[rf.next_idx_[server]-1].Term_,
				Entries_:             make([]LogEntry, 0), // 空则表示是心跳
				Match_idx_:           rf.match_idx_[server],
				Immediately_:         false,
			}

			rf.mu.Unlock()

			// slog.Debug("发送心跳.", "leader", rf.me, "term", rf.cur_term_, "server", server)
			Debug(dHeart, "S%v -> S%v Heartbeat PLT:%v PLI:%v LCI:%v", rf.me, server, args.Prev_log_term_, args.Prev_log_index_, args.Leader_commit_index_)

			if immediately { // for test
				args.Immediately_ = true
			}

			reply := AppendEntriesReply{}

			if rf.SendAppendEntriesRPC(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// FIXME: 对RPC回应做处理之前,判断一下是否和发送RPC之前状态一样
				if rf.cur_term_ == args.Leader_Term_ && rf.role_ == RoleLeader {
					if reply.Term_ > rf.cur_term_ && reply.Success_ == false {
						rf.ResetTicker()
						rf.SetRole(RoleFollower)
						// slog.Info("发送心跳失败，遇到更大的term, leader became follower.", "leader_term", rf.cur_term_, "args_term", args.Leader_Term_, "big_term", reply.Term_)
						Debug(dHeart, "S%v <- S%v Heartbeat failed due to term", rf.me, server)
						rf.SetTerm(reply.Term_)
					}
				}
			} else {
				// TODO(gukele): 是否需要统计心跳回应的count，如果加上自己没有一半，自己也就不是leader了. 不允许小分区leader的存在,并且这个很难啊，需要规定时间内返回的，而且心跳也没编号啊
			}
		}(idx)
	}
}

func (rf *Raft) Replicator(server int) {
	// 我认为应该是，收到客户端的command后，同步给所有的从节点，
	// 然后根据从节点回复的同步情况去继续发送之前没有同步的，开辟一个新goroutine去做这个事情？AppendEntries
	// 这个携程的需要条件变量唤醒，而且循环周期是主节点不再是leader

	// 同步会发生，follower日志多的情况，通常是上个leader发送同步，但是不够大多数而未提交就换了新leader，
	// 这些日志并没有提交，需要覆盖

	// 或许可以给每个follower都开启一个对应的携程，负责该follower的日志同步，那么客户端来了新的command后，我们也只是唤醒所有AppendEntries
	// 避免浪费？

	// 刚成为leader后不知道每个节点同步了多日志，我们就认为每个follower都同步了所有的日志，我们等到客户端
	// 发送一个命令后，传播给所有的follower,通过follower的反馈去知道每个follower同步了多少
	// cv := rf.replicator_cv_[server]
	rf.replicator_cv_[server].L.Lock()
	defer rf.replicator_cv_[server].L.Unlock()

	for !rf.killed() {
		for !rf.NeedReplicating(server) {
			rf.replicator_cv_[server].Wait()
		}
		// send one round append
		rf.AppendEntriesOneRound(server)
	}
}

func (rf *Raft) NeedReplicating(server int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.role_ == RoleLeader && rf.match_idx_[server] < rf.GetLastLogIndex()
}

func (rf *Raft) AppendEntriesOneRound(server int) {
	rf.mu.Lock()
	if rf.role_ != RoleLeader {
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Leader_Term_:         rf.cur_term_,
		Leader_id_:           rf.me,
		Leader_commit_index_: rf.commit_idx_,
		Prev_log_index_:      rf.next_idx_[server] - 1,
		Prev_log_term_:       rf.logs_[rf.next_idx_[server]-1].Term_,
		Entries_:             rf.logs_[rf.next_idx_[server]:],
		Match_idx_:           rf.match_idx_[server],
	}
	rf.mu.Unlock()

	Debug(dLog, "S%v -> S%v Sending PLT:%v PLI:%v LCI:%v LI:[%v, %v]", rf.me, server, args.Prev_log_term_, args.Prev_log_index_, args.Leader_commit_index_, args.Prev_log_index_ + 1, args.Prev_log_index_ + len(args.Entries_))
	reply := AppendEntriesReply{}
	if rf.SendAppendEntriesRPC(server, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// FIXME: 根据返回结果处理,这里是否需要判断是不是还是leader?
		if rf.cur_term_ == args.Leader_Term_ && rf.role_ == RoleLeader {
			if reply.Success_ {

				rf.next_idx_[server] += len(args.Entries_)
				rf.match_idx_[server] = args.Prev_log_index_ + len(args.Entries_)
				Debug(dLog, "S%v <- S%v Ok append MI:%v", rf.me, server, rf.match_idx_[server])

				// 更新commit index, 一种方案是对next_idx_排序,然后中间的那个数就是大多数复制了最大index
				match_idx := make([]int, len(rf.match_idx_))
				copy(match_idx, rf.match_idx_)
				sort.Ints(match_idx)
				new_commit_idx := match_idx[len(rf.peers)/2]

				if rf.commit_idx_ < new_commit_idx {
					// slog.Info("Leader有新的commit index.", "leader", rf.me, "commit index", new_commit_idx)
					Debug(dCommit, "S%v Leader update commit index %v -> %v", rf.me, rf.commit_idx_, new_commit_idx)
					rf.commit_idx_ = new_commit_idx
				}

				rf.Apply()

			} else {
				if reply.Term_ > rf.cur_term_ {
					// slog.Info("日志添加失败，遇到更大的term, leader became follower.", "leader_term", rf.cur_term_, "server_term", reply.Term_)
					Debug(dTerm, "S%v <- S%v No append due to term", rf.me, server)
					rf.SetTerm(reply.Term_)
					rf.SetRole(RoleFollower)
					rf.ResetTicker()
				} else {
					// TODO(gukele): 折半？折半挺扯淡的。。
					// 或者可以考虑递增，类似tcp窗口，或者分块
					// 还有就是找到reply的任期的第一条日志
					// slog.Debug("Leader的next idx回退", "leader", args.Leader_id_, "server", server, "next_index", rf.next_idx_[server])
					Debug(dLog, "S%v <- S%v No append due to next index:%v", rf.me, server, rf.next_idx_[server])
					rf.next_idx_[server] -= 1
				}
			}
		}
	}
}

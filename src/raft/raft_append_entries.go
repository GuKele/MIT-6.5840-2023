package raft

import (
	// "log/slog"
	"sort"
)

type AppendEntriesArgs struct {
	Leader_term_         int
	Leader_id_           int
	Leader_commit_id_ int // 大多数复制的id，用来让follower来判断是否可以apply到上层状态机

	Prev_log_id_ int // 本次发给该节点的log的前一条log的下标和term,用来让follower节点判断是否接受本次新发的日志（接受本次log要保证之前的log都接收到了）
	Prev_log_term_  int

	Entries_ []LogEntry // 空则表示是心跳

	Match_id_ int // 心跳或者append log失败时,用来判断apply
	// TODO(gukele): 发送一条还是多条？如果是多条，出现极端情况下，一直跟follower的log不同步，代价太大了;如果是发送单条，出现了可以发送多种情况时，又浪费
	Immediately_ bool // for test
}

type AppendEntriesReply struct {
	Term_    int // 答复者的term,用来让leader做更新
	Success_ bool

	// optimization, not necessary
	XLen_ int // len of follower's log. if it is to short, leader next id is that

	// 如果leader包含XTerm_的日志，那么就从该term的最后一条作为next开始尝试同步。如果leader不包含该XTerm_的日志，那么就从XIndex_开始尝试同步，即直接跳过follower中该term所有的日志。
	XTerm_  int // term in the conflicting prev entry (if any)
	XId_ int // id of first entry with that term (if any)

	// 如果leader不包含该term的日志，说明follower中该term的日志应该都是错的，是因为网络分区旧leader或者跟旧leader在同一个分区导致的无效的log.那么该term的日志都是无效的，next就可以直接跳过该term。
	// 如果leader包含该term的日志说明该follower中该term的日志可能有一些是正确的，也就是说leader包含的该term的都是正确的，所以从leader该term的最后一条开始发送。

	// 与论文中一条一条日志进行同步相比，这种方法相当于一个一个term进行同步，可以节省大量的RPC数量，从而节省时间。
	// 从直觉上来讲，一个Follower如果与Leader有冲突的日志，那么这个Follower要么是一个旧的Leader，在宕机前接收了日志但是还没来得及与其他Follower同步，要么是一个和旧的Leader在同一个网络分区的Follower。
	// 在网络恢复后或者宕机重启后，新Leader的term必然比之前大，之前的term接收的但未同步的日志是要被覆写的，因此一个一个term进行同步是合理的。
}

func (rf *Raft) AppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term_ = rf.cur_term_
	reply.Success_ = false
	reply.XLen_ = -1
	reply.XTerm_ = -1
	reply.XId_ = -1

	// 如果我是candidate，现在产生了leader的任期比自己大，那么自己应该变成follower吧
	if args.Leader_term_ < rf.cur_term_ {
		if len(args.Entries_) == 0 {
			Debug(dHeart, "S%v -> S%v Not accept heartbeat due to term LT:%v < T:%v", rf.me, args.Leader_id_, args.Leader_term_, rf.cur_term_)

		} else {
			Debug(dLog, "S%v -> S%v Not accept log due to term LT:%v < T:%v", rf.me, args.Leader_id_, args.Leader_term_, rf.cur_term_)
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
	if args.Leader_term_ >= rf.cur_term_ {
		// 只有leader可以发送append RPC,所以这里不管是心跳还是增添日志，我们都应该降低身份，并且更新term，同意该leader
		rf.ResetTicker() // 只要有leader给自己发心跳也好，日志也罢，都更新一下过期时间，不管怎么样现在有一个合法的leader,自己要避免选举
		if args.Leader_term_ > rf.cur_term_ {
			rf.SetTerm(args.Leader_term_)
		}
		// 之所以放在外面是因为，如果两个server同时开始竞选，他们的term是一样的，假如其中一个变成了leader，另外一个也应该在收到心跳或者日志添加后，正确的更新它的现在认同的leader，并且自己转为follower
		rf.SetVotedFor(args.Leader_id_)
		rf.SetRole(RoleFollower) // rf此时可能是old leader或者candidate或者follower

		if len(args.Entries_) == 0 { // 心跳
			reply.Success_ = true
			// BUG: 如果是心跳,不能保证此时更新[old_commit_id, new_commit_id]的log是和leader一致的, 如果此时是分区的旧leader刚连接上,可能会有大量的垃圾log.
			// 比如你之前断开连接但是被不停的start好几个日志，等你连上后变为follower，因为leader发了一个commit id，自己就应用了错误日志.
			// 除非这里也需要判断prev log id、term是否匹配，或者带上match？

			// TODO(gukele)： 心跳也可以通过返回值去纠正一些问题把。
			if rf.GetLastLogId() == args.Prev_log_id_ && rf.GetLastLogTerm() == args.Prev_log_term_ {
				rf.commit_id_ = Min(args.Leader_commit_id_, rf.GetLastLogId())
			}
			Debug(dHeart, "S%v -> S%v Heartbeat reply", rf.me, args.Leader_id_)
		} else { // 日志
			// 如果日志条数对不上,或者prev log term对不上,说明prev log id是错误的
			last_log_id := rf.GetLastLogId()
			pre_log := rf.GetLogOfLogId(args.Prev_log_id_)
			if last_log_id < args.Prev_log_id_ {
				reply.XLen_ = last_log_id
				reply.Success_ = false

				Debug(dLog, "S%v -> S%v Not accept log PLI:%v > LLI:%v", rf.me, args.Leader_id_, args.Prev_log_id_, last_log_id)

			} else if pre_log.Term_ != args.Prev_log_term_ {
				reply.XTerm_ = pre_log.Term_
				reply.XId_, _ = rf.GetFirstLogIndexOfTerm(reply.XTerm_)
				reply.Success_ = false

				Debug(dLog, "S%v -> S%v Not accept log PLT:%v != XT:%v", rf.me, args.Leader_id_, args.Prev_log_term_, reply.XTerm_)

			} else {
				// TODO(gukele): 判断一下是否是重复的？那样就没有必要做剩下的操作了
				reply.Success_ = true
				rf.logs_ = rf.logs_[:pre_log.Id_+1]
				rf.logs_ = append(rf.logs_, args.Entries_...)
				rf.persist()

				rf.commit_id_ = Min(args.Leader_commit_id_, rf.GetLastLogId())

				Debug(dLog, "S%v -> S%v Accept log LI:[%v, %v]", rf.me, args.Leader_id_, args.Prev_log_id_, rf.GetLastLogId())
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
	if rf.last_applied_ < rf.commit_id_ {
		old_last_applied := rf.last_applied_
		begin := rf.GetIndexOfLogId(rf.last_applied_) + 1

		for i := begin; i <= rf.commit_id_; i += 1 {
			rf.apply_ch_ <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs_[i].Command_,
				CommandIndex: i,
			}
			rf.last_applied_ += 1
		}

		Debug(dInfo, "S%v Apply LA:[%v, %v]", rf.me, old_last_applied+1, rf.last_applied_)
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
	for retry := 0; !ok && retry < 1; retry += 1 {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	}

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

		// TODO(gukele): 心跳压根没必要单独处理，直接用发送日志的就行了，如果都复制了的话直接就是空的，那么也就起到一个心跳的作用，如果不是空的还能起到一个传播日志的作用！
		// go rf.AppendEntriesOneRound(idx)

		go func(server int) {
			rf.mu.Lock()
			if rf.role_ != RoleLeader {
				rf.mu.Unlock()
				return
			}

			// if rf.next_idx_[server] == 0 {
			// 	Debug(dHeart, "S%v -> S%v Heartbeat ing. PLT:%v PLI:%v LCI:%v", rf.me, server, "missing", rf.next_idx_[server] - 1, rf.commit_idx_)
			// 	logs := ""
			// 	for i := 0; i < len(rf.logs_); i++ {
			// 		logs += fmt.Sprintf("(%v, %v), ",  rf.logs_[i].Term_, rf.logs_[i].Idx_)
			// 	}
			// 	Debug(dError, "S%v Logs: %v", rf.me, logs)
			// }

			// 发送心跳
			args := AppendEntriesArgs{
				Leader_term_:         rf.cur_term_,
				Leader_id_:           rf.me,
				Leader_commit_id_: rf.commit_id_,
				Prev_log_id_:      rf.next_id_[server] - 1,
				Prev_log_term_:       rf.logs_[rf.next_id_[server]-1].Term_,
				Entries_:             make([]LogEntry, 0), // 空则表示是心跳
				Match_id_:           rf.match_id_[server],
				Immediately_:         false,
			}

			rf.mu.Unlock()

			Debug(dHeart, "S%v -> S%v Heartbeat. PLT:%v PLI:%v LCI:%v", rf.me, server, args.Prev_log_term_, args.Prev_log_id_, args.Leader_commit_id_)

			if immediately { // for test
				args.Immediately_ = true
			}

			reply := AppendEntriesReply{}

			if rf.SendAppendEntriesRPC(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// FIXME: 对RPC回应做处理之前,判断一下是否和发送RPC之前状态一样
				if rf.cur_term_ == args.Leader_term_ && rf.role_ == RoleLeader {
					if reply.Term_ > rf.cur_term_ && reply.Success_ == false {
						rf.ResetTicker()
						rf.SetRole(RoleFollower)
						Debug(dHeart, "S%v <- S%v Heartbeat failed due to term", rf.me, server)
						rf.SetTerm(reply.Term_)
					}
				}
			}
		}(idx)
	}
}

func (rf *Raft) Replicator(server int) {
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

	// return rf.role_ == RoleLeader && rf.match_idx_[server] < rf.GetLastLogId()
	return rf.role_ == RoleLeader && rf.next_id_[server] <= rf.GetLastLogId()
}

func (rf *Raft) AppendEntriesOneRound(server int) {
	rf.mu.Lock()
	if rf.role_ != RoleLeader {
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Leader_term_:         rf.cur_term_,
		Leader_id_:           rf.me,
		Leader_commit_id_: rf.commit_id_,
		Prev_log_id_:      rf.next_id_[server] - 1,

		// Prev_log_term_:       rf.logs_[rf.next_idx_[server]-1].Term_,
		// Entries_:             rf.logs_[rf.next_idx_[server]:],

		Match_id_: rf.match_id_[server],
	}
	idx := rf.GetIndexOfLogId(rf.next_id_[server] - 1)
	args.Prev_log_term_ = rf.logs_[idx].Term_
	args.Entries_ = rf.logs_[idx+1:]

	rf.mu.Unlock()

	if len(args.Entries_) == 0 {
		Debug(dError, "S%v -> S%v Empty append", rf.me, server)
	}

	Debug(dLog, "S%v -> S%v Sending PLT:%v PLI:%v LCI:%v LI:[%v, %v]", rf.me, server, args.Prev_log_term_, args.Prev_log_id_, args.Leader_commit_id_, args.Prev_log_id_+1, args.Prev_log_id_+len(args.Entries_))
	reply := AppendEntriesReply{}
	if rf.SendAppendEntriesRPC(server, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// FIXME: 根据返回结果处理,这里是否需要判断是不是还是leader?
		if rf.cur_term_ == args.Leader_term_ && rf.role_ == RoleLeader && rf.match_id_[server] == args.Match_id_ {
			if reply.Success_ {

				rf.next_id_[server] += len(args.Entries_)
				rf.match_id_[server] = args.Prev_log_id_ + len(args.Entries_)
				Debug(dLog, "S%v <- S%v Ok append MI:%v", rf.me, server, rf.match_id_[server])

				// 更新commit id, 一种方案是对next_idx_排序,然后中间的那个数就是大多数复制了最大id
				match_id := make([]int, len(rf.match_id_))
				copy(match_id, rf.match_id_)
				sort.Ints(match_id)
				new_commit_id := match_id[len(rf.peers)/2]

				if rf.commit_id_ < new_commit_id {
					Debug(dCommit, "S%v Leader update commit id %v -> %v", rf.me, rf.commit_id_, new_commit_id)
					rf.commit_id_ = new_commit_id
				}

				rf.Apply()

			} else {

				if reply.Term_ > rf.cur_term_ {
					// Debug(dTerm, "S%v <- S%v No append due to term", rf.me, server)
					rf.SetTerm(reply.Term_)
					rf.SetRole(RoleFollower)
					rf.ResetTicker()

				} else {
					Debug(dLog, "S%v <- S%v Not accept log, XL:%v XT:%v XI:%v", rf.me, server, reply.XLen_, reply.XTerm_, reply.XId_)
					// TODO(gukele): 可以尝试不同的优化，例如类似tcp滑动窗口每次翻倍尝试从reply任期的第一条日志开始发送
					if reply.XLen_ != -1 {
						Debug(dLog, "S%v change S%v next id XLen %v -> %v", rf.me, server, rf.next_id_[server], rf.GetIndexOfLogId(reply.XLen_)+1)
						rf.next_id_[server] = rf.GetIndexOfLogId(reply.XLen_) + 1
					} else if reply.XTerm_ != -1 {
						last_idx, ok := rf.GetLastLogIndexOfTerm(reply.XTerm_)
						// 如果leader包含XTerm_的日志，那么就从该term的最后一条作为next开始尝试同步。如果leader不包含该XTerm_的日志，那么就从XIndex_开始尝试同步，即直接跳过follower中该term所有的日志。
						if !ok {
							Debug(dLog, "S%v change S%v next id XIndex %v -> %v", rf.me, server, rf.next_id_[server], rf.GetIndexOfLogId(reply.XId_))
							rf.next_id_[server] = rf.GetIndexOfLogId(reply.XId_)
						} else {
							Debug(dLog, "S%v change S%v next id XTerm %v -> %v", rf.me, server, rf.next_id_[server], last_idx)
							rf.next_id_[server] = last_idx
						}
					}
					// rf.next_idx_[server] -= 1
				}
			}
		}
	}
}

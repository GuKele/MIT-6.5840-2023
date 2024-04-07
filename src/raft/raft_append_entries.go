package raft

import (
	// "log/slog"

	"log"
	"sort"
)

type AppendEntriesArgs struct {
	Leader_term_      int
	Leader_id_        int
	Leader_commit_id_ int // 大多数复制的id，用来让follower来判断是否可以apply到上层状态机

	Prev_log_id_   int // 本次发给该节点的log的前一条log的下标和term,用来让follower节点判断是否接受本次新发的日志（接受本次log要保证之前的log都接收到了）
	Prev_log_term_ int

	Entries_ []LogEntry // nil或空表示心跳，但是似乎nil发送出去的时候不会是nil，并且有时候日志添加也是为空，心跳和日志添加的职能完全分开了，所以还是单独用一个字段来表示心跳Entries_

	Is_Heartbeat_ bool
}

type AppendEntriesReply struct {
	Term_    int // 答复者的term,用来让leader做更新
	Success_ bool

	// optimization, not necessary
	XLen_ int // log id of follower's last log. if it is to short, leader next id is that

	// 如果leader包含XTerm_的日志，那么就从该term的最后一条作为next开始尝试同步。如果leader不包含该XTerm_的日志，那么就从XIndex_开始尝试同步，即直接跳过follower中该term所有的日志。
	XTerm_ int // term in the conflicting prev entry (if any)
	XId_   int // id of first entry with that term (if any)

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
		if args.Is_Heartbeat_ {
			Debug(dHeart, "S%v -> S%v Not accept heartbeat due to term LT:%v < T:%v", rf.me, args.Leader_id_, args.Leader_term_, rf.cur_term_)
		} else {
			Debug(dLog, "S%v -> S%v Not accept log due to term LT:%v < T:%v", rf.me, args.Leader_id_, args.Leader_term_, rf.cur_term_)
		}

		reply.Success_ = false
		return
	}

	if args.Leader_term_ > rf.cur_term_ {
		rf.SetTerm(args.Leader_term_)
	}

	// 不管是candidate还是说旧分区重新连接上的旧leader，收到了AppendEntriesRPC就表明应该同意该leader，应该设置自己的身份
	rf.SetRole(RoleFollower)
	// 这里要不要设置voted_for_已经无所谓了把，因为已经竞选完毕了，及时它现在投给同term的其他竞选者，也不可能选出多个leader了
	// rf.SetVotedFor(args.Leader_id_)

	// 尝试添加日志：如果日志条数对不上,或者prev log term对不上,说明prev log id是错误的
	pre_log, pre_log_idx, _ := rf.GetLogOfLogId(args.Prev_log_id_)
	// 日志最大的id都小于pre log id
	if rf.GetLastLogId() < args.Prev_log_id_ {

		if args.Is_Heartbeat_ {
			Debug(dHeart, "S%v -> S%v Not accept heartbeat PLI:%v > LLI:%v", rf.me, args.Leader_id_, args.Prev_log_id_, rf.GetLastLogId())
		} else {
			reply.XLen_ = rf.GetLastLogId()
			Debug(dLog, "S%v -> S%v Not accept log PLI:%v > LLI:%v", rf.me, args.Leader_id_, args.Prev_log_id_, rf.GetLastLogId())
		}

	} else if pre_log.Term_ != args.Prev_log_term_ { // 日志中不存在改pre log(id相同时term不同)
		// 现有条目与新条目（id相同但任期不同）发生冲突，删除当前及以后的所有条目

		if args.Is_Heartbeat_ {

			Debug(dHeart, "S%v -> S%v Not accept heartbeat PLT:%v != XT:%v PLI:%v", rf.me, args.Leader_id_, args.Prev_log_term_, reply.XTerm_, args.Prev_log_id_)

		} else {

			reply.XTerm_ = pre_log.Term_
			first_log_of_term, _ := rf.GetFirstLogOfTerm(pre_log.Term_)
			reply.XId_ = first_log_of_term.Id_
			Debug(dLog, "S%v -> S%v Not accept log PLT:%v != XT:%v PLI:%v XI:%v", rf.me, args.Leader_id_, args.Prev_log_term_, reply.XTerm_, args.Prev_log_id_, reply.XId_)

			rf.logs_ = rf.logs_[:pre_log_idx]
			if rf.GetLastLogId() < rf.commit_id_ {
				Debug(dError, "S%v 丢弃了已经提交的日志 CI:%v LLI:%v", rf.me, rf.commit_id_, rf.GetLastLogId())
				Debug(dTrace, "S%v Logs:%v", rf.me, rf.logs_)
				log.Fatalf("Error!")
			}
		}

	} else { // 日志中存在pre log

		reply.Success_ = true
		if args.Is_Heartbeat_ {
			Debug(dHeart, "S%v -> S%v Accept Heartbeat LCI:%v T:%v LLI:%v", rf.me, args.Leader_id_, args.Leader_commit_id_, rf.cur_term_, rf.GetLastLogId())
		} else {

			// BUG(gukele): 举一个例子:我们发送了增加日志[8,8]的rpc，rpc在网络中堵塞了，然后leader认为失败了，重新下一轮日志增加，然后恰好此时客户端start日志9，leader就会发送增加日志[8,9]rpc，server收到后增加了[8，9]。
			// 但是此时之前堵塞的rpc又到了，server如果不去判断match id，就会新增加[8,8]，导致暂时的将增加的9又丢了！注意是暂时！虽然leader不会再对第一条rpc的reply做出处理，已经认为它失败了，
			// 但是server那里不判断就会暂时的把9丢弃掉。注意下次添加成功的时候其实还会把9加上，但是如果当时server都已经apply了9，但是你又把9暂时的给扔了，这期间就会导致错误的！
			// 并且不只是说在server apply后出问题，即使server没有apply，leader apply后也会出问题：比如有三个节点时，server1把添加的9又给丢了，另外一个server2还没来的即添加9，leader实际上已经apply了9，
			// 还没来得及告诉server1已经apply了，那么server1或者server2无论谁成为了leader，都找不回来了9了，出现问题了！！！！！！！

			// NOTE(gukele):其实论文中都已经说的很清楚了，如果发现了第一个冲突的日志，即id相同term不同时， 就删除掉该id后面所有的，然后添加不在日志中的新条目！也就是说如果没有冲突的，那么日志中比entries中多不用管的！！rpc很有可能是旧的
			actual_first_not_exist := -1
			for i := 0; i < len(args.Entries_); i++ {
				idx := pre_log_idx + 1 + i

				// 发现第一个不存在的
				if idx >= len(rf.logs_) {
					actual_first_not_exist = args.Entries_[i].Id_
					rf.logs_ = append(rf.logs_, args.Entries_[i:]...)
					break
				}

				// 发现第一个冲突的
				if args.Entries_[i].Term_ != rf.logs_[idx].Term_ {
					actual_first_not_exist = args.Entries_[i].Id_
					rf.logs_ = rf.logs_[:idx]
					if rf.GetLastLogId() < rf.commit_id_ {
						Debug(dError, "S%v 丢弃了已经提交的日志 CI:%v LLI:%v", rf.me, rf.commit_id_, rf.GetLastLogId())
						log.Fatalf("Error!")
					}
					rf.logs_ = append(rf.logs_, args.Entries_[i:]...)
					break
				}
			}

			actual_last_add_id := -1
			if actual_first_not_exist != -1 {
				actual_last_add_id = args.Entries_[len(args.Entries_)-1].Id_
			}

			Debug(dLog, "S%v -> S%v Accept log LLI:%v PLI:%v ALI:[%v, %v]", rf.me, args.Leader_id_, rf.GetLastLogId(), args.Prev_log_id_, actual_first_not_exist, actual_last_add_id)
			if actual_first_not_exist != -1 {
				rf.persist()
			}
		}

		// apply的log有两个条件:
		//     1.自己本身已经添加的entries
		//     2.并且leader告诉我们大部分都已经复制了该log,完成了两阶段提交的第一阶段,该进入第二阶段了
		// 此时可以apply了
		if rf.commit_id_ < Min(args.Leader_commit_id_, args.Prev_log_id_+len(args.Entries_)) {
			// 无法确保rf.GetLastLogId == args.Entries_[len(args.Entries_)-1].Id_
			// rf.commit_id_ = Min(args.Leader_commit_id_, rf.GetLastLogId())
			old_commit_id := rf.commit_id_
			rf.commit_id_ = Min(args.Leader_commit_id_, args.Prev_log_id_+len(args.Entries_))
			Debug(dCommit, "S%v Updating CI:%v -> %v", rf.me, old_commit_id, rf.commit_id_)
		}
	}

	rf.Apply()
}

// ApplyMsg是通知状态机,应用到状态机
func (rf *Raft) Apply() {
	if rf.last_applied_ < rf.commit_id_ {
		old_last_applied := rf.last_applied_

		begin, _ := rf.GetIndexOfLogId(rf.last_applied_ + 1)
		end, _ := rf.GetIndexOfLogId(rf.commit_id_)

		for ; begin <= end; begin++ {
			rf.apply_ch_ <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs_[begin].Command_,
				CommandIndex: rf.logs_[begin].Id_,
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
	// FIXME: 不在尝试重新发送，当失败了，就马上返回错误，我们与其在这了重试rpc不如直接重新发送，还能带上新添加的，可能会更高效一些把，因为如果有新的那么本次就算成功也是需要马上在发。
	// for retry := 0; !ok && retry < 1; retry += 1 {
	// 	if rf.killed() {
	// 		return false
	// 	}
	// 	ok = rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	// }

	return ok
}

/*
 * Broadcast heartbeat! 心跳不在承担发送人日志的事情，只是单纯的心跳，最多加上leader commit id的传递！
 */
func (rf *Raft)  BroadcastHeartBeat() {

	rf.ResetHeartBeat()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go rf.HeartbeatOneRound(peer)
	}
}

func (rf *Raft) HeartbeatOneRound(server int) {
	rf.mu.Lock()
	if rf.role_ != RoleLeader {
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Leader_term_: rf.cur_term_,
		Leader_id_:   rf.me,

		// 还是要有pre log的信息，因为server更新commit id需要这个来做判断
		Prev_log_id_:   rf.next_id_[server] - 1,
		Prev_log_term_: rf.logs_[rf.next_id_[server]-1-rf.logs_[0].Id_].Term_,

		Leader_commit_id_: rf.commit_id_,

		Entries_:      nil,
		Is_Heartbeat_: true,
	}

	rf.mu.Unlock()

	Debug(dHeart, "S%v -> S%v Heartbeat LCI:%v LT:%v PLI:%v PLT:%v", rf.me, server, args.Leader_commit_id_, args.Leader_term_, args.Prev_log_id_, args.Prev_log_term_)

	reply := AppendEntriesReply{}
	if rf.SendAppendEntriesRPC(server, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.cur_term_ == args.Leader_term_ && rf.role_ == RoleLeader {
			if !reply.Success_ {

				Debug(dHeart, "S%v <- S%v Not accept heartbeat", rf.me, server)
				if reply.Term_ > rf.cur_term_ {
					rf.SetTerm(reply.Term_)
					rf.SetRole(RoleFollower)
				}

			} else {

				Debug(dHeart, "S%v <- S%v Ok heartbeat", rf.me, server)

			}
		}
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
	return rf.role_ == RoleLeader && rf.match_id_[server] < rf.GetLastLogId()
}

func (rf *Raft) AppendEntriesOneRound(server int) {
	rf.mu.Lock()
	if rf.role_ != RoleLeader {
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Leader_term_:      rf.cur_term_,
		Leader_id_:        rf.me,
		Leader_commit_id_: rf.commit_id_,
		Prev_log_id_:      rf.next_id_[server] - 1,

		// Prev_log_term_:       rf.logs_[rf.next_idx_[server]-1].Term_,
		// Entries_:             rf.logs_[rf.next_idx_[server]:],
		Is_Heartbeat_: false,
	}
	pre_log_idx, exist := rf.GetIndexOfLogId(rf.next_id_[server] - 1)
	if !exist {
		Debug(dError, "S%v Logs:%v", rf.me, rf.logs_)
		log.Fatalf("S%v Log -> S%v Pre log not exist! PLI:%v LLI:%v", rf.me, server, rf.next_id_[server]-1, rf.GetLastLogId())
	}
	args.Prev_log_term_ = rf.logs_[pre_log_idx].Term_
	args.Entries_ = rf.logs_[pre_log_idx+1:]

	old_match_id := rf.match_id_[server]
	rf.mu.Unlock()

	Debug(dLog, "S%v -> S%v Sending PLT:%v PLI:%v LCI:%v LT:%v MI:%v LI:(%v, %v]", rf.me, server, args.Prev_log_term_, args.Prev_log_id_, args.Leader_commit_id_, args.Leader_term_, old_match_id, args.Prev_log_id_, args.Prev_log_id_+len(args.Entries_))

	reply := AppendEntriesReply{}
	if rf.SendAppendEntriesRPC(server, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// NOTE: 根据返回结果处理,这里是否需要判断是不是还是leader? 还需要判断match id吗？因为心跳其实也会改变match id，如果这期间心跳将match id改了那么就没办法正确的处理reply
		// 同时我们是不会同时发送多个日志添加，只可能上一个失败了才会重新发送，所以不会出现多个日志添加乱序的情况，所以不需要判断match id
		// if rf.cur_term_ == args.Leader_term_ && rf.role_ == RoleLeader && rf.match_id_[server] == args.Match_id_ {
		if rf.cur_term_ == args.Leader_term_ && rf.role_ == RoleLeader {
			if reply.Success_ {

				// NOTE(gukele): match_id必须单调递增，首先我们保证了第一条增加日志失败才会重新发送，这样保证了不会处理晚到来的日志增加的reply，同时保证心跳不负责发送日志的责任，从而保证match id递增。
				// 之所以可能希望心跳也可以帮忙修改next和match，是因为刚初始化时一直不添加日志，希望利用心跳来修改正确的next，同时更早的修改match有利于apply之类的，
				// 但是也会带来一些问题，例如虽然保证match增加时才修改match和next，但是如果处于增加日志一直失败回退next的时候，中间穿插的心跳可能会导致一些回退被回退了。。。但是不会回退日志添加成功时的match和next！所以有利有弊吧。

				new_match_id := args.Prev_log_id_ + len(args.Entries_)
				if new_match_id < rf.match_id_[server] {
					Debug(dError, "S%v Decreasing MI:%v -> %v", rf.me, rf.match_id_[server], new_match_id)
					log.Fatalf("Error!")
				}
				rf.next_id_[server] = new_match_id + 1
				rf.match_id_[server] = new_match_id

				Debug(dLog, "S%v <- S%v Ok append MI:%v", rf.me, server, new_match_id)

				// 更新commit id, 一种方案是对next_idx_排序,然后中间的那个数就是大多数复制了最大id
				match_id := make([]int, len(rf.match_id_))
				copy(match_id, rf.match_id_)
				sort.Slice(match_id, func(i, j int) bool {
					return match_id[i] > match_id[j]
				})

				// BUG(gukele)：被提交的日志被覆盖的问题，源论文中图8很详细的解释了这个问题，自己跑测中也发现了这个问题。首先先说总结：leader只能commit自己任期的日志，从而间接的提交之前任期的日志。
				// 直接看原论文吧，讲的很清楚。
				new_commit_id := match_id[len(rf.peers)/2]
				log, _, exist := rf.GetLogOfLogId(new_commit_id)
				if new_commit_id > rf.commit_id_ && exist && log.Term_ == rf.cur_term_ {
					Debug(dCommit, "S%v Updating LCI %v -> %v", rf.me, rf.commit_id_, new_commit_id)
					rf.commit_id_ = new_commit_id
				} else if !exist {
					Debug(dError, "S%v Updating LCI %v -> %v but not find that log, maybe due to snapshot", rf.me, rf.commit_id_, new_commit_id)
				}

				rf.Apply()

			} else {

				if reply.Term_ > rf.cur_term_ {

					rf.SetTerm(reply.Term_)
					rf.SetRole(RoleFollower)

				} else {

					Debug(dLog, "S%v <- S%v Not accept log, PLI:%v, XL:%v XT:%v XI:%v", rf.me, server, args.Prev_log_id_, reply.XLen_, reply.XTerm_, reply.XId_)

					// 回退next id优化
					if reply.XLen_ != -1 {
						Debug(dLog2, "S%v Update Next id of S%v XLen %v -> %v", rf.me, server, rf.next_id_[server], reply.XLen_+1)
						// 失败必须要减小，否则出现之前心跳失败的reply晚到的情况，但是如果处于增加日志一直失败回退next的时候，中间穿插的心跳可能会导致一些回退被回退了。。。但是不会回退日志添加成功时的match和next！
						if reply.XLen_+1 < rf.next_id_[server] {
							rf.next_id_[server] = reply.XLen_ + 1
						}
					} else if reply.XTerm_ != -1 {
						last_log_at_term, exist := rf.GetLastLogAtTerm(reply.XTerm_)
						if !exist {
							Debug(dLog2, "S%v Update Next id of S%v XId %v -> %v", rf.me, server, rf.next_id_[server], reply.XId_)
							rf.next_id_[server] = reply.XId_
						} else {
							Debug(dLog2, "S%v Update Next id of S%v XTerm %v -> %v", rf.me, server, rf.next_id_[server], last_log_at_term.Id_)
							rf.next_id_[server] = last_log_at_term.Id_
						}
					}

				}
			}
		}
	}
}

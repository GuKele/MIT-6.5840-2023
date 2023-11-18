package raft

type AppendEntriesArgs struct {
	Leader_Term_         int
	Leader_id_           int
	Leader_commit_index_ int
	Prev_log_index_      int
	Prev_log_term_       int
	Entries_             []LogEntry // 空则表示是心跳
}

type AppendEntriesReply struct {
	Term_    int // 答复者的term,用来让leader做更新
	Success_ bool
}

func (this *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	this.mu.Lock()
	defer this.mu.Unlock()

	// this.ResetTicker() // 等到真的leader给自己发才重置

	reply.Success_ = false
	reply.Term_ = this.cur_term_

	// 如果我是candidate，现在产生了leader的任期比自己大，那么自己应该变成follower吧
	if args.Leader_Term_ < this.cur_term_ {
		return
	}

	if args.Leader_Term_ >= this.cur_term_ {
		reply.Success_ = true
		this.SetTerm(args.Leader_Term_)
		this.SetRole(RoleFollower) // 可能此时他可能是一个candidate,然后已经产生了新leader。又或者是坏掉的leader
		this.voted_for_ = args.Leader_id_
		this.ResetTicker()
		return
	}
}

/*
 * @brief
 */
func (this *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if this.killed() {
		return false
	}

	ok := this.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	for !ok && this.role_ == RoleLeader {
		if this.killed() {
			return false
		}
		ok = this.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	}

	// TODO(gukele): term小则降为follower

	return ok
}

func (this *Raft) BroadcastHeartBeat() {

	// 发送心跳
	args := AppendEntriesArgs{
		Leader_Term_:         this.cur_term_,
		Leader_id_:           this.me,
		Leader_commit_index_: this.last_applied_,
		Prev_log_index_:      this.GetLastLogIndex(),
		Prev_log_term_:       this.GetLastLogTerm(),
		Entries_:             make([]LogEntry, 0), // 空则表示是心跳
	}

	for idx := range this.peers {
		if idx == this.me {
			continue
		}

		go func(server int) {
			reply := AppendEntriesReply{}

			if this.sendAppendEntries(server, &args, &reply) {
				this.mu.Lock()
				defer this.mu.Unlock()

				if reply.Term_ > this.cur_term_ {
					this.SetRole(RoleFollower)
					this.ResetTicker()
					this.SetTerm(reply.Term_)
				}
			}
		}(idx)
	}
}

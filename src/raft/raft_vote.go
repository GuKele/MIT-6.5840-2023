package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 用来让其它节点判断是否给你投票，一个candidate想要成为leader的前提，
	// 就是必须持有所有committed的log,因为committed的日志代表着大多数节点已经有了
	// Raft通过比较日志中最后一个条目的索引和任期来确定两个日志中哪个是最新(up-to-date)。
	// 如果日志中的最后一个条目具有不同的任期，则带有较新任期的日志将是最新的。 如果日志以相同的任期结尾，则更大索引的日志是最新的。
	Term_          int // candidate的任期号
	Candidate_id_  int // 发起投票的candidate的ID
	Last_log_term_ int // candidate的最高日志条目的任期号
	Last_log_id_   int // candidate的最高日志条目索引.
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term_         int  // 服务器的当前任期号，让candidate更新自己
	Vote_granted_ bool // 如果是true，意味着candidate收到了选票
}

//
// example RequestVoteRPC RPC handler.
// handler!!!
//
func (rf *Raft) RequestVoteRPC(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dVote, "S%v S%v request for vote at T%v", rf.me, args.Candidate_id_, args.Term_)

	reply.Term_ = rf.cur_term_ // maybe bigger than candidate‘s term，for candidate update himself
	reply.Vote_granted_ = false

	// 任期判断
	if args.Term_ < rf.cur_term_ {
		Debug(dVote, "S%v Not vote S%v (T%v > T%v)", rf.me, args.Candidate_id_, rf.cur_term_, args.Term_)
		return
	}

	try_vote := func() bool {
		// Raft使用投票过程来阻止没有所有提交日志的candidate赢得选举。candidate必须联系集群的大多数才能被选举，
		// 这意味着每个提交的条目都必须至少存在于这些服务器中的一个里。
		// 如果candidate的日志与大多数服务器的日志(在下面精确定义了"up-to-date")一样新(up-to-date)，
		// 则它将保存所有提交的条目。

		// Raft通过比较日志中最后一个条目的索引和任期来确定两个日志中哪个是最新(up-to-date)。
		// 1 如果日志中的最后一个条目具有不同的任期，则带有较新任期的日志将是最新的。
		// 2 如果日志以相同的任期结尾，则更大索引的日志是最新的。
		if args.Last_log_term_ > rf.GetLastLogTerm() || (args.Last_log_term_ == rf.GetLastLogTerm() && args.Last_log_id_ >= rf.GetLastLogId()) {
			// 投票后重置自己的election timeout，防止马上自己又升级candidate又开始新选举。只要自己的票投不出去，自己又收不到心跳/日志更新，那么自己就可能变candidate
			rf.SetRoleAndTicker(RoleFollower)
			reply.Vote_granted_ = true
			// FIXME(gukele): voted for和term一起序列化一次就行了
			rf.SetVotedFor(args.Candidate_id_)

			return true
		} else {
			Debug(dVote, "S%v Not vote S%v, the former's LT:%v LI:%v > LT:%v LI:%v", rf.me, args.Candidate_id_, rf.GetLastLogTerm(), rf.GetLastLogId(), args.Last_log_term_, args.Last_log_id_)
			return false
		}
	}

	// 新的选举期了，不管之前是否投过票，不管你是leader还是candidate，现在需要重新投票了
	if args.Term_ > rf.cur_term_ {

		rf.voted_for_ = -1
		rf.SetTerm(args.Term_)

		// NOTE(gukele): 搅屎棍问题，当有一个节点发生网络分区的问题，他就不停的选举失败，term变得很大，当其重新连接到集群，会让大家以为新的一轮选举，但是它日志落后，不可能成为新的leader,最终它就成为一个搅屎棍，然后让集群整个term变大了

		// NOTE(gukele): 投出票才重置ticker，否则会出现不能成为leader的candidate轮流超时重新选举，导致本节点没有给它投票但是仍然重置了自己的超时时间。
		// rf.SetRoleAndTicker(RoleFollower) // 降为follower？可能是搅屎棍让leader降级；也可能是leader掉线后第一轮选举失败，candidate降级

		if rf.role_ == RoleLeader {
			rf.ResetTimeout()
		}
		rf.role_ = RoleFollower

		try_vote()
		return

	}

	if args.Term_ == rf.cur_term_ {

		// 没有投过票 或者 之前的reply丢包了需要重新发
		if rf.GetVotedFor() == -1 || rf.GetVotedFor() == args.Candidate_id_ {
			try_vote()
		} else {
			Debug(dVote, "S%v Not vote S%v, already vote to S%v", rf.me, args.Candidate_id_, rf.GetVotedFor())
		}

	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) SendRequestVoteRPC(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		return false
	}

	// rpcTimer := time.NewTimer(electionTimeoutBase)
	// defer rpcTimer.Stop()

	// ch := make(chan bool, 1)

	// go func() {
	// 	if rf.peers[server].Call("Raft.RequestVoteRPC", args, reply) {
	// 		ch <- true
	// 	}
	// }()

	// select {
	// case <-rpcTimer.C:
	// 	return false
	// case <-ch:
	// 	return true
	// }

	ok := rf.peers[server].Call("Raft.RequestVoteRPC", args, reply)
	return ok
}

/*
 * 选举成功则会马上唤醒ticker
 */
func (rf *Raft) StartElection() {
	rf.mu.Lock()

	// 给自己投票
	rf.cur_term_ += 1
	rf.voted_for_ = rf.me
	rf.SetRoleAndTicker(RoleCandidate)
	rf.persistState()
	voted_count := 1 // 自己给自己的一票

	// NOTE(gukele):逃逸分析！ 局部栈上变量is ok,go支持逃逸分析，会将对象转为堆上对象。
	args := RequestVoteArgs{
		Term_:          rf.cur_term_,
		Candidate_id_:  rf.me,
		Last_log_id_:   rf.GetLastLogId(),
		Last_log_term_: rf.GetLastLogTerm(),
	}

	rf.ResetTimeout()

	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go rf.RequestVoteAndHandleReply(server, &args, &voted_count)
	}
}

func (rf *Raft) RequestVoteAndHandleReply(server int, args *RequestVoteArgs, voted_count *int) {
	reply := RequestVoteReply{}
	// NOTE(gukele): sending request note must not need lock,we want rf option is parallel
	if rf.SendRequestVoteRPC(server, args, &reply) {

		// handle request vote reply
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// log.Printf("%v get a vote result from %v in term %v \n", rf.me, server, args.Term_)
		if rf.role_ == RoleCandidate && rf.cur_term_ == args.Term_ {
			if reply.Term_ > rf.cur_term_ {
				rf.SetRoleAndTicker(RoleFollower)
				rf.SetTerm(reply.Term_)
			} else if reply.Vote_granted_ {
				*voted_count += 1
				Debug(dVote, "S%v <- S%v Got vote", rf.me, server)

				if *voted_count == len(rf.peers)/2+1 { // 只在第一次到达大多数时唤醒

					rf.SetRoleAndTicker(RoleLeader)

					// rf.mu.Unlock()
					// rf.BroadcastHeartBeat()
					// rf.mu.Lock()
				}
			}
		}
	}
}

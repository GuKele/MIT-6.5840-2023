package raft

import (
	"log"
	// "fmt"
	// "time"
)

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
	CandidateId_   int // 发起投票的candidate的ID
	Last_log_term_ int // candidate的最高日志条目的任期号
	LastLog_index_ int // candidate的最高日志条目索引.
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
// example RequestVote RPC handler.
// NOTE(gukele): handler!!!
//
func (this *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here
	this.mu.Lock()
	defer this.mu.Unlock()

	reply.Term_ = this.cur_term_ // maybe bigger than candidate‘s term，for candidate update himself
	reply.Vote_granted_ = false

	// 新的选举期了，不管你之前是否投过票，不管你是leader还是candidate,你现在需要重新投票了
	if args.Term_ > this.cur_term_ {
		this.voted_for_ = -1
		this.SetTerm(args.Term_)
		// NOTE(gukele): 搅屎棍问题，当有一个节点发生网络分区的问题，他就不停的选举失败，term变得很大，当其重新连接到集群，会让大家以为新的一轮选举，但是它日志落后，不可能成为新的leader,最终它就成为一个搅屎棍，然后让集群整个term变大了
		this.SetRole(RoleFollower) // 降为follower？可能是搅屎棍让leader降级；也可能是leader掉线后第一轮选举失败，candidate降级
		// TODO(gukele): 我们这里判断一下，只有重置了身份才reset？
		this.ResetTicker()
	}

	// 任期判断
	if args.Term_ < this.cur_term_ {
		return
	}
	if args.Term_ == this.cur_term_ {
		// 没有投过票 或者 之前的reply丢包了需要重新发
		if this.GetVotedFor() == -1 || this.GetVotedFor() == args.CandidateId_ {
			// Raft通过比较日志中最后一个条目的索引和任期来确定两个日志中哪个是最新(up-to-date)。
			// 1 如果日志中的最后一个条目具有不同的任期，则带有较新任期的日志将是最新的。
			// 2 如果日志以相同的任期结尾，则更大索引的日志是最新的。
			var last_log_term, last_log_index = this.GetLastTermAndIndex()
			if(args.Last_log_term_ > last_log_term || (args.Last_log_term_ == last_log_term && args.LastLog_index_ >= last_log_index)) {
				// 投票后重置自己的election timeout，防止马上自己又升级candidate又开始新选举。只要自己的票投不出去，自己又收不到心跳/日志更新，那么自己就可能变candidate
				this.ResetTicker()
				reply.Vote_granted_ = true;
				// log.Printf("%v vote %v in term %v \n", this.me, args.CandidateId_, args.Term_)
				this.SetVotedFor(args.CandidateId_)
				return
			}
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
func (this *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if this.killed() {
		return false
	}

	ok := this.peers[server].Call("Raft.RequestVote", args, reply)
	// 这里是一直给一个节点发请求投票，如果一直收不到，那么就无限重复？我觉得应该是，身份改变以后就不发了，无论是变成leader还是follower
	for !ok {
		if this.killed() {
			return false
		}
		ok = this.peers[server].Call("Raft.RequestVote", args, reply)
	}

	return ok
}

/*
 * 选举成功则会马上唤醒ticker
 */
func (this *Raft) StartElection() {
	// this.ResetTicker()

	// 给自己投票
	this.cur_term_ += 1
	log.Printf("%v begin election in term %v \n", this.me, this.cur_term_)
	this.voted_for_ = this.me
	voted_count := 1 // 自己给自己的一票

	args := RequestVoteArgs{
		Term_:          this.cur_term_,
		CandidateId_:   this.me,
		LastLog_index_: this.GetLastLogIndex(),
		Last_log_term_: this.GetLastLogTerm(),
	}

	for idx := range this.peers {
		if idx == this.me {
			continue
		}

		// go的lambda直接引用捕获，而且无需关心捕获变量的生存周期,想要实现按值捕获就用参数列表
		go func (server int)  {
			reply := RequestVoteReply{}
			// FIXME(gukele): 如果拉票操作丢包等等，需要重传吗？
			// NOTE(gukele):sending request note must not need lock,we want this option is parallel
			if this.sendRequestVote(server, &args, &reply) {
				this.mu.Lock()
				defer this.mu.Unlock()
				// log.Printf("%v get a vote result from %v in term %v \n", this.me, server, args.Term_)
				if this.role_ == RoleCandidate && this.cur_term_ == args.Term_ {
					if reply.Term_ > this.cur_term_ {
						this.SetRole(RoleFollower)
						this.SetTerm(reply.Term_)
						this.SetVotedFor(-1)
						this.ResetTicker()
					} else if reply.Vote_granted_ {
						// log.Printf("%v get a vote from %v in term %v \n", this.me, server, args.Term_)
						voted_count += 1
						// log.Printf("voted count is %v now, and half is %v\n", voted_count, len(this.peers) / 2 + 1)
						if voted_count == len(this.peers) / 2 + 1 { // 只在第一次到达大多数时唤醒
							log.Printf("============We get a leader %v in term %v=========== \n", this.me, this.cur_term_)
							this.SetRole(RoleLeader)
							log.Printf("%v broadcast heart beat immediately \n", this.me)
							this.BroadcastHeartBeat()
							this.ResetHeartBeat()
						}
					}
				}
			}
		}(idx)
	}

	// half := (this.GetSize() + 1) / 2
	// for this.GetRole() == RoleCandidate {
	// 	granted := <- ch
	// 	if granted {
	// 		voted_count += 1
	// 	} else {
	// 		negative_vote_count += 1
	// 	}

	// 	if(voted_count >= half) {
	// 		this.SetRole(RoleLeader)
	// 		this.ResetHeartBeat()
	// 		term, _ := this.GetState()

	// 		log.Printf("%v is leader in term %v\n", this.me, term)
	// 		// log.Printf("we get a leader! \n")
	// 	}

	// 	if(negative_vote_count >= half) {
	// 		this.SetRole(RoleFollower)
	// 		this.ResetTicker()
	// 	}
	// }
	// return this.GetRole() == RoleFollower
}

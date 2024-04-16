package raft

func (rf *Raft) Replicator(server int) {
	// condition variant中的锁应该是保护临界区资源的，条件变量本身应该是不需要锁的，应该是和c++一样，但是还是wait会释放锁，所以条件变量中的锁不能为nil，也必须在wait前进行lock。
	rf.replicator_cv_[server].L.Lock()
	defer rf.replicator_cv_[server].L.Unlock()

	for !rf.killed() {
		// FIXME(gukele): 之前循环语句是不需要复制时wait，这样好像导致传播慢，而且不知道怎么回事有时候传两下没有收到回复就不传了\
		// FIXME(gukele): 当节点掉线后会无线重发，希望可以改进
		for rf.needReplicating(server) && !rf.killed() {
			// send one round append
			rf.AppendEntriesOneRound(server)
		}
		rf.replicator_cv_[server].Wait()
	}
}

// 返回是否需要复制，是否复制快照。
func (rf *Raft) needReplicating(server int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	ok := rf.role_ == RoleLeader && rf.match_id_[server] < rf.GetLastLogId()

	return ok
}

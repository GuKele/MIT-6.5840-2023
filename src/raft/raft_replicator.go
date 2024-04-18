package raft

func (rf *Raft) Replicator(server int) {
	// condition variant中的锁应该是保护临界区资源的，条件变量本身应该是不需要锁的，应该是和c++一样，但是还是wait会释放锁，所以条件变量中的锁不能为nil，也必须在wait前进行lock。
	rf.replicator_cv_[server].L.Lock()
	defer rf.replicator_cv_[server].L.Unlock()

	for !rf.killed() {
		// NOTE(gukele): 当节点掉线后会无限重发，rpc超时重传次数超过最大阈值，认为peer掉线，暂停日志添加，当收到该peer心跳回复后cv唤醒replicator继续日志添加。
		cnt := 0
		for rf.needReplicating(server) && !rf.killed() && cnt < MaxRetries {
			// send one round append
			if !rf.AppendEntriesOneRound(server) {
				cnt++
			}
		}
		rf.replicator_cv_[server].Wait()
	}
}

// 返回是否需要复制，是否复制快照。
func (rf *Raft) needReplicating(server int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	ok := (rf.role_ == RoleLeader && rf.match_id_[server] < rf.GetLastLogId())

	return ok
}

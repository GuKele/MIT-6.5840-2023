// Code generated by "stringer -type=Role"; DO NOT EDIT.

package raft

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[RoleLeader-0]
	_ = x[RoleFollower-1]
	_ = x[RoleCandidate-2]
}

const _Role_name = "RoleLeaderRoleFollowerRoleCandidate"

var _Role_index = [...]uint8{0, 10, 22, 35}

func (i Role) String() string {
	if i < 0 || i >= Role(len(_Role_index)-1) {
		return "Role(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _Role_name[_Role_index[i]:_Role_index[i+1]]
}

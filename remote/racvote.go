package remote

import "github.com/allvphx/RAC/rlsm"

type RACVote struct {
	TID        int
	Level      rlsm.Level
	From       string
	VoteCommit bool
}

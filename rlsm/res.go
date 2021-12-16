package rlsm

import (
	"fmt"
	"github.com/allvphx/RAC/utils"
	"sync"
)

type KvRes struct { // No lost res, it is handled at the DBMS.
	TID        int
	VoteCommit bool //VoteCommit if the KV voted to commit.
	IsCommit   bool //IsCommit if the KV shard decide to commit,
}

func NewKvRes(id int) *KvRes {
	res := &KvRes{
		TID: id,
	}
	res.Clear()
	return res
}

func (c *KvRes) Committed() bool {
	return c.IsCommit
}

func (c *KvRes) Clear() {
	c.IsCommit = false
	c.VoteCommit = false
}

func (c *KvRes) SetSelfResult(vote bool, commit bool) {
	c.IsCommit = commit
	c.VoteCommit = vote
}

//KvResMakeLost make a lost item with decision
func KvResMakeLost(decision bool) *KvRes {
	return &KvRes{
		TID:        0,
		VoteCommit: decision,
		IsCommit:   decision,
	}
}

//KvResult the result container for the level state manager
type KvResult struct {
	mu           *sync.Mutex
	ips          int
	NShard       int
	KvIDs        []int
	ProtocolRes  []*KvRes
	notCrashed   []bool
	voteCommit   int
	decideCommit int
	crashedCnt   int
}

func (c *KvResult) String() string {
	return fmt.Sprintf("Result - ips:[%d];N:[%d];VoteCommit:[%d];decideCommit:[%d]", c.ips,
		c.NShard, c.voteCommit, c.decideCommit)
}

func NewKvResult(nShard int) *KvResult {
	res := &KvResult{}
	res.Init(nShard)
	return res
}

//Init initialize the mockkv result
func (re *KvResult) Init(nShard int) {
	re.NShard = nShard
	re.voteCommit = 0
	re.decideCommit = 0
	re.crashedCnt = 0
	re.ProtocolRes = make([]*KvRes, nShard)
	re.notCrashed = make([]bool, nShard)
	re.KvIDs = make([]int, nShard)
	re.ips = 0
	re.mu = &sync.Mutex{}
}

// CanCommit4L2: check the Special case mentioned in paper.
func (re *KvResult) CanCommit4L2() bool {
	return re.decideCommit == re.ips && re.ips > 1
}

func (re *KvResult) DecideAllCommit() bool {
	return re.NShard == re.decideCommit
}

//AppendFinished check if we have finished appending the results from all shards.
func (re *KvResult) AppendFinished() bool {
	return re.NShard == re.ips
}

//Append append a KvRes entry to the result.
func (re *KvResult) Append(res *KvRes) bool {
	re.mu.Lock()
	defer re.mu.Unlock()
	i := re.ips
	if i >= re.NShard {
		return utils.Assert(false, "append in KvRes reaches out of limit")
	}
	// It is not maintained now.
	re.KvIDs[i] = res.TID
	re.ProtocolRes[i] = res
	if res.TID != 0 {
		re.notCrashed[i] = true
	}
	if res.IsCommit {
		re.decideCommit++
	}
	if res.VoteCommit {
		re.voteCommit++
	}
	re.ips++
	return true
}

//Correct return if the cohorts work correctly.
func (re *KvResult) Correct() bool {
	return re.decideAllCommit() || re.decideCommit == 0
}

//VoteAllCommit return if all the shards decide to commit.
func (re *KvResult) VoteAllCommit() bool {
	return re.NShard == re.voteCommit
}

func (re *KvResult) voteSomeCommit() bool {
	return !re.VoteAllCommit() && re.voteCommit > 0
}

//decideAllCommit return if all the shards decide to commit.
func (re *KvResult) decideAllCommit() bool {
	return re.NShard == re.decideCommit
}

func (re *KvResult) decideSomeCommit() bool {
	return !re.decideAllCommit() && re.decideCommit > 0
}

func (re *KvResult) detectCrashFailure(shards []string) map[string]bool {
	re.crashedCnt = 0
	res := make(map[string]bool)
	for i, p := range re.notCrashed {
		res[shards[i]] = !p
		if !p {
			re.crashedCnt++
		}
	}
	return res
}

// Analysis analysis the result of an atomic commit (bool CrashFailure, bool NetworkFailure, error).
// Property 4.5 (All broadcast) assumed to be held.
// The crash failure should be handled in collaborator before !!!.
func (re *KvResult) Analysis(shards []string, level Level) (map[string]bool, bool) {
	crashFailure := re.detectCrashFailure(shards)
	if level == NoCFNoNF {
		if re.decideSomeCommit() && re.voteSomeCommit() {
			if re.crashedCnt+re.decideCommit != re.NShard {
				return crashFailure, true
			}
		}
	}
	if level == CFNoNF {
		if re.VoteAllCommit() && !re.decideAllCommit() {
			return crashFailure, true
		} else if re.voteSomeCommit() && re.decideSomeCommit() && re.crashedCnt+re.decideCommit != re.NShard {
			return crashFailure, true
		}
	}
	return crashFailure, false
}

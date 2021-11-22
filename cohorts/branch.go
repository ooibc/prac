package cohorts

import (
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/mockkv"
	"github.com/allvphx/RAC/opt"
	"github.com/allvphx/RAC/remote"
	"github.com/allvphx/RAC/rlsm"
	"github.com/allvphx/RAC/utils"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// cohortBranch is used to handle one specific transaction.
// branch does not need to thread safe, since it will be executed one by one.
type cohortBranch struct {
	Kv       *mockkv.Shard
	Res      *rlsm.KvRes
	finished bool
	Vote     bool
	from     *CohortManager
}

func newCohortBranch(id int, kv *mockkv.Shard, manager *CohortManager) *cohortBranch {
	res := &cohortBranch{
		Kv:       kv,
		finished: false,
		Res:      rlsm.NewKvRes(id),
		from:     manager,
		Vote:     false,
	}
	return res
}

func (c *CohortManager) exist(TID int) bool {
	c.PoolLocks[TID].Lock()
	defer c.PoolLocks[TID].Unlock()
	return c.Pool[TID] != nil
}

// preRead4Opt the thread-safe pre read for each opt.
func (c *cohortBranch) preRead4Opt(ips string, opt opt.RACOpt, tx *remote.RACTransaction, mu *sync.Mutex, received *int, values *map[string]int) {
	mu.Lock()
	if *received == -1 {
		mu.Unlock()
		return
	}
	mu.Unlock()
	if val, ok := c.Kv.ReadTxn(tx.TxnID, opt.Key); !ok || tx.It != opt.Shard {
		mu.Lock()
		*received = -1
		mu.Unlock()
	} else {
		mu.Lock()
		(*values)[ips] = val
		if *received != -1 {
			*received++
		}
		mu.Unlock()
	}
}

// PreRead handles the read and return a map containing results.
func (c *cohortBranch) PreRead(tx *remote.RACTransaction) (bool, map[string]int) {
	values := make(map[string]int)
	result := make(map[string]int)
	valMutex := &sync.Mutex{}
	received := 0
	if !c.Kv.Begin(tx.TxnID) {
		// fail to begin for the incorrect txnID.
		return false, values
	}
	for _, v := range tx.OptList {
		switch v.Cmd {
		case opt.ReadOpt:
			go c.preRead4Opt(utils.Hash(v.Shard, v.Key), v, tx, valMutex, &received, &values)
		default:
			return false, result
		}
	}
	for st := time.Now(); time.Since(st) < c.from.stmt.timeoutForLocks+constants.OptEps; {
		valMutex.Lock()
		if received == -1 {
			valMutex.Unlock()
			return false, result
		}
		if received == len(tx.OptList) {
			result = values
			valMutex.Unlock()
			return true, result
		}
		valMutex.Unlock()
		time.Sleep(time.Millisecond)
	}
	return false, result
}

// forTestPreRead is the test version of pre read.
func (c *cohortBranch) forTestPreRead(tx *remote.RACTransaction) (bool, map[string]int) {
	values := make(map[string]int)
	valMutex := &sync.Mutex{}
	received := 0
	// piggyback message
	c.Kv.Begin(tx.TxnID)
	for _, v := range tx.OptList {
		switch v.Cmd {
		case opt.ReadOpt:
			go c.preRead4Opt(utils.Hash(v.Shard, v.Key), v, tx, valMutex, &received, &values)
		default:
			return false, values
		}
	}
	for st := time.Now(); time.Since(st) < c.from.stmt.timeoutForLocks; {
		valMutex.Lock()
		if received == -1 {
			valMutex.Unlock()
			return false, values
		}
		if received == len(tx.OptList) {
			valMutex.Unlock()
			return true, values
		}
		valMutex.Unlock()
		time.Sleep(time.Millisecond)
	}
	return false, values
}

// forTestPreReadSingleThread is the test version of pre read.
func (c *cohortBranch) forTestPreReadSingleThread(tx *remote.RACTransaction) (bool, []int) {
	values := make([]int, 0)
	// piggyback message
	c.Kv.Begin(tx.TxnID)
	for _, v := range tx.OptList {
		switch v.Cmd {
		case opt.ReadOpt:
			if val, ok := c.Kv.ReadTxn(tx.TxnID, v.Key); !ok || tx.It != v.Shard {
				return false, values
			} else {
				values = append(values, val)
			}
		default:
			return false, values
		}
	}
	return true, values
}

// preRead4Opt the thread-safe pre read for each opt.
func (c *cohortBranch) preUpdate4Opt(opt opt.RACOpt, tx *remote.RACTransaction, received *int32) {
	if atomic.LoadInt32(received) == -1 {
		return
	}
	if ok := c.Kv.UpdateTxn(tx.TxnID, opt.Key, opt.Value); !utils.Warn(ok && tx.It == opt.Shard,
		"The UpdateTxn Opt miss") {
		atomic.StoreInt32(received, -1)
	} else {
		atomic.AddInt32(received, 1)
	}
}

// preWrite checks if we can get all the locks to ensure ACID. concurrent write.
// the tx only contains the write operations, the calculation should be done in manager.
func (c *cohortBranch) preWrite(tx *remote.RACTransaction) bool {
	// piggyback message
	var received int32 = 0
	for _, v := range tx.OptList {
		switch v.Cmd {
		case opt.UpdateOpt:
			go c.preUpdate4Opt(v, tx, &received)
		default:
			atomic.StoreInt32(&received, -1)
			return false
		}
	}
	for st := time.Now(); time.Since(st) < c.from.stmt.timeoutForLocks; {
		recc := atomic.LoadInt32(&received)
		if recc == -1 {
			return false
		}
		if recc == int32(len(tx.OptList)) {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// Agree agreement phase for 3pc.
func (c *cohortBranch) Agree(TID int, isCommit bool) bool {
	c.from.PoolLocks[TID].Lock()
	defer c.from.PoolLocks[TID].Unlock()
	// either the txn is over, or the result does not match.
	if c.from.HaveDecided(TID) || c.Vote != isCommit {
		utils.DPrintf("3PC report : the agreement failed for " + strconv.Itoa(TID))
		return false
	}
	return true
}

func (c *cohortBranch) breakableSleep(duration time.Duration) bool {
	tp := 0
	for st := time.Now(); time.Now().Sub(st) < duration; {
		if tp%20 == 0 && !c.from.exist(c.Res.TID) { // add interval to help decrease contention
			return false
		}
		tp++
		time.Sleep(time.Millisecond)
	}
	return true
}

func (c *cohortBranch) breakableSleep4L1(tx *remote.RACTransaction, duration time.Duration) bool {
	defer utils.TimeTrack(time.Now(), "Sleep", tx.TxnID)
	tp := 0
	for st := time.Now(); time.Since(st) < duration; {
		if tp%20 == 0 && !c.from.exist(c.Res.TID) { // add interval to help decrease contention
			return false
		}
		if tp%20 == 0 && !c.from.checkCommit4L1(tx.TxnID) { // add interval to help decrease contention
			return true
		}
		tp++
		time.Sleep(time.Millisecond)
	}
	return true
}

func (c *cohortBranch) breakableSleep4L2(TID int, N int, duration time.Duration) bool {
	tp := 0
	for st := time.Now(); time.Now().Sub(st) < duration; {
		if tp%20 == 0 && !c.from.exist(c.Res.TID) { // add interval to help decrease contention
			return false
		}
		if tp%20 == 0 && c.from.checkCommit4L2(TID, N) { // add interval to help decrease contention
			return true
		}
		tp++
		time.Sleep(time.Millisecond)
	}
	return true
}

// Propose handle the propose phase of RAC.
func (c *cohortBranch) Propose(tx *remote.RACTransaction) *rlsm.KvRes {
	defer utils.TimeTrack(time.Now(), "Propose", tx.TxnID)
	utils.TPrintf("TXN" + strconv.Itoa(tx.TxnID) + ": " + c.from.stmt.cohortID + " Begin propose")
	if !c.preWrite(tx) {
		// if vote to abort, it will abort regardless of the Level.
		utils.TPrintf("TXN" + strconv.Itoa(tx.TxnID) + ": " + c.from.stmt.cohortID + " Abort for PreWrite")
		c.Res.SetSelfResult(false, false)
		c.from.broadCastVote(tx.TxnID, tx.ProtocolLevel, 0, tx.It, tx.Participants)
		go utils.Assert(c.from.Abort(tx), "Impossible case, abort failed with all locks")
	} else {
		if tx.ProtocolLevel == rlsm.CFNoNF {
			utils.DPrintf("TXN" + strconv.Itoa(tx.TxnID) + ": " + "Yes Voting From " + c.from.stmt.cohortID)
			c.from.broadCastVote(tx.TxnID, rlsm.CFNoNF, 1, tx.It, tx.Participants)
			if !c.breakableSleep4L2(tx.TxnID, len(tx.Participants), c.from.stmt.timeoutForMsgs+c.from.stmt.timeoutForLocks) {
				// Aborted
				c.Res.SetSelfResult(false, false)
				return c.Res
			}
			if c.from.checkCommit4L2(tx.TxnID, len(tx.Participants)) {
				c.Res.SetSelfResult(true, true)
			} else {
				// the Abort is done locally.
				utils.TPrintf("TXN" + strconv.Itoa(tx.TxnID) + ": " + c.from.stmt.cohortID + " Abort for No vote")
				go utils.Assert(c.from.Abort(tx), "Impossible case, abort failed with all locks")
				c.Res.SetSelfResult(true, false)
			}
		} else if tx.ProtocolLevel == rlsm.NoCFNoNF {
			if !c.breakableSleep4L1(tx, c.from.stmt.timeoutForMsgs+constants.KvConcurrencyEps) {
				// Aborted
				c.Res.SetSelfResult(false, false)
				return c.Res
			}
			if c.from.checkCommit4L1(tx.TxnID) {
				c.Res.SetSelfResult(true, true)
			} else {
				// the Abort is done locally.
				go utils.Assert(c.from.Abort(tx), "Impossible case, abort failed with all locks")
				c.Res.SetSelfResult(true, false)
			}
		}
	}
	c.finished = true
	return c.Res
}

// PreWrite get locks and pre-write the changes to temporary logs.
// TODO: integrate read and write locks.
func (c *cohortBranch) PreWrite(tx *remote.RACTransaction) bool {
	defer utils.TimeTrack(time.Now(), "PreWrite", tx.TxnID)
	c.Vote = c.preWrite(tx)
	return c.Vote
}

// Commit commit the transaction branch.
func (c *cohortBranch) Commit(tx *remote.RACTransaction) bool {
	defer utils.TimeTrack(time.Now(), "Commit", tx.TxnID)
	return c.Kv.Commit(tx.TxnID)
}

// Abort abort the transaction branch.
func (c *cohortBranch) Abort(tx *remote.RACTransaction) bool {
	defer utils.TimeTrack(time.Now(), "Abort", tx.TxnID)
	return c.Kv.RollBack(tx.TxnID)
}

package collaborator

import (
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/opt"
	"github.com/allvphx/RAC/remote"
	"github.com/allvphx/RAC/rlsm"
	"github.com/allvphx/RAC/utils"
	"strconv"
	"time"
)

func (ra *DBTransaction) check42PC(isCheckSucceed bool, expectCommitted bool) bool {
	ra.from.LockPool[ra.TxnID].Lock()
	defer ra.from.LockPool[ra.TxnID].Unlock()
	state := ra.from.TxnState[ra.TxnID]
	if isCheckSucceed {
		return len(ra.from.MsgPool[ra.TxnID]) == len(ra.participants)
	} else {
		return state == Abnomal || (expectCommitted && state == Aborted)
	}
}

func (ra *DBTransaction) sendDecide42PC(server string, txn *remote.RACTransaction, isCommit bool) {
	if isCommit {
		ra.from.sendMsg(server, constants.COMMMIT, *txn)
	} else {
		ra.from.sendMsg(server, constants.ABORT, *txn)
	}
}

func (ra *DBTransaction) sendPreWrite42PC(server string, txn *remote.RACTransaction) {
	ra.from.sendMsg(server, constants.PREW, *txn)
}

// PreWrite The timeout is 3 x TimeOut to simulate the blocking
func (ra *DBTransaction) PreWrite42PC(txn *DBTransaction) bool {
	defer utils.TimeTrack(time.Now(), "PreWrite For 2PC", txn.TxnID)
	branches := make(map[string]*remote.RACTransaction)
	for _, v := range ra.participants {
		branches[v] = remote.NewRACTransaction(txn.TxnID, v, rlsm.CFNF, ra.participants)
	}
	for _, v := range txn.OptList {
		switch v.Cmd {
		case opt.UpdateOpt:
			branches[v.Shard].AddUpdate(v.Shard, v.Key, v.Value)
		default:
			utils.Assert(false, "no write should be sent to the PreRead")
		}
	}
	//// build over, broadcast vote ////
	for i, op := range branches {
		go ra.sendPreWrite42PC(i, op)
	}
	// 3 resend
	// higher contention -> higher OptEps
	for st := time.Now(); time.Since(st) < 4*(2*txn.TimeOut+constants.OptEps+constants.ConcurrencyEps+ra.from.LockTimeOutBound); {
		if ra.check42PC(false, true) {
			return false
		}
		if ra.check42PC(true, true) {
			return true
		}
		time.Sleep(1 * time.Millisecond)
	}
	return false
}

func (ra *DBTransaction) decide42PC(txn *DBTransaction, isCommit bool) bool {
	branches := make(map[string]*remote.RACTransaction)
	for _, v := range ra.participants {
		branches[v] = remote.NewRACTransaction(txn.TxnID, v, rlsm.CFNF, ra.participants)
	}
	for _, v := range txn.OptList {
		switch v.Cmd {
		case opt.UpdateOpt:
			branches[v.Shard].AddUpdate(v.Shard, v.Key, v.Value)
		default:
			utils.Assert(false, "no write should be sent to the PreRead")
		}
	}
	//// build over, broadcast vote ////
	for i, op := range branches {
		go ra.sendDecide42PC(i, op, isCommit)
	}
	for st := time.Now(); time.Since(st) < (2*txn.TimeOut + constants.OptEps + constants.ConcurrencyEps); {
		if ra.check42PC(false, false) {
			return false
		}
		if ra.check42PC(true, false) {
			return true
		}
		time.Sleep(1 * time.Millisecond)
	}
	return false
}

// Decide42PC process the decide phase for the 2PC. It will be blocked until finished.
// only 2PC decide would cause error, since others can just be regarded as
func (ra *DBTransaction) Decide42PC(txn *DBTransaction, isCommit bool) bool {
	count := 0
	for count <= constants.UniverseRetryCount { // to avoid the deadlock.
		if ra.decide42PC(txn, isCommit) {
			return true
		}
		count++
	}
	//	utils.Assert(false, "the 2PC retry for too much times in decide")
	return false
}

// TwoPCSubmit submit the write only transaction to the KVs with 2PC.
func (ra *DBTransaction) TwoPCSubmit(read *DBTransaction, write *DBTransaction) bool {
	if !ra.from.CheckAndChange(ra.TxnID, PreRead, PreWrite) {
		return false
	}
	ok := ra.PreWrite42PC(write) // blocking 2 delays.
	if !ok {
		utils.DPrintf("Txn" + strconv.Itoa(ra.TxnID) + ": failed at pre-write")
		if !ra.from.CheckAndChange(ra.TxnID, PreWrite, Aborted) {
			ra.Decide42PC(write, false) // blocking 2 delays.
			return false
		}
	} else if !ra.from.CheckAndChange(ra.TxnID, PreWrite, Committed) {
		ra.Decide42PC(write, false) // blocking 2 delays.
		return false
	}

	ok = ok && ra.Decide42PC(write, ok) // blocking 2 delays.
	return ok
}

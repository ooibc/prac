package collaborator

import (
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/remote"
	"github.com/allvphx/RAC/rlsm"
	"github.com/allvphx/RAC/utils"
	"strconv"
	"time"
)

// Here is a centralized version of PAC. We fix the leader to header node.
// PAC comes to this version if one node is always selected as the leader.

func (ra *DBTransaction) check4PAC(isCheckSucceed bool) bool {
	ra.from.LockPool[ra.TxnID].Lock()
	defer ra.from.LockPool[ra.TxnID].Unlock()
	state := ra.from.TxnState[ra.TxnID]
	if isCheckSucceed {
		// majority
		return len(ra.from.MsgPool[ra.TxnID]) >= (len(ra.participants)+1)/2
	} else {
		return state == Abnomal || state == Aborted
	}
}

func (ra *DBTransaction) FTAgree4PAC(txn *DBTransaction, isCommit bool) bool {
	defer utils.TimeTrack(time.Now(), "3PC Agree", txn.TxnID)
	branches := make(map[string]*remote.RACTransaction)
	for _, v := range ra.participants {
		branches[v] = remote.NewRACTransaction(txn.TxnID, v, rlsm.CFNF, ra.participants)
	}
	//// build over, broadcast vote ////
	for i, op := range branches {
		// Same agreement message reused from 3PC.
		go ra.sendAgree43PC(i, op, isCommit)
	}
	utils.DPrintf("begin send agree")
	for st := time.Now(); time.Since(st) < 2*txn.TimeOut+constants.OptEps+constants.ConcurrencyEps; {
		if ra.check4PAC(false) {
			return false
		}
		if ra.check4PAC(true) {
			return true
		}
		time.Sleep(1 * time.Millisecond)
	}
	return false
}

// PACSubmit submit the write only transaction to the KVs with PAC.
func (ra *DBTransaction) PACSubmit(read *DBTransaction, write *DBTransaction) bool {
	if !ra.from.CheckAndChange(ra.TxnID, PreRead, PreWrite) {
		return false
	}
	// since no leader is changed, no response would come with Decision = true or AcceptVal.
	// the LE + LD phase of PAC degenerate to the same as 3PC.
	ok := ra.PreWrite43PC(write) // non-blocking 2 delays.
	if !ok {
		utils.DPrintf("Txn" + strconv.Itoa(ra.TxnID) + ": failed at pre-write")
		if !ra.from.CheckAndChange(ra.TxnID, PreWrite, AgAborted) {
			ra.Decide42PC(write, false) // blocking 2 delays.
			return false
		}
	} else if !ra.from.CheckAndChange(ra.TxnID, PreWrite, AgCommitted) {
		ra.Decide42PC(write, false) // blocking 2 delays.
		return false
	}

	ok = ra.FTAgree4PAC(write, ok)
	if !ok {
		utils.DPrintf("Txn" + strconv.Itoa(ra.TxnID) + ": failed at agree")
		if !ra.from.CheckAndChange(ra.TxnID, AgAborted, Aborted) {
			ra.Decide43PC(write, false) // blocking 2 delays.
			return false
		}
	} else if !ra.from.CheckAndChange(ra.TxnID, AgCommitted, Committed) {
		ra.Decide43PC(write, false) // blocking 2 delays.
		return false
	}
	// As mentioned in Paper, the decision is sent asynchronously, but will inform the decision at once.
	go ra.Decide42PC(write, ok)
	return ok
}

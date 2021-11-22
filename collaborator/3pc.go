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

func (ra *DBTransaction) check43PC(isCheckSucceed bool, expectCommitted bool) bool {
	ra.from.LockPool[ra.TxnID].Lock()
	defer ra.from.LockPool[ra.TxnID].Unlock()
	state := ra.from.TxnState[ra.TxnID]
	if isCheckSucceed {
		return len(ra.from.MsgPool[ra.TxnID]) == len(ra.participants)
	} else {
		return state == Abnomal || (expectCommitted && state == Aborted)
	}
}

func (ra *DBTransaction) sendAgree43PC(server string, txn *remote.RACTransaction, isCommit bool) {
	if isCommit {
		ra.from.sendMsg(server, constants.AGRC3PC, *txn)
	} else {
		ra.from.sendMsg(server, constants.AGRA3PC, *txn)
	}
}

func (ra *DBTransaction) sendDecide43PC(server string, txn *remote.RACTransaction, isCommit bool) {
	if isCommit {
		ra.from.sendMsg(server, constants.COMMMIT, *txn)
	} else {
		ra.from.sendMsg(server, constants.ABORT, *txn)
	}
}

func (ra *DBTransaction) sendPreWrite43PC(server string, txn *remote.RACTransaction) {
	ra.from.sendMsg(server, constants.PREW, *txn)
}

func (ra *DBTransaction) PreWrite43PC(txn *DBTransaction) bool {
	defer utils.TimeTrack(time.Now(), "3PC PreWrite", txn.TxnID)
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
		go ra.sendPreWrite43PC(i, op)
	}
	for st := time.Now(); time.Since(st) < 2*txn.TimeOut+constants.OptEps+constants.ConcurrencyEps+ra.from.LockTimeOutBound; {
		if ra.check43PC(false, true) {
			return false
		}
		if ra.check43PC(true, true) {
			return true
		}
		time.Sleep(1 * time.Millisecond)
	}
	return false
}

func (ra *DBTransaction) Agree43PC(txn *DBTransaction, isCommit bool) bool {
	defer utils.TimeTrack(time.Now(), "3PC Agree", txn.TxnID)
	branches := make(map[string]*remote.RACTransaction)
	for _, v := range ra.participants {
		branches[v] = remote.NewRACTransaction(txn.TxnID, v, rlsm.CFNF, ra.participants)
	}
	//// build over, broadcast vote ////
	for i, op := range branches {
		go ra.sendAgree43PC(i, op, isCommit)
	}
	utils.DPrintf("begin send agree")
	for st := time.Now(); time.Since(st) < 2*txn.TimeOut+constants.OptEps+constants.ConcurrencyEps; {
		if ra.check43PC(false, true) {
			return false
		}
		if ra.check43PC(true, true) {
			return true
		}
		time.Sleep(1 * time.Millisecond)
	}
	return false
}

// Decide42PC process the decide phase for the .
func (ra *DBTransaction) Decide43PC(txn *DBTransaction, isCommit bool) bool {
	defer utils.TimeTrack(time.Now(), "3PC Decide", txn.TxnID)
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
		go ra.sendDecide43PC(i, op, isCommit)
	}
	// Here the lock release requires a lock, hence 2 opt Eps.
	for st := time.Now(); time.Since(st) < 2*txn.TimeOut+constants.OptEps+constants.ConcurrencyEps; {
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

// TwoPCSubmit submit the write only transaction to the KVs with 2PC.
func (ra *DBTransaction) ThreePCSubmit(read *DBTransaction, write *DBTransaction) bool {
	if !ra.from.CheckAndChange(ra.TxnID, PreRead, PreWrite) {
		return false
	}
	ok := ra.PreWrite43PC(write) // blocking 2 delays.
	if !ok {
		utils.DPrintf("Txn" + strconv.Itoa(ra.TxnID) + ": failed at pre-write")
		if !ra.from.CheckAndChange(ra.TxnID, PreWrite, AgAborted) {
			ra.Decide43PC(write, false) // blocking 2 delays.
			return false
		}
	} else if !ra.from.CheckAndChange(ra.TxnID, PreWrite, AgCommitted) {
		ra.Decide43PC(write, false) // blocking 2 delays.
		return false
	}

	ok = ra.Agree43PC(write, ok) // blocking 2 delays.
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
	ra.Decide43PC(write, ok) // It does not matter if the decide is finished since the agree is correct.
	return ok
}

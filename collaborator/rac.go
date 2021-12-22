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

func (ra *DBTransaction) checkRACF(isCorrect bool) bool {
	ra.from.LockPool[ra.TxnID].Lock()
	defer ra.from.LockPool[ra.TxnID].Unlock()
	state := ra.from.TxnState[ra.TxnID]
	if !isCorrect {
		return state == Abnomal
	} else {
		return len(ra.from.MsgPool[ra.TxnID]) == len(ra.participants)
	}
}

func (ra *DBTransaction) check4Prop(isCorrect bool) bool {
	ra.from.LockPool[ra.TxnID].Lock()
	defer ra.from.LockPool[ra.TxnID].Unlock()
	state := ra.from.TxnState[ra.TxnID]
	if !isCorrect {
		return state != Propose
	} else {
		return len(ra.from.MsgPool[ra.TxnID]) == len(ra.participants)
	}
}

func (ra *DBTransaction) sendPropose(server string, txn *remote.RACTransaction) {
	ra.from.sendMsg(server, constants.PROP, *txn)
}

func (ra *DBTransaction) sendDecide4RAC(server string, txn *remote.RACTransaction, isCommit bool) {
	if isCommit {
		ra.from.sendMsg(server, constants.COMMMIT, *txn)
	} else {
		ra.from.sendMsg(server, constants.ABORT, *txn)
	}
}

func (ra *DBTransaction) RACPropose(txn *DBTransaction, lev rlsm.Level) *rlsm.KvResult {
	defer utils.TimeTrack(time.Now(), "RAC Propose", txn.TxnID)
	branches := make(map[string]*remote.RACTransaction)
	for _, v := range ra.participants {
		branches[v] = remote.NewRACTransaction(txn.TxnID, v, lev, ra.participants)
	}
	for _, v := range txn.OptList {
		switch v.Cmd {
		case opt.UpdateOpt:
			branches[v.Shard].AddUpdate(v.Shard, v.Key, v.Value)
		default:
			utils.Assert(false, "no write should be sent to the PreRead")
		}
	}
	for i, op := range branches {
		ra.sendPropose(i, op)
	}
	for st := time.Now(); time.Since(st) < (2*txn.TimeOut + constants.MsgUpperBound4RAC + constants.ConcurrencyEps + constants.OptEps + ra.from.LockTimeOutBound + constants.KvConcurrencyEps); {
		if ra.check4Prop(true) {
			break
		}
		if ra.check4Prop(false) {
			// Unexpected error.
			return nil
		}
		time.Sleep(1 * time.Millisecond)
	}
	N := len(ra.participants)
	ans := rlsm.NewKvResult(N)
	ra.from.LockPool[ra.TxnID].Lock()
	for _, v := range ra.from.MsgPool[ra.TxnID] {
		tmp := v.(*rlsm.KvRes)
		ok := ans.Append(tmp)
		if !utils.Assert(ok, "The append is caught with failure") {
			return nil
		}
	}
	lostvt := ans.CanCommit4L2()
	rem := N - len(ra.from.MsgPool[ra.TxnID])
	for rem > 0 {
		ans.Append(rlsm.KvResMakeLost(lostvt))
		rem--
	}
	ra.from.LockPool[ra.TxnID].Unlock()
	return ans
}

func (ra *DBTransaction) Decide4RAC(txn *DBTransaction, isCommitted bool) bool {
	defer utils.TimeTrack(time.Now(), "RAC Decide", txn.TxnID)
	branches := make(map[string]*remote.RACTransaction)
	for _, v := range ra.participants {
		branches[v] = remote.NewRACTransaction(txn.TxnID, v, rlsm.CFNF, ra.participants)
	}
	//// build over, broadcast vote ////
	for i, op := range branches {
		go ra.sendDecide4RAC(i, op, isCommitted)
	}

	for st := time.Now(); time.Since(st) < txn.TimeOut+constants.OptEps; {
		if ra.checkRACF(false) {
			return false
		}
		if ra.checkRACF(true) {
			return true
		}
		time.Sleep(1 * time.Millisecond)
	}
	return false
}

// RACSubmit submit the write only transaction to the KVs with level 1 ~ 2.
func (ra *DBTransaction) RACSubmit(read *DBTransaction, write *DBTransaction) bool {
	if !ra.from.CheckAndChange(ra.TxnID, PreRead, Propose) {
		return false
	}
	lev := ra.from.Lsm.Start(write.participants)

	if lev == rlsm.CFNF {
		utils.CheckError(ra.from.Lsm.Finish(ra.participants, nil, lev))
		if !ra.from.CheckAndChange(ra.TxnID, Propose, PreRead) {
			return false
		}
		return ra.ThreePCSubmit(read, write)
	}
	result := ra.RACPropose(write, lev)
	if result == nil {
		println("nil recived")
		return false
	}
	err := ra.from.Lsm.Finish(ra.participants, result, lev)
	if !utils.Assert(err == nil, "The RLSM finish caught with error.") {
		return false
	}

	ok := result.DecideAllCommit()
	if correctness := result.Correct(); !correctness {
		// trigger the blocking decide.
		utils.DPrintf("Txn" + strconv.Itoa(ra.TxnID) + ": failed with validation error")
		//// blocking commit ////
		utils.DPrintf("Txn" + strconv.Itoa(ra.TxnID) + ": failed at propose")
		// can optimize the send here.
		if !ra.from.CheckAndChange(ra.TxnID, Propose, Aborted) {
			ra.Decide42PC(write, false)
			return false
		}
		// this decide must be forced for the validation error
		ok = ok && ra.Decide42PC(write, ok)
		return ok
	}

	/// the unblocking way ///
	if !ok {
		utils.DPrintf("Txn" + strconv.Itoa(ra.TxnID) + ": failed at propose")
		if !ra.from.CheckAndChange(ra.TxnID, Propose, Aborted) {
			ra.Decide42PC(write, false) // blocking 2 delays.
		}
		// No need to send abort since the transaction has been all aborted.
		return false
	} else if !ra.from.CheckAndChange(ra.TxnID, Propose, Committed) {
		ra.Decide42PC(write, false) // blocking 2 delays.
		return false
	}

	// TODO:future work: stable log for commit here.
	ra.Decide4RAC(write, ok) // ok = commit
	return ok
}

package collaborator

import (
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/opt"
	"github.com/allvphx/RAC/remote"
	"github.com/allvphx/RAC/rlsm"
	"github.com/allvphx/RAC/utils"
	"strconv"
	"sync"
	"time"
)

type DBTransaction struct {
	TxnID        int
	OptList      []opt.RACOpt
	TimeOut      time.Duration
	participants []string
	from         *RACManager
}

func NewDBTransaction(TID int, duration time.Duration, parts []string, from *RACManager) *DBTransaction {
	return &DBTransaction{
		TxnID:        TID,
		OptList:      make([]opt.RACOpt, 0),
		TimeOut:      from.MsgTimeOutBound,
		participants: parts,
		from:         from,
	}
}

func (ra *DBTransaction) check4R(isCheckSucceed bool) bool {
	ra.from.LockPool[ra.TxnID].Lock()
	defer ra.from.LockPool[ra.TxnID].Unlock()
	state := ra.from.TxnState[ra.TxnID]
	if isCheckSucceed {
		return len(ra.from.MsgPool[ra.TxnID]) == len(ra.participants)
	} else {
		return state == Abnomal || state == Aborted
	}
}

func (ra *DBTransaction) sendPreRead(server string, txn *remote.RACTransaction) {
	ra.from.sendMsg(server, constants.PRER, *txn)
}

func mergeMap(lock *sync.Mutex, maps []interface{}) map[string]int {
	lock.Lock()
	defer lock.Unlock()
	res := make(map[string]int)
	for _, i := range maps {
		p := i.(*map[string]int)
		for x, y := range *p {
			res[x] = y
		}
	}
	return res
}

func (ra *DBTransaction) PreRead(txn *DBTransaction) (map[string]int, bool) {
	txn.Optimize()
	branches := make(map[string]*remote.RACTransaction)
	for _, v := range ra.participants {
		branches[v] = remote.NewRACTransaction(txn.TxnID, v, rlsm.CFNF, ra.participants)
	}
	for _, v := range txn.OptList {
		switch v.Cmd {
		case opt.ReadOpt:
			branches[v.Shard].AddRead(v.Shard, v.Key)
		default:
			utils.Assert(false, "no write should be sent to the PreRead")
		}
	}
	//// build over, broadcast vote ////
	for i, op := range branches {
		go ra.sendPreRead(i, op)
	}
	//// to simulate less blocking, we use 4 * operation timeout.
	for st := time.Now(); time.Since(st) < 2*(2*txn.TimeOut+constants.LockUpperBound+constants.ConcurrencyEps+constants.OptEps); {
		if ra.check42PC(false, true) {
			return nil, false
		}
		if ra.check42PC(true, true) {
			return mergeMap(ra.from.LockPool[ra.TxnID], ra.from.MsgPool[ra.TxnID]), true
		}
		time.Sleep(1 * time.Millisecond)
	}
	return nil, false
}

func (ra *DBTransaction) AddRead(shard string, key int) {
	ra.OptList = append(ra.OptList, opt.RACOpt{
		Shard: shard,
		Key:   key,
		Value: -1,
		Cmd:   opt.ReadOpt,
	})
}

func (ra *DBTransaction) AddUpdate(shard string, key int, value int) {
	ra.OptList = append(ra.OptList, opt.RACOpt{
		Shard: shard,
		Key:   key,
		Value: value,
		Cmd:   opt.UpdateOpt,
	})
}

func hashOpt(sd string, key, val, cmd int) string {
	return sd + ";" + strconv.Itoa(key) + ";" + strconv.Itoa(val) + ";" + strconv.Itoa(cmd)
}

// Optimize only the later write for the same location would remain.
func (ra *DBTransaction) Optimize() {
	exist := make(map[string]bool)
	for i := len(ra.OptList) - 1; i >= 0; i-- {
		val := hashOpt(ra.OptList[i].Shard, ra.OptList[i].Key, ra.OptList[i].Value, ra.OptList[i].Cmd)
		if exist[val] {
			ra.OptList = append(ra.OptList[:i], ra.OptList[i+1:]...)
		} else {
			exist[val] = true
		}
	}
}

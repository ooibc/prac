package collaborator

import (
	"encoding/json"
	"github.com/allvphx/RAC/cohorts"
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/remote"
	"github.com/allvphx/RAC/rlsm"
	"github.com/allvphx/RAC/utils"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	// 2PC: PreRead -> PreWrite -> Committed/Aborted -> Finished
	// 3PC: PreRead -> PreWrite -> AgCommitted/AgAborted -> Committed/Aborted -> Finished
	// RAC: PreRead -> Propose -> Committed/Aborted -> Finished
	None        = 0
	PreRead     = 1
	PreWrite    = 2
	Committed   = 3
	Aborted     = 4
	AgCommitted = 5
	Abnomal     = 6
	Finished    = 7
	AgAborted   = 8
	Propose     = 9
	ThreePC     = "3PC"
	TwoPC       = "2PC"
	RobAdapC    = "RAC"
	CPAC        = "C-PAC"
)

type RACManager struct {
	MsgTimeOutBound  time.Duration
	LockTimeOutBound time.Duration
	VoteTimeOutBound time.Duration
	Lsm              *rlsm.LevelStateManager
	Cohorts          []string

	connections []*net.Conn
	connLocks   []*sync.Mutex
	revMap      map[string]int

	TxnState []int32
	MsgPool  [][]interface{}
	LockPool []*sync.Mutex
	// the msg pool is cleared every time change the state.
	// So that the DBTransaction can judge if the message is received by locking at the MsgPool
}

func NewRACManager(timeout time.Duration, parts []string) *RACManager {
	res := &RACManager{
		MsgTimeOutBound:  constants.MsgUpperBound,
		Lsm:              rlsm.NewLSMManger(parts),
		Cohorts:          parts,
		connections:      make([]*net.Conn, 0),
		TxnState:         make([]int32, cohorts.MaxTxnID),
		MsgPool:          make([][]interface{}, cohorts.MaxTxnID),
		LockPool:         make([]*sync.Mutex, cohorts.MaxTxnID),
		LockTimeOutBound: constants.LockUpperBound,
		VoteTimeOutBound: constants.LockUpperBound + constants.MsgUpperBound + constants.OptEps,
		revMap:           make(map[string]int),
	}
	res.connLocks = make([]*sync.Mutex, 0)
	for i, v := range res.Cohorts {
		res.connLocks = append(res.connLocks, &sync.Mutex{})
		res.connections = append(res.connections, nil)
		res.revMap[v] = i
	}
	for i := 0; i < cohorts.MaxTxnID; i++ {
		res.LockPool[i] = &sync.Mutex{}
	}
	return res
}

func (c *RACManager) start(txnID int) {
	c.TxnState[txnID] = 0
	c.MsgPool[txnID] = make([]interface{}, 0)
}

func (c *RACManager) release(TID int) {
	c.TxnState[TID] = 0
	c.MsgPool[TID] = nil
}

func (c *RACManager) toAbnomal(TID int) {
	c.TxnState[TID] = Abnomal
	c.release(TID)
}

func (c *RACManager) handleRAC(TID int, val *rlsm.KvRes) {
	c.LockPool[TID].Lock()
	defer c.LockPool[TID].Unlock()
	if c.TxnState[TID] != Propose {
		c.toAbnomal(TID)
		return
	}
	c.MsgPool[TID] = append(c.MsgPool[TID], val)
}

func (c *RACManager) handleACK(TID int, mark string, ok bool) {
	c.LockPool[TID].Lock()
	defer c.LockPool[TID].Unlock()
	state := c.TxnState[TID]
	if mark == constants.PREWACK {
		if state != PreWrite && state != Aborted {
			// it can be aborted by many one
			c.toAbnomal(TID)
			return
		}
		c.MsgPool[TID] = append(c.MsgPool[TID], ok)
		if !ok {
			c.TxnState[TID] = Aborted
		}
	} else if mark == constants.FINISH {
		if state != Committed && state != Aborted {
			// the error state
			c.toAbnomal(TID)
			return
		}
		if (!ok && state != Aborted) || (ok && state != Committed) {
			c.toAbnomal(TID)
			os.Exit(0)
			return
		}
		c.MsgPool[TID] = append(c.MsgPool[TID], ok)
	} else if mark == constants.INFO3PC {
		if state != AgAborted && state != AgCommitted {
			c.toAbnomal(TID)
			return
		}
		c.MsgPool[TID] = append(c.MsgPool[TID], ok)
		if !ok { // failed txn.
			c.TxnState[TID] = Aborted
		}
	}
}

func (c *RACManager) handlePreRead(TID int, val *map[string]int, ok bool) {
	c.LockPool[TID].Lock()
	defer c.LockPool[TID].Unlock()
	if c.TxnState[TID] != PreRead {
		c.toAbnomal(TID)
		return
	}
	if !ok {
		c.TxnState[TID] = Aborted
		return
	}
	c.MsgPool[TID] = append(c.MsgPool[TID], val)
}

func (c *RACManager) sendMsg(server string, mark string, txn remote.RACTransaction) {
	utils.DPrintf("TXN" + strconv.Itoa(txn.TxnID) + ": " + "CA send message for " + server + " with Mark " + mark)
	msg := remote.Message4CO{Mark: mark, Txn: txn}
	msgBytes, err := json.Marshal(msg)
	utils.CheckError(err)
	sendMsg(c, server, msgBytes)
}

func (c *RACManager) SkipRead(TID int) {
	c.LockPool[TID].Lock()
	defer c.LockPool[TID].Unlock()
	if c.TxnState[TID] == None {
		c.TxnState[TID] = PreRead
	}
}

func (c *RACManager) CheckAndChange(TID int, begin int32, end int32) bool {
	c.LockPool[TID].Lock()
	defer c.LockPool[TID].Unlock()
	if c.TxnState[TID] != begin {
		c.toAbnomal(TID)
		return false
	}
	c.TxnState[TID] = end
	c.MsgPool[TID] = c.MsgPool[TID][:0]
	return true
}

func (c *RACManager) SubmitTxn(txn *DBTransaction, protocol string, latency *time.Duration, levels *int64) bool {
	c.SkipRead(txn.TxnID)
	defer utils.TimeLoad(time.Now(), "Submit transaction", txn.TxnID, latency)
	txn.Optimize()
	switch protocol {
	case RobAdapC:
		return txn.RACSubmit(nil, txn, levels)
	case TwoPC:
		return txn.TwoPCSubmit(nil, txn)
	case ThreePC:
		return txn.ThreePCSubmit(nil, txn)
	case CPAC:
		return txn.PACSubmit(nil, txn)
	default:
		utils.Assert(false, "Incorrect protocol "+protocol)
		return false
	}
}

func (c *RACManager) PreRead(txn *DBTransaction) (map[string]int, bool) {
	return make(map[string]int), true
	utils.Assert(txn.from.CheckAndChange(txn.TxnID, 0, PreRead), "?????")
	return txn.PreRead(txn)
}

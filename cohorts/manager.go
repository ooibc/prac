package cohorts

import (
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/mockkv"
	"github.com/allvphx/RAC/remote"
	"github.com/allvphx/RAC/rlsm"
	"github.com/allvphx/RAC/utils"
	"net"
	"sync"
	"sync/atomic"
)

// CohortManager manage the rpc calls From others. And maintain the shardKv here.
// committed logs can be done here
type CohortManager struct {
	stmt           *CohortStmt
	Pool           []*cohortBranch
	PoolLocks      []*sync.Mutex
	Kv             *mockkv.Shard
	L1VoteReceived [][]bool
	L2VoteReceived [][]bool
	connections    []*net.Conn
	Broken         int32
	NF             int32
}

// Break Recover IsBroken For test
func (c *CohortManager) Break() {
	atomic.StoreInt32(&c.Broken, 1)
	atomic.StoreInt32(&constants.TestCF, 1)
}

func (c *CohortManager) NetBreak() {
	atomic.StoreInt32(&c.NF, 1)
	atomic.StoreInt32(&constants.TestNF, 1)
}

func (c *CohortManager) Recover() {
	atomic.StoreInt32(&c.Broken, 0)
	atomic.StoreInt32(&constants.TestCF, 0)
}

func (c *CohortManager) NetRecover() {
	atomic.StoreInt32(&c.NF, 0)
	atomic.StoreInt32(&constants.TestNF, 0)
}

func (c *CohortManager) IsBroken() bool {
	return atomic.LoadInt32(&c.Broken) == 1
}

func (c *CohortManager) IsNF() bool {
	return atomic.LoadInt32(&c.NF) == 1
}

const MaxTxnID = constants.Max_Txn_ID

func NewCohortManager(stmt *CohortStmt, storageSize int) *CohortManager {
	res := &CohortManager{
		Pool:           make([]*cohortBranch, MaxTxnID),
		PoolLocks:      make([]*sync.Mutex, MaxTxnID),
		Kv:             mockkv.NewKV(storageSize),
		L1VoteReceived: make([][]bool, MaxTxnID),
		L2VoteReceived: make([][]bool, MaxTxnID),
		connections:    make([]*net.Conn, 0),
		stmt:           stmt,
		Broken:         0,
		NF:             0,
	}
	for i := 0; i < MaxTxnID; i++ {
		res.PoolLocks[i] = &sync.Mutex{}
	}
	return res
}

func (c *CohortManager) checkCommit4L1(TID int) bool {
	c.PoolLocks[TID].Lock()
	defer c.PoolLocks[TID].Unlock()
	return c.L1VoteReceived[TID] == nil
}

func (c *CohortManager) checkCommit4L2(TID int, expected int) bool {
	c.PoolLocks[TID].Lock()
	defer c.PoolLocks[TID].Unlock()
	if c.L2VoteReceived[TID] == nil || len(c.L2VoteReceived[TID]) != expected {
		return false
	}
	for _, op := range c.L2VoteReceived[TID] {
		if !op {
			return false
		}
	}
	return true
}

func (c *CohortManager) init(TID int) {
	c.Pool[TID] = newCohortBranch(TID, c.Kv, c)
	c.Kv.Begin(TID)
}

// PreRead only handles the read.
func (c *CohortManager) PreRead(tx *remote.RACTransaction) (bool, map[string]int) {
	TID := tx.TxnID
	c.PoolLocks[TID].Lock()
	if !utils.Assert(c.Pool[TID] == nil, "The transaction ID is not unique") {
		c.PoolLocks[TID].Unlock()
		return false, nil
	}
	c.PoolLocks[TID].Unlock()
	c.Pool[TID] = newCohortBranch(TID, c.Kv, c)
	return c.Pool[TID].PreRead(tx)
}

// forTestPreRead is the test version of pre read. can have multiple same TID.
func (c *CohortManager) forTestPreRead(tx *remote.RACTransaction) (bool, map[string]int) {
	TID := tx.TxnID
	c.PoolLocks[TID].Lock()
	if c.Pool[TID] == nil {
		c.init(TID)
	}
	c.PoolLocks[TID].Unlock()
	return c.Pool[TID].forTestPreRead(tx)
}

// PreWrite checks if we can get all the locks to ensure ACID.
// the tx only contains the write operations, the calculation is done in manager.
func (c *CohortManager) PreWrite(tx *remote.RACTransaction) bool {
	TID := tx.TxnID
	c.PoolLocks[TID].Lock()
	if !utils.Warn(c.Pool[TID] != nil, "The preRead is needed") {
		c.init(TID)
	}
	c.PoolLocks[TID].Unlock()
	return c.Pool[TID].PreWrite(tx)
}

// Agree 3PC perform the agree phase.
func (c *CohortManager) Agree(tx *remote.RACTransaction, isCommit bool) bool {
	TID := tx.TxnID
	c.PoolLocks[TID].Lock()
	if !utils.Warn(c.Pool[TID] != nil, "The preRead is needed") {
		c.init(TID)
	}
	c.PoolLocks[TID].Unlock()

	return c.Pool[TID].Agree(tx.TxnID, isCommit)
}

// Propose handles the propose phase of the RAC algorithm
func (c *CohortManager) Propose(tx *remote.RACTransaction) *rlsm.KvRes {
	TID := tx.TxnID
	c.PoolLocks[TID].Lock()
	if !utils.Warn(c.Pool[TID] != nil, "The preRead is needed") {
		c.init(TID)
	}
	c.PoolLocks[TID].Unlock()

	return c.Pool[TID].Propose(tx)
}

// Commit The return can be regarded as ACK.
func (c *CohortManager) Commit(tx *remote.RACTransaction) bool {
	TID := tx.TxnID
	c.PoolLocks[TID].Lock()
	if c.Pool[TID] == nil {
		// the transaction has been committed.
		return true
	}
	c.PoolLocks[TID].Unlock()
	res := c.Pool[TID].Commit(tx)
	c.PoolLocks[TID].Lock()
	c.Pool[TID] = nil
	c.L1VoteReceived[TID] = nil
	c.L2VoteReceived[TID] = nil
	c.PoolLocks[TID].Unlock()
	return res
}

// Abort The return can be regarded as ACK.
func (c *CohortManager) Abort(tx *remote.RACTransaction) bool {
	TID := tx.TxnID
	c.PoolLocks[TID].Lock()
	if c.Pool[TID] == nil {
		return true
	}
	c.PoolLocks[TID].Unlock()
	res := c.Pool[TID].Abort(tx)
	c.PoolLocks[TID].Lock()
	c.Pool[TID] = nil
	c.L1VoteReceived[TID] = nil
	c.L2VoteReceived[TID] = nil
	c.PoolLocks[TID].Unlock()
	return res
}

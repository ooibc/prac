package mockkv

import (
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/lock"
	"github.com/allvphx/RAC/rlsm"
	"github.com/allvphx/RAC/utils"
	"strconv"
	"sync"
	"time"
)

const DefaultTimeOut = 100 * time.Millisecond
const MaxTxnID = constants.Max_Txn_ID

// Shard shard maintains a local kv-store and all information needed.
type Shard struct {
	values []*shardEntry
	mu     *sync.Mutex
	length int

	logs     *shardLogManager
	rLocks   [][]*lock.RWLocker
	wLocks   [][]*lock.RWLocker
	lockMaps []map[int]LogOpt
	lockPool []*sync.Mutex

	TimeOut time.Duration //TimeOut the time out for locks.
}

func newShardKV(len int) *Shard {
	res := &Shard{
		values:   make([]*shardEntry, len),
		mu:       &sync.Mutex{},
		length:   len,
		TimeOut:  DefaultTimeOut,
		rLocks:   make([][]*lock.RWLocker, MaxTxnID),
		wLocks:   make([][]*lock.RWLocker, MaxTxnID),
		lockMaps: make([]map[int]LogOpt, MaxTxnID),
		lockPool: make([]*sync.Mutex, MaxTxnID),
	}
	res.logs = newShardLogManager(res)
	for i := 0; i < MaxTxnID; i++ {
		res.lockPool[i] = &sync.Mutex{}
	}
	for i := 0; i < len; i++ {
		res.values[i] = newShardEntry(-1, 0, i, res)
	}
	return res
}

// GetDDL get the deadline for getting lock.
func (c *Shard) GetDDL() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.TimeOut
}

// SetDDL set the deadline for getting lock.
func (c *Shard) SetDDL(t time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.TimeOut = t
}

func (c *Shard) updateKV(txnID int, key int, value int, isForce bool, holdLock bool) bool {
	if !utils.Assert(key < c.length && key >= 0, "RPC Out of Range") {
		return false
	}
	if holdLock {
		return c.values[key].lockUpdate(txnID, value, isForce)
	} else {
		return c.values[key].update(txnID, value, isForce)
	}
}

// Update time-out update for a[key] = value, return succeed or not. Used for time-limited wait of locks.
func (c *Shard) Update(key int, value int) bool {
	return c.updateKV(-1, key, value, false, false)
}

// Read read a[key], return the value and if succeed. Used for time-limited wait of locks.
func (c *Shard) Read(key int) (int, bool) {
	if !utils.Assert(key < c.length && key >= 0, "RPC Out of Range") || c.values[key] == nil {
		return -1, false
	}
	return c.values[key].read(true)
}

// For transactions.

// Begin start a transaction with txnID.
func (c *Shard) Begin(txnID int) bool {
	utils.Assert(txnID < MaxTxnID && txnID >= 0, "Invalid transaction ID")
	if !utils.Warn(c.rLocks[txnID] == nil, "The transaction has been started") {
		return true
	}
	c.lockPool[txnID].Lock()
	defer c.lockPool[txnID].Unlock()
	c.rLocks[txnID] = make([]*lock.RWLocker, 0)
	c.wLocks[txnID] = make([]*lock.RWLocker, 0)
	c.lockMaps[txnID] = make(map[int]LogOpt)
	c.logs.logs[txnID] = make([]*kvLog, 0)
	return true
}

func (c *Shard) needLock(txnID int, key int, opt LogOpt) (bool, LogOpt) {
	// The value is initially None = 0.
	utils.Assert(txnID < MaxTxnID && txnID >= 0, "Invalid transaction ID")
	c.lockPool[txnID].Lock()
	defer c.lockPool[txnID].Unlock()
	if c.lockMaps[txnID] == nil {
		// It is already aborted.
		return false, LogOpt(-1)
	}
	return c.lockMaps[txnID][key] >= opt, c.lockMaps[txnID][key]
}

func (c *Shard) addLock(txnID int, key int, opt LogOpt, locker *lock.RWLocker) {
	// The value is initially None = 0.
	if txnID == -1 {
		// no record should be stored for the stable logs.
		return
	}
	// This is because of the non-concurrency of map[][]
	if c.lockMaps[txnID] == nil {
		// The transaction is already aborted.
		return
	}
	c.lockMaps[txnID][key] = LogOpt(rlsm.Max(int(c.lockMaps[txnID][key]), int(opt)))
	if opt == Read {
		c.rLocks[txnID] = append(c.rLocks[txnID], locker)
	} else if opt == Write {
		c.wLocks[txnID] = append(c.wLocks[txnID], locker)
	}
}

// release releases the locks for transaction txnID. Not thread-safe
func (c *Shard) release(txnID int) {
	cnt := 0
	ch := make(chan bool)
	for _, c := range c.rLocks[txnID] {
		cnt++
		go func(c *lock.RWLocker) {
			c.RUnlock()
			ch <- true
		}(c)
	}
	for _, c := range c.wLocks[txnID] {
		cnt++
		go func(c *lock.RWLocker) {
			c.Unlock()
			ch <- true
		}(c)
	}
	c.rLocks[txnID] = nil
	c.wLocks[txnID] = nil
	c.lockMaps[txnID] = nil
	for cnt > 0 {
		utils.Assert(<-ch, "The Lock Release Got Error")
		cnt--
	}
	utils.DPrintf("TXN" + strconv.Itoa(txnID) + ": The lock is cleared")
}

// ReadTxn used for PCC.
func (c *Shard) ReadTxn(txnID int, key int) (int, bool) {
	if !utils.Assert(key < c.length && key >= 0, "Key Out of Range") {
		return -1, false
	}
	return c.values[key].lockRead(txnID, true)
}

// read4Undo used for undo.
func (c *Shard) read4Undo(key int) int {
	if !utils.Assert(key < c.length && key >= 0, "Key Out of Range") {
		return -1
	}
	return c.values[key].value
}

// update4Undo used for undo
func (c *Shard) update4Undo(key int, value int) bool {
	if !utils.Assert(key < c.length && key >= 0, "Key Out of Range") {
		return false
	}
	c.values[key].value = value
	return true
}

// UpdateTxn used for PCC.
func (c *Shard) UpdateTxn(txnID int, key int, value int) bool {
	return c.updateKV(txnID, key, value, false, true)
}

// Commit commit the transaction txnID
func (c *Shard) Commit(txnID int) bool {
	c.lockPool[txnID].Lock()
	defer c.lockPool[txnID].Unlock()
	recLogs := c.logs.getRecoveryLog(txnID)
	if recLogs != nil {
		c.mu.Lock()                   // the logs[-1] is a common log, hence lock needed.
		for _, log := range recLogs { // stable the logs.
			log.values[0] = -1
			c.logs.logs[MaxTxnID-1] = append(c.logs.logs[MaxTxnID-1], log)
		}
		c.mu.Unlock()
	}
	c.release(txnID)
	c.logs.clear(txnID)
	return true
}

// Force force write for a[key] = value, return error found or not.
func (c *Shard) Force(txnID int, key int, value int) bool {
	return c.updateKV(txnID, key, value, true, false)
}

// RollBack rollback the transaction branch txnID. rollback will block all the things.
func (c *Shard) RollBack(txnID int) bool {
	c.lockPool[txnID].Lock()
	defer c.lockPool[txnID].Unlock()
	recLogs := c.logs.getRecoveryLog(txnID)
	if recLogs != nil {
		for i, _ := range recLogs {
			if !utils.Assert(recLogs[len(recLogs)-i-1].undo(c), "The RollBack failed Because of a failed Force Write.") {
				return false
			}
		}
	}
	c.logs.clear(txnID)
	c.release(txnID)
	return true
}

package mockkv

import (
	"github.com/allvphx/RAC/utils"
	"sync"
)

const (
	None  LogOpt = 0
	Read  LogOpt = 1
	Write LogOpt = 2
)

type LogOpt int

type kvLog struct {
	opt    LogOpt
	values []int
}

// undo the log to make the recovery.
func (c *kvLog) undo(shard *Shard) bool {
	_ = shard.read4Undo(c.values[1])
	//	utils.Assert(val == c.values[3], "The rollback succeeds to meet the log.")
	return shard.update4Undo(c.values[1], c.values[2])
}

// shardLogManager thread-safe log manager for local KV.
type shardLogManager struct {
	mu   *sync.Mutex
	logs [][]*kvLog
	from *Shard
}

func newShardLogManager(shard *Shard) *shardLogManager {
	res := &shardLogManager{
		from: shard,
		logs: make([][]*kvLog, MaxTxnID),
		mu:   &sync.Mutex{},
	}
	res.logs[MaxTxnID-1] = make([]*kvLog, 0) // The stable log.
	return res
}

// getRecoveryLog get the logs for rollback, nil for empty log. Not thread-safe
func (s *shardLogManager) getRecoveryLog(txnID int) []*kvLog {
	utils.Assert(txnID != -1, "The stable log is accessed for rollback")
	return s.logs[txnID]
}

// TODO: recovery and stablize in crash failures.

// appendWrite append a write log to the log manager of the shard. -1 for nil oldValue
// the log with txnID -1 is regarded as a stable write, only used for crash failure recovery.
func (s *shardLogManager) appendWrite(txnID int, key int, oldValue int, newValue int) {
	if txnID == -1 {
		// Need stable write, hence is slow.
		txnID = MaxTxnID - 1
		s.from.mu.Lock()
	}
	if s.from.lockMaps[txnID] == nil && txnID != MaxTxnID-1 {
		// The transaction is already aborted.
		return
	}
	s.logs[txnID] = append(s.logs[txnID], &kvLog{
		opt:    Write,
		values: []int{txnID, key, oldValue, newValue},
	})
	if txnID == MaxTxnID-1 {
		s.from.mu.Unlock()
	}
}

// appendNoLock the lock free version of appendWrite
// this is used for temporary data since
func (s *shardLogManager) appendNoLock(txnID int, key int, oldValue int, newValue int) {
	if txnID == -1 {
		// Need stable write, hence is slow.
		txnID = MaxTxnID - 1
	}
	s.logs[txnID] = append(s.logs[txnID], &kvLog{
		opt:    Write,
		values: []int{txnID, key, oldValue, newValue},
	})
}

// clear clear the logs for transaction txnID. Not thread-safe
func (s *shardLogManager) clear(txnID int) {
	utils.Assert(txnID != -1, "The stable log is cleared")
	s.logs[txnID] = nil
}

package mockkv

import (
	"github.com/allvphx/RAC/lock"
	"github.com/allvphx/RAC/utils"
	"strconv"
	"time"
)

type shardEntry struct {
	value int
	addr  int
	mu    *lock.RWLocker
	from  *Shard
}

func newShardEntry(txnID int, value int, addr int, from *Shard) *shardEntry {
	from.logs.appendNoLock(txnID, addr, -1, value)
	return &shardEntry{
		value: value,
		addr:  addr,
		mu:    lock.NewRWLocker(),
		from:  from,
	}
}

func (s *shardEntry) read(isForce bool) (int, bool) {
	U := s.from.GetDDL()
	if isForce {
		U = time.Duration(-1)
	}
	if ok, _, _ := s.mu.RLock(U); ok {
		res := s.value
		s.mu.RUnlock()
		return res, true
	} else {
		return -1, false
	}
}

func (s *shardEntry) update(txnID int, newVal int, isForce bool) bool {
	U := s.from.GetDDL()
	if isForce {
		U = time.Duration(-1)
	}
	if ok, _, _ := s.mu.Lock(U); ok {
		s.from.logs.appendWrite(txnID, s.addr, s.value, newVal)
		s.value = newVal
		s.mu.Unlock()
		return true
	} else {
		return false
	}
}

func (s *shardEntry) lockRead(txnID int, isForce bool) (int, bool) {
	U := s.from.GetDDL()
	if isForce {
		U = time.Duration(-1)
	}
	ok, cur := s.from.needLock(txnID, s.addr, Read)
	if cur == None { // try to add the lock
		ok, _, _ = s.mu.RLock(U)
	}
	if ok {
		s.from.lockPool[txnID].Lock()
		defer s.from.lockPool[txnID].Unlock()
		// avoid new log after deciding to the rollback
		if s.from.rLocks[txnID] == nil {
			// It has already stopped
			s.mu.RUnlock()
			return -1, false
		}
		s.from.addLock(txnID, s.addr, Read, s.mu)
		res := s.value
		return res, true
	} else {
		return -1, false
	}
}

func (s *shardEntry) lockUpdate(txnID int, newVal int, isForce bool) bool {
	U := s.from.GetDDL()
	if isForce {
		U = time.Duration(-1)
	}
	ok, cur := s.from.needLock(txnID, s.addr, Write)
	if cur == Read { // try to add the lock.
		ok, _, _ = s.mu.UpgradeLock(U)
	} else if cur == None {
		ok, _, _ = s.mu.Lock(U)
	}
	if ok {
		s.from.lockPool[txnID].Lock()
		defer s.from.lockPool[txnID].Unlock()
		// avoid new log after deciding to the rollback
		if s.from.rLocks[txnID] == nil {
			// It has already stopped
			s.mu.Unlock()
			return false
		}
		s.from.addLock(txnID, s.addr, Write, s.mu)
		s.from.logs.appendWrite(txnID, s.addr, s.value, newVal)
		utils.DPrintf("TXN" + strconv.Itoa(txnID) + ": " + strconv.Itoa(s.addr) + " - " + strconv.Itoa(s.value) + " = " + strconv.Itoa(newVal))
		s.value = newVal
		return true
	} else {
		return false
	}
}

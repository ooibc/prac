package rlsm

import (
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/lock"
	"github.com/allvphx/RAC/utils"
)

type Level int

const (
	NoCFNoNF Level = 1
	CFNoNF   Level = 2
	CFNF     Level = 3
)

// LevelStateMachine is the thread safe level machine maintained on the DBMS, each shard is assigned with one.
type LevelStateMachine struct {
	id        int
	he_mu     *lock.RWLocker // mutex for heurstic method, to access the model sequentially.
	level     Level          // the current level of shards robustness
	H         int
	downClock int
	from      *LevelStateManager
}

func NewLSM(s *LevelStateManager) *LevelStateMachine {
	return &LevelStateMachine{
		he_mu:     lock.NewRWLocker(),
		level:     NoCFNoNF,
		H:         0,
		downClock: 0,
		from:      s,
	}
}

func (c *LevelStateMachine) GetLevel() Level {
	return c.level
}

func (c *LevelStateMachine) Down() {
	c.level = NoCFNoNF
}

//Next thread safely upward transform the state machine with the results handled.
func (c *LevelStateMachine) Next(CrashF bool, NetF bool, comLevel Level, id string) error {
	if c.level <= comLevel {
		// the level has been updated by another client, current result is no longer valid.
		if c.level == NoCFNoNF {
			if NetF {
				c.level = CFNF
				utils.LPrintf("upppppp!!!!! to NF" + id)
			} else if CrashF {
				c.level = CFNoNF
				utils.LPrintf("upppppp!!!!!" + id)
			}
		} else if c.level == CFNoNF {
			if NetF {
				c.level = CFNF
				utils.LPrintf("upppppp!!!!! to NF" + id)
			}
		}
	}

	// For downward transitions. operations that are too close are abandoned by he_mu
	c.downClock++
	if c.downClock == constants.DownBatchSize {
		ok, _, _ := c.he_mu.Lock(AccessInterval)
		level := c.level
		if ok {
			c.Trans(level, NetF || CrashF, id)
			c.he_mu.Unlock()
		}
		c.downClock = 0
	}
	return nil
}

package rlsm

import (
	"fmt"
	"github.com/allvphx/RAC/lock"
	"sync"
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
	mu        *sync.Mutex
	he_mu     *lock.RWLocker // mutex for heurstic method, to access the model sequentially.
	level     Level          // the current level of shards robustness
	H         int
	downClock int
}

func NewLSM() *LevelStateMachine {
	return &LevelStateMachine{
		mu:        &sync.Mutex{},
		he_mu:     lock.NewRWLocker(),
		level:     NoCFNoNF,
		H:         0,
		downClock: 0,
	}
}

//GetLevel thread safe method
func (c *LevelStateMachine) GetLevel() Level {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.level
}

//Next thread safely upward transform the state machine with the results handled.
func (c *LevelStateMachine) Next(CrashF bool, NetF bool, comLevel Level, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Upper transformations: comLevel only used for up.
	if c.level < comLevel && comLevel == CFNF {
		// NF cannot be located to one single node.
		c.level = comLevel
	} else if c.level > comLevel {
		// the level has been updated by another client, current result is no longer valid.
		return nil
	}

	if c.level == NoCFNoNF {
		if NetF {
			c.level = CFNF
			fmt.Println("upppppp!!!!! to NF", id)
		} else if CrashF {
			c.level = CFNoNF
			fmt.Println("upppppp!!!!!", id)
		}
	} else if c.level == CFNoNF {
		if NetF {
			c.level = CFNF
			fmt.Println("upppppp!!!!! to NF", id)
		}
	}

	// For downward transitions. operations that are too close are abandoned by he_mu
	c.downClock++
	if c.downClock == 100 {
		ok, _, _ := c.he_mu.Lock(AccessInterval)
		level := c.level
		if ok {
			go func() {
				c.trans(level, NetF || CrashF, id)
				c.he_mu.Unlock()
			}()
		}
		c.downClock = 0
	}
	return nil
}

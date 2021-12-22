package rlsm

import (
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
	mu    *sync.Mutex
	level Level // the current level of shards robustness
}

func NewLSM() *LevelStateMachine {
	return &LevelStateMachine{
		mu:    &sync.Mutex{},
		level: NoCFNoNF,
	}
}

//GetLevel thread safe method
func (c *LevelStateMachine) GetLevel() Level {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.level
}

//Next thread safely upward transform the state machine with the results handled.
func (c *LevelStateMachine) Next(CrashF bool, NetF bool, comLevel Level) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Upper transformations: comLevel only used for up.
	if c.level < comLevel {
		c.level = comLevel
	} else if c.level >= comLevel {
		// the level has been updated by another client, current result is no longer valid.
		return nil
	}

	if c.level == NoCFNoNF {
		if NetF {
			c.level = CFNF
		} else if CrashF {
			c.level = CFNoNF
		}
	} else if c.level == CFNoNF {
		if NetF {
			c.level = CFNF
		}
	}
	return nil
}

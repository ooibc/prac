package rlsm

import (
	"sync"
)

type Level int

const (
	NoCFNoNF                       Level = 1
	CFNoNF                         Level = 2
	CFNF                           Level = 3
	DefaultnoCrashFailureThreshold       = 200
)

// LevelStateMachine is the thread safe level machine maintained on the DBMS, each shard is assigned with one.
type LevelStateMachine struct {
	mu                      sync.Mutex
	level                   Level // the current level of shards robustness
	noCFCnt                 int   // the number of continuous no-crash-failure executions.
	noCrashFailureThreshold int
}

func NewLSM() *LevelStateMachine {
	return &LevelStateMachine{
		mu:                      sync.Mutex{},
		level:                   NoCFNoNF,
		noCFCnt:                 0,
		noCrashFailureThreshold: DefaultnoCrashFailureThreshold,
	}
}

//GetLevel thread safe method
func (c *LevelStateMachine) SetCrashThreshold(val int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.noCrashFailureThreshold = val
}

//GetLevel thread safe method
func (c *LevelStateMachine) GetLevel() Level {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.level
}

//Next thread safely transform the state machine with the results handled.
func (c *LevelStateMachine) Next(CrashF bool, NetF bool, comLevel Level) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.level < comLevel {
		// update the level into the common sense.
		c.level = comLevel
	} else if c.level > comLevel {
		// the level has been updated by another client, current result is no longer valid.
		return nil
	}

	if !CrashF {
		c.noCFCnt++
	} else {
		c.noCFCnt = 0
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
		} else if c.noCFCnt > c.noCrashFailureThreshold {
			c.level = NoCFNoNF
		}
	} else {
		if c.noCFCnt > c.noCrashFailureThreshold {
			c.level = NoCFNoNF
		}
	}
	return nil
}

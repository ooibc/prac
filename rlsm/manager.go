package rlsm

import (
	"sync"
)

type LevelStateManager struct {
	mu     sync.Mutex
	states map[string]*LevelStateMachine
}

func NewLSMManger(parts []string) *LevelStateManager {
	res := &LevelStateManager{
		mu:     sync.Mutex{},
		states: make(map[string]*LevelStateMachine),
	}
	for _, s := range parts {
		res.states[s] = NewLSM()
	}
	return res
}

//Start get the common levels from shardSet
func (c *LevelStateManager) Start(shardSet []string) Level {
	return c.synLevels(shardSet)
}

//Finish finish one round for the state machines
func (c *LevelStateManager) Finish(shardSet []string, results *KvResult, comLevel Level) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	CrashF, NetF := make(map[string]bool), false // For level 3, no failure.

	if results != nil {
		CrashF, NetF = results.Analysis(shardSet, comLevel)
	}

	for _, i := range shardSet {
		err := c.states[i].Next(CrashF[i], NetF, comLevel)
		if err != nil {
			return err
		}
	}
	return nil
}

//synLevels get the common levels from shardSet
func (c *LevelStateManager) synLevels(shardSet []string) Level {
	c.mu.Lock()
	defer c.mu.Unlock()

	comLevel := NoCFNoNF
	for _, i := range shardSet {
		comLevel = MaxLevel(comLevel, c.states[i].GetLevel())
	}
	return comLevel
}

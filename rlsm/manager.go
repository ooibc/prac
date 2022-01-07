package rlsm

import (
	"github.com/allvphx/RAC/constants"
	ds "github.com/allvphx/RAC/downserver"
	"github.com/allvphx/RAC/utils"
	"sync"
)

type LevelStateManager struct {
	mu     sync.Mutex
	states map[string]*LevelStateMachine
}

func Stop() {

}

func NewLSMManger(parts []string) *LevelStateManager {
	res := &LevelStateManager{
		mu:     sync.Mutex{},
		states: make(map[string]*LevelStateMachine),
	}
	for _, s := range parts {
		res.states[s] = NewLSM(res)
	}
	return res
}

//Start get the common levels from shardSet
func (c *LevelStateManager) Start(shardSet []string) (Level, int) {
	return c.synLevels(shardSet)
}

var BatchCount = 0
var Rem = 0
var TimeStamp4NFRec = 0

//Finish finish one round for the state machines
func (c *LevelStateManager) Finish(shardSet []string, results *KvResult, comLevel Level, ts int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ts != TimeStamp4NFRec {
		// avoid CF, NF from earlier stages.
		return nil
	}

	CrashF, NetF := make(map[string]bool), false // For level 3, no failure.

	if results != nil {
		CrashF, NetF = results.Analysis(shardSet, comLevel)
		for _, i := range shardSet {
			err := c.states[i].Next(CrashF[i], NetF, comLevel, i)
			if err != nil {
				return err
			}
		}
	} else {
		BatchCount++
		if BatchCount == constants.DownBatchSize {
			BatchCount = 0
			if Rem == 0 {
				ds.Send(3, "N.A.", false)
				Rem = ds.Action(3, "N.A.")
				if Rem == 0 { // the down for all nodes. (NF could cover others)
					utils.LPrintf("common down!!!!")
					for _, x := range c.states {
						x.level = NoCFNoNF
					}
					TimeStamp4NFRec++
				}
			} else {
				Rem--
			}
		}
	}
	return nil
}

//synLevels get the common levels from shardSet
func (c *LevelStateManager) synLevels(shardSet []string) (Level, int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	comLevel := NoCFNoNF
	for _, i := range shardSet {
		comLevel = MaxLevel(comLevel, c.states[i].GetLevel())
		if i[len(i)-1] == '1' {
			//			println("comLevel = ", comLevel)
		}
	}
	return comLevel, TimeStamp4NFRec
}

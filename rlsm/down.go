package rlsm

import (
	"fmt"
	ds "github.com/allvphx/RAC/downserver"
	"time"
)

const AccessInterval time.Duration = 20 * time.Millisecond

func (c *LevelStateMachine) downTrans(initLevel Level) {
	// down to the first level.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.level <= initLevel {
		// conflict solve, can see paper.
		fmt.Println("downnnnnnnn!!!!!")
		c.level = NoCFNoNF
	}
}

func (c *LevelStateMachine) GetAction(curLevel Level, cid string) int {
	// When level changed, it would get reseted.
	ds.Send(int(curLevel), cid)
	return ds.Action(cid)
}

func (c *LevelStateMachine) trans(curLevel Level, failure bool, cid string) {
	if curLevel == NoCFNoNF {
		return
	}
	c.H = c.GetAction(curLevel, cid)
	if c.H == 0 {
		c.downTrans(curLevel)
	}
}

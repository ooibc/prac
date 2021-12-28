package rlsm

import (
	ds "github.com/allvphx/RAC/downserver"
	"github.com/allvphx/RAC/utils"
	"time"
)

const AccessInterval time.Duration = 10 * time.Millisecond

func (c *LevelStateMachine) downTrans(initLevel Level) {
	// down to the first level.
	if c.level <= initLevel {
		// conflict solve, can see paper.
		utils.LPrintf("downnnnnnnn!!!!!" + string(rune(c.id)))
		c.level = NoCFNoNF
		TimeStamp4NFRec++
	}
}

func (c *LevelStateMachine) getAction(curLevel Level, cid string, failure bool) int {
	// When level changed, it would get reseted.
	ds.Send(int(curLevel), cid, failure)
	return ds.Action(int(curLevel), cid)
}

func (c *LevelStateMachine) Trans(curLevel Level, failure bool, cid string) {
	c.H = c.getAction(curLevel, cid, failure)
	if c.H == 0 {
		c.downTrans(curLevel)
	}
}

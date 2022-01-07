package downserver

import (
	"github.com/allvphx/RAC/constants"
	"time"
)

type Simple_Learner struct {
	level int
	cnt   int
	react int
}

var Fixed = []Simple_Learner{Simple_Learner{level: 1, cnt: constants.InitCnt, react: -1},
	Simple_Learner{level: 1, cnt: constants.InitCnt, react: -1},
	Simple_Learner{level: 1, cnt: constants.InitCnt, react: -1},
	Simple_Learner{level: 1, cnt: constants.InitCnt, react: -1}}

func (tes *Simple_Learner) Send(level int, cid string, failure bool) {
	if level == 1 {
		tes.react = 1
		return
	}
	if level != tes.level || failure {
		// reset
		tes.level = level
		tes.cnt = constants.InitCnt
		tes.react = 1
	} else {
		// transition
		if tes.cnt == 0 {
			// back to initial level
			//			println("react 0")
			tes.react = 0
			tes.level = 1
		} else {
			tes.react = 1
			tes.cnt--
		}
	}
}

func (tes *Simple_Learner) Action(cid string) int {
	for {
		rec := tes.react
		if rec == -1 {
			time.Sleep(5 * time.Millisecond)
			continue
		} else {
			tes.react = -1
			return int(rec)
		}
	}
}

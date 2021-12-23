package downserver

import (
	"time"
)

const init_Cnt = 2

type Simple_Learner struct {
	level int
	cnt   int
	react int
}

var tes = []Simple_Learner{Simple_Learner{level: 1, cnt: init_Cnt, react: -1},
	Simple_Learner{level: 1, cnt: init_Cnt, react: -1},
	Simple_Learner{level: 1, cnt: init_Cnt, react: -1}}

func Send(level int, cid string) {
	i := int(cid[len(cid)-1] - '1')
	if level != tes[i].level {
		// reset
		tes[i].level = level
		tes[i].cnt = init_Cnt
		tes[i].react = 1
	} else {
		// transition
		tes[i].cnt--
		if tes[i].cnt == 0 {
			// back to initial level
			tes[i].react = 0
			tes[i].level = 1
		}
	}
}

func Action(cid string) int {
	i := int(cid[len(cid)-1] - '1')
	for {
		rec := tes[i].react
		if rec == -1 {
			time.Sleep(5 * time.Millisecond)
			continue
		} else {
			tes[i].react = -1
			return int(rec)
		}
	}
}

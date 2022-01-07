package downserver

import (
	"github.com/allvphx/RAC/constants"
	"sync"
)

type Learner interface {
	Send(level int, cid string)
	Action(cid string) int
}

var lock = &sync.Mutex{}
var CommittedTxn = 0
var acessCnt = 500

func Add_th() {
	lock.Lock()
	defer lock.Unlock()
	CommittedTxn++
}

func GetReward() float32 {
	lock.Lock()
	defer lock.Unlock()
	res := float32(CommittedTxn)
	CommittedTxn = 0
	return res
}

func Send(level int, cid string, failure bool) {
	i := int(cid[len(cid)-1] - '1')
	if level == 3 {
		i = 3
	}
	if constants.InitCnt > 0 {
		Fixed[i].Send(level, cid, failure)
	} else {
		QT[i].Send(level, cid, failure)
	}
}

func Action(level int, cid string) int {
	i := int(cid[len(cid)-1] - '1')
	if level == 3 {
		i = 3
	}
	if constants.InitCnt > 0 {
		return Fixed[i].Action(cid)
	} else {
		res := QT[i].Action(cid)
		if res >= 0 {
			return res
		} else {
			// reinforcement learning have ended, get the answer.
			res = -res - 3
			if res < 1 {
				res = 1
			}
			constants.SetDown(res)
		}
		return QT[i].Action(cid)
	}
}

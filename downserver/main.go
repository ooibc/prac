package downserver

import "github.com/allvphx/RAC/constants"

type Learner interface {
	Send(level int, cid string)
	Action(cid string) int
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
		return QT[i].Action(cid)
	}
}

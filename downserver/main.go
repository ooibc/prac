package downserver

import "github.com/allvphx/RAC/constants"

type Learner interface {
	Send(level int, cid string)
	Action(cid string) int
}

func Send(level int, cid string) {
	if constants.InitCnt > 0 {
		i := int(cid[len(cid)-1] - '1')
		Fixed[i].Send(level, cid)
	} else {
		i := int(cid[len(cid)-1] - '1')
		QT[i].Send(level, cid)
	}
}

func Action(cid string) int {
	if constants.InitCnt > 0 {
		i := int(cid[len(cid)-1] - '1')
		return Fixed[i].Action(cid)
	} else {
		i := int(cid[len(cid)-1] - '1')
		return QT[i].Action(cid)
	}
}

package downserver

import (
	"context"
	rl "github.com/allvphx/RAC/downserver/.rlaction"
	"github.com/allvphx/RAC/utils"
	"google.golang.org/grpc"
	"log"
	"time"
)

type QT_Learner struct {
	level int
	react int
}

var QT = []*QT_Learner{NewQT(), NewQT(), NewQT(), NewQT()}

func Stop() {
}

func NewQT() *QT_Learner {
	tes := QT_Learner{}
	tes.level = 1
	tes.react = -1
	return &tes
}

func (tes *QT_Learner) Reset(level int32, cid string) {
	conn, err := grpc.Dial("localhost:5003", grpc.WithInsecure())
	defer conn.Close()
	utils.CheckError(err)
	reset := rl.NewResetClient(conn)
	re := GetReward()
	_, err = reset.Reset(context.Background(), &rl.Info{Cid: &cid, Level: &level, Reward: &re})
	if err != nil {
		log.Fatal(err)
	}
}

func (tes *QT_Learner) Trans(level int32, cid string) int {
	conn, err := grpc.Dial("localhost:5003", grpc.WithInsecure())
	defer conn.Close()
	utils.CheckError(err)
	act := rl.NewActionClient(conn)
	re := GetReward()
	reply, err := act.Action(context.Background(), &rl.Info{Cid: &cid, Level: &level, Reward: &re})
	if err != nil {
		log.Fatal(err)
	}
	return int(reply.GetAction())
}

func (tes *QT_Learner) Send(level int, cid string, failure bool) {
	if level != tes.level || failure {
		// reset
		tes.Reset(int32(level), cid)
		tes.level = level
		tes.react = 1
	} else {
		// transition
		tes.react = tes.Trans(int32(level), cid)
		if tes.react == 0 {
			// back to initial level
			tes.level = 1
		}
	}
}

func (tes *QT_Learner) Action(cid string) int {
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

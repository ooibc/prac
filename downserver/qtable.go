package downserver

import (
	"context"
	rl "github.com/allvphx/RAC/downserver/.rlaction"
	"google.golang.org/grpc"
	"log"
	"time"
)

type QT_Learner struct {
	level int
	conn  *grpc.ClientConn
	react int
}

var QT = []*QT_Learner{NewQT(), NewQT(), NewQT()}

func NewQT() *QT_Learner {
	tes := QT_Learner{}
	tes.level = 1
	var err error = nil
	tes.react = -1
	tes.conn, err = grpc.Dial("localhost:5003", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	return &tes
}

func (tes *QT_Learner) Reset(level int32, cid string) {
	client := rl.NewResetClient(tes.conn)
	_, err := client.Reset(context.Background(), &rl.Info{Cid: &cid, Level: &level})
	if err != nil {
		log.Fatal(err)
	}
}

func (tes *QT_Learner) Trans(level int32, cid string) int {
	client := rl.NewActionClient(tes.conn)
	reply, err := client.Action(context.Background(), &rl.Info{Cid: &cid, Level: &level})
	if err != nil {
		log.Fatal(err)
	}
	return int(reply.GetAction())
}

func (tes *QT_Learner) Send(level int, cid string) {
	if level != tes.level {
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

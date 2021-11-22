package collaborator

import (
	"github.com/allvphx/RAC/cohorts"
	"github.com/allvphx/RAC/utils"
	"strconv"
	"testing"
	"time"
)

var addrs = []string{"127.0.0.1:6001", "127.0.0.1:6002", "127.0.0.1:6003"}

const defaultTimeOUt time.Duration = 100 * time.Millisecond

func CollaboratorTestKit() (*CollaboratorStmt, []*cohorts.CohortStmt) {
	stmt := &CollaboratorStmt{}
	var Arg = []string{"*", "*", caID}
	ch := make(chan bool)
	go begin(stmt, Arg, ch)
	<-ch

	return stmt, cohorts.OU_CohortsTestKit()
}

func CheckVal(coh *cohorts.CohortManager, ans []int) {
	for i := 0; i < len(ans); i++ {
		v, ok := coh.Kv.Read(i)
		utils.Assert(ok && ans[i] == v, "CKv : PreRead Failed with different value "+strconv.Itoa(v))
	}
}

func Test2PCPreWrite(t *testing.T) {
	ca, cohorts := CollaboratorTestKit()
	w := NewDBTransaction(1, defaultTimeOUt, addrs, ca.Manager)
	for i := 0; i < 5; i++ {
		w.AddUpdate(addrs[0], i, i+1)
		w.AddUpdate(addrs[1], i, i+2)
		w.AddUpdate(addrs[2], i, i+3)
	}
	txn := NewDBTransaction(1, defaultTimeOUt, addrs, ca.Manager)
	txn.from.CheckAndChange(1, 0, PreRead)
	res := txn.TwoPCSubmit(nil, w)
	utils.Assert(res, "The 2PC Failed")
	CheckVal(cohorts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(cohorts[1].Cohort, []int{2, 3, 4, 5, 6})
	CheckVal(cohorts[2].Cohort, []int{3, 4, 5, 6, 7})
}

func Test2PCPreWriteAbort(t *testing.T) {
	ca, cohorts := CollaboratorTestKit()
	w := NewDBTransaction(1, defaultTimeOUt, addrs, ca.Manager)
	for i := 0; i < 5; i++ {
		w.AddUpdate(addrs[0], i, i+1)
		w.AddUpdate(addrs[1], i, i+2)
		w.AddUpdate(addrs[2], i, i+3)
	}
	w.from.CheckAndChange(1, 0, PreRead)
	cohorts[0].Cohort.Kv.TimeOut = 0
	res := w.TwoPCSubmit(nil, w)
	utils.Assert(!res, "The 2PC Failed")
	cohorts[0].Cohort.Kv.TimeOut = 100 * time.Millisecond
	CheckVal(cohorts[0].Cohort, []int{0, 1, 2, 3, 4})
	CheckVal(cohorts[1].Cohort, []int{0, 1, 2, 3, 4})
	CheckVal(cohorts[2].Cohort, []int{0, 1, 2, 3, 4})
}

func Test2PCConcate(t *testing.T) {
	ca, cohorts := CollaboratorTestKit()
	w1 := NewDBTransaction(1, defaultTimeOUt, addrs[:1], ca.Manager)
	w2 := NewDBTransaction(2, defaultTimeOUt, addrs[1:], ca.Manager)
	for i := 0; i < 5; i++ {
		w1.AddUpdate(addrs[0], i, i+1)
		w2.AddUpdate(addrs[1], i, i+2)
		w2.AddUpdate(addrs[2], i, i+3)
	}
	w1.from.CheckAndChange(1, 0, PreRead)
	w2.from.CheckAndChange(2, 0, PreRead)
	res := w1.TwoPCSubmit(nil, w1)
	res = res && w2.TwoPCSubmit(nil, w2)
	utils.Assert(res, "The 2PC Failed")
	CheckVal(cohorts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(cohorts[1].Cohort, []int{2, 3, 4, 5, 6})
	CheckVal(cohorts[2].Cohort, []int{3, 4, 5, 6, 7})
}

func Test2PCConcurrent1(t *testing.T) {
	ca, cohorts := CollaboratorTestKit()
	w1 := NewDBTransaction(1, defaultTimeOUt, addrs[:1], ca.Manager)
	w2 := NewDBTransaction(2, defaultTimeOUt, addrs[1:], ca.Manager)
	for i := 0; i < 5; i++ {
		w1.AddUpdate(addrs[0], i, i+1)
		w2.AddUpdate(addrs[1], i, i+2)
		w2.AddUpdate(addrs[2], i, i+3)
	}
	w1.from.CheckAndChange(1, 0, PreRead)
	w2.from.CheckAndChange(2, 0, PreRead)
	ch := make(chan bool)
	go func() {
		res := w1.TwoPCSubmit(nil, w1)
		utils.Assert(res, "The 2PC Failed")
		ch <- res
	}()
	go func() {
		res := w2.TwoPCSubmit(nil, w2)
		utils.Assert(res, "The 2PC Failed")
		ch <- res
	}()
	<-ch
	<-ch
	CheckVal(cohorts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(cohorts[1].Cohort, []int{2, 3, 4, 5, 6})
	CheckVal(cohorts[2].Cohort, []int{3, 4, 5, 6, 7})
}

func Test2PCConcurrent2(t *testing.T) {
	ca, cohorts := CollaboratorTestKit()
	w1 := NewDBTransaction(1, defaultTimeOUt, addrs, ca.Manager)
	w2 := NewDBTransaction(2, defaultTimeOUt, addrs, ca.Manager)
	for i := 0; i < 2; i++ {
		w1.AddUpdate(addrs[0], i, i+1)
		w1.AddUpdate(addrs[1], i, i+2)
		w1.AddUpdate(addrs[2], i, i+3)
		w2.AddUpdate(addrs[0], i+2, i+3)
		w2.AddUpdate(addrs[1], i+2, i+4)
		w2.AddUpdate(addrs[2], i+2, i+5)
	}
	w2.AddUpdate(addrs[0], 4, 5)
	w2.AddUpdate(addrs[1], 4, 6)
	w2.AddUpdate(addrs[2], 4, 7)
	w1.from.CheckAndChange(1, 0, PreRead)
	w2.from.CheckAndChange(2, 0, PreRead)
	ch := make(chan bool)
	go func() {
		res := w1.TwoPCSubmit(nil, w1)
		utils.Assert(res, "The 2PC Failed")
		ch <- res
	}()
	go func() {
		res := w2.TwoPCSubmit(nil, w2)
		utils.Assert(res, "The 2PC Failed")
		ch <- res
	}()
	<-ch
	<-ch
	CheckVal(cohorts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(cohorts[1].Cohort, []int{2, 3, 4, 5, 6})
	CheckVal(cohorts[2].Cohort, []int{3, 4, 5, 6, 7})
}

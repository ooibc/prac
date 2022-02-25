package collaborator

import (
	"github.com/allvphx/RAC/utils"
	"testing"
	"time"
)

func TestPACPreRead(t *testing.T) {
	ca, cohorts := CollaboratorTestKit()
	r := NewDBTransaction(1, defaultTimeOUt, addrs, ca.Manager)
	CheckVal(cohorts[0].Cohort, []int{0, 1, 2, 3, 4})
	CheckVal(cohorts[1].Cohort, []int{0, 1, 2, 3, 4})
	CheckVal(cohorts[2].Cohort, []int{0, 1, 2, 3, 4})

	for i := 0; i < 5; i++ {
		r.AddRead(addrs[0], i)
		r.AddRead(addrs[1], i)
		r.AddRead(addrs[2], i)
	}
	txn := NewDBTransaction(1, defaultTimeOUt, addrs, ca.Manager)
	txn.from.CheckAndChange(1, 0, PreRead)
	res, ok := txn.PreRead(r)
	utils.Assert(ok, "The PreRead Failed")
	for i := 0; i < 5; i++ {
		utils.Assert(res[utils.Hash(addrs[0], i)] == i, "Check PreRead value failed")
		utils.Assert(res[utils.Hash(addrs[1], i)] == i, "Check PreRead value failed")
		utils.Assert(res[utils.Hash(addrs[2], i)] == i, "Check PreRead value failed")
	}
}

func TestPACPreWrite(t *testing.T) {
	ca, cohorts := CollaboratorTestKit()
	w := NewDBTransaction(1, defaultTimeOUt, addrs, ca.Manager)
	for i := 0; i < 5; i++ {
		w.AddUpdate(addrs[0], i, i+1)
		w.AddUpdate(addrs[1], i, i+2)
		w.AddUpdate(addrs[2], i, i+3)
	}
	txn := NewDBTransaction(1, defaultTimeOUt, addrs, ca.Manager)
	txn.from.CheckAndChange(1, 0, PreRead)
	res := txn.PACSubmit(nil, w)
	utils.Assert(res, "The PAC Failed")
	CheckVal(cohorts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(cohorts[1].Cohort, []int{2, 3, 4, 5, 6})
	CheckVal(cohorts[2].Cohort, []int{3, 4, 5, 6, 7})
}

func TestPACPreWriteAbort(t *testing.T) {
	ca, cohorts := CollaboratorTestKit()
	w := NewDBTransaction(1, defaultTimeOUt, addrs, ca.Manager)
	for i := 0; i < 5; i++ {
		w.AddUpdate(addrs[0], i, i+1)
		w.AddUpdate(addrs[1], i, i+2)
		w.AddUpdate(addrs[2], i, i+3)
	}
	w.from.CheckAndChange(1, 0, PreRead)
	cohorts[0].Cohort.Kv.TimeOut = 0
	res := w.PACSubmit(nil, w)
	utils.Assert(!res, "The 3PC Failed")
	cohorts[0].Cohort.Kv.TimeOut = 100 * time.Millisecond
	CheckVal(cohorts[0].Cohort, []int{0, 1, 2, 3, 4})
	CheckVal(cohorts[1].Cohort, []int{0, 1, 2, 3, 4})
	CheckVal(cohorts[2].Cohort, []int{0, 1, 2, 3, 4})
}

func TestPACConcate(t *testing.T) {
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
	res := w1.PACSubmit(nil, w1)
	res = res && w2.PACSubmit(nil, w2)
	utils.Assert(res, "The 3PC Failed")
	CheckVal(cohorts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(cohorts[1].Cohort, []int{2, 3, 4, 5, 6})
	CheckVal(cohorts[2].Cohort, []int{3, 4, 5, 6, 7})
}

func TestPACConcurrent1(t *testing.T) {
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
		res := w1.PACSubmit(nil, w1)
		utils.Assert(res, "The 3PC Failed")
		ch <- res
	}()
	go func() {
		res := w2.PACSubmit(nil, w2)
		utils.Assert(res, "The 3PC Failed")
		ch <- res
	}()
	<-ch
	<-ch
	CheckVal(cohorts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(cohorts[1].Cohort, []int{2, 3, 4, 5, 6})
	CheckVal(cohorts[2].Cohort, []int{3, 4, 5, 6, 7})
}

func TestPACConcurrent2(t *testing.T) {
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
		res := w1.PACSubmit(nil, w1)
		utils.Assert(res, "The 2PC Failed")
		ch <- res
	}()
	go func() {
		res := w2.PACSubmit(nil, w2)
		utils.Assert(res, "The 2PC Failed")
		ch <- res
	}()
	<-ch
	<-ch
	CheckVal(cohorts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(cohorts[1].Cohort, []int{2, 3, 4, 5, 6})
	CheckVal(cohorts[2].Cohort, []int{3, 4, 5, 6, 7})
}

package collaborator

import (
	"github.com/allvphx/RAC/utils"
	"testing"
	"time"
)

func TestRACNorm(t *testing.T) {
	ca, cohorts := CollaboratorTestKit()
	w := NewDBTransaction(1, defaultTimeOUt, addrs, ca.Manager)
	for i := 0; i < 5; i++ {
		w.AddUpdate(addrs[0], i, i+1)
		w.AddUpdate(addrs[1], i, i+2)
		w.AddUpdate(addrs[2], i, i+3)
	}
	txn := NewDBTransaction(1, defaultTimeOUt, addrs, ca.Manager)
	txn.from.CheckAndChange(1, 0, PreRead)
	res := txn.RACSubmit(nil, w)
	utils.Assert(res, "The RAC Failed")
	CheckVal(cohorts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(cohorts[1].Cohort, []int{2, 3, 4, 5, 6})
	CheckVal(cohorts[2].Cohort, []int{3, 4, 5, 6, 7})
}

func TestRACNormAbort(t *testing.T) {
	ca, cohorts := CollaboratorTestKit()
	w := NewDBTransaction(1, defaultTimeOUt, addrs, ca.Manager)
	for i := 0; i < 5; i++ {
		w.AddUpdate(addrs[0], i, i+1)
		w.AddUpdate(addrs[1], i, i+2)
		w.AddUpdate(addrs[2], i, i+3)
	}
	w.from.CheckAndChange(1, 0, PreRead)
	cohorts[0].Cohort.Kv.TimeOut = 0
	res := w.RACSubmit(nil, w)
	utils.Assert(!res, "The RAC Failed")
	cohorts[0].Cohort.Kv.TimeOut = 100 * time.Millisecond
	CheckVal(cohorts[0].Cohort, []int{0, 1, 2, 3, 4})
	CheckVal(cohorts[1].Cohort, []int{0, 1, 2, 3, 4})
	CheckVal(cohorts[2].Cohort, []int{0, 1, 2, 3, 4})
}

func TestRACNormConcate(t *testing.T) {
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
	res := w1.RACSubmit(nil, w1)
	res = res && w2.RACSubmit(nil, w2)
	utils.Assert(res, "The RAC Failed")
	CheckVal(cohorts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(cohorts[1].Cohort, []int{2, 3, 4, 5, 6})
	CheckVal(cohorts[2].Cohort, []int{3, 4, 5, 6, 7})
}

func TestRACNormConcurrent1(t *testing.T) {
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
		res := w1.RACSubmit(nil, w1)
		utils.Assert(res, "The 3PC Failed")
		ch <- res
	}()
	go func() {
		res := w2.RACSubmit(nil, w2)
		utils.Assert(res, "The 3PC Failed")
		ch <- res
	}()
	<-ch
	<-ch
	CheckVal(cohorts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(cohorts[1].Cohort, []int{2, 3, 4, 5, 6})
	CheckVal(cohorts[2].Cohort, []int{3, 4, 5, 6, 7})
}

func TestRACNormConcurrent2(t *testing.T) {
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
		res := w1.RACSubmit(nil, w1)
		utils.Assert(res, "The RAC Failed")
		ch <- res
	}()
	time.Sleep(20 * time.Millisecond) // need to make sure the contention is not too high.
	go func() {
		res := w2.RACSubmit(nil, w2)
		utils.Assert(res, "The RAC Failed")
		ch <- res
	}()
	<-ch
	<-ch
	CheckVal(cohorts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(cohorts[1].Cohort, []int{2, 3, 4, 5, 6})
	CheckVal(cohorts[2].Cohort, []int{3, 4, 5, 6, 7})
}

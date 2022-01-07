package cohorts

import (
	"github.com/allvphx/RAC/remote"
	"github.com/allvphx/RAC/rlsm"
	"github.com/allvphx/RAC/utils"
	"testing"
)

func CheckVal(coh *CohortManager, ans []int) {
	for i := 0; i < len(ans); i++ {
		v, ok := coh.Kv.Read(i)
		utils.Assert(ok && ans[i] == v, "CKv : PreRead Failed with different value")
	}
}

func TestConcurrent(t *testing.T) {
	stmts := CohortsTestKit()
	txn1 := remote.NewRACTransaction(1, stmts[0].cohortID, rlsm.NoCFNoNF, utils.OU_addrs)
	stmts[0].Cohort.forTestPreRead(txn1)
	txn2 := remote.NewRACTransaction(2, stmts[0].cohortID, rlsm.NoCFNoNF, utils.OU_addrs)
	stmts[0].Cohort.forTestPreRead(txn2)
	CheckVal(stmts[0].Cohort, []int{0, 1, 2, 3, 4})

	txn1.OptList = txn1.OptList[:0]
	txn2.OptList = txn2.OptList[:0]
	for i := 0; i < 2; i++ {
		txn1.AddUpdate(stmts[0].cohortID, i, i+1)
		txn2.AddUpdate(stmts[0].cohortID, i+2, i+1)
	}
	ch := make(chan bool)
	go func() {
		ok := stmts[0].Cohort.PreWrite(txn1)
		if !ok {
			println("fail txn1")
		}
		ch <- stmts[0].Cohort.Commit(txn1)
	}()
	go func() {
		ok := stmts[0].Cohort.PreWrite(txn2)
		if !ok {
			println("fail txn2")
		}
		ch <- stmts[0].Cohort.Commit(txn2)
	}()
	<-ch
	<-ch

	CheckVal(stmts[0].Cohort, []int{1, 2, 1, 2, 4})
	StopServers(stmts)
}

func TestNOCFNONF(t *testing.T) {
	stmts := CohortsTestKit()
	txn11 := remote.NewRACTransaction(1, stmts[0].cohortID, rlsm.NoCFNoNF, utils.OU_addrs)
	stmts[0].Cohort.forTestPreRead(txn11)
	txn12 := remote.NewRACTransaction(1, stmts[1].cohortID, rlsm.NoCFNoNF, utils.OU_addrs)
	stmts[1].Cohort.forTestPreRead(txn12)
	CheckVal(stmts[0].Cohort, []int{0, 1, 2, 3, 4})
	CheckVal(stmts[1].Cohort, []int{0, 1, 2, 3, 4})

	txn11.OptList = txn11.OptList[:0]
	txn12.OptList = txn12.OptList[:0]
	for i := 0; i < 5; i++ {
		txn11.AddUpdate(stmts[0].cohortID, i, i+1)
		txn12.AddUpdate(stmts[1].cohortID, i, i+2)
	}
	ch := make(chan bool)
	ans := rlsm.NewKvResult(2)
	go func() {
		res := stmts[0].Cohort.Propose(txn11)
		ans.Append(res)
		ch <- true
	}()
	go func() {
		res := stmts[1].Cohort.Propose(txn12)
		ans.Append(res)
		ch <- true
	}()
	<-ch
	<-ch
	print(ans.String())
	stmts[0].Cohort.Commit(txn11)
	stmts[1].Cohort.Commit(txn12)

	CheckVal(stmts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(stmts[1].Cohort, []int{2, 3, 4, 5, 6})
	StopServers(stmts)
}

func TestCFNONF(t *testing.T) {
	stmts := CohortsTestKit()
	txn11 := remote.NewRACTransaction(1, stmts[0].cohortID, rlsm.CFNoNF, utils.OU_addrs[:2])
	stmts[0].Cohort.forTestPreRead(txn11)
	txn12 := remote.NewRACTransaction(1, stmts[1].cohortID, rlsm.CFNoNF, utils.OU_addrs[:2])
	stmts[1].Cohort.forTestPreRead(txn12)
	CheckVal(stmts[0].Cohort, []int{0, 1, 2, 3, 4})
	CheckVal(stmts[1].Cohort, []int{0, 1, 2, 3, 4})

	txn11.OptList = txn11.OptList[:0]
	txn12.OptList = txn12.OptList[:0]
	for i := 0; i < 5; i++ {
		txn11.AddUpdate(stmts[0].cohortID, i, i+1)
		txn12.AddUpdate(stmts[1].cohortID, i, i+2)
	}
	ch := make(chan bool)
	ans := rlsm.NewKvResult(2)
	go func() {
		res := stmts[0].Cohort.Propose(txn11)
		ans.Append(res)
		ch <- true
	}()
	go func() {
		res := stmts[1].Cohort.Propose(txn12)
		ans.Append(res)
		ch <- true
	}()
	<-ch
	<-ch
	print(ans.String())
	stmts[0].Cohort.Commit(txn11)
	stmts[1].Cohort.Commit(txn12)

	CheckVal(stmts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(stmts[1].Cohort, []int{2, 3, 4, 5, 6})
	StopServers(stmts)
}

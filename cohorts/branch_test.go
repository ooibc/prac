package cohorts

import (
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/remote"
	"github.com/allvphx/RAC/rlsm"
	"github.com/allvphx/RAC/utils"
	"testing"
	"time"
)

var addrs = []string{"127.0.0.1:6001", "127.0.0.1:6002", "127.0.0.1:6003"}

func CohortsTestKit() []*CohortStmt {
	stmts := make([]*CohortStmt, 3)
	ch := make(chan bool)

	var Args1 = []string{"*", "*", addrs[0], "5"}
	stmts[0] = &CohortStmt{}
	go begin(stmts[0], ch, Args1[2])
	<-ch

	var Args2 = []string{"*", "*", addrs[1], "5"}
	stmts[1] = &CohortStmt{}
	go begin(stmts[1], ch, Args2[2])
	<-ch

	var Args3 = []string{"*", "*", addrs[2], "5"}
	stmts[2] = &CohortStmt{}
	go begin(stmts[2], ch, Args3[2])
	<-ch

	for i := 0; i < 3; i++ {
		stmts[i].Cohort.Kv.SetDDL(time.Millisecond * 200)
		for j := 0; j < 5; j++ {
			stmts[i].Cohort.Kv.Update(j, j)
		}
	}
	return stmts
}

func StopServers(stmt []*CohortStmt) {
	for _, v := range stmt {
		v.Stop()
	}
	time.Sleep(constants.OptEps * 10)
}

func CheckPreRead(coh *CohortManager, txn *remote.RACTransaction, ans []int, shardID string) {
	if len(txn.OptList) > 0 {
		txn.OptList = txn.OptList[:0]
	}
	for i := 0; i < 5; i++ {
		txn.AddRead(shardID, i)
	}
	ok, val := coh.forTestPreRead(txn)
	utils.Assert(ok && len(val) == len(ans), "PreRead Failed or With different length")
	for i := 0; i < len(ans); i++ {
		utils.Assert(ans[i] == val[utils.Hash(coh.stmt.cohortID, i)], "PreRead Failed with different value")
	}
}

func TestBuildTestCases(t *testing.T) {
	stmts := CohortsTestKit()
	CheckVal(stmts[0].Cohort, []int{0, 1, 2, 3, 4})
	CheckVal(stmts[1].Cohort, []int{0, 1, 2, 3, 4})
	CheckVal(stmts[2].Cohort, []int{0, 1, 2, 3, 4})
	StopServers(stmts)
}

func TestCohortsPreRead(t *testing.T) {
	stmts := CohortsTestKit()
	txn1 := remote.NewRACTransaction(1, stmts[0].cohortID, rlsm.CFNF, addrs)
	txn2 := remote.NewRACTransaction(2, stmts[1].cohortID, rlsm.CFNF, addrs)

	c := make(chan bool)

	go func() {
		CheckPreRead(stmts[1].Cohort, txn2, []int{0, 1, 2, 3, 4}, stmts[1].cohortID)
		c <- true
	}()
	go func() {
		CheckPreRead(stmts[0].Cohort, txn1, []int{0, 1, 2, 3, 4}, stmts[0].cohortID)
		c <- true
	}()
	<-c
	<-c
	StopServers(stmts)
}

func TestCohortsPreWrite(t *testing.T) {
	stmts := CohortsTestKit()
	txn1 := remote.NewRACTransaction(1, stmts[0].cohortID, rlsm.CFNF, addrs)
	CheckPreRead(stmts[0].Cohort, txn1, []int{0, 1, 2, 3, 4}, stmts[0].cohortID)

	txn1.OptList = txn1.OptList[:0]
	for i := 0; i < 5; i++ {
		txn1.AddUpdate(stmts[0].cohortID, i, i+1)
	}
	stmts[0].Cohort.PreWrite(txn1)
	stmts[0].Cohort.Commit(txn1)
	CheckVal(stmts[0].Cohort, []int{1, 2, 3, 4, 5})
	StopServers(stmts)
}

func Test2PCCommit(t *testing.T) {
	stmts := CohortsTestKit()
	txn1 := remote.NewRACTransaction(1, stmts[0].cohortID, rlsm.CFNF, addrs)
	CheckPreRead(stmts[0].Cohort, txn1, []int{0, 1, 2, 3, 4}, stmts[0].cohortID)
	txn2 := remote.NewRACTransaction(1, stmts[1].cohortID, rlsm.CFNF, addrs)
	CheckPreRead(stmts[1].Cohort, txn2, []int{0, 1, 2, 3, 4}, stmts[1].cohortID)
	txn1.OptList = txn1.OptList[:0]
	txn2.OptList = txn2.OptList[:0]
	for i := 0; i < 5; i++ {
		txn1.AddUpdate(stmts[0].cohortID, i, i+1)
		txn2.AddUpdate(stmts[1].cohortID, i, i+2)
	}

	c := make(chan bool)

	go func() {
		ok := stmts[0].Cohort.PreWrite(txn1)
		c <- utils.Assert(ok, "PreWrite Made Failed")
	}()
	go func() {
		ok := stmts[1].Cohort.PreWrite(txn2)
		c <- utils.Assert(ok, "PreWrite Made Failed")
	}()
	<-c
	<-c

	go func() {
		ok := stmts[0].Cohort.Commit(txn1)
		c <- utils.Assert(ok, "Commit Made Failed")
	}()
	go func() {
		ok := stmts[1].Cohort.Commit(txn2)
		c <- utils.Assert(ok, "Commit Made Failed")
	}()
	<-c
	<-c

	CheckPreRead(stmts[0].Cohort, txn1, []int{1, 2, 3, 4, 5}, stmts[0].cohortID)
	CheckVal(stmts[0].Cohort, []int{1, 2, 3, 4, 5})
	CheckVal(stmts[1].Cohort, []int{2, 3, 4, 5, 6})
	StopServers(stmts)
}

func Test2PCAbort(t *testing.T) {
	stmts := CohortsTestKit()
	txn1 := remote.NewRACTransaction(1, stmts[0].cohortID, rlsm.CFNF, addrs)
	CheckPreRead(stmts[0].Cohort, txn1, []int{0, 1, 2, 3, 4}, stmts[0].cohortID)
	txn2 := remote.NewRACTransaction(1, stmts[1].cohortID, rlsm.CFNF, addrs)
	CheckPreRead(stmts[1].Cohort, txn2, []int{0, 1, 2, 3, 4}, stmts[1].cohortID)
	txn1.OptList = txn1.OptList[:0]
	txn2.OptList = txn2.OptList[:0]
	for i := 0; i < 5; i++ {
		txn1.AddUpdate(stmts[0].cohortID, i, i+1)
		txn2.AddUpdate(stmts[1].cohortID, i, i+2)
	}
	stmts[0].Cohort.Kv.SetDDL(0)

	ok := stmts[0].Cohort.PreWrite(txn1)
	ok = ok && stmts[1].Cohort.PreWrite(txn2)
	utils.Assert(!ok, "PreWrite Fail Made Failed")

	ok = stmts[0].Cohort.Abort(txn1)
	ok = ok && stmts[1].Cohort.Abort(txn2) // if the first term is false, the commit will not be executed.
	utils.Assert(ok, "Abort Made Failed")

	CheckPreRead(stmts[0].Cohort, txn1, []int{0, 1, 2, 3, 4}, stmts[0].cohortID)
	CheckPreRead(stmts[1].Cohort, txn2, []int{0, 1, 2, 3, 4}, stmts[1].cohortID)
	StopServers(stmts)
}

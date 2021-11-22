package collaborator

import (
	"github.com/allvphx/RAC/utils"
	"testing"
)

func TestPreRead(t *testing.T) {
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

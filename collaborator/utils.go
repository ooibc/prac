package collaborator

import (
	"github.com/allvphx/RAC/cohorts"
	"time"
)

var caID = "10.148.0.2:2001"

func RemoteTestkit() *CollaboratorStmt {
	stmt := &CollaboratorStmt{}
	var Arg = []string{"*", "*", caID}
	ch := make(chan bool)
	go begin(stmt, Arg, ch)
	<-ch
	return stmt
}

func CollaboratorBatchTestKit() (*CollaboratorStmt, []*cohorts.CohortStmt) {
	stmt := &CollaboratorStmt{}
	var Arg = []string{"*", "*", caID}
	ch := make(chan bool)
	go begin(stmt, Arg, ch)
	<-ch

	return stmt, cohorts.OU_CohortsTestKitBatch()
}

func CollaboratorTPCTestKit() (*CollaboratorStmt, []*cohorts.CohortStmt) {
	stmt := &CollaboratorStmt{}
	var Arg = []string{"*", "*", caID}
	ch := make(chan bool)
	go begin(stmt, Arg, ch)
	<-ch
	time.Sleep(10 * time.Millisecond)

	return stmt, cohorts.OU_CohortsTestKitTPC()
}

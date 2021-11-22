package experiment

import (
	"github.com/allvphx/RAC/collaborator"
	"testing"
)

func TestTPCCLocal(t *testing.T) {
	st := TPCStmt{}
	st.TPCC_Test(nil)
}

func TestTPCCRemote(t *testing.T) {
	st := TPCStmt{}
	ca := collaborator.RemoteTestkit()
	st.TPCC_Test(ca)
}

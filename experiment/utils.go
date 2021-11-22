package experiment

import (
	"github.com/allvphx/RAC/constants"
)

func TestYCSB(protocol string, addr string) {
	st := YCSBStmt{}
	constants.SetProtocol(protocol)
	st.YCSB_Test()
	st.Stop()
}

func TestTPC(protocol string, addr string) {
	st := TPCStmt{}
	constants.SetProtocol(protocol)
	st.TPCC_Test(nil)
	st.Stop()
}

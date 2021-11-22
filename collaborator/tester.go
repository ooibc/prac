package collaborator

import (
	"github.com/allvphx/RAC/constants"
	"sync"
)

var idMu = sync.Mutex{}
var txnID = 0

func GetTxnID() int {
	idMu.Lock()
	defer idMu.Unlock()
	txnID = (txnID + 1) % constants.Max_Txn_ID
	return txnID
}

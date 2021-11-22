package remote

import (
	"github.com/allvphx/RAC/opt"
	"github.com/allvphx/RAC/rlsm"
)

// RACTransaction storages the transaction information for the RAC here we only handle read and writes.
// It can be transferred.
type RACTransaction struct {
	TxnID         int
	It            string // the current shard id
	ProtocolLevel rlsm.Level
	OptList       []opt.RACOpt
	// shall be changed to help handle the RPC.
	Participants []string // the id of participants of the transaction.
}

// NewRACTransaction the shardID is a rank.
func NewRACTransaction(TID int, shardID string, level rlsm.Level, parts []string) *RACTransaction {
	res := &RACTransaction{
		TxnID:         TID,
		It:            shardID,
		ProtocolLevel: level,
		OptList:       make([]opt.RACOpt, 0),
		Participants:  parts,
	}
	return res
}

func (c *RACTransaction) AddRead(shard string, key int) {
	c.OptList = append(c.OptList, opt.RACOpt{
		Shard: shard,
		Key:   key,
		Value: -1,
		Cmd:   opt.ReadOpt,
	})
}

func (c *RACTransaction) AddUpdate(shard string, key int, value int) {
	c.OptList = append(c.OptList, opt.RACOpt{
		Shard: shard,
		Key:   key,
		Value: value,
		Cmd:   opt.UpdateOpt,
	})
}

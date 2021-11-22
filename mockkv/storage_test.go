package mockkv

import (
	"fmt"
	"testing"
	"time"
)

func newShardTestKit() *Shard {
	res := newShardKV(5)
	res.SetDDL(time.Millisecond * 20)
	return res
}

// aimVal = -1 for any result
func printRes(c *Shard, txnID int, key int, aimVal int) {
	val, ok := 0, false
	if txnID != -1 {
		val, ok = c.ReadTxn(txnID, key)
	} else {
		val, ok = c.Read(key)
	}
	if ok && aimVal != val && aimVal != -1 {
		_ = fmt.Errorf("t%d: val[%d] = %d\n", txnID, key, val)
	} else if !ok && aimVal != -1 {
		_ = fmt.Errorf("t%d: [%d]invalid\n", txnID, key)
	}
}

func tryUpdate(c *Shard, txnID int, key int, val int) {
	ok := false
	if txnID == -1 {
		ok = c.Update(key, val)
		txnID = 0
	} else {
		ok = c.UpdateTxn(txnID, key, val)
	}
	if ok {
		//		fmt.Printf("t%d : %d is updated to %d\n", txnID, key, val)
	} else {
		_ = fmt.Errorf("t$%d : %d update failed\n", txnID, key)
	}
}

func TestKVInteractive(t *testing.T) {
	c := newShardTestKit()
	c.Update(0, 1)
	go func() {
		for i := 0; i < 5; i++ {
			tryUpdate(c, -1, i, i+1)
			printRes(c, -1, i, -1)
			time.Sleep(time.Millisecond)
		}
	}()
	go func() {
		for i := 0; i < 5; i++ {
			tryUpdate(c, -1, i, i+2)
			printRes(c, -1, i, -1)
			time.Sleep(time.Millisecond)
		}
	}()
	time.Sleep(time.Second)
}

func TestKVTransaction(t *testing.T) {
	c := newShardTestKit()
	for i := 0; i < 5; i++ {
		c.Update(i, i)
	}
	go func() {
		c.Begin(1)
		for i := 0; i < 5; i++ {
			printRes(c, 1, i, i)
			tryUpdate(c, 1, i, i+1)
			printRes(c, 1, i, i+1)
		}
		c.Commit(1)
	}()
	time.Sleep(time.Millisecond * 10)
	go func() {
		c.Begin(2)
		for i := 0; i < 5; i++ {
			printRes(c, 2, i, i+1)
			tryUpdate(c, 2, i, i+2)
			printRes(c, 2, i, i+2)
		}
		c.Commit(2)
	}()
	time.Sleep(time.Second)
	for i := 0; i < 5; i++ {
		printRes(c, -1, i, i+2)
	}
}

func TestKVRollBack(t *testing.T) {
	c := newShardTestKit()
	for i := 0; i < 5; i++ {
		c.Update(i, i)
	}
	go func() {
		c.Begin(1)
		for i := 0; i < 5; i++ {
			printRes(c, 1, i, i)
			tryUpdate(c, 1, i, i+1)
			printRes(c, 1, i, i+1)
		}
		c.RollBack(1)
	}()
	time.Sleep(time.Millisecond * 10)
	go func() {
		c.Begin(2)
		for i := 0; i < 5; i++ {
			printRes(c, 2, i, i)
			tryUpdate(c, 2, i, i+2)
			printRes(c, 2, i, i+2)
		}
		c.RollBack(2)
	}()
	time.Sleep(time.Second)
	for i := 0; i < 5; i++ {
		printRes(c, -1, i, i)
	}
}

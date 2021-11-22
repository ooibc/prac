package mockkv

import (
	"github.com/allvphx/RAC/utils"
	"testing"
	"time"
)

func newEntriesTestKit() *shardEntry {
	From := newShardKV(10)
	return newShardEntry(1, -1, -1, From)
}

func TestKVEntries(t *testing.T) {
	c := newEntriesTestKit()
	go func() {
		for i := 0; i < 100; i++ {
			utils.Assert(c.update(-1, 2, true), "The Write is failed")
		}
	}()
	go func() {
		for i := 0; i < 100; i++ {
			c.update(-1, 3, false)
		}
	}()
	for i := 0; i < 200; i++ {
		val, ok := c.read(true)
		utils.Assert(ok, "The Read is lost")
		utils.Assert(val == 2 || val == 3 || val == -1, "The read value is invalid")
		time.Sleep(time.Millisecond)
	}
}

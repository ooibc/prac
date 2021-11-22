package collaborator

import (
	"github.com/allvphx/RAC/rlsm"
	"sync"
)

type PD_Server struct {
	mu           *sync.Mutex
	state        *rlsm.LevelStateManager // the state machine Manager
	shardMapper  map[int]int             // the map from an overall address to the shard id.
	offsetMapper map[int]int             // the map from an overall address to the offset in the shard.
}

// Register register a location in PD.
func (c *PD_Server) Register(addr int, shard int, offset int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if val, ok := c.shardMapper[addr]; ok && (val != shard || c.offsetMapper[addr] != offset) {
		return false
	}
	c.shardMapper[addr] = shard
	c.offsetMapper[addr] = offset
	return true
}

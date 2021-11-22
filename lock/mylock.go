package lock

import (
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

const (
	MaxTimeOut     = 60 * 60 * 1000 * time.Millisecond
	WriteProtectNs = 5 * 1000
)

func getTimeOut(t time.Duration) time.Duration {
	if t >= 0 {
		return t
	} else {
		return MaxTimeOut
	}
}

type RWLocker struct {
	read                int
	write               int
	writeProtectEndTime int64
	prevStack           []byte
	mu                  sync.Mutex
}

func (c *RWLocker) upgradeLock() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.write == 1 || c.read > 1 {
		// avoid the write lock starvation caused by multiple read lock requests.
		c.writeProtectEndTime = time.Now().UnixNano() + WriteProtectNs
		return false
	}
	c.write = 1
	c.read = 0
	c.writeProtectEndTime = time.Now().UnixNano()
	return true
}

func (c *RWLocker) UpgradeLock(t time.Duration) (successful bool, prevStack string, currStack string) {
	t = getTimeOut(t)
	for st := time.Now(); time.Now().Sub(st) < t; {
		if c.upgradeLock() {
			successful = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if successful == false {
		if c.prevStack != nil && len(c.prevStack) > 0 {
			prevStack = string(c.prevStack)
		}
		currStack = string(debug.Stack())
	}
	return
}

func (c *RWLocker) WaitUpgradeLock() {
	successful, prevStack, currStack := c.UpgradeLock(-1)
	if successful == false {
		fmt.Printf("RWLocker:WaitRLock():{PrevStack:%s, currStack:%s}\n", prevStack, currStack)
	}
}

func (c *RWLocker) lock() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.write == 1 || c.read > 0 {
		// avoid the write lock starvation caused by multiple read lock requests.
		c.writeProtectEndTime = time.Now().UnixNano() + WriteProtectNs
		return false
	}
	c.write = 1
	c.writeProtectEndTime = time.Now().UnixNano()
	return true
}

func (c *RWLocker) Lock(t time.Duration) (successful bool, prevStack string, currStack string) {
	t = getTimeOut(t)
	for st := time.Now(); time.Now().Sub(st) < t; {
		if c.lock() {
			successful = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if successful == false {
		if c.prevStack != nil && len(c.prevStack) > 0 {
			prevStack = string(c.prevStack)
		}
		currStack = string(debug.Stack())
	}
	return
}

func (c *RWLocker) WaitLock() {
	successful, prevStack, currStack := c.Lock(-1)
	if successful == false {
		fmt.Printf("RWLocker:WaitLock():{PrevStack:%s, currStack:%s}\n", prevStack, currStack)
	}
}

func (c *RWLocker) Unlock() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.write = 0
}

func (c *RWLocker) rlock() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.write == 1 || time.Now().UnixNano() < c.writeProtectEndTime {
		return false
	}
	c.read += 1

	return true
}

func (c *RWLocker) RLock(t time.Duration) (successful bool, prevStack string, currStack string) {
	t = getTimeOut(t)
	for st := time.Now(); time.Now().Sub(st) < t; {
		if c.rlock() {
			successful = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if successful == false {
		if c.prevStack != nil && len(c.prevStack) > 0 {
			prevStack = string(c.prevStack)
		}
		currStack = string(debug.Stack())
	}
	return
}

func (c *RWLocker) WaitRLock() {
	successful, prevStack, currStack := c.RLock(-1)
	if successful == false {
		fmt.Printf("RWLocker:WaitRLock():{PrevStack:%s, currStack:%s}\n", prevStack, currStack)
	}
}

func (c *RWLocker) RUnlock() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.read > 0 {
		c.read--
	}
}

func NewRWLocker() *RWLocker {
	return &RWLocker{}
}

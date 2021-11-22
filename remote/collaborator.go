package remote

import (
	"github.com/allvphx/RAC/rlsm"
)

type Message4CA struct {
	TID  int
	Mark string
	Res  rlsm.KvRes
	Read map[string]int
	ACK  bool
}

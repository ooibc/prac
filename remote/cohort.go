package remote

type Message4CO struct {
	Mark string
	Txn  RACTransaction
	Vt   RACVote
}

func (c *Message4CO) String() string {
	return c.Mark
}

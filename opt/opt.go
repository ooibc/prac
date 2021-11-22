package opt

const ReadOpt int = 1
const UpdateOpt int = 2

// ReadOpt is used in the transaction.
type RACOpt struct {
	Cmd   int
	Shard string
	Key   int
	Value int
}

func (r *RACOpt) GetKey() (string, int) {
	return r.Shard, r.Key
}

func (r *RACOpt) GetValue() (int, bool) {
	return r.Value, r.Cmd == UpdateOpt
}

package mockkv

// NewKV external API for creating a local KV.
func NewKV(len int) *Shard {
	return newShardKV(len)
}

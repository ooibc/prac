package utils

import (
	"github.com/allvphx/RAC/constants"
	"hash/fnv"
)

func hash(s string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	CheckError(err)
	return int(h.Sum32()) % constants.NUM_ELEMENTS
}

func TransTableItem(tableName string, ware int, key int) int {
	if tableName == "order" {
		return key
	} else {
		return 100000 + 10000*ware + key
	}
}

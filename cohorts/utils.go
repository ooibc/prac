package cohorts

import (
	"encoding/csv"
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/utils"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

var OU_addrs = []string{"10.170.0.2:2001", "10.140.0.2:2001", "10.140.0.3:2001"}

func comStart() []*CohortStmt {
	stmts := make([]*CohortStmt, 3)
	ch := make(chan bool)
	stmts[0] = &CohortStmt{}
	go begin(stmts[0], ch, OU_addrs[0])
	<-ch

	stmts[1] = &CohortStmt{}
	go begin(stmts[1], ch, OU_addrs[1])
	<-ch

	stmts[2] = &CohortStmt{}
	go begin(stmts[2], ch, OU_addrs[2])
	<-ch
	return stmts
}

// OU_CohortsTestKit For test
func OU_CohortsTestKit() []*CohortStmt {
	stmts := comStart()
	for i := 0; i < 3; i++ {
		for j := 0; j < 5; j++ {
			stmts[i].Cohort.Kv.Update(j, j)
		}
	}
	return stmts
}

func OU_CohortsTestKitBatch() []*CohortStmt {
	stmts := comStart()

	for i := 0; i < constants.NUM_PARTITIONS; i++ {
		for j := 0; j < constants.NUM_ELEMENTS; j++ {
			stmts[i].Cohort.Kv.Update(j, j)
		}
	}
	return stmts
}

func OU_CohortsTestKitTPC() []*CohortStmt {
	stmts := comStart()

	for i := 0; i < constants.NUM_PARTITIONS; i++ {
		stmts[i].LoadStock()
		time.Sleep(10 * time.Millisecond)
	}
	return stmts
}

var stockLock = sync.Mutex{}

// LoadStock load the history data of each warehouse for TPC-C benchmark
func (c *CohortStmt) LoadStock() {
	stockLock.Lock()
	defer stockLock.Unlock()
	file, err := os.Open("./experiment/data/stock.csv")
	if err != nil {
		file, err = os.Open("../experiment/data/stock.csv")
	}
	utils.CheckError(err)
	defer file.Close()
	s := csv.NewReader(file)
	cnt := 0
	for {
		row, err := s.Read()
		if err == io.EOF {
			break
		}
		item, err := strconv.Atoi(row[0])
		utils.CheckError(err)
		ware, err := strconv.Atoi(row[1])
		utils.CheckError(err)
		count, err := strconv.Atoi(row[2])
		utils.CheckError(err)
		ware--
		if OU_addrs[ware] == c.cohortID {
			cnt++
			c.Cohort.Kv.Update(utils.TransTableItem("stock", ware, item), count)
		}
	}
	utils.TPrintf("Initialize the stock over")
}

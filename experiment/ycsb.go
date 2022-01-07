package experiment

import (
	"fmt"
	"github.com/allvphx/RAC/cohorts"
	"github.com/allvphx/RAC/collaborator"
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/opt"
	"github.com/allvphx/RAC/utils"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"
)

type YCSBStmt struct {
	txnCount       int32
	countCommitted int32
	latencySum     int64
	ca             *collaborator.CollaboratorStmt
	co             []*cohorts.CohortStmt
	stop           int32
}

type YCSBClient struct {
	md   int
	from *YCSBStmt
}

// md = 0, 1, 2 single co. md = 3, 4, 5 double co. md = 6 ~ 9, triple.
func (c *YCSBClient) generateTxnKVpairs(parts []string) []opt.RACOpt {
	NUM_ELEMENTS := 10000
	rand.Seed(time.Now().UTC().UnixNano())
	res := make([]opt.RACOpt, 0)
	var key, j, val int
	md := c.md
	utils.TPrintf(" --- generate begin --- " + strconv.Itoa(md))

	for i := 0; i < constants.WORK_LOAD; i++ {
		/* Code for contentious key selection */
		j = random(0, constants.NUM_PARTITIONS-1)
		if md < 3 {
			j = md
		} else if md <= 5 {
			md -= 3
			if i < 2 {
				j = i
				if j >= md {
					j = (j + 1) % 3
				}
			}
		}
		//		j = 0
		val = rand.Intn(NUM_ELEMENTS)
		if i < constants.NUM_PARTITIONS && md > 5 {
			/* Ensure txn spans all partitions */
			j = i
			val = constants.NULL + 1
		}
		/* Based on the contention ratio, choose what key to use for this operation.
		   If contention ratio is 90:10 ==> 90% of ops work on 10% of data. */
		dataItems := int(float64(100-constants.CONTENTION) / 100 * float64(NUM_ELEMENTS))
		if random(1, 100) <= constants.CONTENTION {
			key = random(0, dataItems-1)
		} else {
			/* Choose id from the less contentious data */
			key = random(dataItems, NUM_ELEMENTS-1)
		}
		/* Access the key from different partitions */
		utils.TPrintf(strconv.Itoa(j) + "[" + strconv.Itoa(key) + "] = " + strconv.Itoa(val))

		res = append(res, opt.RACOpt{
			Cmd:   opt.UpdateOpt,
			Shard: parts[j],
			Key:   key,
			Value: val,
		})
	}
	utils.TPrintf(" --- generate end --- ")
	c.md = (c.md + 1) % 10
	return res
}

func (c *YCSBClient) performTransactions(TID int, cohorts []string, kvD []opt.RACOpt, latency *time.Duration) bool {
	kvData := kvD
	defer utils.TimeTrack(time.Now(), "performTransactions", TID)
	if kvD == nil { // if given transaction, perform it.
		kvData = c.generateTxnKVpairs(cohorts)
	}
	exist := make(map[string]bool)
	parts := make([]string, 0)
	for _, v := range kvData {
		sd := v.Shard
		if exist[sd] == false {
			exist[sd] = true
			parts = append(parts, sd)
		}
	}
	txn := collaborator.NewDBTransaction(TID, 0, parts, c.from.ca.Manager)
	txn.OptList = kvData
	return c.from.ca.Manager.SubmitTxn(txn, constants.TPCC_Protocol, latency, nil)
}

func analysisYCSB(successes int, txnCount int, start time.Time, totalTimePerTxn time.Duration) {
	var totalTime time.Duration

	totalTime = time.Since(start)
	msg := "count:" + strconv.Itoa(int(txnCount)) + ";"
	msg += "concurrency:" + strconv.Itoa(constants.CONCURRENCY) + ";"
	if txnCount == 0 {
		msg += "latency:" + "0ms" + ";"
	} else {
		msg += "latency:" + time.Duration(totalTimePerTxn.Nanoseconds()/int64(txnCount)).String() + ";"
	}
	msg += "totalTime:" + totalTime.String() + ";"
	msg += "partitions:" + strconv.Itoa(constants.NUM_PARTITIONS) + ";"
	msg += "success:" + strconv.Itoa(int(successes)) + ";"
	msg += "storage_size:" + strconv.Itoa(constants.NUM_ELEMENTS)
	fmt.Println(msg)
}

func (stmt *YCSBStmt) logResult(start time.Time) {
	analysisYCSB(int(stmt.countCommitted), int(stmt.txnCount),
		start, time.Duration(stmt.latencySum))
	atomic.StoreInt32(&stmt.txnCount, 0)
	atomic.StoreInt32(&stmt.countCommitted, 0)
	atomic.StoreInt64(&stmt.latencySum, 0)
}

func (stmt *YCSBStmt) Stopped() bool {
	return atomic.LoadInt32(&stmt.stop) != 0
}

func (stmt *YCSBStmt) startYCSBClient() {
	client := YCSBClient{md: 0, from: stmt}
	for !stmt.Stopped() {
		latency := time.Duration(0)
		TID := collaborator.GetTxnID()
		if client.performTransactions(TID, utils.OU_addrs, nil, &latency) {
			atomic.AddInt64(&stmt.latencySum, int64(latency))
			atomic.AddInt32(&stmt.txnCount, 1)
			atomic.AddInt32(&stmt.countCommitted, 1)
		} else {
			atomic.AddInt64(&stmt.latencySum, int64(latency))
			atomic.AddInt32(&stmt.txnCount, 1)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (stmt *YCSBStmt) Stop() {
	stmt.ca.Stop()
	atomic.StoreInt32(&stmt.stop, 1)
	if stmt.co == nil {
		return
	}
	for _, v := range stmt.co {
		v.Stop()
	}
}

func (stmt *YCSBStmt) YCSB_Test() {
	if utils.LocalTest {
		stmt.ca, stmt.co = collaborator.CollaboratorBatchTestKit()
	} else {
		stmt.ca = collaborator.RemoteTestkit("10.184.0.2:2001")
		stmt.co = nil
	}
	stmt.latencySum = 0
	stmt.txnCount = 0
	stmt.countCommitted = 0
	for i := 0; i < constants.CONCURRENCY; i++ {
		go stmt.startYCSBClient()
		time.Sleep(2 * time.Millisecond)
	}
	utils.TPrintf("All clients Started")
	time.Sleep(5 * time.Second)
	atomic.StoreInt32(&stmt.txnCount, 0)
	atomic.StoreInt32(&stmt.countCommitted, 0)
	atomic.StoreInt64(&stmt.latencySum, 0)
	st := time.Now()
	time.Sleep(5 * time.Second)
	stmt.logResult(st)
}

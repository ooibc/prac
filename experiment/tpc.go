package experiment

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/allvphx/RAC/cohorts"
	"github.com/allvphx/RAC/collaborator"
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/utils"
	set "github.com/deckarep/golang-set"
	"github.com/jinzhu/copier"
	"io"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type TPCClient struct {
	needStock   set.Set
	payed       set.Set
	allOrderIDs set.Set
	pop         int
	from        *TPCStmt
}

func NewTPCClient() *TPCClient {
	c := &TPCClient{}
	c.needStock = set.NewSet()
	c.payed = set.NewSet() // <= 1000
	c.allOrderIDs = set.NewSet()
	return c
}

type TPCStmt struct {
	ca         *collaborator.CollaboratorStmt
	co         []*cohorts.CohortStmt
	protocol   string
	orderLine  int32
	OrderPoll  []*TPCOrder
	txnCount   int32
	failS      int32
	success    int32
	latencySum int64
	levelSum   int64
	startTime  time.Time
	stop       int32
}

func (c *TPCStmt) ReadOrder(o *csv.Reader, ol *csv.Reader) *TPCOrder {
	row, err := o.Read()
	if err == io.EOF {
		return nil
	}
	utils.CheckError(err)

	res := &TPCOrder{Items: make([]*TPCOrderLine, 0)}
	res.Order, err = strconv.Atoi(row[0])
	utils.CheckError(err)
	res.Ware, err = strconv.Atoi(row[1])
	utils.CheckError(err)
	res.Ware--
	res.Customer, err = strconv.Atoi(row[2])
	utils.CheckError(err)
	count, err := strconv.Atoi(row[3])
	utils.CheckError(err)
	for i := 0; i < count; i++ {
		row, err = ol.Read()
		if err == io.EOF {
			return nil
		}
		utils.CheckError(err)
		item, err := strconv.Atoi(row[0])
		utils.CheckError(err)
		amount, err := strconv.Atoi(row[1])
		utils.CheckError(err)
		if i < 5 {
			res.Items = append(res.Items, &TPCOrderLine{
				Item:  item,
				Count: amount})
		}
	}
	return res
}

func JPrint(v interface{}) {
	byt, _ := json.Marshal(v)
	fmt.Println(string(byt))
}

func analysisTPC(txnCnt int32, sucess int32, latencySum time.Duration, levelSum int64, start time.Time, failS int32) {
	totalTime := time.Since(start)
	msg := "count:" + strconv.Itoa(int(txnCnt)) + ";"
	msg += "concurrency:" + strconv.Itoa(constants.CONCURRENCY) + ";"
	msg += "success:" + strconv.Itoa(int(sucess)) + ";"
	msg += "prec:" + strconv.Itoa(int(failS)) + ";"
	if txnCnt == 0 {
		msg += "latency:nil;"
		msg += "avglevel:nil;"
	} else {
		msg += "latency:" + time.Duration(latencySum.Nanoseconds()/int64(txnCnt)).String() + ";"
		msg += "avglevel:" + fmt.Sprintf("%f", float64(levelSum)/float64(txnCnt)) + ";"
	}
	msg += "totalTime:" + totalTime.String() + ";"
	fmt.Println(msg)
}

func (stmt *TPCStmt) logResults() {
	analysisTPC(atomic.LoadInt32(&stmt.txnCount),
		atomic.LoadInt32(&stmt.success),
		time.Duration(atomic.LoadInt64(&stmt.latencySum)),
		atomic.LoadInt64(&stmt.levelSum),
		stmt.startTime,
		atomic.LoadInt32(&stmt.failS))
	stmt.startTime = time.Now()
	atomic.StoreInt32(&stmt.txnCount, 0)
	atomic.StoreInt32(&stmt.failS, 0)
	atomic.StoreInt64(&stmt.latencySum, 0)
	atomic.StoreInt64(&stmt.levelSum, 0)
	atomic.StoreInt32(&stmt.success, 0)
	atomic.StoreInt32(&stmt.failS, 0)
}

func (c *TPCStmt) GetOrder() *TPCOrder {
	return c.OrderPoll[random(0+100, len(c.OrderPoll)-100)]
}

func (c *TPCStmt) RandomizeRead() {
	file_order, err := os.Open("./data/new_order.csv")
	if err != nil {
		file_order, err = os.Open("./experiment/data/new_order.csv")
	}
	utils.CheckError(err)
	defer file_order.Close()
	order_line, err := os.Open("./data/order_line.csv")
	if err != nil {
		order_line, err = os.Open("./experiment/data/order_line.csv")
	}
	utils.CheckError(err)
	defer order_line.Close()

	order := csv.NewReader(file_order)
	lines := csv.NewReader(order_line)
	for {
		tp := c.ReadOrder(order, lines)
		if tp == nil {
			break
		} else {
			c.OrderPoll = append(c.OrderPoll, tp)
		}
	}
	utils.TPrintf("Order all loaded")
	rand.Shuffle(len(c.OrderPoll), func(i, j int) { c.OrderPoll[i], c.OrderPoll[j] = c.OrderPoll[j], c.OrderPoll[i] })
}

func (stmt *TPCStmt) Stop() {
	stmt.ca.Stop()
	atomic.StoreInt32(&stmt.stop, 1)
	if stmt.co == nil {
		return
	}
	if stmt.co == nil {
		return
	}
	for _, v := range stmt.co {
		v.Stop()
	}
}

func (stmt *TPCStmt) Stopped() bool {
	return atomic.LoadInt32(&stmt.stop) != 0
}

func (stmt *TPCStmt) TPCClient() {
	client := NewTPCClient()
	for !stmt.Stopped() {
		for count := 0; count < 20 && !stmt.Stopped(); count++ {
			tmp := &TPCOrder{}
			utils.CheckError(copier.CopyWithOption(&tmp, stmt.GetOrder(), copier.Option{DeepCopy: true}))
			if !stmt.Stopped() {
				stmt.HandleOrder(client, tmp, &stmt.latencySum, &stmt.levelSum, &stmt.txnCount, &stmt.success, &stmt.failS)
			}
			if count%20 == 10 && !stmt.Stopped() {
				stmt.HandleOrderStatus(client)
			}
		}
		if !stmt.Stopped() {
			stmt.HandleDelivery(client)
			stmt.HandleStockLevel(client)
		}
	}
}

func (stmt *TPCStmt) RunTPC() {
	//	atomic.StoreInt32(&stmt.stop, 0)
	stmt.txnCount = 0
	stmt.success = 0
	stmt.latencySum = 0
	stmt.levelSum = 0
	stmt.startTime = time.Now()
	stmt.failS = 0
	for i := 0; i < constants.CONCURRENCY; i++ {
		go stmt.TPCClient()
		time.Sleep(2 * time.Millisecond)
	}
	utils.TPrintf("All clients Started")
	if constants.ServerTimeOut < 0 {
		time.Sleep(constants.WarmUpTime + 2*time.Duration(-constants.ServerTimeOut)*200*time.Millisecond)
	} else if constants.NFInterval > 0 {
		time.Sleep(constants.WarmUpTime + 2*time.Duration(constants.NFInterval)*200*time.Millisecond)
	} else {
		time.Sleep(constants.WarmUpTime)
	}
	atomic.StoreInt32(&stmt.txnCount, 0)
	atomic.StoreInt32(&stmt.success, 0)
	atomic.StoreInt64(&stmt.latencySum, 0)
	atomic.StoreInt64(&stmt.levelSum, 0)
	atomic.StoreInt32(&stmt.failS, 0)
	stmt.startTime = time.Now()
	if constants.ServerTimeOut < 0 {
		for i := 0; i < 16*-constants.ServerTimeOut; i++ {
			time.Sleep(100 * time.Millisecond)
			stmt.logResults()
		}
	} else if constants.NFInterval > 0 {
		for i := 0; i < 16*constants.NFInterval; i++ {
			time.Sleep(100 * time.Millisecond)
			stmt.logResults()
		}
	} else {
		for i := 0; i < 5; i++ {
			time.Sleep(1 * time.Second)
		}
		stmt.logResults()
	}
}

func (stmt *TPCStmt) TPCC_Test(ca *collaborator.CollaboratorStmt) {
	stmt.Init(constants.TPCC_Protocol)
	stmt.RunTPC()
}

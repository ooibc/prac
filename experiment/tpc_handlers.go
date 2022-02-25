package experiment

import (
	"github.com/allvphx/RAC/collaborator"
	"github.com/allvphx/RAC/mockkv"
	"github.com/allvphx/RAC/utils"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	CustomReactionTime = 10 * time.Millisecond // Minimum reaction time for payment and new-order
	Newed              = 0
	Payed              = 1
	Delivered          = 2
	NWareHouse         = 3
	PayedSize          = 100
	NeedStockSize      = 20
	AllOrderSize       = 2
)

func (c *TPCStmt) Init(pro string) {
	if utils.LocalTest {
		c.ca, c.co = collaborator.CollaboratorTPCTestKit()
	} else {
		c.ca = collaborator.RemoteTestkit("10.184.0.2:2001")
		c.co = nil
	}
	c.protocol = pro
	c.orderLine = 0
	c.OrderPoll = make([]*TPCOrder, 0)
	c.RandomizeRead()
}

type TPCOrderLine struct {
	Item  int
	Count int // Count is always 5 in the data.
	Stock int
}

type TPCOrder struct {
	Order    int
	Ware     int
	Customer int
	Items    []*TPCOrderLine
}

func random(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min) + min
}

func (c *TPCStmt) checkAndLoadStock(client *TPCClient, order *TPCOrder, res map[string]int) bool {
	for _, v := range order.Items {
		key := utils.TransTableItem("stock", v.Stock, v.Item)
		if res[utils.Hash(utils.OU_addrs[v.Stock], key)] < 10 {
			if len(client.needStock.ToSlice()) < NeedStockSize {
				client.needStock.Add(v.Item*NWareHouse + v.Stock)
			}
		}
	}
	for _, v := range order.Items {
		key := utils.TransTableItem("stock", v.Stock, v.Item)
		if res[utils.Hash(utils.OU_addrs[v.Stock], key)] < 5 {
			// TPCC Standard: no such false considered.
			return true
		}
	}
	return true
}

func (c *TPCStmt) payment(client *TPCClient, order *TPCOrder, parts []string) bool {
	noTID := collaborator.GetTxnID()
	paWrite := collaborator.NewDBTransaction(noTID, mockkv.DefaultTimeOut, parts, c.ca.Manager)
	for _, v := range parts {
		key := utils.TransTableItem("order", -1, order.Order)
		paWrite.AddUpdate(v, key, Payed)
	}
	ok := c.ca.Manager.SubmitTxn(paWrite, c.protocol, nil, nil)
	for _, v := range order.Items {
		// TPCC standard: only one order payed, quick response.
		if len(client.payed.ToSlice()) < PayedSize {
			client.payed.Add(order.Order*NWareHouse + v.Stock)
		}
	}
	return ok
}

func (c *TPCStmt) newOrder(client *TPCClient, order *TPCOrder, parts []string, latency *time.Duration, levels *int64) bool {
	noTID := collaborator.GetTxnID()
	defer utils.TimeTrack(time.Now(), "For New order", noTID)
	noRead := collaborator.NewDBTransaction(noTID, mockkv.DefaultTimeOut, parts, c.ca.Manager)
	for _, v := range order.Items {
		noRead.AddRead(utils.OU_addrs[v.Stock], utils.TransTableItem("stock", v.Stock, v.Item))
	}
	//	val, ok := make(map[string]int), true
	val, ok := c.ca.Manager.PreRead(noRead)
	if !ok {
		utils.TPrintf("TXN" + strconv.Itoa(noTID) + ": " + "Failed for blocked PreRead")
		return false
	}
	noWrite := collaborator.NewDBTransaction(noTID, mockkv.DefaultTimeOut, parts, c.ca.Manager)
	for _, v := range order.Items {
		key := utils.TransTableItem("stock", v.Stock, v.Item)
		noWrite.AddUpdate(utils.OU_addrs[v.Stock], key, val[utils.Hash(utils.OU_addrs[v.Stock], key)]-5)
	}
	for _, v := range parts {
		key := utils.TransTableItem("order", -1, order.Order)
		noWrite.AddUpdate(v, key, Newed)
	}
	ok = c.ca.Manager.SubmitTxn(noWrite, c.protocol, latency, levels)
	if !ok {
		utils.TPrintf("TXN" + strconv.Itoa(noTID) + ": " + "Failed for commit")
	}
	if len(client.allOrderIDs.ToSlice()) < AllOrderSize {
		client.allOrderIDs.Add(order.Ware + order.Order*NWareHouse)
	}
	c.checkAndLoadStock(client, order, val)
	return ok
}

// HandleOrder handle an Order from tpcc-generator
func (c *TPCStmt) HandleOrder(client *TPCClient, order *TPCOrder, latencySum *int64, levelSum *int64, txnCount, success, failS *int32) bool {
	exist := make(map[int]bool)
	parts := make([]string, 0)
	rand.Seed(time.Now().Unix())
	// > 15% transactions are distributed.
	localStock := rand.Int()%100 > 15 // If processes the Order with local Stock.
	for _, v := range order.Items {
		if !localStock {
			v.Stock = random(0, NWareHouse)
		} else {
			v.Stock = order.Ware
		}
		if !exist[v.Stock] {
			exist[v.Stock] = true
			parts = append(parts, utils.OU_addrs[v.Stock])
		}
	}
	/// NewOrderTxn
	latency := time.Duration(0)
	levelS := int64(0)
	if c.newOrder(client, order, parts, &latency, &levelS) {
		utils.TPrintf("NewOrder Success")
		atomic.AddInt64(latencySum, int64(latency))
		if levelS < 0 {
			levelS *= -1
			atomic.AddInt32(failS , 1)
		}
		atomic.AddInt64(levelSum, levelS)
		atomic.AddInt32(txnCount, 1)
		atomic.AddInt32(success, 1)
	} else {
		utils.TPrintf("NewOrder Failed")
		if levelS < 0 {
			levelS *= -1
			atomic.AddInt32(failS, 1)
		}
		atomic.AddInt64(levelSum, levelS)
		atomic.AddInt32(txnCount, 1)
		return false
	}
	// TPCC standard: Reaction time + operating time for the Customer.
	//	time.Sleep(CustomReactionTime)
	c.payment(client, order, parts)
	return true
}

func (c *TPCStmt) stockLevel(order *TPCOrder, parts []string) bool {
	slTID := collaborator.GetTxnID()
	slRead := collaborator.NewDBTransaction(slTID, mockkv.DefaultTimeOut, parts, c.ca.Manager)
	for _, v := range order.Items {
		slRead.AddRead(utils.OU_addrs[v.Stock], utils.TransTableItem("stock", v.Stock, v.Item))
	}
	val, ok := c.ca.Manager.PreRead(slRead)
	if !ok {
		utils.TPrintf("Failed for Stock read")
		return false
	}
	slWrite := collaborator.NewDBTransaction(slTID, mockkv.DefaultTimeOut, parts, c.ca.Manager)
	for _, v := range order.Items {
		key := utils.TransTableItem("stock", v.Stock, v.Item)
		slWrite.AddUpdate(utils.OU_addrs[v.Stock], key, val[utils.Hash(utils.OU_addrs[v.Stock], key)]+100)
	}
	ok = c.ca.Manager.SubmitTxn(slWrite, c.protocol, nil, nil)
	return ok
}

// HandleStockLevel introduce Items with saved Stock requests.
func (c *TPCStmt) HandleStockLevel(client *TPCClient) {
	exist := make(map[int]bool)
	parts := make([]string, 0)
	tmp := &TPCOrder{Items: make([]*TPCOrderLine, 0)}
	for _, v := range client.needStock.ToSlice() {
		ware := v.(int) % NWareHouse
		item := v.(int) / NWareHouse
		if !exist[ware] {
			exist[ware] = true
			parts = append(parts, utils.OU_addrs[ware])
		}
		tmp.Items = append(tmp.Items, &TPCOrderLine{
			Stock: ware,
			Item:  item,
		})
	}
	c.stockLevel(tmp, parts)
}

func (c *TPCStmt) delivery(order *TPCOrder, parts []string) bool {
	deTID := collaborator.GetTxnID()
	deWrite := collaborator.NewDBTransaction(deTID, mockkv.DefaultTimeOut, parts, c.ca.Manager)
	for _, v := range order.Items {
		key := utils.TransTableItem("order", v.Stock, v.Item)
		deWrite.AddUpdate(utils.OU_addrs[v.Stock], key, Delivered)
	}
	ok := c.ca.Manager.SubmitTxn(deWrite, c.protocol, nil, nil)
	return ok
}

// HandleDelivery deliver the payed Items.
func (c *TPCStmt) HandleDelivery(client *TPCClient) {
	exist := make(map[int]bool)
	parts := make([]string, 0)
	tmp := &TPCOrder{Items: make([]*TPCOrderLine, 0)}
	BatchSize := random(1, 10)
	// TPCC standard: pay for 1~10 random orders
	for i := 0; i < BatchSize; i++ {
		v := client.payed.Pop()
		if v == nil {
			break
		}
		ware := v.(int) % NWareHouse
		order := v.(int) / NWareHouse
		if !exist[ware] {
			exist[ware] = true
			parts = append(parts, utils.OU_addrs[ware])
		}
		tmp.Items = append(tmp.Items, &TPCOrderLine{
			Stock: ware,
			Item:  order,
		})
	}
	c.delivery(tmp, parts)
}

func (c *TPCStmt) orderStatus(order *TPCOrder, parts []string) bool {
	osTID := collaborator.GetTxnID()
	osRead := collaborator.NewDBTransaction(osTID, mockkv.DefaultTimeOut, parts, c.ca.Manager)
	for _, v := range order.Items {
		key := utils.TransTableItem("order", v.Stock, v.Item)
		osRead.AddRead(utils.OU_addrs[v.Stock], key)
	}
	_, ok := c.ca.Manager.PreRead(osRead)
	osWrite := collaborator.NewDBTransaction(osTID, mockkv.DefaultTimeOut, parts, c.ca.Manager)
	ok = ok && c.ca.Manager.SubmitTxn(osWrite, c.protocol, nil, nil)
	return ok
}

// HandleOrderStatus check the status for one random orders.
func (c *TPCStmt) HandleOrderStatus(client *TPCClient) {
	exist := make(map[int]bool)
	parts := make([]string, 0)
	tmp := &TPCOrder{Items: make([]*TPCOrderLine, 0)}
	// Get a random order from a random warehouse.
	v := client.allOrderIDs.Pop()
	ware := v.(int) % NWareHouse
	order := v.(int) / NWareHouse
	if !exist[ware] {
		parts = append(parts, utils.OU_addrs[ware])
		exist[ware] = true
	}
	tmp.Items = append(tmp.Items, &TPCOrderLine{
		Stock: ware,
		Item:  order,
	})
	c.orderStatus(tmp, parts)
}

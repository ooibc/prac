package cohorts

import (
	"bufio"
	"encoding/json"
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/remote"
	"github.com/allvphx/RAC/rlsm"
	"github.com/allvphx/RAC/utils"
	"io"
	"net"
	"strconv"
	"time"
)

func loadRequest(request_ []byte) remote.Message4CO {
	/* Convert JSON format of incoming request to a map */
	var request remote.Message4CO
	err := json.Unmarshal(request_, &request)
	utils.CheckError(err)
	return request
}

func sendMsg(stmt *CohortStmt, service string, msg []byte) {
	if stmt.Cohort.IsBroken() {
		utils.Warn(false, "Message is Lost for cohort Crash")
		return
	}
	sid := stmt.revMap[service]
	stmt.connectLock[sid].Lock()
	defer stmt.connectLock[sid].Unlock()
	var conn net.Conn
	if stmt.Cohort.connections[sid] == nil {
		tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
		utils.CheckError(err)
		conn, err = net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			utils.TPrintf("Connection Refused")
			return
		}
		utils.CheckError(err)
		stmt.Cohort.connections[sid] = &conn
		go connHandler(stmt, conn, sid)
	} else {
		conn = *stmt.Cohort.connections[sid]
	}
	msg = append(msg, "\n"...)
	if conn != nil {
		utils.Assert(conn.SetWriteDeadline(time.Now().Add(1*time.Second)) == nil, "DDL set fail")
		_, err := conn.Write(msg)
		if err != nil {
			handleConnClosed(stmt, conn)
		}
	}
}

// sendBackCA send back to the collaborator.
func sendBackCA(stmt *CohortStmt, TID int, mark string, value interface{}) {
	utils.DPrintf("TXN" + strconv.Itoa(TID) + ": " + "Send back message from " + stmt.cohortID + " with Mark " + mark)
	msg := remote.Message4CA{Mark: mark, TID: TID}
	switch mark {
	case constants.FAILREAD, constants.SUCREAD:
		msg.Read = value.(map[string]int)
	case constants.FINISH, constants.INFO3PC, constants.PREWACK:
		msg.ACK = value.(bool)
	case constants.RACRES:
		msg.Res = value.(rlsm.KvRes)
	}
	msgBytes, err := json.Marshal(msg)
	utils.CheckError(err)
	sendMsg(stmt, stmt.collaborator, msgBytes)
}

func handleRequestType(stmt *CohortStmt, requestBytes []byte) {
	/* Checks the kind of request sent to coordinator. Calls relevant functions based
	on the request. */
	if stmt.Cohort.IsBroken() {
		return
	}
	request := loadRequest(requestBytes)
	utils.DPrintf("TXN" + strconv.Itoa(request.Txn.TxnID) + ": " + "Pending message for " + stmt.cohortID + " with Mark " + request.Mark)
	txn := request.Txn
	if request.Mark == constants.PRER {
		/* A new Txn started involving this replica. For all protocols */
		ok, res := stmt.Cohort.PreRead(&txn)
		if !ok {
			sendBackCA(stmt, txn.TxnID, constants.FAILREAD, res)
		} else {
			sendBackCA(stmt, txn.TxnID, constants.SUCREAD, res)
		}
	} else if request.Mark == constants.PREW {
		// For the 2PC and 3PC.
		res := stmt.Cohort.PreWrite(&txn)
		sendBackCA(stmt, txn.TxnID, constants.PREWACK, res)
	} else if request.Mark == constants.COMMMIT {
		// For all protocols
		utils.Assert(stmt.Cohort.Commit(&txn), "The commit is not executed")
		sendBackCA(stmt, txn.TxnID, constants.FINISH, true)
	} else if request.Mark == constants.ABORT {
		// For all protocols
		utils.Assert(stmt.Cohort.Abort(&txn), "The abort is not executed")
		sendBackCA(stmt, txn.TxnID, constants.FINISH, false)
	} else if request.Mark == constants.PROP {
		// For RAC only.
		res := stmt.Cohort.Propose(&txn)
		utils.Assert(res != nil, "Nil ptr encountered for result")
		sendBackCA(stmt, txn.TxnID, constants.RACRES, *res)
	} else if request.Mark == constants.AGRC3PC {
		// For 3PC only.
		res := stmt.Cohort.Agree(&txn, true)
		sendBackCA(stmt, txn.TxnID, constants.INFO3PC, res)
	} else if request.Mark == constants.AGRA3PC {
		// For 3PC only.
		res := stmt.Cohort.Agree(&txn, false)
		sendBackCA(stmt, txn.TxnID, constants.INFO3PC, res)
	} else if request.Mark == constants.RACVT {
		// For RAC only.
		if !stmt.Cohort.IsNF() {
			stmt.Cohort.HandleVote(&request.Vt)
		}
	}
}

func handleRequest(stmt *CohortStmt, conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		data, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		utils.CheckError(err)
		go handleRequestType(stmt, []byte(data))
	}
}

func handleConnClosed(stmt *CohortStmt, c net.Conn) {
	for k, v := range stmt.Cohort.connections {
		if v != nil && *v == c {
			stmt.Cohort.connections[k] = nil
		}
	}
	_ = c.Close()
}

func connHandler(stmt *CohortStmt, conn net.Conn, sid int) {
	ch := make(chan []byte)
	eCh := make(chan error)
	reader := bufio.NewReader(conn)
	// Start a goroutine to read From our net connection
	go func(ch chan []byte, eCh chan error) {
		for {
			// try to read the data
			data, err := reader.ReadString('\n')
			if err != nil {
				// send an error if it's encountered
				eCh <- err
				return
			}
			// send data if we read some.
			ch <- []byte(data)
		}
	}(ch, eCh)

	ticker := time.Tick(time.Second)
	// continuously read From the connection
	for {
		select {
		// This case means we recieved data on the connection
		case data := <-ch:
			handleRequestType(stmt, data)
		// This case means we got an error and the goroutine has finished
		case err := <-eCh:
			// handle our error then exit for loop
			utils.TPrintf(err.Error())
			stmt.connectLock[sid].Lock()
			handleConnClosed(stmt, conn)
			stmt.connectLock[sid].Unlock()
		// This will timeout on the read.
		case <-ticker:
			// do nothing? this is just so we can time out if we need to.
			// you probably don't even need to have this here unless you want
			// do something specifically on the timeout.
		}
	}
}

// HaveDecided returns if the transaction has been committed or aborted.
func (c *CohortManager) HaveDecided(TID int) bool {
	return c.Pool[TID] == nil || c.Pool[TID].finished
}

func (c *CohortManager) sendKvVote(aim string, vote remote.RACVote) {
	c.PoolLocks[vote.TID].Lock()
	defer c.PoolLocks[vote.TID].Unlock()
	if !utils.Warn(c.Pool[vote.TID] != nil, "The preRead is needed") {
		c.init(vote.TID)
	}
	msg := remote.Message4CO{
		Mark: constants.RACVT,
		Vt:   vote,
		Txn:  remote.RACTransaction{},
	}
	msgBytes, err := json.Marshal(msg)
	utils.CheckError(err)

	sendMsg(c.stmt, aim, msgBytes)
}

// HandleVote handle the vote with message buffer for RAC.
func (c *CohortManager) HandleVote(vt *remote.RACVote) {
	c.PoolLocks[vt.TID].Lock()
	defer c.PoolLocks[vt.TID].Unlock()
	utils.DPrintf("TXN" + strconv.Itoa(vt.TID) + ": " + "Vote get From " + vt.From + " to " + c.stmt.cohortID)
	if vt.Level == rlsm.NoCFNoNF && !c.HaveDecided(vt.TID) && c.L1VoteReceived[vt.TID] == nil {
		// if vote received, it must be an Abort
		c.L1VoteReceived[vt.TID] = make([]bool, 0)
		c.L1VoteReceived[vt.TID] = append(c.L1VoteReceived[vt.TID], vt.VoteCommit)
	}
	if vt.Level == rlsm.CFNoNF && !c.HaveDecided(vt.TID) {
		if c.L2VoteReceived[vt.TID] == nil {
			c.L2VoteReceived[vt.TID] = make([]bool, 0)
		}
		c.L2VoteReceived[vt.TID] = append(c.L2VoteReceived[vt.TID], vt.VoteCommit)
	}
}

// broadCastVote broadcast votes to other Cohort managers.
func (c *CohortManager) broadCastVote(TID int, level rlsm.Level, vote int, CID string, parts []string) {
	vt := remote.RACVote{
		TID:        TID,
		Level:      level,
		VoteCommit: vote == 1,
		From:       CID,
	}
	for _, tp := range parts {
		if tp == CID {
			c.HandleVote(&vt)
		} else {
			go c.sendKvVote(tp, vt)
		}
	}
}

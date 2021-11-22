package collaborator

import (
	"bufio"
	"encoding/json"
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/remote"
	"github.com/allvphx/RAC/utils"
	"io"
	"net"
	"strconv"
	"time"
)

func loadRequest(request_ []byte) remote.Message4CA {
	/* Convert JSON format of incoming request to a map */
	var request remote.Message4CA
	err := json.Unmarshal(request_, &request)
	utils.CheckError(err)
	return request
}

func handleRequestType(c *RACManager, requestBytes []byte) {
	request := loadRequest(requestBytes)
	if request.ACK {
		utils.DPrintf("TXN" + strconv.Itoa(request.TID) + ": CA Got message with Mark " + request.Mark + " sign: true")
	} else {
		utils.DPrintf("TXN" + strconv.Itoa(request.TID) + ": CA Got message with Mark " + request.Mark + " sign: false")
	}
	if request.Mark == constants.FAILREAD {
		c.handlePreRead(request.TID, &request.Read, false)
	} else if request.Mark == constants.SUCREAD {
		c.handlePreRead(request.TID, &request.Read, true)
	} else if request.Mark == constants.FINISH || request.Mark == constants.INFO3PC ||
		request.Mark == constants.PREWACK {
		c.handleACK(request.TID, request.Mark, request.ACK)
	} else if request.Mark == constants.RACRES {
		c.handleRAC(request.TID, &request.Res)
	}
}

func sendMsg(c *RACManager, service string, msg []byte) {
	sid, ok := c.revMap[service]
	if !utils.Assert(ok, "Invalid service address") {
		return
	}
	c.connLocks[sid].Lock()
	defer c.connLocks[sid].Unlock()
	var conn net.Conn
	if c.connections[sid] == nil {
		tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
		utils.CheckError(err)
		conn, err = net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			utils.TPrintf("Connection Refused")
			return
		}
		utils.CheckError(err)
		c.connections[sid] = &conn
		go connHandler(c, conn, sid)
	} else {
		conn = *c.connections[sid]
	}
	msg = append(msg, "\n"...)
	if conn != nil {
		utils.Assert(conn.SetWriteDeadline(time.Now().Add(1*time.Second)) == nil, "DDL set fail")
		_, err := conn.Write(msg)
		if err != nil {
			handleConnClosed(c, conn)
		}
	}
}

func handleRequest(c *RACManager, conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		data, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		utils.CheckError(err)
		go handleRequestType(c, []byte(data))
	}
}

func handleConnClosed(c *RACManager, cc net.Conn) {
	for k, v := range c.connections {
		if v == nil || *v == cc {
			c.connections[k] = nil
		}
	}
	_ = cc.Close()
}

func connHandler(c *RACManager, conn net.Conn, sid int) {
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
			handleRequestType(c, data)
		// This case means we got an error and the goroutine has finished
		case err := <-eCh:
			// handle our error then exit for loop
			utils.TPrintf(err.Error())
			c.connLocks[sid].Lock()
			handleConnClosed(c, conn)
			c.connLocks[sid].Unlock()
		// This will timeout on the read.
		case <-ticker:
			// do nothing? this is just so we can time out if we need to.
			// you probably don't even need to have this here unless you want
			// do something specifically on the timeout.
		}
	}
}

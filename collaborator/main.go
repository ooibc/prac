package collaborator

import (
	"encoding/json"
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/utils"
	"io/ioutil"
	"net"
	"os"
	"sync"
	"time"
)

// CohortStmt records the statement context for an cohort node.
type CollaboratorStmt struct {
	// string address -> node.
	Manager         *RACManager
	collaboratorID  string
	cohorts         []string
	timeoutForMsgs  time.Duration
	timeoutForLocks time.Duration
	done            chan bool
	listener        net.Listener
}

var con_Lock = sync.Mutex{}
var config map[string]interface{}

// [] [address] [storageSize]
func initData(stmt *CollaboratorStmt, Args []string) {
	loadConfig(stmt, &config)
	stmt.collaboratorID = Args[2]
	stmt.Manager = NewRACManager(stmt.timeoutForMsgs, stmt.cohorts)
}

func loadConfig(stmt *CollaboratorStmt, config *map[string]interface{}) {
	con_Lock.Lock()
	defer con_Lock.Unlock()
	/* Read the config file and store it in 'config' variable */
	raw, err := ioutil.ReadFile("./config.json")
	if err != nil {
		raw, err = ioutil.ReadFile("../config.json")
	}
	utils.CheckError(err)

	err = json.Unmarshal([]byte(raw), &config)
	tmp, _ := ((*config)["cohorts"]).(map[string]interface{})
	stmt.cohorts = make([]string, 0)
	for _, p := range tmp {
		stmt.cohorts = append(stmt.cohorts, p.(string))
	}
	tmp, _ = ((*config)["collaborators"]).(map[string]interface{})
	for _, p := range tmp {
		stmt.collaboratorID = p.(string)
	}
	stmt.timeoutForLocks = constants.LockUpperBound
	stmt.timeoutForMsgs = constants.MsgUpperBound
	stmt.done = make(chan bool, 1)
	utils.CheckError(err)
}

func (c *CollaboratorStmt) Stop() {
	c.done <- true
	utils.CheckError(c.listener.Close())
}

func begin(stmt *CollaboratorStmt, Args []string, ch chan bool) {
	initData(stmt, Args)

	service := Args[2]
	utils.DPrintf(service)
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	utils.CheckError(err)
	stmt.listener, err = net.ListenTCP("tcp", tcpAddr)
	utils.CheckError(err)

	utils.DPrintf("build finished for " + Args[2])
	ch <- true

	for {
		conn, err := stmt.listener.Accept()
		if err != nil {
			select {
			case <-stmt.done:
				// If we called stop() then there will be a value in es.done, so
				// we'll get here and we can exit without showing the error.
				return
			default:
				utils.CheckError(err)
			}
		}
		go handleRequest(stmt.Manager, conn)
	}
}

func Main() {
	stmt := &CollaboratorStmt{}
	ch := make(chan bool)
	begin(stmt, os.Args, ch)
}

package cohorts

import (
	"encoding/json"
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/utils"
	"io/ioutil"
	"net"
	"sync"
	"time"
)

// CohortStmt records the statement context for an Cohort node.
type CohortStmt struct {
	mu              *sync.Mutex
	revMap          map[string]int
	timeoutForMsgs  time.Duration
	timeoutForLocks time.Duration
	collaborator    string
	cohorts         []string
	cohortID        string

	Cohort *CohortManager // the cohort manager

	done        chan bool
	listener    net.Listener
	connectLock []*sync.Mutex
}

var con_Lock = sync.Mutex{}
var config map[string]interface{}

// [] [address] [storageSize]
func initData(stmt *CohortStmt, service string) {
	loadConfig(stmt, &config)
	utils.TPrintf("Load config finished")
	stmt.mu = &sync.Mutex{}
	stmt.cohortID = service
	storageSize := constants.NUM_ELEMENTS
	stmt.Cohort = NewCohortManager(stmt, storageSize)
	stmt.Cohort.Kv.SetDDL(stmt.timeoutForLocks)
	stmt.connectLock = make([]*sync.Mutex, 0)
	stmt.revMap = make(map[string]int)
	for i, v := range stmt.cohorts {
		stmt.connectLock = append(stmt.connectLock, &sync.Mutex{})
		stmt.revMap[v] = i
		stmt.Cohort.connections = append(stmt.Cohort.connections, nil)
	}
	stmt.connectLock = append(stmt.connectLock, &sync.Mutex{})
	stmt.revMap[stmt.collaborator] = len(stmt.connectLock) - 1
	stmt.Cohort.connections = append(stmt.Cohort.connections, nil)
}

func loadConfig(stmt *CohortStmt, config *map[string]interface{}) {
	con_Lock.Lock()
	defer con_Lock.Unlock()
	/* Read the config file and store it in 'config' variable */
	raw, err := ioutil.ReadFile(constants.ConfigLocation)
	if err != nil {
		raw, err = ioutil.ReadFile("." + constants.ConfigLocation)
	}
	utils.CheckError(err)

	err = json.Unmarshal([]byte(raw), &config)
	tmp, _ := ((*config)["cohorts"]).(map[string]interface{})
	stmt.cohorts = make([]string, 0)
	id_key := "1"
	for i, p := range tmp {
		stmt.cohorts = append(stmt.cohorts, p.(string))
		if p.(string) == stmt.cohortID {
			id_key = i
		}
	}
	tmp, _ = ((*config)["collaborators"]).(map[string]interface{})
	for _, p := range tmp {
		stmt.collaborator = p.(string)
	}
	tmp, _ = ((*config)["delays"]).(map[string]interface{})
	for i, p := range tmp {
		if i == id_key {
			constants.SetBasicT(p.(float64))
		}
	}
	stmt.timeoutForLocks = constants.LockUpperBound
	stmt.timeoutForMsgs = constants.MsgUpperBound4RAC
	stmt.done = make(chan bool, 1)
	utils.CheckError(err)
}

// Stop stop the running cohort process.
func (st *CohortStmt) Stop() {
	st.done <- true
	utils.CheckError(st.listener.Close())
}

func begin(stmt *CohortStmt, ch chan bool, service string) {
	utils.TPrintf("Initing -- ")
	initData(stmt, service)
	utils.DPrintf(service)
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	utils.CheckError(err)
	stmt.listener, err = net.ListenTCP("tcp", tcpAddr)
	utils.CheckError(err)

	utils.DPrintf("build finished for " + service)

	ch <- true
	go func() {
		if constants.ServerTimeOut < 0 {
			time.Sleep(3*time.Second + constants.WarmUpTime + 12*time.Duration(-constants.ServerTimeOut)*200*time.Millisecond)
		} else if constants.NFInterval > 0 {
			time.Sleep(3*time.Second + constants.WarmUpTime + 12*time.Duration(constants.NFInterval)*200*time.Millisecond)
		} else if constants.ServerTimeOut > 0 {
			// defined timeout.
			time.Sleep(time.Second * time.Duration(constants.ServerTimeOut))
		} else {
			time.Sleep(22*time.Second + constants.WarmUpTime)
		}
		stmt.Stop()
	}()

	if constants.ServerTimeOut <= 0 && stmt.cohortID[len(stmt.cohortID)-1] == '1' {
		if constants.ServerTimeOut == 0 {
			stmt.Cohort.Break()
		} else {
			go func() {
				for {
					time.Sleep(time.Duration(-constants.ServerTimeOut) * 200 * time.Millisecond)
					stmt.Cohort.Break()
					//					println("CF bk")
					time.Sleep(time.Duration(-constants.ServerTimeOut) * 200 * time.Millisecond)
					stmt.Cohort.Recover()
					//					println("CF rc")
				}
			}()
		}
	}

	if constants.NFInterval >= 0 && stmt.cohortID[len(stmt.cohortID)-1] == '1' {
		if constants.NFInterval == 0 {
			stmt.Cohort.NetBreak()
		} else {
			go func() {
				for {
					time.Sleep(time.Duration(constants.NFInterval) * 200 * time.Millisecond)
					stmt.Cohort.NetBreak()
					//				println("Nt bk")
					time.Sleep(time.Duration(constants.NFInterval) * 200 * time.Millisecond)
					stmt.Cohort.NetRecover()
					//				println("Nt rc")
				}
			}()
		}
	}
	// TODO: crash and recovery generator later added here

	for {
		conn, err := stmt.listener.Accept()
		if err != nil {
			select {
			case <-stmt.done:
				return
			default:
				utils.CheckError(err)
			}
		}
		go handleRequest(stmt, conn)
	}
}

func Main(preload bool, addr string) {
	stmt := &CohortStmt{}
	ch := make(chan bool)
	go func() {
		<-ch
		if preload {
			stmt.LoadStock()
		}
	}()
	begin(stmt, ch, addr)
}

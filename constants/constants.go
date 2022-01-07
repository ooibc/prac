package constants

import (
	"time"
)

const PRER string = "PreRead"
const PREW string = "PreWrite"
const PROP string = "Propose"
const FAILREAD string = "FailRead"
const SUCREAD string = "SuccessRead"
const PREWACK string = "PreWriteACK"
const COMMMIT string = "Commit"
const ABORT string = "Abort"
const RACRES string = "RACResult"
const AGRC3PC string = "Agree-Commit-3PC"
const AGRA3PC string = "Agree-Abort-3PC"
const INFO3PC string = "Inform-3PC"
const RACVT string = "RAC_vote"
const FINISH string = "transaction_finished"

// contention ~ C(con, 2)
const OptEps = 2 * time.Millisecond                                       // the concurrency cost, it should increase with concurrency.
const MsgUpperBound = time.Duration(1.2 * 90 * float64(time.Millisecond)) // between Kvs.
const LockUpperBound = 5 * time.Millisecond
const UniverseRetryCount = 3

var NUM_ELEMENTS int = 200000

// for ycsb = 10000
// for tpc = 200000
const NUM_PARTITIONS int = 3
const NULL int = 0
const WORK_LOAD = 5
const Max_Txn_ID = 1000000

////// Client side constants /////
var MsgUpperBound4RAC time.Duration = 0 // between Kvs.
var rt float64 = 0                      // between Kvs.
var CONCURRENCY int = 10
var KvConcurrencyEps time.Duration = 0
var ConcurrencyEps time.Duration = 0
var TPCC_Protocol string = "RAC"
var ServerTimeOut = 20
var BasicWaitTime time.Duration = 60
var InitCnt int = 0
var NFInterval int = -1
var TestCF int32 = 0
var TestNF int32 = 0
var MinLevel = 0
var ConfigLocation = "./configs/remote.json"

const DownBatchSize = 200
const CONTENTION int = 90
const WarmUpTime time.Duration = 5 * time.Second

/*
* 	ThreePC     = "3PC"
	TwoPC       = "2PC"
	RobAdapC    = "RAC"
*/

func SetProtocol(pro string) {
	if pro == "2pc" {
		pro = "2PC"
	}
	if pro == "3pc" {
		pro = "3PC"
	}
	if pro == "rac" {
		pro = "RAC"
	}
	if pro != "2PC" && pro != "3PC" && pro != "RAC" {
		pro = "2PC"
	}
	TPCC_Protocol = pro
}

func SetR(r float64) {
	rt = r
}

func SetBasicT(t float64) {
	BasicWaitTime = time.Duration(t * float64(time.Millisecond))
	SetMsgDelay4RAC(rt)
}

func SetMsgDelay4RAC(r float64) {
	MsgUpperBound4RAC = time.Duration((r + 0.2) * float64(BasicWaitTime))
	//	println(MsgUpperBound4RAC.String())
}

func SetDown(d int) {
	InitCnt = d
}

func SetNF(d int) {
	NFInterval = d
}

func SetMinLevel(l int) {
	MinLevel = l
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func SetServerTimeOut(val int) {
	ServerTimeOut = val
}

func SetConcurrency(con int) {
	CONCURRENCY = con
	KvConcurrencyEps = 5 * time.Millisecond
	ConcurrencyEps = 1 * time.Millisecond * time.Duration(Min(CONCURRENCY, 2000))
}

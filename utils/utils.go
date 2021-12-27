package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Debugging
const Debug = false
const ShowWarn = false
const Test = false

var LocalTest = true

func SetLocal() {
	LocalTest = true
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		fmt.Printf(time.Now().Format("15:04:05.00")+" <---> "+format+"\n", a...)
	}
	return
}

func TimeTrack(start time.Time, name string, TID int) {
	tim := time.Since(start).String()
	if name == "RAC Propose" {
		//	fmt.Println("1:" + tim)
	} else if name == "RAC Decide" {
		//	fmt.Println("2:" + tim)
	}
	TPrintf("TXN" + strconv.Itoa(TID) + ": Time cost for " + name + " : " + tim)
}

func TimeLoad(start time.Time, name string, TID int, latency *time.Duration) {
	if latency == nil {
		return
	}
	*latency = time.Since(start)
	TPrintf("TXN" + strconv.Itoa(TID) + ": Time cost for " + name + " : " + (*latency).String())
}

func TPrintf(format string, a ...interface{}) {
	if Test {
		fmt.Printf(time.Now().Format("15:04:05.00")+" <---> "+format+"\n", a...)
	}
	return
}

func JPrint(v interface{}) {
	byt, _ := json.Marshal(v)
	fmt.Println(string(byt))
}

func Hash(shard string, key int) string {
	return shard + "_" + strconv.Itoa(key)
}

func Assert(cond bool, msg string) bool {
	if !cond {
		fmt.Fprintf(os.Stderr, "[ERROR] Assert error at "+msg+"\n")
		os.Exit(1)
	}
	return cond
}

func Warn(cond bool, msg string) bool {
	if ShowWarn && !cond {
		fmt.Printf("[WARNNING] :" + msg + "\n")
	}
	return cond
}
func CheckError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

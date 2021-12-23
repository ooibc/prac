package main

import (
	"flag"
	"github.com/allvphx/RAC/cohorts"
	"github.com/allvphx/RAC/constants"
	"github.com/allvphx/RAC/experiment"
	"github.com/allvphx/RAC/utils"
)

var (
	part     string
	protocol string
	con      int
	tl       int
	bench    string
	local    bool
	preload  bool
	r        float64
	addr     string
)

func usage() {
	flag.PrintDefaults()
}

func init() {
	flag.StringVar(&part, "node", "ca", "the node to start")
	flag.StringVar(&bench, "bench", "tpc", "the benchmark used for the test")
	flag.StringVar(&protocol, "p", "rac", "the protocol used for this test")
	flag.StringVar(&addr, "addr", "127.0.0.1:5001", "the address for this node")
	flag.IntVar(&con, "c", 800, "the number of client used for test")
	flag.IntVar(&tl, "tl", 0, "the timeout for started cohort node")

	flag.BoolVar(&local, "local", false, "if the test is executed locally")
	flag.BoolVar(&preload, "preload", false, "preload the data for tpc-c into shard")

	flag.Float64Var(&r, "r", 3, "the factor for the rac protocol")
	flag.Usage = usage
}

func main() {
	flag.Parse()
	//	println(con, r, preload, local, protocol, bench, part)
	constants.SetConcurrency(con)
	constants.SetR(r)
	constants.SetServerTimeOut(tl)
	if local {
		utils.SetLocal()
	}

	if part == "co" {
		cohorts.Main(preload, addr)
	} else if part == "ca" {
		if bench == "ycsb" {
			experiment.TestYCSB(protocol, addr)
		} else if bench == "tpc" {
			experiment.TestTPC(protocol, addr)
		}
	}
}

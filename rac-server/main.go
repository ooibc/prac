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
	nf       int
	down     int
	minLevel int
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
	// Common
	flag.StringVar(&addr, "addr", "127.0.0.1:5001", "the address for this node")
	flag.StringVar(&part, "node", "ca", "the node to start")
	flag.Float64Var(&r, "r", 3, "the factor for the rac protocol")

	// For collaborator
	flag.StringVar(&bench, "bench", "tpc", "the benchmark used for the test")
	flag.StringVar(&protocol, "p", "rac", "the protocol used for this test")
	flag.IntVar(&con, "c", 1000, "the number of client used for test")
	flag.IntVar(&down, "d", 1, "The heuristic method used: x for fixed timeout, 0 for RL.")
	flag.BoolVar(&local, "local", false, "if the test is executed locally")

	// For cohorts.
	flag.IntVar(&tl, "tl", 28, "the timeout for started cohort node, -x for change, 0 for crash")
	flag.IntVar(&nf, "nf", -1, "the interval for network failure")
	flag.IntVar(&minLevel, "ml", 1, "The smallest level can be used.")
	flag.BoolVar(&preload, "preload", false, "preload the data for tpc-c into shard")

	flag.Usage = usage
}

func main() {
	flag.Parse()
	//	println(con, r, preload, local, protocol, bench, part)
	constants.SetConcurrency(con)
	constants.SetR(r)
	constants.SetServerTimeOut(tl)
	if down > 0 {
		constants.SetDown(down)
	} else {
		constants.SetDown(0)
	}
	constants.SetNF(nf)
	constants.SetMinLevel(minLevel)
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

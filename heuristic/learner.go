package heuristic

type Learner struct {
}

func (l *Learner) Report(H int, success bool, delta float32) {
	// ReportH RAC report the result of executing to heuristic method.
}

func (l *Learner) Action(H int) int {
	// Action current remain steps finished in RAC, get next action. H can be used to reset.
	return 0
}

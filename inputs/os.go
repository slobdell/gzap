package inputs

import (
	"os"
	"runtime"
)

func MaxParallelism() int {
	maxProcs := runtime.GOMAXPROCS(0)
	numCPU := runtime.NumCPU()
	if maxProcs < numCPU {
		return maxProcs
	}
	return numCPU
}

func GoogleProjectID() string {
	return os.Getenv("GOOGLE_PROJECT_ID")
}

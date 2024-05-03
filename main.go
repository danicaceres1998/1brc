package main

import (
	"fmt"
	"go-1brc/pkg/process"
	"log"
	"os"
	"runtime/pprof"
	"time"
)

func main() {
	defer cpuProfile()()
	start := time.Now()
	stations, err := process.StartRowsProcess(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	process.PrintResults(stations)
	fmt.Printf("[INFO]: Process finished in: %0.6fs\n", time.Since(start).Seconds())
}

func cpuProfile() func() {
	if len(os.Args) >= 3 && os.Args[2] == "true" {
		f, err := os.Create("cpu_profile.prof")
		if err != nil {
			panic(err)
		}

		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
		return func() {
			pprof.StopCPUProfile()
			f.Close()
		}
	}
	return func() {}
}

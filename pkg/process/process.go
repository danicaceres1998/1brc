package process

import (
	"fmt"
	"go-1brc/pkg/csvreader"
	"go-1brc/pkg/types"
)

const (
	PoolSize = 60
)

func StartRowsProcess(file string) (*types.StationManager, error) {
	sm := types.NewStationManager(PoolSize)

	lines, err := csvreader.ReadFile(file)
	if err != nil {
		return sm, err
	}

	for chunkData := range lines {
		sm.Queue(chunkData)
	}

	sm.Stop()

	return sm, nil
}

func PrintResults(sm *types.StationManager) {
	print("{")
	sm.Merge().Iter(func(_ uint64, v *types.Station) (stop bool) {
		fmt.Printf(
			"%s=%.1f/%.1f/%.1f, ",
			v.Name,
			float64(v.Min)/10,
			v.AvgTemperature(),
			float64(v.Max)/10,
		)
		return false
	})
	print("}\n")
}

package process

import (
	"fmt"
	"go-1brc/pkg/parser"
	"go-1brc/pkg/types"
)

const PoolSize = 75

func StartRowsProcess(file string) (*types.StationManager, error) {
	fo, err := types.NewFileObject(file)
	if err != nil {
		return nil, err
	}
	sm := types.NewStationManager(fo, PoolSize)

	sm.ProcessFile()
	sm.Wait()

	return sm, nil
}

func PrintResults(sm *types.StationManager) {
	fmt.Print("{")
	sm.Merge().Iter(func(_ uint64, v *parser.Station) (stop bool) {
		fmt.Printf(
			"%s=%.1f/%.1f/%.1f, ",
			v.Name,
			float64(v.Min)/10,
			v.AvgTemperature(),
			float64(v.Max)/10,
		)
		return false
	})
	fmt.Print("}\n")
}

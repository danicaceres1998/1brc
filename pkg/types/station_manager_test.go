package types

import (
	"go-1brc/pkg/parser"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dolthub/swiss"
	"github.com/stretchr/testify/assert"
)

const (
	StdGoRoutines = 2
)

func TestSManagerCreateWorker(t *testing.T) {
	sm, numWorkers := StationManager{workersWg: &sync.WaitGroup{}}, 5
	for range numWorkers {
		sm.createWorker(&FileObject{})
	}

	assert.Equal(t, numWorkers, len(sm.workers))
}

func TestSManagerMerge(t *testing.T) {
	sm := StationManager{
		workers: []Worker{
			{stations: swiss.NewMap[uint64, *parser.Station](2)},
			{stations: swiss.NewMap[uint64, *parser.Station](2)},
		},
		tWorker: newTrashWorker(),
	}

	stations := make(map[uint64]*parser.Station)
	stations[7571807575422721] = &parser.Station{Name: "Yaoundé", Min: 100, Max: 100, Count: 1, Sum: 100}
	stations[6952756473232] = &parser.Station{Name: "Sana'a", Min: 200, Max: 200, Count: 1, Sum: 200}
	for _, w := range sm.workers {
		for k, s := range stations {
			w.stations.Put(k, s)
		}
	}

	result := sm.Merge()
	result.Iter(func(k uint64, v *parser.Station) (stop bool) {
		assert.Equal(t, v, stations[k])
		return false
	})
	assert.Equal(t, result.Count(), len(stations))
}

func TestSManagerWait(t *testing.T) {
	sm := StationManager{
		workersWg: &sync.WaitGroup{}, tWWg: &sync.WaitGroup{}, tWorker: newTrashWorker(),
	}
	sm.workersWg.Add(1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		sm.workersWg.Done()
	}()

	sm.Wait()

	_, ok := <-sm.tWorker.in
	assert.False(t, ok)
}

func TestNewStationManager(t *testing.T) {
	file, deleteFile := createTmpFile()
	defer deleteFile()

	fo, err := NewFileObject(file.Name())
	assert.Nil(t, err)

	sm := NewStationManager(fo, 1)

	sm.ProcessFile()
	sm.Wait()

	result := sm.Merge()
	assert.Equal(t, 1, result.Count())
	result.Iter(func(k uint64, v *parser.Station) (stop bool) {
		assert.Equal(t, "Yaoundé", v.Name)
		assert.Equal(t, (335 + 105 + 405 + (-35)), v.Sum)
		assert.Equal(t, 4, v.Count)
		assert.Equal(t, -35, v.Min)
		assert.Equal(t, 405, v.Max)
		return false
	})
}

// Auxiliary Functions //

const fileContent = "Yaoundé;33.5\nYaoundé;10.5\nYaoundé;40.5\nYaoundé;-3.5\n"

func createTmpFile() (*os.File, func()) {
	file, err := os.CreateTemp("/var/tmp", "test-file-")
	if err != nil {
		panic(err)
	}

	_, err = file.WriteString(fileContent)
	if err != nil {
		panic(err)
	}

	return file, func() {
		if file != nil {
			file.Close()
			os.Remove(file.Name())
		}
	}
}

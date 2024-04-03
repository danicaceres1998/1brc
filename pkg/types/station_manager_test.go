package types

import (
	"runtime"
	"testing"
	"time"

	"github.com/dolthub/swiss"
	"github.com/stretchr/testify/assert"
)

const (
	StdGoRoutines = 2
)

func TestSManagerCreateWorker(t *testing.T) {
	sm, numWorkers := StationManager{}, 5
	for range numWorkers {
		sm.createWorker()
	}
	assert.Equal(t, (runtime.NumGoroutine() - StdGoRoutines), numWorkers)

	for _, w := range sm.workers {
		close(w.in)
	}

	sm.workersWg.Wait()
	assert.Equal(t, runtime.NumGoroutine(), StdGoRoutines)
}

func TestSManagerMerge(t *testing.T) {
	sm := StationManager{
		workers: []Worker{
			{stations: swiss.NewMap[uint64, *Station](2)},
			{stations: swiss.NewMap[uint64, *Station](2)},
		},
	}

	stations := make(map[uint64]*Station)
	stations[7571807575422721] = &Station{Name: "Yaoundé", Min: 100, Max: 100, Count: 1, Sum: 100}
	stations[6952756473232] = &Station{Name: "Sana'a", Min: 200, Max: 200, Count: 1, Sum: 200}
	for _, w := range sm.workers {
		for k, s := range stations {
			w.stations.Put(k, s)
		}
	}

	result := sm.Merge()
	result.Iter(func(k uint64, v *Station) (stop bool) {
		assert.Equal(t, v, stations[k])
		return false
	})
	assert.Equal(t, result.Count(), len(stations))
}

func TestSManagerStop(t *testing.T) {
	sm := StationManager{managerQueue: make(chan []byte)}
	sm.workersWg.Add(1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		sm.workersWg.Done()
	}()

	sm.Stop()
	_, ok := <-sm.managerQueue
	assert.False(t, ok)
}

func TestNewStationManager(t *testing.T) {
	sm := NewStationManager(2)

	sm.Queue([]byte("Yaoundé;33.5"))
	sm.Queue([]byte("Yaoundé;-3.5"))
	sm.Queue([]byte("Yaoundé;10.5"))
	sm.Queue([]byte("Yaoundé;40.5"))
	sm.Stop()

	result := sm.Merge()
	assert.Equal(t, 1, result.Count())
	result.Iter(func(k uint64, v *Station) (stop bool) {
		assert.Equal(t, "Yaoundé", v.Name)
		assert.Equal(t, (335 + (-35) + 105 + 405), v.Sum)
		assert.Equal(t, 4, v.Count)
		assert.Equal(t, -35, v.Min)
		assert.Equal(t, 405, v.Max)
		return false
	})
}

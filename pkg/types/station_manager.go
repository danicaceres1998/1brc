package types

import (
	"sync"

	"github.com/dolthub/swiss"
)

const ResultSize = 10000

func NewStationManager(numWorkers int) *StationManager {
	sm := &StationManager{
		workers: make([]Worker, 0, numWorkers),
	}
	for range numWorkers {
		sm.createWorker()
	}
	sm.startListening()

	return sm
}

type StationManager struct {
	workers      []Worker
	workersWg    sync.WaitGroup
	managerQueue chan []byte
}

func (sm *StationManager) Queue(data []byte) {
	sm.managerQueue <- data
}

func (sm *StationManager) Stop() {
	close(sm.managerQueue)
	sm.workersWg.Wait()
}

func (sm *StationManager) Merge() *swiss.Map[uint64, *Station] {
	result := swiss.NewMap[uint64, *Station](ResultSize)

	for _, w := range sm.workers {
		w.stations.Iter(func(k uint64, v *Station) (stop bool) {
			if s, ok := result.Get(k); ok {
				if v.Min < s.Min {
					s.Min = v.Min
				}
				if v.Max > s.Max {
					s.Max = v.Max
				}
				s.Sum += v.Sum
				s.Count += v.Count
			} else {
				result.Put(k, v)
			}
			return false
		})
	}

	return result
}

func (sm *StationManager) startListening() {
	sm.managerQueue = make(chan []byte, MapSize*2)
	go func() {
		i, maxIdx := 0, len(sm.workers)-1
		for chunkData := range sm.managerQueue {
			sm.workers[i].in <- chunkData
			i++
			if i > maxIdx {
				i = 0
			}
		}
		for _, w := range sm.workers {
			close(w.in)
		}
	}()
}

func (sm *StationManager) createWorker() {
	w := Worker{
		stations: swiss.NewMap[uint64, *Station](MapSize),
		in:       make(chan []byte, 100),
	}
	sm.workersWg.Add(1)
	sm.workers = append(sm.workers, w)

	go w.consume(&sm.workersWg)
}

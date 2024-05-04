package types

import (
	"go-1brc/pkg/parser"
	"sync"

	"github.com/dolthub/swiss"
)

const (
	ResultSize = 10000
)

func NewStationManager(fo *FileObject, size int) *StationManager {
	sm := &StationManager{
		workers:   make([]Worker, 0, size),
		workersWg: &sync.WaitGroup{},
		tWWg:      &sync.WaitGroup{},
	}

	sm.createTrashWorker(size)
	for range size {
		sm.createWorker(fo)
	}

	return sm
}

type StationManager struct {
	workers   []Worker
	tWorker   TrashWorker
	workersWg *sync.WaitGroup
	tWWg      *sync.WaitGroup
}

func (sm *StationManager) ProcessFile() {
	go sm.tWorker.consume(sm.tWWg)

	for _, w := range sm.workers {
		go w.consume(sm.workersWg, sm.tWorker.in)
	}

	sm.wait()
}

func (sm *StationManager) Merge() *swiss.Map[uint64, *parser.Station] {
	result := swiss.NewMap[uint64, *parser.Station](ResultSize)
	merge := func(k uint64, v *parser.Station) (stop bool) {
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
	}

	sm.tWorker.stations.Iter(merge)
	for _, w := range sm.workers {
		w.stations.Iter(merge)
	}

	return result
}

func (sm *StationManager) wait() {
	sm.workersWg.Wait()
	close(sm.tWorker.in)
	sm.tWWg.Wait()
}

func (sm *StationManager) createWorker(fo *FileObject) {
	w := newWorker(fo)
	sm.workersWg.Add(1)
	sm.workers = append(sm.workers, w)
}

func (sm *StationManager) createTrashWorker(size int) {
	sm.tWWg.Add(1)
	sm.tWorker = newTrashWorker(size)
}

package types

import (
	"go-1brc/pkg/parser"
	"sync"

	"github.com/dolthub/swiss"
)

const (
	MapSize = 1024
)

type Station struct {
	Name  string
	Min   int
	Max   int
	Sum   int
	Count int
}

func (s *Station) Update(std parser.StationData) {
	if s.Min > std.Temperature {
		s.Min = std.Temperature
	}
	if s.Max < std.Temperature {
		s.Max = std.Temperature
	}

	s.Sum += std.Temperature
	s.Count++
}

func (s *Station) AvgTemperature() float64 {
	return (float64(s.Sum) / 10) / float64(s.Count)
}

type Worker struct {
	stations *swiss.Map[uint64, *Station]
	in       chan []byte
}

func (w *Worker) consume(wg *sync.WaitGroup) {
	for d := range w.in {
		for std := range parser.ParseLines(d) {
			if s, ok := w.stations.Get(std.HashId); ok {
				s.Update(std)
			} else {
				w.stations.Put(std.HashId, &Station{
					Name:  std.Name,
					Min:   std.Temperature,
					Max:   std.Temperature,
					Sum:   std.Temperature,
					Count: 1,
				})
			}
		}
	}
	wg.Done()
}

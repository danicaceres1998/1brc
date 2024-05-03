package types

import (
	"go-1brc/pkg/parser"
	"os"
	"sync"
)

func NewFileObject(fileName string) (*FileObject, error) {
	file, err := os.Open(fileName)
	return &FileObject{
		file:    file,
		lock:    sync.Mutex{},
		lockIdx: 0,
	}, err
}

type FileObject struct {
	file    *os.File
	lock    sync.Mutex
	lockIdx int
}

func (fo *FileObject) getIndex() int {
	fo.lockIdx++
	return fo.lockIdx
}

func (fo *FileObject) Read(buff []byte) (idx int, n int, err error) {
	defer fo.lock.Unlock()

	fo.lock.Lock()
	idx = fo.getIndex()
	n, err = fo.file.Read(buff)

	return idx, n, err
}

type RemainingItem struct {
	Idx     int
	Content string
	Initial bool
}

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

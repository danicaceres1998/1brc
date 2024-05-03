package types

import (
	"go-1brc/pkg/parser"
	"io"
	"sync"

	"github.com/dolthub/swiss"
)

const (
	SizeWorkerChan = 100
	WorkerBuffSize = 2048 * 2048
	MapSize        = 1024
)

func updateStationData(data *swiss.Map[uint64, *Station], std parser.StationData) {
	if s, ok := data.Get(std.HashId); ok {
		s.Update(std)
	} else {
		data.Put(std.HashId, &Station{
			Name:  std.Name,
			Min:   std.Temperature,
			Max:   std.Temperature,
			Sum:   std.Temperature,
			Count: 1,
		})
	}
}

func newWorker(fo *FileObject) Worker {
	return Worker{
		file:     fo,
		stations: swiss.NewMap[uint64, *Station](MapSize),
	}
}

type Worker struct {
	file     *FileObject
	stations *swiss.Map[uint64, *Station]
}

func (w *Worker) updateStd(std parser.StationData) {
	updateStationData(w.stations, std)
}

func (w *Worker) consume(wg *sync.WaitGroup, trash chan *RemainingItem) {
	readBuffer := make([]byte, WorkerBuffSize)

	for {
		idx, n, err := w.file.Read(readBuffer)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		// ignoring first line
		start := 0
		for i := 0; i < n; i++ {
			if readBuffer[i] == parser.NewLine {
				start = i + 1
				break
			}
		}
		trash <- &RemainingItem{idx - 1, string(readBuffer[:start]), false}

		// ignoring last line
		final := 0
		for i := n - 1; i >= 0; i-- {
			if readBuffer[i] == parser.NewLine {
				final = i + 1
				break
			}
		}
		trash <- &RemainingItem{idx, string(readBuffer[final:n]), true}

		parser.ParseLines(readBuffer[start:final], w.updateStd)
	}

	wg.Done()
}

func newTrashWorker() TrashWorker {
	return TrashWorker{
		stations: swiss.NewMap[uint64, *Station](1024),
		in:       make(chan *RemainingItem, 70*2),
	}
}

type TrashWorker struct {
	stations *swiss.Map[uint64, *Station]
	in       chan *RemainingItem
}

func (w *TrashWorker) updateStd(std parser.StationData) {
	updateStationData(w.stations, std)
}

func (rw *TrashWorker) consume(wg *sync.WaitGroup) {
	defer wg.Done()

	buffer, can := make([]byte, MapSize), swiss.NewMap[int, *RemainingItem](SizeWorkerChan)
	for item := range rw.in {
		if item.Idx == 0 {
			total := len(item.Content)
			copy(buffer[:total], item.Content)
			parser.ParseLines(buffer[:total], rw.updateStd)
			continue
		}

		if c, ok := can.Get(item.Idx); ok {
			rw.saveCan(buffer, c, item)
			can.Delete(c.Idx)
		} else {
			can.Put(item.Idx, item)
		}
	}
}

func (rw *TrashWorker) saveCan(buff []byte, ref, oth *RemainingItem) {
	if ref.Initial {
		copy(buff[:len(ref.Content)], ref.Content)
		copy(buff[len(ref.Content):], oth.Content)
	} else {
		copy(buff[:len(oth.Content)], oth.Content)
		copy(buff[len(oth.Content):], ref.Content)
	}
	total := len(ref.Content) + len(oth.Content)

	parser.ParseLines(buff[:total], rw.updateStd)
}

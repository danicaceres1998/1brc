package types

import (
	"go-1brc/pkg/parser"
	"sync"
	"testing"

	"github.com/dolthub/swiss"
	"github.com/stretchr/testify/assert"
)

func TestUpdateStdData(t *testing.T) {
	data := swiss.NewMap[uint64, *Station](5)
	stationId := uint64(370594201177)
	stData := []parser.StationData{
		{Name: "Yaoundé", Temperature: 100, HashId: stationId},
		{Name: "Yaoundé", Temperature: -100, HashId: stationId},
	}

	for _, std := range stData {
		updateStationData(data, std)
	}
	station, ok := data.Get(stationId)
	assert.True(t, ok)
	assert.Equal(t, 100, station.Max)
	assert.Equal(t, -100, station.Min)
	assert.Equal(t, 2, station.Count)
	assert.Equal(t, "Yaoundé", station.Name)
}

func TestWorkersUpdateStd(t *testing.T) {
	type worker interface {
		updateStd(parser.StationData)
	}

	stationId := uint64(370594201177)
	stsData := []parser.StationData{
		{Name: "Yaoundé", Temperature: 100, HashId: stationId},
		{Name: "Yaoundé", Temperature: -100, HashId: stationId},
	}

	data := []struct {
		name  string
		w     worker
		wType string
	}{
		{"worker", nil, "normal"},
		{"trash-worker", nil, "trash"},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			stations := swiss.NewMap[uint64, *Station](5)
			d.w = func() worker {
				if d.wType == "normal" {
					w := newWorker(&FileObject{})
					w.stations = stations
					return &w
				}
				w := newTrashWorker()
				w.stations = stations
				return &w
			}()

			for _, std := range stsData {
				d.w.updateStd(std)
			}

			station, ok := stations.Get(stationId)
			assert.True(t, ok)
			assert.Equal(t, 100, station.Max)
			assert.Equal(t, -100, station.Min)
			assert.Equal(t, 2, station.Count)
			assert.Equal(t, "Yaoundé", station.Name)
		})
	}
}

func TestWorkerConsume(t *testing.T) {
	f, cleanTmp := createTmpFile()
	defer cleanTmp()
	fo, err := NewFileObject(f.Name())
	assert.Nil(t, err)

	var wg sync.WaitGroup
	trashCh := make(chan *RemainingItem)
	// Trash bin
	go func() {
		for i := range 2 {
			trash := <-trashCh
			assert.Equal(t, i, trash.Idx)
			if i == 0 {
				assert.Equal(t, trash.Content, "Yaoundé;33.5\n")
			} else {
				assert.Equal(t, trash.Content, "")
			}
		}
		close(trashCh)
	}()

	w := newWorker(fo)
	wg.Add(1)
	w.consume(&wg, trashCh)
	wg.Wait()

	data := []struct {
		key        uint64
		city       string
		sum, count int
		min, max   int
	}{
		//"Yaoundé;33.5\nYaoundé;10.5\nYaoundé;40.5\nYaoundé;-3.5\n"
		{7571807575422721, "Yaoundé", (105 + 405 + (-35)), 3, -35, 405},
	}
	for _, d := range data {
		if s, ok := w.stations.Get(d.key); ok {
			assert.Equal(t, d.sum, s.Sum)
			assert.Equal(t, d.city, s.Name)
			assert.Equal(t, d.count, s.Count)
			continue
		}
		t.Errorf("station not found: %s", d.city)
	}
	assert.Equal(t, w.stations.Count(), len(data))
}

func TestTrashWorkerConsume(t *testing.T) {
	tWorker := newTrashWorker()
	// Trash sender
	go func() {
		data := "Yaoundé;10.5\n"
		for i := range 4 {
			ri1 := RemainingItem{i, data, false}
			tWorker.in <- &ri1
			ri2 := RemainingItem{i + 1, data, true}
			tWorker.in <- &ri2
		}
		close(tWorker.in)
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	tWorker.consume(&wg)
	wg.Wait()

	tWorker.stations.Iter(func(k uint64, v *Station) (stop bool) {
		assert.Equal(t, "Yaoundé", v.Name)
		assert.Equal(t, 105, v.Min)
		assert.Equal(t, 105, v.Max)
		assert.Equal(t, 105*7, v.Sum)
		assert.Equal(t, 7, v.Count)
		return false
	})
}

func TestTrashWorkerSaveCan(t *testing.T) {
	data := []struct {
		name string
		ref  *RemainingItem
		oth  *RemainingItem
	}{
		{
			"ref-initial",
			&RemainingItem{0, "Yaoundé", true},
			&RemainingItem{0, ";33.5\n", false},
		},
		{
			"oth-initial",
			&RemainingItem{0, ";33.5\n", false},
			&RemainingItem{0, "Yaoundé", true},
		},
	}

	buff := make([]byte, 1024)
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			tWorker := newTrashWorker()
			tWorker.saveCan(buff, d.ref, d.oth)

			s, ok := tWorker.stations.Get(7571807575422721)
			assert.True(t, ok)
			assert.Equal(t, "Yaoundé", s.Name)
			assert.Equal(t, 335, s.Min)
			assert.Equal(t, 335, s.Max)
			assert.Equal(t, 335, s.Sum)
			assert.Equal(t, 1, s.Count)
		})
	}
}

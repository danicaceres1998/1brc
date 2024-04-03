package types

import (
	"go-1brc/pkg/parser"
	"sync"
	"testing"

	"github.com/dolthub/swiss"
	"github.com/stretchr/testify/assert"
)

func TestStatioUpdate(t *testing.T) {
	s := Station{}
	s.Update(parser.StationData{
		Temperature: 10,
	})
	assert.Equal(t, s.Max, 10)
	assert.Equal(t, s.Min, 0)
	assert.Equal(t, s.Sum, 10)
	assert.Equal(t, s.Count, 1)
}

func TestStationAvgTemp(t *testing.T) {
	s := Station{Sum: 100, Count: 2}
	assert.Equal(t, s.AvgTemperature(), 5.0)
}

func TestWorkerConsume(t *testing.T) {
	w := Worker{in: make(chan []byte), stations: swiss.NewMap[uint64, *Station](3)}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		w.in <- []byte("Yaoundé;33.5\nYaoundé;18.0\nSana'a;17.7")
		close(w.in)
	}()

	w.consume(&wg)
	wg.Wait()

	data := []struct {
		key        uint64
		city       string
		sum, count int
	}{
		{7571807575422721, "Yaoundé", (335 + 180), 2},
		{6952756473232, "Sana'a", 177, 1},
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

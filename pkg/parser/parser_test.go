package parser

import (
	"testing"

	"github.com/dolthub/swiss"
	"github.com/stretchr/testify/assert"
)

func TestStatioUpdate(t *testing.T) {
	s := Station{}
	s.Update(10)
	assert.Equal(t, 10, s.Max)
	assert.Equal(t, 0, s.Min)
	assert.Equal(t, 10, s.Sum)
	assert.Equal(t, 1, s.Count)

	s.Update(-1)
	assert.Equal(t, -1, s.Min)
	assert.Equal(t, 2, s.Count)
}

func TestStationAvgTemp(t *testing.T) {
	s := Station{Sum: 100, Count: 2}
	assert.Equal(t, s.AvgTemperature(), 5.0)
}

func TestParseLines(t *testing.T) {
	data := []struct {
		name     string
		input    []byte
		expected []Station
	}{
		{
			"success", []byte("Yaoundé;33.5\nWichita;18.0\nSana'a;17.7"),
			[]Station{
				createStationData("Yaoundé", 335),
				createStationData("Wichita", 180),
				createStationData("Sana'a", 177),
			},
		},
		{
			"unparsable-bytes", []byte("Yaoundé;33.Sana'a;17.7"),
			[]Station{createStationData("Yaoundé", 370594201177)},
		},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			stations := swiss.NewMap[uint64, *Station](3)
			ParseLines(d.input, stations)
			assert.Equal(t, len(d.expected), stations.Count())

			stations.Iter(func(k uint64, v *Station) (stop bool) {
				assert.Contains(t, d.expected, *v)
				return false
			})
		})
	}
}

func TestProcessCSVLine(t *testing.T) {
	data := []struct {
		name     string
		line     []byte
		expected Station
		parsed   bool
	}{
		{"success", []byte("Yaoundé;33.5"), createStationData("Yaoundé", 335), true},
		{"success-minus", []byte("New York;-10.5"), createStationData("New York", -105), true},
		{"not-parsed", []byte("asdf3.245"), Station{}, false},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			stations := swiss.NewMap[uint64, *Station](1)
			processCSVLine(d.line, stations)

			if d.parsed {
				assert.Equal(t, 1, stations.Count())
				s, ok := stations.Get(hash([]byte(d.expected.Name)))
				assert.True(t, ok)
				assert.Equal(t, d.expected, *s)
			}
		})
	}

	t.Run("station-update", func(t *testing.T) {
		stations := swiss.NewMap[uint64, *Station](1)
		for range 2 {
			processCSVLine([]byte("Yaoundé;33.5"), stations)
		}
		assert.Equal(t, 1, stations.Count())
		cityName := "Yaoundé"
		s, ok := stations.Get(hash([]byte(cityName)))
		assert.True(t, ok)
		assert.Equal(t, 2, s.Count)
		assert.Equal(t, 335, s.Max)
		assert.Equal(t, 335, s.Min)
		assert.Equal(t, cityName, s.Name)
	})
}

func TestBytesToInt(t *testing.T) {
	data := []struct {
		name     string
		input    []byte
		expected int
	}{
		{"normal-int", []byte("1234"), 1234},
		{"point-number", []byte("12.3"), 123},
		{"minus-number", []byte("-12.3"), -123},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			assert.Equal(t, d.expected, bytesToInt(d.input))
		})
	}
}

func BenchmarkParseCSVLine(b *testing.B) {
	for i := 0; i < b.N; i++ {
		processCSVLine([]byte("Yaoundé;33.5"), swiss.NewMap[uint64, *Station](1))
	}
}

func BenchmarkBytesToInt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = bytesToInt([]byte("-12.3"))
	}
}

// secondary functions //
func createStationData(name string, tmp int) Station {
	return Station{
		Name:  name,
		Min:   tmp,
		Max:   tmp,
		Sum:   tmp,
		Count: 1,
	}
}

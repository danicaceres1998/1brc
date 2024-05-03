package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseLines(t *testing.T) {
	data := []struct {
		name     string
		input    []byte
		expected []StationData
	}{
		{
			"success", []byte("Yaoundé;33.5\nWichita;18.0\nSana'a;17.7"),
			[]StationData{
				createStationData("Yaoundé", 335),
				createStationData("Wichita", 180),
				createStationData("Sana'a", 177),
			},
		},
		{
			"unparsable-bytes", []byte("Yaoundé;33.Sana'a;17.7"),
			[]StationData{createStationData("Yaoundé", 370594201177)},
		},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			counter := 0
			ParseLines(d.input, func(sd StationData) {
				assert.Contains(t, d.expected, sd)
				counter++
			})
			assert.Equal(t, len(d.expected), counter)
		})
	}
}

func TestParceCSVLine(t *testing.T) {
	data := []struct {
		name     string
		line     []byte
		expected StationData
	}{
		{"success", []byte("Yaoundé;33.5"), StationData{HashId: hash([]byte("Yaoundé")), Name: "Yaoundé", Temperature: 335}},
		{"success-minus", []byte("New York;-10.5"), StationData{HashId: hash([]byte("New York")), Name: "New York", Temperature: -105}},
		{"not-parsed", []byte("asdf3.245"), StationData{}},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			result := parseCSVLine(d.line)

			assert.Equal(t, d.expected.Name, result.Name)
			assert.Equal(t, d.expected.Temperature, result.Temperature)
			assert.Equal(t, d.expected.HashId, result.HashId)
		})
	}
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
		_ = parseCSVLine([]byte("Yaoundé;33.5"))
	}
}

func BenchmarkBytesToInt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = bytesToInt([]byte("-12.3"))
	}
}

// secondary functions //
func createStationData(name string, tmp int) StationData {
	return StationData{
		Name:        name,
		Temperature: tmp,
		HashId:      hash([]byte(name)),
	}
}

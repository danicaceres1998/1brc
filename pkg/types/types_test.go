package types

import (
	"go-1brc/pkg/parser"
	"io"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatioUpdate(t *testing.T) {
	s := Station{}
	s.Update(parser.StationData{
		Temperature: 10,
	})
	assert.Equal(t, 10, s.Max)
	assert.Equal(t, 0, s.Min)
	assert.Equal(t, 10, s.Sum)
	assert.Equal(t, 1, s.Count)

	s.Update(parser.StationData{
		Temperature: -1,
	})
	assert.Equal(t, -1, s.Min)
	assert.Equal(t, 2, s.Count)
}

func TestStationAvgTemp(t *testing.T) {
	s := Station{Sum: 100, Count: 2}
	assert.Equal(t, s.AvgTemperature(), 5.0)
}

func TestNewFileObject(t *testing.T) {
	data := []struct {
		name      string
		validFile bool
	}{
		{"valid-file", true},
		{"invalid-file", false},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			var (
				f        *os.File
				cleanTmp func()
			)
			if d.validFile {
				f, cleanTmp = createTmpFile()
				defer cleanTmp()
			}

			fo, err := NewFileObject(func() string {
				if f == nil {
					return "invalid-file-431.txt"
				}
				return f.Name()
			}())

			if d.validFile {
				assert.Nil(t, err)
				assert.Equal(t, f.Name(), fo.file.Name())
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestFileObjectGetIndex(t *testing.T) {
	fo := &FileObject{}
	for i := range 5 {
		assert.Equal(t, i+1, fo.getIndex())
	}
}

func TestFileObjectRead(t *testing.T) {
	numReaders := 2
	out := make(chan []int, numReaders)
	f, cleanTmp := createTmpFile()
	defer cleanTmp()

	fo, err := NewFileObject(f.Name())
	assert.Nil(t, err)

	for i := range numReaders {
		go func(i int) {
			buff, idxs := make([]byte, 14), make([]int, 0, 5)
			for {
				idx, _, err := fo.Read(buff)
				if err == io.EOF {
					out <- idxs
					return
				} else if err != nil {
					panic(err)
				}

				idxs = append(idxs, idx)
			}
		}(i)
	}

	results := make([]int, 0, 10)
	for range numReaders {
		results = append(results, (<-out)...)
	}
	close(out)

	sort.Slice(results, func(i, j int) bool {
		return i < j
	})
	for i := range len(results) {
		assert.Equal(t, i+1, results[i])
	}
}

func BenchmarkReader(b *testing.B) {
	readBuff := make([]byte, WorkerBuffSize)

	for i := 0; i < b.N; i++ {
		fo, err := NewFileObject("../../measurements.txt")
		if err != nil {
			b.Fatal(err)
		}
		for {
			_, _, err := fo.Read(readBuff)
			if err == io.EOF {
				break
			} else if err != nil {
				b.Fatal(err)
			}
		}
	}
}

package csvreader

import (
	"testing"
)

func BenchmarkReader(b *testing.B) {
	for i := 0; i < b.N; i++ {
		data, err := ReadFile("../../measurements.txt")
		if err != nil {
			b.Fatal(err)
		}
		for range data {
			// do nothing
		}
	}
}

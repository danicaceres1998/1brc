package parser

import "github.com/dolthub/swiss"

const (
	// Bytes to string
	NewLine   = 10 // "\n"
	Semicolon = 59 // ";"
	Dot       = 46 // "."
	Minus     = 45 // "-"
)

type Station struct {
	Name  string
	Min   int
	Max   int
	Sum   int
	Count int
}

func (s *Station) Update(tmp int) {
	if s.Min > tmp {
		s.Min = tmp
	}
	if s.Max < tmp {
		s.Max = tmp
	}

	s.Sum += tmp
	s.Count++
}

func (s *Station) AvgTemperature() float64 {
	return (float64(s.Sum) / 10) / float64(s.Count)
}

func ParseLines(buffer []byte, data *swiss.Map[uint64, *Station]) {
	walk, lastIdx := 0, len(buffer)-1

	for i, v := range buffer {
		if v == NewLine {
			processCSVLine(buffer[getIndex(i-walk):i], data)
			walk = 0
		} else if i == lastIdx {
			processCSVLine(buffer[getIndex(i-walk):], data)
			break
		}
		walk++
	}
}

// Private Functions //

func processCSVLine(line []byte, data *swiss.Map[uint64, *Station]) {
	for i, v := range line {
		if v == Semicolon {
			hashId := hash(line[:i])
			tmp := bytesToInt(line[i+1:])
			if s, ok := data.Get(hashId); ok {
				s.Update(tmp)
			} else {
				data.Put(hashId, &Station{
					Name:  string(line[:i]),
					Min:   tmp,
					Max:   tmp,
					Sum:   tmp,
					Count: 1,
				})
			}
			break
		}
	}
}

func getIndex(i int) int {
	if i == 0 {
		return i
	}
	return i + 1
}

func bytesToInt(byteArray []byte) int {
	var (
		result   int
		start    int
		negative bool
	)

	if byteArray[0] == Minus {
		negative = true
		start++
	}

	for i := start; i < len(byteArray); i++ {
		if byteArray[i] == Dot {
			continue
		}

		result = result*10 + int(byteArray[i]-48)
	}

	if negative {
		return -result
	}

	return result
}

func hash(name []byte) uint64 {
	var h uint64 = 5381
	for _, b := range name {
		h = (h << 5) + h + uint64(b)
	}
	return h
}

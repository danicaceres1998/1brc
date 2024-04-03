package parser

import (
	"unsafe"
)

const (
	// Bytes to string
	NewLine   = 10 // "\n"
	Semicolon = 59 // ";"
	Dot       = 46 // "."
	Minus     = 45 // "-"
	// Buffers size
	SBufferSize = 1024
	// Step Pointer
	Step = 1
)

type StationData struct {
	Name        string
	Temperature int
	HashId      uint64
}

func ParseLines(buffer []byte) <-chan StationData {
	out := make(chan StationData, SBufferSize)

	go readBuffer(buffer, out)

	return out
}

// Private Functions //

func readBuffer(buffer []byte, out chan StationData) {
	walk, lastIdx := 0, len(buffer)-1
	for i, v := range buffer {
		if v == NewLine {
			out <- parseCSVLine(buffer[getIndex(i-walk):i])
			walk = 0
		} else if i == lastIdx {
			out <- parseCSVLine(buffer[getIndex(i-walk):])
			break
		}
		walk++
	}

	close(out)
}

func parseCSVLine(line []byte) StationData {
	std := StationData{}
	pointer := unsafe.Pointer(&line[0])

	for i := 0; i < len(line); i++ {
		v := *(*byte)(pointer)
		if v == Semicolon {
			std.HashId = hash(line[:i])
			std.Name = unsafe.String(unsafe.SliceData(line[:i]), i)
			std.Temperature = bytesToInt(line[i+1:])
			break
		}

		pointer = unsafe.Pointer(uintptr(pointer) + Step)
	}

	return std
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

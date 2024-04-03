package csvreader

import (
	"io"
	"os"
)

const (
	BufferSize   = 2048 * 2048
	LeftBuffSize = 1024
	NewLine      = 10
)

func ReadFile(fileName string) (<-chan []byte, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	out := make(chan []byte, 1024)
	go readFile(file, out)

	return out, nil
}

// Private Functions //

func readFile(file *os.File, out chan []byte) {
	defer file.Close()

	readBuffer := make([]byte, BufferSize)
	leftoverBuffer := make([]byte, LeftBuffSize)
	leftoverSize := 0

	for {
		n, err := file.Read(readBuffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		// Find the last '\n' (byte=10)
		m := 0
		for i := n - 1; i >= 0; i-- {
			if readBuffer[i] == NewLine {
				m = i
				break
			}
		}

		data := make([]byte, m+leftoverSize)
		copy(data, leftoverBuffer[:leftoverSize])
		copy(data[leftoverSize:], readBuffer[:m])
		copy(leftoverBuffer, readBuffer[m+1:n])
		leftoverSize = n - m - 1
		out <- data
	}

	close(out)
}

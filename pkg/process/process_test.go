package process

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStartRowsProcess(t *testing.T) {
	data := []struct {
		name      string
		validFile bool
	}{
		{"valid-file", true},
		{"invalid-file", false},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			var file *os.File
			if d.validFile {
				var deleteFile func()
				file, deleteFile = createTmpFile()
				defer deleteFile()
			}

			sm, err := StartRowsProcess(
				func() string {
					if file != nil {
						return file.Name()
					}
					return "test-file-asdf1234.txt"
				}(),
			)
			if d.validFile {
				assert.Nil(t, err)
				assert.False(t, sm == nil)
			} else {
				assert.Error(t, err)
				assert.Nil(t, sm)
			}
		})
	}
}

func TestPrintResults(t *testing.T) {
	// Testing Valid Station Manager
	output := captureOutput(func() {
		file, deleteFile := createTmpFile()
		defer deleteFile()

		sm, err := StartRowsProcess(file.Name())
		assert.Nil(t, err)

		PrintResults(sm)
	})

	cities := []string{"Yaoundé=33.5/33.5/33.5", "Sana'a=17.7/17.7/17.7", "Wichita=18.0/18.0/18.0"}
	for _, c := range cities {
		assert.Contains(t, output, c)
	}
}

// Auxiliary Functions //

const fileContent = "Yaoundé;33.5\nWichita;18.0\nSana'a;17.7\n"

func createTmpFile() (*os.File, func()) {
	file, err := os.CreateTemp("/var/tmp", "test-file-")
	if err != nil {
		panic(err)
	}

	_, err = file.WriteString(fileContent)
	if err != nil {
		panic(err)
	}

	return file, func() {
		if file != nil {
			file.Close()
			os.Remove(file.Name())
		}
	}
}

func captureOutput(f func()) string {
	orig := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	f()
	os.Stdout = orig
	w.Close()
	out, _ := io.ReadAll(r)
	return string(out)
}

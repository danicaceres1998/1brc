package types

import (
	"os"
	"sync"
)

func NewFileObject(fileName string) (*FileObject, error) {
	file, err := os.Open(fileName)
	return &FileObject{
		file:    file,
		lock:    sync.Mutex{},
		lockIdx: 0,
	}, err
}

type FileObject struct {
	file    *os.File
	lock    sync.Mutex
	lockIdx int
}

func (fo *FileObject) getIndex() int {
	fo.lockIdx++
	return fo.lockIdx
}

func (fo *FileObject) Read(buff []byte) (idx int, n int, err error) {
	defer fo.lock.Unlock()

	fo.lock.Lock()
	idx = fo.getIndex()
	n, err = fo.file.Read(buff)

	return idx, n, err
}

type RemainingItem struct {
	Idx     int
	Content string
	Initial bool
}

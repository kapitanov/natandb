package storage

import (
	"fmt"
	"io"
	"os"
)

type writeAheadLogFileImpl struct {
	Path string
}

// Read opens WAL log file for reading
func (impl *writeAheadLogFileImpl) Read() (io.ReadSeeker, error) {
	f, err := os.Open(impl.Path)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// Write opens WAL log file for writing
func (impl *writeAheadLogFileImpl) Write() (io.WriteCloser, error) {
	f, err := os.Open(impl.Path)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// NewWriteAheadLogFile creates an instance of WriteAheadLogFile based on physical files
func NewWriteAheadLogFile(directoryPath string) (WriteAheadLogFile, error) {
	err := os.Mkdir(directoryPath, 0)
	if err != nil {
		return nil, err
	}

	path := fmt.Sprintf("%s/wal.dat", directoryPath)

	impl := writeAheadLogFileImpl{path}
	return &impl, nil
}

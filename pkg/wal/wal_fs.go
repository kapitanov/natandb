package wal

import (
	"fmt"
	"io"
	"os"
)

type fileSystemImpl struct {
	Path string
}

// OpenRead opens WAL log file for reading
func (impl *fileSystemImpl) OpenRead() (io.ReadSeeker, error) {
	f, err := os.Open(impl.Path)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// OpenWrite opens WAL log file for writing
func (impl *fileSystemImpl) OpenWrite() (io.WriteCloser, error) {
	f, err := os.Open(impl.Path)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// NewFileSystem creates an instance of FileSystem based on physical files
func NewFileSystem(directoryPath string) (FileSystem, error) {
	err := os.Mkdir(directoryPath, 0)
	if err != nil {
		return nil, err
	}

	path := fmt.Sprintf("%s/wal.dat", directoryPath)

	impl := fileSystemImpl{path}
	return &impl, nil
}

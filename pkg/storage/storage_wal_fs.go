package storage

import (
	"io"
	"os"
	"path/filepath"
	
	"github.com/kapitanov/natandb/pkg/fs"
)

type writeAheadLogFileImpl struct {
	path string
}

// NewWriteAheadLogFile creates an instance of WriteAheadLogFile based on physical files
func NewWriteAheadLogFile(filePath string) (WriteAheadLogFile, error) {
	filePath, err := filepath.Abs(filePath)
	if err != nil {
		log.Printf("storage: malformed path \"%s\": %s", filePath, err)
		return nil, err
	}

	log.Printf("storage: using path \"%s\"", filePath)
	directoryPath := filepath.Dir(filePath)
	err = fs.MkDir(directoryPath)
	if err != nil {
		log.Printf("storage: error! %s", err)
		return nil, err
	}

	impl := writeAheadLogFileImpl{filePath}
	return &impl, nil
}

// Read opens WAL log file for reading
func (impl *writeAheadLogFileImpl) Read() (io.ReadSeeker, error) {
	f, err := os.OpenFile(impl.path, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Printf("storage: unable to open file \"%s\" for reading: %s", impl.path, err)
		return nil, err
	}

	return f, nil
}

// Write opens WAL log file for writing
func (impl *writeAheadLogFileImpl) Write() (io.WriteCloser, error) {
	f, err := os.OpenFile(impl.path, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Printf("storage: unable to open file \"%s\" for writing: %s", impl.path, err)
		return nil, err
	}

	return f, nil
}
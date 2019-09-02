package storage

import (
	"io"
	"os"
	"path/filepath"

	"github.com/kapitanov/natandb/pkg/fs"
)

type driverImpl struct {
	walFilePath      string
	snapshotFilePath string
}

// NewDriver creates an instance of Driver based on physical files
func NewDriver(directory string) (Driver, error) {
	directory, err := filepath.Abs(directory)
	if err != nil {
		log.Errorf("malformed path \"%s\": %s", directory, err)
		return nil, err
	}

	log.Verbosef("using directory \"%s\"", directory)
	err = fs.MkDir(directory)
	if err != nil {
		log.Errorf("unable to initialize storage driver. %s", err)
		return nil, err
	}

	walFilePath := filepath.Join(directory, "journal.bin")
	snapshotFilePath := filepath.Join(directory, "snapshot.bin")

	impl := driverImpl{walFilePath, snapshotFilePath}
	return &impl, nil
}

// ReadWalFile opens WAL log file for reading
func (impl *driverImpl) ReadWalFile() (io.ReadSeeker, error) {
	f, err := os.OpenFile(impl.walFilePath, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("unable to open file \"%s\" for reading: %s", impl.walFilePath, err)
		return nil, err
	}

	log.Verbosef("opened \"%s\" for reading", impl.walFilePath)
	return f, nil
}

// WriteWalFile opens WAL log file for writing
func (impl *driverImpl) WriteWalFile() (io.WriteCloser, error) {
	f, err := os.OpenFile(impl.walFilePath, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("unable to open file \"%s\" for writing: %s", impl.walFilePath, err)
		return nil, err
	}

	log.Verbosef("opened \"%s\" for writing", impl.walFilePath)
	return f, nil
}

// ReadSnapshotFile opens data snapshot file file for reading
func (impl *driverImpl) ReadSnapshotFile() (io.ReadCloser, error) {
	f, err := os.OpenFile(impl.snapshotFilePath, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("unable to open file \"%s\" for reading: %s", impl.snapshotFilePath, err)
		return nil, err
	}

	log.Verbosef("opened \"%s\" for reading", impl.snapshotFilePath)
	return f, nil
}

// WriteSnapshotFile opens data snapshot file for writing
func (impl *driverImpl) WriteSnapshotFile() (io.WriteCloser, error) {
	f, err := os.OpenFile(impl.snapshotFilePath, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("storage: unable to open file \"%s\" for writing: %s", impl.snapshotFilePath, err)
		return nil, err
	}

	log.Verbosef("opened \"%s\" for writing", impl.snapshotFilePath)
	return f, nil
}

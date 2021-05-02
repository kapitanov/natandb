package storage

import (
	"fmt"
	"github.com/kapitanov/natandb/pkg/util"
	"io"
	"os"
	"path/filepath"
)

type driver struct {
	wal      *walFile
	snapshot *snapshotFile
}

type driverOptions struct {
	walFilePath      string
	snapshotFilePath string
}

// DriverOption is a configuration function for NewDriver
type DriverOption func(*driverOptions) error

// DirectoryOption sets path to directory that contains WAL and snapshot files
func DirectoryOption(path string) DriverOption {
	return func(options *driverOptions) error {
		absPath, err := filepath.Abs(path)
		if err != nil {
			log.Errorf("malformed path \"%s\": %s", path, err)
			return err
		}
		path = absPath

		walFilePath := filepath.Join(absPath, "journal.dat")
		err = WALFileOption(walFilePath)(options)
		if err != nil {
			return err
		}

		snapshotFilePath := filepath.Join(absPath, "snapshot.dat")
		err = SnapshotFileOption(snapshotFilePath)(options)
		if err != nil {
			return err
		}

		return nil
	}
}

// WALFileOption sets path to WAL file
func WALFileOption(path string) DriverOption {
	return func(options *driverOptions) error {
		absPath, err := filepath.Abs(path)
		if err != nil {
			log.Errorf("malformed path \"%s\": %s", path, err)
			return err
		}

		directory, _ := filepath.Split(absPath)
		err = util.MkDir(directory)
		if err != nil {
			return err
		}

		options.walFilePath = absPath
		return nil
	}
}

// SnapshotFileOption sets path to snapshot file
func SnapshotFileOption(path string) DriverOption {
	return func(options *driverOptions) error {
		absPath, err := filepath.Abs(path)
		if err != nil {
			log.Errorf("malformed path \"%s\": %s", path, err)
			return err
		}

		directory, _ := filepath.Split(absPath)
		err = util.MkDir(directory)
		if err != nil {
			return err
		}

		options.snapshotFilePath = absPath
		return nil
	}
}

// NewDriver creates an instance of Driver based on physical files
func NewDriver(opts ...DriverOption) (Driver, error) {
	options := &driverOptions{}
	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return nil, err
		}
	}

	if options.walFilePath == "" {
		return nil, fmt.Errorf("wal path is not configured")
	}

	if options.snapshotFilePath == "" {
		return nil, fmt.Errorf("wal path is not configured")
	}

	log.Verbosef("got wal file path \"%s\"", options.walFilePath)
	log.Verbosef("got snapshot file path \"%s\"", options.snapshotFilePath)

	d := &driver{
		wal:      &walFile{options.walFilePath},
		snapshot: &snapshotFile{options.snapshotFilePath},
	}
	return d, nil
}

// WALFile provides access to WAL file
func (d *driver) WALFile() WALFile {
	return d.wal
}

// SnapshotFile provides access to snapshot file
func (d *driver) SnapshotFile() SnapshotFile {
	return d.snapshot
}

// walFile provides access to WAL file
type walFile struct {
	path string
}

// Read opens WAL file for reading
func (f *walFile) Read() (WALReader, error) {
	file, err := os.OpenFile(f.path, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("unable to open file \"%s\": %s", f.path, err)
		return nil, err
	}

	return newWALReader(file)
}

// Write opens WAL file for writing
func (f *walFile) Write() (WALWriter, error) {
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		log.Errorf("unable to open file \"%s\": %s", f.path, err)
		return nil, err
	}

	return newWALWriter(file)
}

// SnapshotFile provides access to snapshot file
type snapshotFile struct {
	path string
}

// Read opens snapshot file for reading
func (f *snapshotFile) Read() (io.ReadCloser, error) {
	file, err := os.OpenFile(f.path, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("unable to open file \"%s\": %s", f.path, err)
		return nil, err
	}

	return file, nil
}

// Write opens snapshot file for writing
func (f *snapshotFile) Write() (io.WriteCloser, error) {
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		log.Errorf("unable to open file \"%s\": %s", f.path, err)
		return nil, err
	}

	err = file.Truncate(0)
	if err != nil {
		return nil, err
	}

	return file, nil
}

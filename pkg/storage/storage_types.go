package storage

import (
	"io"

	l "github.com/kapitanov/natandb/pkg/log"
)

var log = l.New("storage")

// Driver provides access to persistent data streams
type Driver interface {
	// ReadWalFile opens WAL log file for reading
	ReadWalFile() (io.ReadSeeker, error)
	// WriteWalFile opens WAL log file for writing
	WriteWalFile() (io.WriteCloser, error)

	// ReadSnapshotFile opens data snapshot file file for reading
	ReadSnapshotFile() (io.ReadCloser, error)
	// WriteSnapshotFile opens data snapshot file for writing
	WriteSnapshotFile() (io.WriteCloser, error)
}

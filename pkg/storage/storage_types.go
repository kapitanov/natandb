package storage

import (
	"io"
)

// SnapshotFile provides access to snapshot storage files
type SnapshotFile interface {
	// Read opens data snapshot file file for reading
	Read() (io.ReadCloser, error)
	// Write opens data snapshot file for writing
	Write() (io.WriteCloser, error)
}

// WriteAheadLogFile provides access to write-ahead log data streams
type WriteAheadLogFile interface {
	// Read opens WAL log file for reading
	Read() (io.ReadSeeker, error)
	// Write opens WAL log file for writing
	Write() (io.WriteCloser, error)
}

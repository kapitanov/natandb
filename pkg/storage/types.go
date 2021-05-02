package storage

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	l "github.com/kapitanov/natandb/pkg/log"
)

const (
	// WALVersion is current WAL file schema version
	WALVersion uint32 = 1
)

var log = l.New("storage")

var (
	ErrAlreadyInTx = errors.New("already in tx")
	ErrNotInTx     = errors.New("not in tx")
)

// Driver provides access to persistent data streams
type Driver interface {
	// WALFile provides access to WAL file
	WALFile() WALFile

	// SnapshotFile provides access to snapshot file
	SnapshotFile() SnapshotFile
}

// WALFile provides access to WAL file
type WALFile interface {
	// Read opens WAL file for reading
	Read() (WALReader, error)

	// Write opens WAL file for writing
	Write() (WALWriter, error)
}

// SnapshotFile provides access to snapshot file
type SnapshotFile interface {
	// Read opens snapshot file for reading
	Read() (io.ReadCloser, error)

	// Write opens snapshot file for writing
	Write() (io.WriteCloser, error)
}

// WALRecordType is a type code for a write-ahead log record
type WALRecordType = uint8

const (
	// WALNone marks an empty record
	WALNone WALRecordType = iota
	// WALAddValue marks a record that adds a value to a key
	WALAddValue
	// WALRemoveValue marks a record that removes a value from a key
	WALRemoveValue
	// WALRemoveKey marks a record that removes a key entirely
	WALRemoveKey
	// WALCommitTx marks a record that commits a transaction
	WALCommitTx
)

// WALRecord is a single record from a write-ahead log
type WALRecord struct {
	// Record ID
	ID uint64
	// Record transaction ID
	TxID uint64
	// Record type
	Type WALRecordType
	// Record payload - key
	Key string
	// Record payload - value
	Value []byte
}

// ValueLength returns a value length even if Value is nil
func (r *WALRecord) ValueLength() int {
	if r.Value == nil {
		return 0
	}

	return len(r.Value)
}

// String converts a record into its string representation
func (r *WALRecord) String() string {
	// String format:
	// #ID 0xTYPE \"KEY\"/\"VALUE\"

	var typeStr string
	switch r.Type {
	default:
		typeStr = fmt.Sprintf("0x%02x", r.Type)
		break
	}

	var valueStr string
	if r.Value != nil {
		valueStr = fmt.Sprintf("\"%s\"", base64.StdEncoding.EncodeToString(r.Value))
	} else {
		valueStr = "null"
	}

	return fmt.Sprintf("#%08d %s \"%s\"/%s", r.ID, typeStr, r.Key, valueStr)
}

// Equals compares two WAL records for equality
func (r *WALRecord) Equals(other *WALRecord) bool {
	if r.ID != other.ID {
		return false
	}

	if r.Type != other.Type {
		return false
	}

	if r.Key != other.Key {
		return false
	}

	valueLength := r.ValueLength()
	if valueLength != other.ValueLength() {
		return false
	}

	if valueLength > 0 {
		for i := 0; i < valueLength; i++ {
			if r.Value[i] != other.Value[i] {
				return false
			}
		}
	}

	return true
}

// WALReader provides write-ahead log reading functions
type WALReader interface {
	// Read read a record from a WAL file. Returns io.EOF if there are no more records to read
	Read() (*WALRecord, error)

	// Close shuts down WAL
	Close() error
}

// WALWriter provides write-ahead log writing functions
type WALWriter interface {
	// BeginTx starts a WAL transaction
	BeginTx() error

	// CommitTx commits a WAL transaction
	CommitTx() error

	// RollbackTx rolls a WAL transaction back
	RollbackTx() error

	// Write writes a single record to a WAL file and sets its ID
	Write(record *WALRecord) error

	// Close shuts down WAL
	Close() error
}

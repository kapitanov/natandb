package writeahead

import (
	"encoding/base64"
	"fmt"
	"io"
)

// RecordType is a type code for a write-ahead log record
type RecordType = uint8

const (
	// None marks an empty record
	None RecordType = iota
	// AddValue marks a record than adds a value to a key
	AddValue
	// RemoveValue marks a record than removes a value from a key
	RemoveValue
	// RemoveKey marks a record than removes a key entirely
	RemoveKey
)

// Record is a single record from a write-ahead log
type Record struct {
	// Record ID
	ID uint64
	// Record Type
	Type RecordType
	// Record payload - key
	Key string
	// Record payload - value
	Value []byte
}

// ValueLength returns a value length even if Value is nil
func (r *Record) ValueLength() int {
	if r.Value == nil {
		return 0
	}

	return len(r.Value)
}

// String converts a record into its string representation
func (r *Record) String() string {
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
func (r *Record) Equals(other *Record) bool {
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

// RecordChunk is a chunk of WAL records
type RecordChunk []*Record

// Empty returns true if a chunk is empty
func (c RecordChunk) Empty() bool {
	return len(c) == 0
}

// Log provides write-ahead log functions
type Log interface {
	// WriteOne writes a single record to a WAL file and sets its ID
	WriteOne(record *Record) error
	// WriteMany writes a list of records to a WAL file and sets their IDs
	WriteMany(records []*Record) error
	// ReadChunkForward reads a list of records from a WAL file in forward direction with filtering by ID
	ReadChunkForward(minID uint64, limit int) (RecordChunk, error)
	// ReadChunkBackward reads a list of records from a WAL file in backward direction with filtering by ID
	ReadChunkBackward(maxID uint64, limit int) (RecordChunk, error)
	// Close shuts down WAL
	Close() error
}

// RecordMetadata contains WAL record metadata
type RecordMetadata struct {
	// Record ID
	ID uint64
	// Record length (including header)
	Length uint32
}

// Serializer converts WAL records from and into its binary form
type Serializer interface {
	// Serialize writes a WAL record into its binary representation
	Serialize(record *Record, w io.Writer) error

	// Deserialize reads a WAL record from its binary representation
	Deserialize(r io.Reader) (*Record, error)

	// CalcBinaryLength calculates how many bytes will take binary representation of a record
	CalcBinaryLength(record *Record) int64

	// GetRecordMetadataForward reads record metadata from binary stream and seeks back to original position
	// GetRecordMetadataForward assumes that input stream is at record's start
	GetRecordMetadataForward(r io.ReadSeeker, metadata *RecordMetadata) error

	// GetRecordMetadataBackward reads record metadata from binary stream and seeks back to original position
	// GetRecordMetadataBackward assumes that input stream is at record's end
	GetRecordMetadataBackward(r io.ReadSeeker, metadata *RecordMetadata) error
}

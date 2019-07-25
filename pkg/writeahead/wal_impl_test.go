package writeahead_test

import (
	"io"
	"testing"

	"github.com/kapitanov/natandb/pkg/storage"
	writeahead "github.com/kapitanov/natandb/pkg/writeahead"
)

const (
	WriteCount = 11
)

func TestEmptyLog(t *testing.T) {
	log, err := writeahead.NewLog(NewInMemoryWriteAheadLogFile(t), writeahead.NewSerializer())
	if err != nil {
		t.Errorf("ERROR: NewLog() failed: %s", err)
		return
	}

	chunk, err := log.ReadChunkForward(0, 100)
	if err != nil {
		t.Errorf("ERROR: ReadChunk() failed: %s", err)
		return
	}
	if !chunk.Empty() {
		t.Errorf("ERROR: chunk is not empty: %d", len(chunk))
	}
}

func TestReadAfterWriteOne(t *testing.T) {
	log, err := writeahead.NewLog(NewInMemoryWriteAheadLogFile(t), writeahead.NewSerializer())
	if err != nil {
		t.Errorf("ERROR: NewLog() failed: %s", err)
		return
	}

	count := WriteCount
	for i := 0; i < count; i++ {
		record := &writeahead.Record{
			Type:  writeahead.AddValue,
			Key:   "foo/bar",
			Value: []byte("FooBar"),
		}
		err = log.WriteOne(record)
		if err != nil {
			t.Errorf("ERROR: WriteOne() failed: %s", err)
			return
		}
	}

	chunk, err := log.ReadChunkForward(0, count)
	if err != nil {
		t.Errorf("ERROR: ReadChunk() failed: %s", err)
		return
	}
	if len(chunk) != count {
		t.Errorf("ERROR: chunk size mismatch: %d != %d", len(chunk), count)
		return
	}

	for i := 0; i < count; i++ {
		if chunk[i].ID != uint64(i+1) {
			t.Errorf("ERROR: wrong chunk id (at %d): %d != %d", i, chunk[i].ID, i+1)
			return
		}
	}
}

func TestReadAfterWriteMany(t *testing.T) {
	log, err := writeahead.NewLog(NewInMemoryWriteAheadLogFile(t), writeahead.NewSerializer())
	if err != nil {
		t.Errorf("ERROR: NewLog() failed: %s", err)
		return
	}

	count := WriteCount
	records := make([]*writeahead.Record, count)
	for i := 0; i < count; i++ {
		record := &writeahead.Record{
			Type:  writeahead.AddValue,
			Key:   "foo/bar",
			Value: []byte("FooBar"),
		}
		records[i] = record
	}

	err = log.WriteMany(records)
	if err != nil {
		t.Errorf("ERROR: WriteMany() failed: %s", err)
		return
	}

	chunk, err := log.ReadChunkForward(0, count)
	if err != nil {
		t.Errorf("ERROR: ReadChunk() failed: %s", err)
		return
	}
	if len(chunk) != count {
		t.Errorf("ERROR: chunk size mismatch: %d != %d", len(chunk), count)
		return
	}

	for i := 0; i < count; i++ {
		if chunk[i].ID != uint64(i+1) {
			t.Errorf("ERROR: wrong chunk id (at %d): %d != %d", i, chunk[i].ID, i+1)
			return
		}
	}
}

func TestReadChunkForward(t *testing.T) {
	log, err := writeahead.NewLog(NewInMemoryWriteAheadLogFile(t), writeahead.NewSerializer())
	if err != nil {
		t.Errorf("ERROR: NewLog() failed: %s", err)
		return
	}

	count := WriteCount
	records := make([]*writeahead.Record, count)
	for i := 0; i < count; i++ {
		record := &writeahead.Record{
			Type:  writeahead.AddValue,
			Key:   "foo/bar",
			Value: []byte("FooBar"),
		}
		records[i] = record
	}

	err = log.WriteMany(records)
	if err != nil {
		t.Errorf("ERROR: WriteMany() failed: %s", err)
		return
	}

	totalCount := 0
	chunkSize := 10
	minId := uint64(0)
	for totalCount < count {
		chunk, err := log.ReadChunkForward(minId+1, chunkSize)
		if err != nil {
			t.Errorf("ERROR: ReadChunk() failed: %s", err)
			return
		}

		if chunk.Empty() {
			break
		}

		newMinID := minId
		for i := 0; i < len(chunk); i++ {
			if chunk[i].ID != uint64(totalCount+1) {
				t.Errorf("ERROR: wrong chunk id (at %d): %d != %d", i, chunk[i].ID, totalCount+1)
				return
			}

			if chunk[i].ID > newMinID {
				newMinID = chunk[i].ID
			}

			totalCount++
		}
		if newMinID == minId {
			t.Errorf("ERROR: ReadChunk(%d, %d) didn't update minID (%d)", minId, chunkSize, minId)
			return
		}

		minId = newMinID
	}

	if totalCount != count {
		t.Errorf("ERROR: expected to read %d but got %d", count, totalCount)
		return
	}
}

func TestReadChunkBackward(t *testing.T) {
	log, err := writeahead.NewLog(NewInMemoryWriteAheadLogFile(t), writeahead.NewSerializer())
	if err != nil {
		t.Errorf("ERROR: NewLog() failed: %s", err)
		return
	}

	count := WriteCount
	records := make([]*writeahead.Record, count)
	for i := 0; i < count; i++ {
		record := &writeahead.Record{
			Type:  writeahead.AddValue,
			Key:   "foo/bar",
			Value: []byte("FooBar"),
		}
		records[i] = record
	}

	err = log.WriteMany(records)
	if err != nil {
		t.Errorf("ERROR: WriteMany() failed: %s", err)
		return
	}

	totalCount := 0
	chunkSize := 10
	maxId := ^uint64(0) - 1
	for totalCount < count {
		t.Logf("ReadChunkBackward(%d, %d)", maxId, chunkSize)
		chunk, err := log.ReadChunkBackward(maxId, chunkSize)
		if err != nil {
			t.Errorf("ERROR: ReadChunk() failed: %s", err)
			return
		}

		if chunk.Empty() {
			break
		}

		newMaxID := maxId
		for i := 0; i < len(chunk); i++ {
			expectedID := uint64(WriteCount - totalCount)
			t.Logf("chunk[%d].ID = %d (expected %d)", i, chunk[i].ID, expectedID)
			if chunk[i].ID != expectedID {
				t.Errorf("ERROR: wrong chunk id (at %d): %d != %d", i, chunk[i].ID, totalCount+1)
				return
			}

			if chunk[i].ID < newMaxID {
				newMaxID = chunk[i].ID
			}

			totalCount++
		}
		if newMaxID == maxId {
			t.Errorf("ERROR: ReadChunk(%d, %d) didn't update maxID (%d)", maxId, chunkSize, maxId)
			return
		}

		maxId = newMaxID
	}

	if totalCount != count {
		t.Errorf("ERROR: expected to read %d but got %d", count, totalCount)
		return
	}
}

// --------------------------------------------------------------------------------------------------------------------
// Test helpers
// --------------------------------------------------------------------------------------------------------------------

type inMemoryFileSystem struct {
	t           *testing.T
	buffer      []byte
	readOffset  int
	writeOffset int
	capacity    int
}

type inMemoryFileSystemReaderWriter struct {
	stream *inMemoryFileSystem
}

func (s *inMemoryFileSystem) Read() (io.ReadSeeker, error) {
	s.readOffset = 0
	return &inMemoryFileSystemReaderWriter{s}, nil
}

func (s *inMemoryFileSystem) Write() (io.WriteCloser, error) {
	return &inMemoryFileSystemReaderWriter{s}, nil
}

func (r *inMemoryFileSystemReaderWriter) Read(p []byte) (int, error) {
	s := r.stream
	n := len(p)

	if n > s.capacity-s.readOffset {
		n = s.capacity - s.readOffset
	}

	if n == 0 {
		return 0, io.EOF
	}

	for i := 0; i < n; i++ {
		if i+s.readOffset >= len(s.buffer) {
			s.t.Logf("Bad READ: cap=%d, ro=%d, i=%d, len=%d", s.capacity, s.readOffset, i, len(s.buffer))
		}
		if i >= len(p) {
			s.t.Logf("Bad READ: i=%d, len=%d", i, len(p))
		}
		p[i] = s.buffer[i+s.readOffset]
	}

	s.t.Logf("Read(%d): %d..%d (%d)", len(p), s.readOffset, s.readOffset+n, n)
	s.readOffset += n
	return n, nil
}

func (w *inMemoryFileSystemReaderWriter) Write(p []byte) (int, error) {
	s := w.stream
	n := len(p)
	for len(s.buffer) < s.writeOffset+n {
		newBuffer := make([]byte, len(s.buffer)+1024)
		for i := 0; i < len(s.buffer); i++ {
			newBuffer[i] = s.buffer[i]
		}
		s.buffer = newBuffer
	}

	from := s.writeOffset
	for i := 0; i < n; i++ {
		s.buffer[i+s.writeOffset] = p[i]
	}

	s.writeOffset += n

	if s.capacity < s.writeOffset {
		s.t.Logf("Write(): %d..%d (capacity %d -> %d)", from, from+n, s.capacity, s.writeOffset)
		s.capacity = s.writeOffset
	} else {
		s.t.Logf("Write(): %d..%d", from, from+n)
	}

	return n, nil
}

func (r *inMemoryFileSystemReaderWriter) Seek(offset int64, whence int) (int64, error) {
	s := r.stream
	was := s.readOffset
	switch whence {
	case io.SeekCurrent:
		if s.readOffset+int(offset) < 0 {
			return int64(s.readOffset), io.EOF
		}
		s.readOffset += int(offset)
		break
	case io.SeekStart:
		s.readOffset = int(offset)
		break
	case io.SeekEnd:
		s.readOffset = s.capacity - int(offset)
		break
	}

	s.t.Logf("Seek(%d, %d): %d -> %d", offset, whence, was, s.readOffset)
	return int64(s.readOffset), nil
}

func (r *inMemoryFileSystemReaderWriter) Close() error {
	return nil
}

func NewInMemoryWriteAheadLogFile(t *testing.T) storage.WriteAheadLogFile {
	return &inMemoryFileSystem{
		t:      t,
		buffer: make([]byte, 0),
	}
}

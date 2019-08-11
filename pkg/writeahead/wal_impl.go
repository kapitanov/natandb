package writeahead

import (
	"io"
	"sync"

	"github.com/kapitanov/natandb/pkg/storage"
)

type walImpl struct {
	fs         storage.WriteAheadLogFile
	serializer Serializer

	lastID uint64
	mutex  *sync.Mutex
	file   io.WriteCloser
}

// NewLog creates new write-ahead log instance
func NewLog(fs storage.WriteAheadLogFile, serializer Serializer) (Log, error) {
	wal := &walImpl{
		fs:         fs,
		serializer: serializer,
		mutex:      &sync.Mutex{},
	}
	err := wal.Initialize()
	if err != nil {
		return nil, err
	}
	return wal, nil
}

func (w *walImpl) Initialize() error {
	// TODO add version check
	// TODO scan back and drop unterminated records

	chunk, err := w.ReadChunkBackward(^uint64(0), 1)
	if err != nil {
		return err
	}

	if !chunk.Empty() {
		w.lastID = chunk[0].ID
	} else {
		w.lastID = 0
	}

	w.file, err = w.fs.Write()
	if err != nil {
		return err
	}

	return nil
}

// WriteOne writes a single record to a WAL file and sets its ID
func (w *walImpl) WriteOne(record *Record) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	return w.writeOneImpl(record)
}

// WriteMany writes a list of records to a WAL file and sets their IDs
func (w *walImpl) WriteMany(records []*Record) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	for _, record := range records {
		err := w.writeOneImpl(record)
		if err != nil {
			return err
		}
	}
	return nil
}

// WriteOne writes a single record to a WAL file and sets its ID
func (w *walImpl) writeOneImpl(record *Record) error {
	w.lastID++
	record.ID = w.lastID

	err := w.serializer.Serialize(record, w.file)
	if err != nil {
		return err
	}

	return nil
}

// ReadChunkForward reads a list of records from a WAL file in forward direction with filtering by ID
func (w *walImpl) ReadChunkForward(minID uint64, limit int) (RecordChunk, error) {
	file, err := w.fs.Read()
	if err != nil {
		return nil, err
	}

	defer func() {
		closer, ok := file.(io.Closer)
		if ok {
			closer.Close()
		}
	}()

	// Scan file forward until we find a record with ID >= minID
	var metadata RecordMetadata
	for {
		err = w.serializer.GetRecordMetadataForward(file, &metadata)
		if err != nil {
			// On EOF - return immediatelly
			if err == io.EOF {
				return RecordChunk([]*Record{}), nil
			}
			return nil, err
		}

		if metadata.ID >= minID {
			break
		}

		// Seek forward to the next record
		_, err = file.Seek(int64(metadata.Length), io.SeekCurrent)
		if err != nil {
			return nil, err
		}
	}

	// Then read up to LIMIT records
	count := 0
	records := make([]*Record, limit)
	for i := 0; i < len(records); i++ {
		record, err := w.serializer.Deserialize(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		records[i] = record
		count++
	}

	// Trim output array
	records = records[0:count]

	return RecordChunk(records), nil
}

// ReadChunkBackward reads a list of records from a WAL file in backward direction with filtering by ID
func (w *walImpl) ReadChunkBackward(maxID uint64, limit int) (RecordChunk, error) {
	file, err := w.fs.Read()
	if err != nil {
		log.Printf("unable to read wal file: %s", err)
		return nil, err
	}

	defer func() {
		closer, ok := file.(io.Closer)
		if ok {
			closer.Close()
		}
	}()

	// Seek to the end of file
	length, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	// Scan file backward until we find a record with ID < maxID
	if length == 0 {
		return RecordChunk(make([]*Record, 0)), nil
	}

	var metadata RecordMetadata
	for {
		err = w.serializer.GetRecordMetadataBackward(file, &metadata)
		if err != nil {
			// On EOF - return immediatelly
			if err == io.EOF {
				return RecordChunk([]*Record{}), nil
			}
			return nil, err
		}

		if metadata.ID < maxID {
			break
		}

		// Seek backward to the previous record
		_, err = file.Seek(-int64(metadata.Length), io.SeekCurrent)
		if err != nil {
			return nil, err
		}
	}

	// Then read up to LIMIT records
	count := 0
	records := make([]*Record, limit)
	for i := 0; i < len(records); i++ {
		err = w.serializer.GetRecordMetadataBackward(file, &metadata)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Seek backward to the current record start
		_, err = file.Seek(-int64(metadata.Length), io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		// Read record
		record, err := w.serializer.Deserialize(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		records[i] = record
		count++

		// Seek backward to the previous record
		recordLength := w.serializer.CalcBinaryLength(record)
		_, err = file.Seek(-recordLength, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
	}

	// Trim output array
	records = records[0:count]

	return RecordChunk(records), nil
}

// Close shuts down WAL
func (w *walImpl) Close() error {
	err := w.file.Close()
	if err != nil {
		return err
	}
	return nil
}

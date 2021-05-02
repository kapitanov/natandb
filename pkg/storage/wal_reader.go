package storage

import (
	"io"
	"os"
)

type walReader struct {
	file *os.File
}

func newWALReader(f *os.File) (WALReader, error) {
	_, err := walInit(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	_, err = f.Seek(WALHeaderLength, io.SeekStart)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	reader := &walReader{f}
	return reader, nil
}

// Read read a record from a WAL file. Returns io.EOF if there are no more records to read
func (r *walReader) Read() (*WALRecord, error) {
	return ReadWALRecord(r.file)
}

// Close shuts down WAL
func (r *walReader) Close() error {
	err := r.file.Close()
	if err != nil {
		log.Errorf("unable to close wal file reader: %s", err)
		return err
	}
	log.Verbosef("WALReader: closed")
	return nil
}

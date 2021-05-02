package storage

import (
	"fmt"
	"github.com/kapitanov/natandb/pkg/util"
	"io"
	"os"
)

type walWriter struct {
	file           *os.File
	txCounter      uint64
	idCounter      uint64
	currentTxId    uint64
	isInTx         bool
	position       int64
	prevTxPosition int64
}

func newWALWriter(f *os.File) (WALWriter, error) {
	result, err := walInit(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	position, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	writer := &walWriter{
		file:           f,
		idCounter:      result.LastID,
		txCounter:      result.LastTxID,
		currentTxId:    0,
		isInTx:         false,
		position:       position,
		prevTxPosition: 0,
	}
	return writer, nil
}

// InitializeEmptyFile performs a WAL init routine on an empty WAL file
func (w *walWriter) InitializeEmptyFile() error {
	// Write WAL header
	err := util.WriteUint32(w.file, WALVersion)
	if err != nil {
		return err
	}

	// Reset TxCounter and IdCounter
	w.idCounter = 0
	w.txCounter = 0
	return nil
}

// InitializeNonEmptyFile performs a WAL init routine on a non-empty WAL file
func (w *walWriter) InitializeNonEmptyFile() error {
	// Read WAL header
	_, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	version, err := util.ReadUint32(w.file)
	if err != nil {
		return err
	}
	if version != WALVersion {
		return fmt.Errorf("wal file version v%d is not supported (expected v%d)", version, WALVersion)
	}

	// Scan all WAL records
	hasAnyRecords := false
	prevRecordWasCommitTx := false
	var idCounter uint64 = 0
	var lastValidRecordID uint64 = 0
	var txCounter uint64 = 0
	for {
		record, err := ReadWALRecord(w.file)
		if err != nil {
			if err == io.EOF {
				// End of WAL file reached
				// Last record must be a WALCommitTx
				if hasAnyRecords && !prevRecordWasCommitTx {
					log.Errorf("wal file is damaged: tx #%d was not committed properly (at #%d)", txCounter, idCounter)
					return fmt.Errorf("wal file is damaged")
				}

				// No errors occurred
				w.idCounter = idCounter
				w.txCounter = txCounter
				return nil
			}

			return err
		}

		if !hasAnyRecords {
			// First record gets special handling
			idCounter = record.ID
			txCounter = record.TxID
			hasAnyRecords = true
		} else {
			// Check record ID - it must be prev record Id + 1
			if idCounter+1 != record.ID {
				log.Errorf("wal file is damaged: expected record #%d after #%d but got #%d", idCounter+1, idCounter, record.ID)

				// Perform an automatic error correction (with inevitable data loss)
				return w.TrimAfter(lastValidRecordID)
			}
			idCounter = record.ID

			// Check TxID
			// TxID change is only allowed if prev record was a WALCommitTx record
			if txCounter != record.TxID {
				if !prevRecordWasCommitTx {
					log.Errorf("wal file is damaged: tx #%d was not committed properly (at #%d)", txCounter, record.ID)

					// Perform an automatic error correction (with inevitable data loss)
					return w.TrimAfter(lastValidRecordID)
				}

				txCounter = record.TxID
			}
		}

		prevRecordWasCommitTx = record.Type == WALCommitTx
		if record.Type == WALCommitTx {
			// If we got this far - this transaction is committed properly
			lastValidRecordID = record.ID
		}
	}
}

// TrimAfter drops every record after specified
func (w *walWriter) TrimAfter(lastValidRecordID uint64) error {
	// Read WAL header
	_, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = util.ReadUint32(w.file)
	if err != nil {
		return err
	}

	// Scan all WAL records
	for {
		record, err := ReadWALRecord(w.file)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if record.ID >= lastValidRecordID {
			// Trim all records after current position
			position, err := w.file.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}

			err = w.file.Truncate(position)
			return err
		}
	}
}

// BeginTx starts a WAL transaction
func (w *walWriter) BeginTx() error {
	// Check if prev transaction is committed
	// If it is, return an error
	if w.isInTx {
		log.Errorf("BeginTx: already in transaction")
		return ErrAlreadyInTx
	}

	// Start new transaction
	w.txCounter++
	w.currentTxId = w.txCounter
	w.isInTx = true
	w.prevTxPosition = w.position

	log.Verbosef("BeginTx: txID=%d started, now at %d, rollback to %d", w.currentTxId, w.position, w.prevTxPosition)
	return nil
}

// CommitTx commits a WAL transaction
func (w *walWriter) CommitTx() error {
	// Check if prev transaction is not committed
	// If it is, return an error
	if !w.isInTx {
		log.Errorf("CommitTx: not in transaction")
		return ErrNotInTx
	}

	if w.prevTxPosition != w.position {

		// Write a WALCommitTx record
		record := &WALRecord{
			Type: WALCommitTx,
		}
		err := w.WriteImpl(record)
		if err != nil {
			return err
		}

		log.Verbosef("CommitTx: txID=%d committed, now at %d", w.currentTxId, w.position)
	} else {
		w.txCounter--
	}

	// Reset transaction state
	w.currentTxId = 0
	w.isInTx = false
	w.prevTxPosition = 0

	return nil
}

// RollbackTx rolls a WAL transaction back
func (w *walWriter) RollbackTx() error {
	// Check if prev transaction is not committed
	// If it is, return an error
	if !w.isInTx {
		log.Errorf("RollbackTx: not in transaction")
		return ErrNotInTx
	}

	if w.prevTxPosition != w.position {
		// Reset to prevTxPosition and truncate file
		// Trim all records after current position
		_, err := w.file.Seek(w.prevTxPosition, io.SeekStart)
		if err != nil {
			return err
		}

		log.Verbosef("RollbackTx: truncate from %d to %d", w.position, w.prevTxPosition)
		err = w.file.Truncate(w.prevTxPosition)
		if err != nil {
			return err
		}

		log.Verbosef("RollbackTx: txID=%d rolled back, now at %d", w.currentTxId, w.prevTxPosition)
	} else {
		w.txCounter--
	}

	// Reset transaction state
	w.currentTxId = 0
	w.isInTx = false
	w.position = w.prevTxPosition
	w.prevTxPosition = 0
	return nil
}

// Write writes a single record to a WAL file and sets its ID
func (w *walWriter) Write(record *WALRecord) error {
	// Check record type
	switch record.Type {
	case WALNone:
	case WALCommitTx:
		return fmt.Errorf("wal write error: invalid record type %d", record.Type)
	}

	// Check transaction state
	if !w.isInTx {
		return fmt.Errorf("wal write error: not in transaction")
	}

	return w.WriteImpl(record)
}

// WriteImpl writes a single record to a WAL file and sets its ID
func (w *walWriter) WriteImpl(record *WALRecord) error {
	// Initialize record fields
	w.idCounter++
	record.ID = w.idCounter
	record.TxID = w.currentTxId

	// Write a record
	length, err := WriteWALRecord(w.file, record)
	if err != nil {
		return err
	}

	// Remember new file position
	w.position += length
	log.Verbosef("Write: ID=%d, txID=%d, type=%d, %d bytes, now at %d", record.ID, record.TxID, record.Type, length, w.position)
	return nil
}

// Close shuts down WAL
func (w *walWriter) Close() error {
	err := w.file.Close()
	if err != nil {
		log.Errorf("unable to close wal file writer: %s", err)
		return err
	}
	log.Verbosef("WALWriter: closed")
	return nil
}

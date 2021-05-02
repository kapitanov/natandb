package storage

import (
	"fmt"
	"github.com/kapitanov/natandb/pkg/util"
	"io"
	"os"
)

const (
	// WALHeaderLength is a byte-length of WAL file header
	WALHeaderLength = 4
)

// ReadWALRecord reads a WALRecord from a file
func ReadWALRecord(f io.Reader) (*WALRecord, error) {
	record := &WALRecord{}
	var err error

	// Record ID
	record.ID, err = util.ReadUint64(f)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("failed to read wal record id: %s", err)
	}

	// Record transaction ID
	record.TxID, err = util.ReadUint64(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read wal record tx id: %s", err)
	}

	// Record type
	recordType, err := util.ReadUint8(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read wal record type: %s", err)
	}
	record.Type = recordType

	// "Key" field length
	keyLength, err := util.ReadUint32(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read wal record key length: %s", err)
	}

	// "Value" field length
	valueLength, err := util.ReadUint32(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read wal record value length: %s", err)
	}

	// "Key" field
	if keyLength > 0 {
		record.Key, err = util.ReadString(f, int(keyLength))
		if err != nil {
			return nil, fmt.Errorf("failed to read wal record key: %s", err)
		}
	} else {
		record.Key = ""
	}

	// "Value" field
	if valueLength > 0 {
		record.Value, err = util.ReadBytes(f, int(valueLength))
		if err != nil {
			return nil, fmt.Errorf("failed to read wal record value: %s", err)
		}
	} else {
		record.Value = make([]byte, 0)
	}

	log.Verbosef("got wal record ID=%d, TxID=%d, type=%d", record.ID, record.TxID, record.Type)
	return record, nil
}

// WriteWALRecord writes a WALRecord to a writer
func WriteWALRecord(f io.Writer, record *WALRecord) (int64, error) {
	var length int64 = 0
	// Record ID
	err := util.WriteUint64(f, record.ID)
	if err != nil {
		return 0, fmt.Errorf("failed to write wal record id: %s", err)
	}
	length += 8

	// Record transaction ID
	err = util.WriteUint64(f, record.TxID)
	if err != nil {
		return 0, fmt.Errorf("failed to write wal record tx id: %s", err)
	}
	length += 8

	// Record type
	err = util.WriteUint8(f, record.Type)
	if err != nil {
		return 0, fmt.Errorf("failed to write wal record type: %s", err)
	}
	length += 1

	// "Key" field length
	keyLength := uint32(len(record.Key))
	err = util.WriteUint32(f, keyLength)
	if err != nil {
		return 0, fmt.Errorf("failed to write wal record key length: %s", err)
	}
	length += 4

	// "Value" field length
	valueLength := uint32(0)
	if record.Value != nil {
		valueLength = uint32(len(record.Value))
	}
	err = util.WriteUint32(f, valueLength)
	if err != nil {
		return 0, fmt.Errorf("failed to write wal record value length: %s", err)
	}
	length += 4

	// "Key" field
	if keyLength > 0 {
		err = util.WriteString(f, record.Key)
		if err != nil {
			return 0, fmt.Errorf("failed to write wal record key: %s", err)
		}

		length += int64(keyLength)
	}

	// "Value" field
	if valueLength > 0 {
		err = util.WriteBytes(f, record.Value)
		if err != nil {
			return 0, fmt.Errorf("failed to write wal record value: %s", err)
		}

		length += int64(valueLength)
	}

	return length, nil
}

type walInitResult struct {
	IsEmpty  bool
	LastID   uint64
	LastTxID uint64
}

// walInit performs WAL file init and error correction routine
func walInit(file *os.File) (*walInitResult, error) {
	// Check if file is empty
	length, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	if length == 0 {
		log.Verbosef("wal file is empty")
		return walInitEmptyFile(file)
	} else {
		return walInitNonEmptyFile(file)
	}
}

// walInitEmptyFile performs a WAL init routine on an empty WAL file
func walInitEmptyFile(file *os.File) (*walInitResult, error) {
	// Write WAL header
	err := util.WriteUint32(file, WALVersion)
	if err != nil {
		return nil, err
	}

	return &walInitResult{
		IsEmpty:  true,
		LastID:   0,
		LastTxID: 0,
	}, nil
}

// walInitNonEmptyFile performs a WAL init routine on a non-empty WAL file
func walInitNonEmptyFile(file *os.File) (*walInitResult, error) {
	// Read WAL header
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	version, err := util.ReadUint32(file)
	if err != nil {
		return nil, err
	}
	if version != WALVersion {
		return nil, fmt.Errorf("wal file version v%d is not supported (expected v%d)", version, WALVersion)
	}

	// Scan all WAL records
	hasAnyRecords := false
	prevRecordWasCommitTx := false
	var idCounter uint64 = 0
	var lastValidRecordID uint64 = 0
	var txCounter uint64 = 0
	for {
		record, err := ReadWALRecord(file)
		if err != nil {
			if err == io.EOF {
				// End of WAL file reached
				// Last record must be a WALCommitTx
				if hasAnyRecords && !prevRecordWasCommitTx {
					log.Errorf("wal file is damaged: tx #%d was not committed properly (at #%d)", txCounter, idCounter)

					// Perform an automatic error correction (with inevitable data loss)
					err = walTrimAfter(file, lastValidRecordID)
					if err != nil {
						return nil, err
					}
				}

				// No errors occurred
				break
			}

			return nil, err
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
				err = walTrimAfter(file, lastValidRecordID)
				if err != nil {
					return nil, err
				}
				break
			}
			idCounter = record.ID

			// Check TxID
			// TxID change is only allowed if prev record was a WALCommitTx record
			if txCounter != record.TxID {
				if !prevRecordWasCommitTx {
					log.Errorf("wal file is damaged: tx #%d was not committed properly (at #%d)", txCounter, record.ID)

					// Perform an automatic error correction (with inevitable data loss)
					err = walTrimAfter(file, lastValidRecordID)
					if err != nil {
						return nil, err
					}
					break
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

	return &walInitResult{
		IsEmpty:  false,
		LastID:   lastValidRecordID,
		LastTxID: txCounter,
	}, nil
}

// walTrimAfter drops every record after specified
func walTrimAfter(file *os.File, lastValidRecordID uint64) error {
	// Read WAL header
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = util.ReadUint32(file)
	if err != nil {
		return err
	}

	// Scan all WAL records
	for {
		record, err := ReadWALRecord(file)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if record.ID >= lastValidRecordID {
			// Trim all records after current position
			position, err := file.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}

			log.Verbosef("wal truncated %d", position)
			err = file.Truncate(position)
			return err
		}
	}
}

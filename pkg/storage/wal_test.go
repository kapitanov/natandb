package storage_test

import (
	"github.com/kapitanov/natandb/pkg/util"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"

	l "github.com/kapitanov/natandb/pkg/log"
	"github.com/kapitanov/natandb/pkg/storage"
)

func TestEmptyLog(t *testing.T) {
	log.SetOutput(io.Discard)
	l.SetMinLevel(l.Verbose)

	dir, err := os.MkdirTemp(os.TempDir(), "*")
	if err != nil {
		t.Fatal(err)
	}

	driver, err := storage.NewDriver(storage.DirectoryOption(dir))
	if err != nil {
		t.Errorf("ERROR: NewDriver() failed: %s", err)
		return
	}

	err = readTx(t, driver, func(wal *walValidator) {
		wal.ExpectEOF()
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSingleItemTransaction(t *testing.T) {
	log.SetOutput(io.Discard)
	l.SetMinLevel(l.Verbose)

	dir, err := os.MkdirTemp(os.TempDir(), "*")
	if err != nil {
		t.Fatal(err)
	}

	driver, err := storage.NewDriver(storage.DirectoryOption(dir))
	if err != nil {
		t.Errorf("ERROR: NewDriver() failed: %s", err)
		return
	}

	const txCount = 10

	// Write single record to WAL file
	for txID := uint64(1); txID <= txCount; txID++ {
		err = writeTx(t, driver, 1)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Read WAL file from head
	err = readTx(t, driver, func(wal *walValidator) {
		id := uint64(1)
		for txID := uint64(1); txID <= txCount; txID++ {
			wal.Expect(id+0, txID, storage.WALAddValue)
			wal.Expect(id+1, txID, storage.WALCommitTx)
			id += 2
		}
		wal.ExpectEOF()
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMultiItemTransaction(t *testing.T) {
	log.SetOutput(io.Discard)
	l.SetMinLevel(l.Verbose)

	dir, err := os.MkdirTemp(os.TempDir(), "*")
	if err != nil {
		t.Fatal(err)
	}

	driver, err := storage.NewDriver(storage.DirectoryOption(dir))
	if err != nil {
		t.Errorf("ERROR: NewDriver() failed: %s", err)
		return
	}

	// Write multi-record to WAL file
	const txCount = 10
	for txID := uint64(1); txID <= txCount; txID++ {
		err = writeTx(t, driver, 4)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Read WAL file from head
	err = readTx(t, driver, func(wal *walValidator) {
		id := uint64(1)
		for txID := uint64(1); txID <= txCount; txID++ {
			wal.Expect(id+0, txID, storage.WALAddValue)
			wal.Expect(id+1, txID, storage.WALAddValue)
			wal.Expect(id+2, txID, storage.WALAddValue)
			wal.Expect(id+3, txID, storage.WALAddValue)
			wal.Expect(id+4, txID, storage.WALCommitTx)

			id += 5
		}
		wal.ExpectEOF()

	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestErrorCorrection tests reading of damaged WAL file:
//   WALHeader
//   WALRecord 1 (TxID=1, WALAddValue)
//   WALRecord 2 (TxID=1, WALCommitTx) - commits transaction #1
//   WALRecord 3 (TxID=2, WALAddValue) - transaction #2, incomplete
// Expected to get only 2 first record
// and 3rd record should be dropped as damaged
func TestErrorCorrection(t *testing.T) {
	log.SetOutput(io.Discard)
	l.SetMinLevel(l.Verbose)

	dir, err := os.MkdirTemp(os.TempDir(), "*")
	if err != nil {
		t.Fatal(err)
	}

	walFilePath := filepath.Join(dir, "journal.dat")
	snapshotFilePath := filepath.Join(dir, "snapshot.dat")

	f, err := os.OpenFile(walFilePath, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		t.Fatal(err)
	}

	// WAL header
	err = util.WriteUint32(f, storage.WALVersion)
	if err != nil {
		t.Fatal(err)
	}

	// Record 1
	record := &storage.WALRecord{ID: 1, TxID: 1, Type: storage.WALAddValue}
	_, err = storage.WriteWALRecord(f, record)
	if err != nil {
		t.Fatal(err)
	}

	// Record 2
	record.ID = 2
	record.Type = storage.WALCommitTx
	_, err = storage.WriteWALRecord(f, record)
	if err != nil {
		t.Fatal(err)
	}

	// Record 3
	record.ID = 3
	record.TxID = 2
	record.Type = storage.WALAddValue
	_, err = storage.WriteWALRecord(f, record)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	driver, err := storage.NewDriver(storage.WALFileOption(walFilePath), storage.SnapshotFileOption(snapshotFilePath))
	if err != nil {
		t.Errorf("ERROR: NewDriver() failed: %s", err)
		return
	}

	err = readTx(t, driver, func(wal *walValidator) {
		wal.Expect(1, 1, storage.WALAddValue)
		wal.Expect(2, 1, storage.WALCommitTx)
		wal.ExpectEOF()
	})
	if err != nil {
		t.Fatal(err)
	}
}

func writeTx(t *testing.T, driver storage.Driver, count int) error {
	writer, err := driver.WALFile().Write()
	if err != nil {
		t.Errorf("ERROR: WALFile().Write() failed: %s", err)
		return err
	}

	defer func() {
		_ = writer.Close()
	}()

	err = writer.BeginTx()
	if err != nil {
		t.Errorf("ERROR: BeginTx() failed: %s", err)
		return err
	}

	for i := 0; i < count; i++ {
		record := &storage.WALRecord{
			Type:  storage.WALAddValue,
			Key:   "foo/bar",
			Value: []byte("FooBar"),
		}
		err = writer.Write(record)
		if err != nil {
			t.Errorf("ERROR: WriteOne() failed: %s", err)
			return err
		}
	}

	err = writer.CommitTx()
	if err != nil {
		t.Errorf("ERROR: WALFile().CommitTx() failed: %s", err)
		return err
	}

	return nil
}

type walValidator struct {
	t      *testing.T
	reader storage.WALReader
}

func (v *walValidator) Expect(id, txID uint64, recordType storage.WALRecordType) *walValidator {
	record, err := v.reader.Read()
	if err != nil {
		v.t.Errorf("expected to read a record but got %s", err)
		v.t.Fail()
		return v
	}

	if record.ID != id {
		v.t.Errorf("expected record id %d but got %d", id, record.Type)
		v.t.Fail()
		return v
	}

	if record.TxID != txID {
		v.t.Errorf("expected record txID %d but got %d", recordType, record.TxID)
		v.t.Fail()
		return v
	}

	if record.Type != recordType {
		v.t.Errorf("expected record type %d but got %d", recordType, record.Type)
		v.t.Fail()
		return v
	}

	return v
}

func (v *walValidator) ExpectEOF() *walValidator {
	_, err := v.reader.Read()
	if err != io.EOF {
		v.t.Errorf("expected EOF but got %s", err)
		v.t.Fail()
		return v
	}

	return v
}

func readTx(t *testing.T, driver storage.Driver, check func(v *walValidator)) error {
	reader, err := driver.WALFile().Read()
	if err != nil {
		t.Errorf("ERROR: WALFile().Read() failed: %s", err)
		t.Fail()
		return err
	}

	defer func() {
		_ = reader.Close()
	}()

	check(&walValidator{t, reader})
	return nil
}

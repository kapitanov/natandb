package wal_test

import (
	"bytes"
	"encoding/base64"
	"testing"

	. "github.com/kapitanov/natandb/pkg/wal"
)

func TestNoneRecord(t *testing.T) {
	record := &Record{
		ID:    123456780,
		Type:  None,
		Key:   "",
		Value: nil,
	}

	testWriteAndRead(t, record)
}

func TestAddValueRecord(t *testing.T) {
	record := &Record{
		ID:    123456780,
		Type:  AddValue,
		Key:   "foo/bar",
		Value: []byte("FooBar"),
	}

	testWriteAndRead(t, record)
}

func TestRemoveValueRecord(t *testing.T) {
	record := &Record{
		ID:    123456780,
		Type:  AddValue,
		Key:   "foo/bar",
		Value: []byte("FooBar"),
	}

	testWriteAndRead(t, record)
}

func TestRemoveKeyRecord(t *testing.T) {
	record := &Record{
		ID:    123456780,
		Type:  RemoveKey,
		Key:   "foo/bar",
		Value: nil,
	}

	testWriteAndRead(t, record)
}

func TestBrokenRecord(t *testing.T) {
	record := &Record{
		ID:    123456780,
		Type:  RemoveKey,
		Key:   "",
		Value: []byte("FooBar"),
	}

	testWriteAndRead(t, record)
}

func testWriteAndRead(t *testing.T, record *Record) {
	serializer := NewSerializer()

	var outputBuffer bytes.Buffer
	err := serializer.Serialize(record, &outputBuffer)
	if err != nil {
		t.Errorf("serializer.Serialize() failed: %s", err)
		return
	}

	buffer := outputBuffer.Bytes()
	expectedLength := serializer.CalcBinaryLength(record)
	if len(buffer) != int(expectedLength) {
		t.Errorf("serializer.Serialize(): wrong buffer size (%d != %d)", len(buffer), expectedLength)
		return
	}

	intputBuffer := bytes.NewBuffer(buffer)
	deserializedRecord, err := serializer.Deserialize(intputBuffer)
	if err != nil {
		t.Errorf("serializer.Deserialize() failed: %s <%s> ", err, base64.StdEncoding.EncodeToString(buffer))
		return
	}

	if !record.Equals(deserializedRecord) {
		t.Errorf("record content mismatch: %s != %s", record, deserializedRecord)
	}
}

package wal

// WAL binary format:
//
// +---+---------+---------+----------------------+
// | # | Length  | Group   | Field                |
// +---+---------+---------+----------------------+
// | 1 | 8 bytes | Header  | Record ID            |
// | 2 | 1 byte  | Header  | Record type          |
// | 3 | 4 bytes | Header  | "Key" field length   |
// | 4 | 4 bytes | Header  | "Value" field length |
// | 5 | N bytes | Payload | "Key" field          |
// | 6 | N bytes | Payload | "Value" field        |
// | 7 | 4 bytes | Trailer | Record length        |
// +---+---------+---------+----------------------+

import (
	"encoding/binary"
	"fmt"
	"io"
)

type serializerImpl struct {
	writer *binaryWriter
	reader *binaryReader
}

const (
	walRecordHeaderLength  = 8 + 1 + 4 + 4
	walRecordTrailerLength = 4
)

// NewSerializer creates new WAL record serializer
func NewSerializer() Serializer {
	return &serializerImpl{
		writer: &binaryWriter{
			buffer:    make([]byte, 8),
			byteOrder: binary.LittleEndian,
		},
		reader: &binaryReader{
			buffer:    make([]byte, 8),
			byteOrder: binary.LittleEndian,
		},
	}
}

type binaryWriter struct {
	buffer    []byte
	byteOrder binary.ByteOrder
}

func (writer *binaryWriter) WriteUint64(w io.Writer, value uint64) error {
	writer.byteOrder.PutUint64(writer.buffer, value)
	return writer.write(w, writer.buffer[0:8])
}

func (writer *binaryWriter) WriteUint32(w io.Writer, value uint32) error {
	writer.byteOrder.PutUint32(writer.buffer, value)
	return writer.write(w, writer.buffer[0:4])
}

func (writer *binaryWriter) WriteUint8(w io.Writer, value uint8) error {
	writer.buffer[0] = byte(value)
	return writer.write(w, writer.buffer[0:1])
}

func (writer *binaryWriter) WriteString(w io.Writer, value string) error {
	return writer.write(w, []byte(value))
}

func (writer *binaryWriter) WriteBytes(w io.Writer, value []byte) error {
	return writer.write(w, value)
}

func (writer *binaryWriter) write(w io.Writer, buffer []byte) error {
	n, err := w.Write(buffer)
	if err == nil && n < len(buffer) {
		err = fmt.Errorf("not enough data has been written (expected %d, got %d)", len(buffer), n)
	}

	return err
}

type binaryReader struct {
	buffer    []byte
	byteOrder binary.ByteOrder
}

func (reader *binaryReader) ReadUint64(r io.Reader) (uint64, error) {
	buffer := reader.buffer[0:8]
	err := reader.read(r, buffer)
	if err != nil {
		return 0, err
	}

	value := reader.byteOrder.Uint64(buffer)
	return value, nil
}

func (reader *binaryReader) ReadUint32(r io.Reader) (uint32, error) {
	buffer := reader.buffer[0:4]
	err := reader.read(r, buffer)
	if err != nil {
		return 0, err
	}

	value := reader.byteOrder.Uint32(buffer)
	return value, nil
}

func (reader *binaryReader) ReadUint8(r io.Reader) (uint8, error) {
	buffer := reader.buffer[0:1]
	err := reader.read(r, buffer)
	if err != nil {
		return 0, err
	}

	value := uint8(buffer[0])
	return value, nil
}

func (reader *binaryReader) ReadString(r io.Reader, len int) (string, error) {
	buffer := make([]byte, len)
	err := reader.read(r, buffer)
	if err != nil {
		return "", err
	}
	return string(buffer), nil
}

func (reader *binaryReader) ReadBytes(r io.Reader, len int) ([]byte, error) {
	buffer := make([]byte, len)
	err := reader.read(r, buffer)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func (reader *binaryReader) read(r io.Reader, buffer []byte) error {
	n, err := r.Read(buffer)
	if err == nil && n < len(buffer) {
		err = fmt.Errorf("not enough data has been read (expected %d, got %d)", len(buffer), n)
	}

	return err
}

// Serialize writes a WAL record into its binary representation
func (s *serializerImpl) Serialize(record *Record, w io.Writer) error {
	// Record ID
	err := s.writer.WriteUint64(w, record.ID)
	if err != nil {
		return fmt.Errorf("failed to write wal record id: %s", err)
	}

	// Record type
	err = s.writer.WriteUint8(w, uint8(record.Type))
	if err != nil {
		return fmt.Errorf("failed to write wal record type: %s", err)
	}

	// "Key" field length
	err = s.writer.WriteUint32(w, uint32(len([]byte(record.Key))))
	if err != nil {
		return fmt.Errorf("failed to write wal record key length: %s", err)
	}

	// "Value" field length
	if record.Value != nil {
		err = s.writer.WriteUint32(w, uint32(len(record.Value)))
	} else {
		err = s.writer.WriteUint32(w, 0)
	}
	if err != nil {
		return fmt.Errorf("failed to write wal record value length: %s", err)
	}

	// "Key" field
	err = s.writer.WriteString(w, record.Key)
	if err != nil {
		return fmt.Errorf("failed to write wal record key: %s", err)
	}

	// "Value" field
	if record.Value != nil {
		err = s.writer.WriteBytes(w, record.Value)
		if err != nil {
			return fmt.Errorf("failed to write wal record value: %s", err)
		}
	}

	// "Length" field
	length := walRecordHeaderLength + walRecordTrailerLength + len(record.Key)
	if record.Value != nil {
		length += len(record.Value)
	}
	err = s.writer.WriteUint32(w, uint32(length))
	if err != nil {
		return fmt.Errorf("failed to write wal record length: %s", err)
	}

	return nil
}

// Deserialize reads a WAL record from its binary representation
func (s *serializerImpl) Deserialize(r io.Reader) (*Record, error) {
	record := Record{}
	var err error

	// Record ID
	record.ID, err = s.reader.ReadUint64(r)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("failed to read wal record id: %s", err)
	}

	// Record type
	recordType, err := s.reader.ReadUint8(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read wal record type: %s", err)
	}
	record.Type = RecordType(recordType)

	// "Key" field length
	keyLength, err := s.reader.ReadUint32(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read wal record key length: %s", err)
	}

	// "Value" field length
	valueLength, err := s.reader.ReadUint32(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read wal record value length: %s", err)
	}

	// "Key" field
	if keyLength > 0 {
		record.Key, err = s.reader.ReadString(r, int(keyLength))
		if err != nil {
			return nil, fmt.Errorf("failed to read wal record key: %s", err)
		}
	} else {
		record.Key = ""
	}

	// "Value" field
	if valueLength > 0 {
		record.Value, err = s.reader.ReadBytes(r, int(valueLength))
		if err != nil {
			return nil, fmt.Errorf("failed to read wal record value: %s", err)
		}
	} else {
		record.Value = make([]byte, 0)
	}

	// "Length" field
	length, err := s.reader.ReadUint32(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read wal record length: %s", err)
	}

	calculatedLength := walRecordHeaderLength + walRecordTrailerLength + len(record.Key)
	if record.Value != nil {
		calculatedLength += len(record.Value)
	}

	if uint32(calculatedLength) != length {
		return nil, fmt.Errorf("wal record length mismatch: %d != %d", calculatedLength, length)
	}

	return &record, nil
}

// CalcBinaryLength calculates how many bytes will take binary representation of a record
func (s *serializerImpl) CalcBinaryLength(record *Record) int64 {
	n := walRecordHeaderLength + walRecordTrailerLength
	n += len([]byte(record.Key))
	if record.Value != nil {
		n += len(record.Value)
	}

	return int64(n)
}

// GetRecordMetadataForward reads record metadata from binary stream and seeks back to original position
// GetRecordMetadataForward assumes that input stream is at record's start
func (s *serializerImpl) GetRecordMetadataForward(r io.ReadSeeker, metadata *RecordMetadata) error {
	// Read metadata
	err := s.getRecordMetadata(r, metadata)
	if err != nil {
		return err
	}

	// Seek back to original position
	_, err = r.Seek(-walRecordHeaderLength, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to seek to original position: %s", err)
	}

	return nil
}

// GetRecordMetadataBackward reads record metadata from binary stream and seeks back to original position
// GetRecordMetadataBackward assumes that input stream is at record's end
func (s *serializerImpl) GetRecordMetadataBackward(r io.ReadSeeker, metadata *RecordMetadata) error {
	// Seek backward to trailer start
	_, err := r.Seek(-walRecordTrailerLength, io.SeekCurrent)
	if err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("failed to seek to trailer start: %s", err)
	}

	// Read record length
	length, err := s.reader.ReadUint32(r)
	if err != nil {
		return fmt.Errorf("failed to read wal record length: %s", err)
	}

	// Seek to record start
	_, err = r.Seek(-int64(length), io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to seek to record start: %s", err)
	}

	// Read metadata
	err = s.getRecordMetadata(r, metadata)
	if err != nil {
		return err
	}

	// Check record length
	if metadata.Length != length {
		return fmt.Errorf("wal record length mismatch: %d != %d", metadata.Length, length)
	}

	// Seek back to original position
	offset := metadata.Length - walRecordHeaderLength
	_, err = r.Seek(int64(offset), io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to seek to original position: %s", err)
	}

	return nil
}

func (s *serializerImpl) getRecordMetadata(r io.ReadSeeker, metadata *RecordMetadata) error {
	var err error

	// Record ID
	metadata.ID, err = s.reader.ReadUint64(r)
	if err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("failed to read wal record id: %s", err)
	}

	// Record type (skip)
	_, err = r.Seek(1, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to seek: %s", err)
	}

	// "Key" field length
	keyLength, err := s.reader.ReadUint32(r)
	if err != nil {
		return fmt.Errorf("failed to read wal record key length: %s", err)
	}

	// "Value" field
	valueLength, err := s.reader.ReadUint32(r)
	if err != nil {
		return fmt.Errorf("failed to read wal record value length: %s", err)
	}

	// Calculate record length
	metadata.Length = uint32(walRecordHeaderLength) + uint32(walRecordTrailerLength) + keyLength + valueLength

	return nil
}

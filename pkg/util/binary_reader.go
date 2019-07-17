package util

import (
	"encoding/binary"
	"fmt"
	"io"
)

var (
	readerBuffer    = make([]byte, 8)
	readerByteOrder = binary.LittleEndian
)

// ReadUint64 reads an uint64 binary value from a stream
func ReadUint64(r io.Reader) (uint64, error) {
	buffer := readerBuffer[0:8]
	err := readBuffer(r, buffer)
	if err != nil {
		return 0, err
	}

	value := readerByteOrder.Uint64(buffer)
	return value, nil
}

// ReadUint32 reads an uint32 binary value from a stream
func ReadUint32(r io.Reader) (uint32, error) {
	buffer := readerBuffer[0:4]
	err := readBuffer(r, buffer)
	if err != nil {
		return 0, err
	}

	value := readerByteOrder.Uint32(buffer)
	return value, nil
}

// ReadUint8 reads an uint8 binary value from a stream
func ReadUint8(r io.Reader) (uint8, error) {
	buffer := readerBuffer[0:1]
	err := readBuffer(r, buffer)
	if err != nil {
		return 0, err
	}

	value := uint8(buffer[0])
	return value, nil
}

// ReadString reads an UTF-8 string from a stream
func ReadString(r io.Reader, len int) (string, error) {
	buffer := make([]byte, len)
	err := readBuffer(r, buffer)
	if err != nil {
		return "", err
	}
	return string(buffer), nil
}

// ReadBytes reads a byte array string from a stream
func ReadBytes(r io.Reader, len int) ([]byte, error) {
	buffer := make([]byte, len)
	err := readBuffer(r, buffer)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func readBuffer(r io.Reader, buffer []byte) error {
	n, err := r.Read(buffer)
	if err == nil && n < len(buffer) {
		err = fmt.Errorf("not enough data has been read (expected %d, got %d)", len(buffer), n)
	}

	return err
}

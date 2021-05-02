package util

import (
	"encoding/binary"
	"fmt"
	"io"
)

var (
	writerBuffer    = make([]byte, 8)
	writerByteOrder = binary.LittleEndian
)

// WriteUint64 writes an uint64 value into a stream
func WriteUint64(w io.Writer, value uint64) error {
	writerByteOrder.PutUint64(writerBuffer, value)
	return writeBuffer(w, writerBuffer[0:8])
}

// WriteUint32 writes an uint32 value into a stream
func WriteUint32(w io.Writer, value uint32) error {
	writerByteOrder.PutUint32(writerBuffer, value)
	return writeBuffer(w, writerBuffer[0:4])
}

// WriteUint16 writes an uint32 value into a stream
func WriteUint16(w io.Writer, value uint16) error {
	writerByteOrder.PutUint16(writerBuffer, value)
	return writeBuffer(w, writerBuffer[0:2])
}

// WriteUint8 writes an uint8 value into a stream
func WriteUint8(w io.Writer, value uint8) error {
	writerBuffer[0] = value
	return writeBuffer(w, writerBuffer[0:1])
}

// WriteString writes an UTF-8 string into a stream
func WriteString(w io.Writer, value string) error {
	return writeBuffer(w, []byte(value))
}

// WriteBytes writes a byte array into a stream
func WriteBytes(w io.Writer, value []byte) error {
	return writeBuffer(w, value)
}

func writeBuffer(w io.Writer, buffer []byte) error {
	n, err := w.Write(buffer)
	if err == nil && n < len(buffer) {
		err = fmt.Errorf("not enough data has been written (expected %d, got %d)", len(buffer), n)
	}

	return err
}

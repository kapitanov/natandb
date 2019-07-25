package model

import (
	"fmt"

	"github.com/kapitanov/natandb/pkg/writeahead"
)

// Value is a node value
type Value []byte

// Equal compares two values
func (v Value) Equal(other Value) bool {
	if len(v) != len(other) {
		return false
	}
	for i := range v {
		if v[i] != other[i] {
			return false
		}
	}

	return true
}

// Node describes a single node model
type Node struct {
	// Node key
	Key string
	// ID of last change applied to node
	LastChangeID uint64
	// Node values
	Values []Value
}

func (n *Node) String() string {
	return fmt.Sprintf("{ $%d, \"%s\" %s }", n.LastChangeID, n.Key, n.Values)
}

// Contains returns true if node contains specified value
func (n *Node) Contains(value Value) bool {
	for i := range n.Values {
		if n.Values[i].Equal(value) {
			return true
		}
	}

	return false
}

// Apply applied a write-ahead log record to a data model node
func (n *Node) apply(record *writeahead.Record) error {
	if record.ID <= n.LastChangeID {
		return nil
	}

	switch record.Type {
	case writeahead.None:
		break
	case writeahead.RemoveKey:
		n.Values = n.Values[0:0]
		break
	case writeahead.AddValue:
		n.Values = append(n.Values, record.Value)
		break
	case writeahead.RemoveValue:
		n.removeValue(record.Value)
		break
	default:
		return fmt.Errorf("unknown wal record type: %d", record.Type)
	}

	n.LastChangeID = record.ID
	return nil
}

// removeValue removes a value from a node
func (n *Node) removeValue(value Value) bool {
	values := n.Values

	// Scan value array forward
	for i := range values {
		// If a matching value is found
		if values[i].Equal(value) {
			// Shift value array, overwritting (and thus removing) i-th element
			for j := i; j < len(values)-1; j++ {
				values[j] = values[j+1]
			}

			// Trim an array, removing last element
			values = values[0 : len(values)-1]
			n.Values = values
			return true
		}
	}

	return false
}

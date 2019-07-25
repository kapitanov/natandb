package db

import (
	"fmt"

	"github.com/kapitanov/natandb/pkg/model"
)

// Key is NatanDB node key type
type Key string

// Value is a NatanDB node value
type Value = model.Value

// Node is a NatanDB node snapshot
type Node struct {
	// Node key
	Key Key

	// Node version
	Version uint64

	// Node value
	Values []Value
}

func (n *Node) String() string {
	return fmt.Sprintf("{ key: \"%s\", value: %s, version: %d }", n.Key, n.Values, n.Version)
}

// PagedNodeList is a paged list of node values
type PagedNodeList struct {
	// Array of nodes
	Nodes []*Node

	// Current DB version
	Version uint64

	// Total count of nodes
	TotalCount uint
}

func (n *PagedNodeList) String() string {
	return fmt.Sprintf("{ nodes: [%d], total_count: %d, version: %d }", len(n.Nodes), n.TotalCount, n.Version)
}

// Error is a lightweight error type
type Error string

func (e Error) Error() string {
	return string(e)
}

func (e Error) String() string {
	return string(e)
}

const (
	// ErrNoSuchKey is returned when a non-existing key is requested
	ErrNoSuchKey = Error("no such key")

	// ErrDuplicateValue is returned when trying to add a conflicting value to a node
	ErrDuplicateValue = Error("duplicate value")

	// ErrNoSuchValue is returned when trying to remove a non-existing value from a node
	ErrNoSuchValue = Error("no such value")

	// ErrDataOutOfDate is returned when a data change detected while iterating through nodes
	ErrDataOutOfDate = Error("out of date")
)

// Engine is a public interface for NatanDB engine
type Engine interface {
	// List returns paged list of DB keys (with values)
	// Optionally list might be filtered by key prefix
	// If data version is changed, a ErrDataOutOfDate error is returned
	// ErrDataOutOfDate is not returned if version parameter contains zero
	List(prefix Key, skip uint, limit uint, version uint64) (*PagedNodeList, error)

	// GetVersion retunrs current data version
	GetVersion() uint64

	// Get gets a node value by its key
	// If specified node doesn't exist, a ErrNoSuchKey error is returned
	Get(key Key) (*Node, error)

	// Set sets a node value, rewritting its value if node already exists
	// If specified node doesn't exists, it will be created
	Set(key Key, values []Value) (*Node, error)

	// AddValue defines an "append value" operation
	// If specified node doesn't exists, it will be created
	// A specified value will be added to node even if it already exists
	AddValue(key Key, value Value) (*Node, error)

	// AddUniqueValue defines an "append value" operation
	// If specified node doesn't exists, it will be created
	// If node already contains the same value and "unique" parameter is set to "true", a ErrDuplicateValue error is returned
	AddUniqueValue(key Key, value Value) (*Node, error)

	// RemoveValue defines an "remove value" operation
	// If specified node doesn't exist, a ErrNoSuchKey error is returned
	// If specified value doesn't exist within a node, a ErrNoSuchValue error is returned
	RemoveValue(key Key, value Value) (*Node, error)

	// RemoveAllValues defines an "remove value" operation
	// If specified node doesn't exist, a ErrNoSuchKey error is returned
	// If node contains specified value multiple times, all values are removed
	// If specified value doesn't exist within a node, a ErrNoSuchValue error is returned
	RemoveAllValues(key Key, value Value) (*Node, error)

	// Drop removes a key completely
	// If specified node doesn't exist, a ErrNoSuchKey error is returned
	Drop(key Key) error

	// Close shuts engine down gracefully
	Close() error
}

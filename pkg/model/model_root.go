package model

import (
	"fmt"
	"sort"

	"github.com/kapitanov/natandb/pkg/writeahead"
)

// Error is a lightweight error type
type Error string

func (e Error) Error() string {
	return string(e)
}

func (e Error) String() string {
	return string(e)
}

const (
	// ErrChangeAlreadyApplied is returned when a write-ahead record has been applied to a model already
	ErrChangeAlreadyApplied = Error("change already applied")
)

// Root describes data model root
type Root struct {
	// ID of last change applied to model
	LastChangeID uint64
	// Map of nodes
	NodesMap map[string]*Node
}

// New creates new instance of Root
func New() *Root {
	model := &Root{
		LastChangeID: 0,
		NodesMap:     make(map[string]*Node),
	}
	return model
}

// Keys returns all node keys
func (m *Root) Keys() []string {
	keys := make([]string, len(m.NodesMap))

	i := 0
	for key := range m.NodesMap {
		keys[i] = key
		i++
	}

	sort.Strings(keys)

	return keys
}

// GetNode returns a node by its key if exists
func (m *Root) GetNode(key string) *Node {
	node, exists := m.NodesMap[key]
	if !exists {
		return nil
	}

	return node
}

// GetOrCreateNode returns a node by its key if exists, creates a new node otherwise
func (m *Root) GetOrCreateNode(key string) *Node {
	node, exists := m.NodesMap[key]
	if !exists {
		node = &Node{
			Key:          key,
			LastChangeID: 0,
			Values:       make([]Value, 0),
		}
		m.NodesMap[key] = node

		return node
	}

	return node
}

// replayWriteAheadLog syncs data model with write-ahead log
func (m *Root) replayWriteAheadLog(log writeahead.Log) error {
	minID := m.LastChangeID
	chunkSize := 1000

	for {
		chunk, err := log.ReadChunkForward(minID+1, chunkSize)
		if err != nil {
			return err
		}

		if chunk.Empty() {
			break
		}

		for _, record := range chunk {
			if minID < record.ID {
				minID = record.ID
			}

			err = m.Apply(record)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Apply applied a write-ahead log record to a data model
func (m *Root) Apply(record *writeahead.Record) error {
	if record.ID <= m.LastChangeID {
		return ErrChangeAlreadyApplied
	}

	if record.Key != "" {
		switch record.Type {
		case writeahead.None:
			break

		case writeahead.AddValue:
			node := m.GetOrCreateNode(record.Key)
			err := node.apply(record)
			if err != nil {
				return err
			}
			break

		case writeahead.RemoveValue:
			node := m.GetNode(record.Key)
			if node != nil {
				err := node.apply(record)
				if err != nil {
					return err
				}
			}
			break

		case writeahead.RemoveKey:
			node := m.GetNode(record.Key)
			if node != nil {
				err := node.apply(record)
				if err != nil {
					return err
				}

				delete(m.NodesMap, record.Key)
			}

			break

		default:
			return fmt.Errorf("unknown wal record type: %d", record.Type)
		}
	}

	m.LastChangeID = record.ID
	return nil
}

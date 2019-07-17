package model

import (
	"fmt"
	"sort"

	"github.com/kapitanov/natandb/pkg/writeahead"
)

// Root describes data model root
type Root struct {
	// ID of last change applied to model
	lastChangeID uint64
	// Map of nodes
	nodes map[string]*Node
}

// New creates new instance of Root
func New() *Root {
	model := &Root{
		lastChangeID: 0,
		nodes:        make(map[string]*Node),
	}
	return model
}

// LastChangeID returns node last applied change ID
func (m *Root) LastChangeID() uint64 {
	return m.lastChangeID
}

// Keys returns all node keys
func (m *Root) Keys() []string {
	keys := make([]string, len(m.nodes))

	i := 0
	for key := range m.nodes {
		keys[i] = key
		i++
	}

	sort.Strings(keys)

	return keys
}

// GetNode returns a node by its key if exists
func (m *Root) GetNode(key string) *Node {
	node, exists := m.nodes[key]
	if !exists {
		return nil
	}

	return node
}

// GetOrCreateNode returns a node by its key if exists, creates a new node otherwise
func (m *Root) GetOrCreateNode(key string) *Node {
	node, exists := m.nodes[key]
	if !exists {
		node = &Node{
			key:          key,
			lastChangeID: 0,
			values:       make([]Value, 0),
		}
		m.nodes[key] = node

		return node
	}

	return node
}

// replayWriteAheadLog syncs data model with write-ahead log
func (m *Root) replayWriteAheadLog(log writeahead.Log) error {
	minID := m.lastChangeID
	chunkSize := 1000

	for {
		chunk, err := log.ReadChunkForward(minID, chunkSize)
		if err != nil {
			return err
		}

		if chunk.Empty() {
			break
		}

		for _, record := range chunk {
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
	if record.ID <= m.lastChangeID {
		return nil
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
			delete(m.nodes, record.Key)
			break

		default:
			return fmt.Errorf("unknown wal record type: %d", record.Type)
		}
	}

	m.lastChangeID = record.ID
	return nil
}

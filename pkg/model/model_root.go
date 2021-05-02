package model

import (
	"fmt"
	"io"
	"sort"

	l "github.com/kapitanov/natandb/pkg/log"
	"github.com/kapitanov/natandb/pkg/storage"
)

// Error is a lightweight error type
type Error string

func (e Error) Error() string {
	return string(e)
}

func (e Error) String() string {
	return string(e)
}

var log = l.New("model")

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
func (m *Root) replayWriteAheadLog(wal storage.WALReader) error {
	minID := m.LastChangeID

	for {
		record, err := wal.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if minID < record.ID {
			minID = record.ID

			err = m.Apply(record)
			if err != nil {
				return err
			}
		}
	}

	log.Verbosef("replayed journal [%d..%d]", minID, m.LastChangeID)
	return nil
}

// Apply applied a write-ahead log record to a data model
func (m *Root) Apply(record *storage.WALRecord) error {
	if record.ID <= m.LastChangeID {
		log.Errorf("change #%d is already applied to model", record.ID)
		return ErrChangeAlreadyApplied
	}

	if record.Key != "" {
		switch record.Type {
		case storage.WALNone:
			if log.IsEnabled(l.Verbose) {
				log.Verbosef("empty wal record: #%d", record.ID)
			}
			break

		case storage.WALCommitTx:
			break

		case storage.WALAddValue:
			node := m.GetOrCreateNode(record.Key)
			err := node.apply(record)
			if err != nil {
				return err
			}
			break

		case storage.WALRemoveValue:
			node := m.GetNode(record.Key)
			if node != nil {
				err := node.apply(record)
				if err != nil {
					return err
				}
			} else {
				if log.IsEnabled(l.Verbose) {
					log.Verbosef("node \"%s\" is not found while applying wal record: #%d", record.Key, record.ID)
				}
			}
			break

		case storage.WALRemoveKey:
			node := m.GetNode(record.Key)
			if node != nil {
				err := node.apply(record)
				if err != nil {
					return err
				}

				delete(m.NodesMap, record.Key)
			} else {
				if log.IsEnabled(l.Verbose) {
					log.Verbosef("node \"%s\" is not found while applying wal record: #%d", record.Key, record.ID)
				}
			}
			break

		default:
			log.Errorf("unknown wal record type: %d", record.Type)
			return fmt.Errorf("unknown wal record type: %d", record.Type)
		}
	}

	if record.Type != storage.WALCommitTx {
		m.LastChangeID = record.ID
	}

	if log.IsEnabled(l.Verbose) {
		log.Verbosef("applied wal record #%d", record.ID)
	}

	return nil
}

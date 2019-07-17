package model

import (
	"fmt"
	"io"

	"github.com/kapitanov/natandb/pkg/storage"
	"github.com/kapitanov/natandb/pkg/util"
	"github.com/kapitanov/natandb/pkg/writeahead"
)

// Model binary format:
//
// +---+----------+----------------+
// | # | Length   | Field          |
// +---+----------+----------------+
// | 1 | 4 bytes  | Schema version |
// | 2 | variable | Nodes[0]       |
// | 3 | variable | Nodes[1]       |
// |   | ...      | ...            |
// | N | variable | Nodes[N-1]     |
// +---+----------+----------------+
//
// Where each node has the following format:
//
// +-----+---------+-----------------------+
// | #   | Length  | Field                 |
// +-----+---------+-----------------------+
// | 1   | 8 bytes | Node last change ID   |
// | 2   | 4 bytes | len(Node.Key)         |
// | 3   | N bytes | Node.Key              |
// | 4   | 4 bytes | len(Node.Values)      |
// | 5   | 4 bytes | len(Node.Values[0])   |
// | 6   | N bytes | Node.Values[0]        |
// | 7   | 4 bytes | len(Node.Values[1])   |
// | 8   | N bytes | Node.Values[1]        |
// |     | ...     | ...                   |
// | N-1 | 4 bytes | len(Node.Values[N-1]) |
// | N   | N bytes | Node.Values[N-1]      |
// +-----+---------+-----------------------+

const (
	schemaVersion uint32 = 1
)

// Restore restores a data model from persistent storage and syncs it with WAL log
func Restore(log writeahead.Log, snapshot storage.SnapshotFile) (*Root, error) {
	// Load a snapshot from a persistent storage
	file, err := snapshot.Read()
	if err != nil {
		return nil, err
	}

	model, err := ReadSnapshot(file)
	if file != nil {
		file.Close()
	}
	if err != nil {
		return nil, err
	}

	// Then replay write-ahead log to restore model's actual state
	lastChangeID := model.lastChangeID
	err = model.replayWriteAheadLog(log)
	if err != nil {
		return nil, err
	}

	// If model stage was not in sync with write-ahead log,
	// then new model snapshot should be created
	if lastChangeID != model.lastChangeID {
		file, err := snapshot.Write()
		if err != nil && err != io.EOF {
			return nil, err
		}

		err = model.WriteSnapshot(file)
		if err != nil {
			file.Close()
			return nil, err
		}

		err = file.Close()
		if err != nil {
			return nil, err
		}
	}

	return model, nil
}

// ReadSnapshot restores model snapshot from its binary form
func ReadSnapshot(file io.Reader) (*Root, error) {
	model := New()

	if file != nil {
		version, err := util.ReadUint32(file)
		if err != nil {
			return nil, err
		}

		if version != schemaVersion {
			return nil, fmt.Errorf("incompatible schema: #%d", version)
		}

		for {
			node, err := readNodeFromSnapshot(file)
			if err != nil {
				if err == io.EOF {
					return model, nil
				}

				return nil, err
			}

			existingNode := model.GetNode(node.key)
			if existingNode != nil {
				return nil, fmt.Errorf("malformed snapshot: duplicate key \"%s\"", node.key)
			}

			model.nodes[node.key] = node
			if model.lastChangeID < node.lastChangeID {
				model.lastChangeID = node.lastChangeID
			}
		}
	}

	return model, nil
}

// WriteSnapshot writes model snapshot into its binary form
func (m *Root) WriteSnapshot(file io.Writer) error {
	err := util.WriteUint32(file, schemaVersion)
	if err != nil {
		return err
	}

	for _, node := range m.nodes {
		err = node.writeSnapshot(file)
		if err != nil {
			return err
		}
	}
	return nil
}

// readNodeFromSnapshot restores a model node from its binary form
func readNodeFromSnapshot(file io.Reader) (*Node, error) {
	// Node last change ID
	lastChangeID, err := util.ReadUint64(file)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("failed to read node snapshot lcid: %s", err)
	}

	// Key length
	keyLength, err := util.ReadUint32(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read node snapshot key lenght: %s", err)
	}

	// Key value
	key, err := util.ReadString(file, int(keyLength))
	if err != nil {
		return nil, fmt.Errorf("failed to read node snapshot lcid: %s", err)
	}

	// Value array length
	valueCount, err := util.ReadUint32(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read node snapshot value count: %s", err)
	}

	// Value array
	values := make([]Value, valueCount)
	for i := 0; i < int(valueCount); i++ {
		// Value[i] length
		valueLength, err := util.ReadUint32(file)
		if err != nil {
			return nil, fmt.Errorf("failed to read node snapshot %d-th value lenght: %s", i, err)
		}

		// Value[i] value
		value, err := util.ReadBytes(file, int(valueLength))
		if err != nil {
			return nil, fmt.Errorf("failed to read node snapshot %d-th value: %s", i, err)
		}

		values[i] = Value(value)
	}

	node := &Node{
		key:          key,
		lastChangeID: lastChangeID,
		values:       values,
	}
	return node, nil
}

// writeSnapshot writes model node snapshot into its binary form
func (n *Node) writeSnapshot(file io.Writer) error {
	// Node last change ID
	err := util.WriteUint64(file, n.lastChangeID)
	if err != nil {
		return err
	}

	// Key length
	err = util.WriteUint32(file, uint32(len(n.key)))
	if err != nil {
		return err
	}

	// Key value
	err = util.WriteString(file, n.key)
	if err != nil {
		return err
	}

	// Value array length
	err = util.WriteUint32(file, uint32(len(n.values)))
	if err != nil {
		return err
	}

	// Value array
	for _, value := range n.values {
		// Value[i] length
		err = util.WriteUint32(file, uint32(len(value)))
		if err != nil {
			return err
		}

		// Value[i] value
		err = util.WriteBytes(file, value)
		if err != nil {
			return err
		}
	}

	return nil
}

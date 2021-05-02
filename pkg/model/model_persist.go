package model

import (
	"fmt"
	"io"

	"github.com/kapitanov/natandb/pkg/storage"
	"github.com/kapitanov/natandb/pkg/util"
)

// Model binary format:
//
// +---+----------+------------------------+
// | # | Length   | Field                  |
// +---+----------+------------------------+
// | 1 | 4 bytes  | Schema version         |
// | 2 | 8 bytes  | Model's last change ID |
// | 3 | variable | Nodes[0]               |
// | 4 | variable | Nodes[1]               |
// |   | ...      | ...                    |
// | N | variable | Nodes[N-1]             |
// +---+----------+------------------------+
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
func Restore(driver storage.Driver) (*Root, error) {
	log.Printf("restoring model state")

	// Load a snapshot from a persistent storage
	snapshot, err := driver.SnapshotFile().Read()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = snapshot.Close()
	}()

	model, err := ReadSnapshot(snapshot)
	if err != nil {
		return nil, err
	}

	// Then replay write-ahead log to restore model's actual state
	wal, err := driver.WALFile().Read()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = wal.Close()
	}()

	lastChangeID := model.LastChangeID
	err = model.replayWriteAheadLog(wal)
	if err != nil {
		return nil, err
	}

	// If model stage was not in sync with write-ahead log,
	// then new model snapshot should be created
	if lastChangeID != model.LastChangeID {
		file, err := driver.SnapshotFile().Write()
		if err != nil {
			return nil, err
		}
		defer func() {
			_ = file.Close()
		}()

		err = model.WriteSnapshot(file)
		if err != nil {
			return nil, err
		}
	}

	return model, nil
}

// ReadSnapshot restores model snapshot from its binary form
func ReadSnapshot(file io.Reader) (*Root, error) {
	model := New()

	log.Verbosef("reading data snapshot")
	if file != nil {
		// First, read a schema version
		version, err := util.ReadUint32(file)
		if err != nil {
			if err == io.EOF {
				// File is empty, which should not produce any errors
				return model, nil
			}
			return nil, err
		}

		// Check schema version (no migration support so far)
		if version != schemaVersion {
			return nil, fmt.Errorf("incompatible schema: #%d", version)
		}

		// Second, read a last change ID
		model.LastChangeID, err = util.ReadUint64(file)
		if err != nil {
			return nil, err
		}

		// Then, read node snapshots until we see an EOF
		for {
			node, err := readNodeFromSnapshot(file)
			if err != nil {
				if err == io.EOF {
					break
				}

				return nil, err
			}

			existingNode := model.GetNode(node.Key)
			if existingNode != nil {
				return nil, fmt.Errorf("malformed snapshot: duplicate key \"%s\"", node.Key)
			}

			model.NodesMap[node.Key] = node
			if model.LastChangeID < node.LastChangeID {
				model.LastChangeID = node.LastChangeID
			}
		}

		log.Verbosef("restored %d nodes from snapshot", len(model.NodesMap))
	}

	return model, nil
}

// WriteSnapshot writes model snapshot into its binary form
func (m *Root) WriteSnapshot(file io.Writer) error {
	log.Verbosef("writing data snapshot")

	// First, write a schema version
	err := util.WriteUint32(file, schemaVersion)
	if err != nil {
		return err
	}

	// Second, write a last change ID
	err = util.WriteUint64(file, m.LastChangeID)
	if err != nil {
		return err
	}

	// Then, write node snapshots sequentially
	for _, node := range m.NodesMap {
		err = node.writeSnapshot(file)
		if err != nil {
			return err
		}
	}

	log.Verbosef("data snapshot has been written")
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

		values[i] = value
	}

	node := &Node{
		Key:          key,
		LastChangeID: lastChangeID,
		Values:       values,
	}
	return node, nil
}

// writeSnapshot writes model node snapshot into its binary form
func (n *Node) writeSnapshot(file io.Writer) error {
	// Node last change ID
	err := util.WriteUint64(file, n.LastChangeID)
	if err != nil {
		return err
	}

	// Key length
	err = util.WriteUint32(file, uint32(len(n.Key)))
	if err != nil {
		return err
	}

	// Key value
	err = util.WriteString(file, n.Key)
	if err != nil {
		return err
	}

	// Value array length
	err = util.WriteUint32(file, uint32(len(n.Values)))
	if err != nil {
		return err
	}

	// Value array
	for _, value := range n.Values {
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

// WriteToWAL writes model snapshot into WAL
func (m *Root) WriteToWAL(wal storage.WALWriter) error {
	log.Verbosef("writing wal data snapshot")

	err := wal.BeginTx()
	if err != nil {
		return err
	}

	// Write node snapshots sequentially
	for _, node := range m.NodesMap {
		err = node.writeSnapshotToWAL(wal)
		if err != nil {
			return err
		}
	}

	err = wal.CommitTx()
	if err != nil {
		return err
	}

	return nil
}

// writeSnapshotToWAL writes model node snapshot into WAL
func (n *Node) writeSnapshotToWAL(wal storage.WALWriter) error {
	// First we need to drop key entirely
	// Otherwise reloading model from snapshot and WAL will re-add existing values
	record := &storage.WALRecord{
		Key:  n.Key,
		Type: storage.WALRemoveKey,
	}
	err := wal.Write(record)
	if err != nil {
		return err
	}

	// Then - to add all values to this key
	for _, value := range n.Values {
		record := &storage.WALRecord{
			Key:   n.Key,
			Value: value,
			Type:  storage.WALAddValue,
		}
		err := wal.Write(record)
		if err != nil {
			return err
		}
	}

	return nil
}

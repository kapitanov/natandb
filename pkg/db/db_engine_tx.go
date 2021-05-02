package db

import (
	"github.com/kapitanov/natandb/pkg/model"
	"github.com/kapitanov/natandb/pkg/storage"
	"sort"
	"strings"
)

type transaction struct {
	Engine       *engine
	Model        *model.Root
	WAL          storage.WALWriter
	ShouldCommit bool
}

func newTransaction(engine *engine) *transaction {
	tx := &transaction{
		Engine:       engine,
		Model:        engine.Model,
		WAL:          engine.WAL,
		ShouldCommit: false,
	}
	return tx
}

// Commit marks transaction for committing
func (t *transaction) Commit() {
	t.ShouldCommit = true
}

// Close terminates a transaction
func (t *transaction) Close() error {
	var err error
	if t.ShouldCommit {
		err = t.Engine.WAL.CommitTx()
	} else {
		err = t.Engine.WAL.RollbackTx()
	}

	if err != nil {
		return err
	}

	t.Engine.EndTx()
	return nil
}

// List returns paged list of DB keys (with values)
// Optionally list might be filtered by key prefix
// If data version is changed, a ErrDataOutOfDate error is returned
// ErrDataOutOfDate is not returned if version parameter contains zero
func (t *transaction) List(prefix Key, skip uint, limit uint, version uint64) (*PagedNodeList, error) {
	if version != 0 && version != t.Model.LastChangeID {
		return nil, ErrDataOutOfDate
	}

	// TODO dirty and inefficient implementation
	array := make([]*Node, 0)
	for k, n := range t.Model.NodesMap {
		if prefix == "" || strings.Index(k, string(prefix)) == 0 {
			array = append(array, mapNode(n))
		}
	}

	cmp := func(i, j int) bool {
		return strings.Compare(string(array[i].Key), string(array[j].Key)) < 0
	}
	sort.Slice(array, cmp)

	lowIndex := int(skip)
	if len(array) < lowIndex {
		lowIndex = len(array) - 1
	}

	count := int(limit)
	if len(array)-lowIndex < count {
		count = len(array) - lowIndex
	}

	list := &PagedNodeList{
		Nodes:      array[lowIndex:count],
		Version:    t.Model.LastChangeID,
		TotalCount: uint(len(array)),
	}

	return list, nil
}

// GetVersion returns current data version
func (t *transaction) GetVersion() uint64 {
	return t.Model.LastChangeID
}

// Get gets a node value by its key
// If specified node doesn't exist, a ErrNoSuchKey error is returned
func (t *transaction) Get(key Key) (*Node, error) {
	node := t.Model.GetNode(string(key))
	if node == nil {
		return nil, ErrNoSuchKey
	}

	return mapNode(node), nil
}

// Set sets a node value, rewriting its value if node already exists
// If specified node doesn't exists, it will be created
func (t *transaction) Set(key Key, values []Value) (*Node, error) {
	// If new node value is empty or nil - just drop the node and exit
	if values == nil || len(values) == 0 {
		node := t.Model.GetNode(string(key))
		if node == nil {
			// No need to drop node if it doesn't exist
			return &Node{
				Key:     key,
				Values:  make([]Value, 0),
				Version: t.Model.LastChangeID,
			}, nil
		}

		// Drop existing node
		err := t.write(storage.WALRemoveKey, node.Key, nil)
		if err != nil {
			return nil, err
		}
		return mapNode(node), nil
	}

	node := t.Model.GetOrCreateNode(string(key))
	changeCount := len(node.Values) + len(values)

	var err error
	if changeCount == 1 {
		// Optimistic path for new nodes
		err = t.write(storage.WALAddValue, node.Key, values[0])
	} else {
		// First, drop all node's values
		for _, v := range node.Values {
			err = t.write(storage.WALRemoveValue, node.Key, v)
			if err != nil {
				return nil, err
			}
		}

		// Then add all new values
		for _, v := range values {
			err = t.write(storage.WALAddValue, node.Key, v)
			if err != nil {
				return nil, err
			}
		}
	}

	return mapNode(node), nil
}

// AddValue defines an "append value" operation
// If specified node doesn't exists, it will be created
// A specified value will be added to node even if it already exists
func (t *transaction) AddValue(key Key, value Value) (*Node, error) {
	node := t.Model.GetOrCreateNode(string(key))
	err := t.write(storage.WALAddValue, node.Key, value)
	if err != nil {
		return nil, err
	}

	return mapNode(node), nil
}

// AddUniqueValue defines an "append value" operation
// If specified node doesn't exists, it will be created
// If node already contains the same value and "unique" parameter is set to "true", a ErrDuplicateValue error is returned
func (t *transaction) AddUniqueValue(key Key, value Value) (*Node, error) {
	node := t.Model.GetOrCreateNode(string(key))

	if node.Contains(value) {
		return nil, ErrDuplicateValue
	}

	err := t.write(storage.WALAddValue, node.Key, value)
	if err != nil {
		return nil, err
	}

	return mapNode(node), nil
}

// RemoveValue defines an "remove value" operation
// If specified node doesn't exist, a ErrNoSuchKey error is returned
// If specified value doesn't exist within a node, a ErrNoSuchValue error is returned
func (t *transaction) RemoveValue(key Key, value Value) (*Node, error) {
	node := t.Model.GetNode(string(key))
	if node == nil {
		return nil, ErrNoSuchKey
	}

	for _, v := range node.Values {
		if v.Equal(value) {
			// If node contained only one value - node should be removed
			var err error
			if len(node.Values) == 1 {
				err = t.write(storage.WALRemoveKey, node.Key, nil)
			} else {
				err = t.write(storage.WALRemoveValue, node.Key, value)
			}

			if err != nil {
				return nil, err
			}

			return mapNode(node), nil
		}
	}

	return nil, ErrNoSuchValue
}

// RemoveAllValues defines an "remove value" operation
// If specified node doesn't exist, a ErrNoSuchKey error is returned
// If node contains specified value multiple times, all values are removed
// If specified value doesn't exist within a node, a ErrNoSuchValue error is returned
func (t *transaction) RemoveAllValues(key Key, value Value) (*Node, error) {
	node := t.Model.GetNode(string(key))
	if node == nil {
		return nil, ErrNoSuchKey
	}

	// Generate change list
	count := 0
	for i := 0; i < len(node.Values); i++ {
		v := node.Values[i]
		if v.Equal(value) {
			err := t.write(storage.WALRemoveValue, node.Key, value)
			if err != nil {
				return nil, err
			}

			i--
			count++
		}
	}

	// Return an error when trying to remove a non-existing value
	if count == 0 {
		return nil, ErrNoSuchValue
	}

	if len(node.Values) <= 0 {
		// If node is empty after RemoveAllValues operation - just drop entire node
		err := t.write(storage.WALRemoveKey, node.Key, nil)
		if err != nil {
			return nil, err
		}
	}

	return mapNode(node), nil
}

// RemoveKey removes a key completely
// If specified node doesn't exist, a ErrNoSuchKey error is returned
func (t *transaction) RemoveKey(key Key) error {
	node := t.Model.GetNode(string(key))
	if node == nil {
		return ErrNoSuchKey
	}

	err := t.write(storage.WALRemoveKey, node.Key, nil)
	if err != nil {
		return err
	}

	return nil
}

// write writes and applies one change record
func (t *transaction) write(recordType storage.WALRecordType, key string, value model.Value) error {
	record := &storage.WALRecord{
		Type:  recordType,
		Key:   key,
		Value: value,
	}

	err := t.WAL.Write(record)
	if err != nil {
		return err
	}

	err = t.Model.Apply(record)
	if err != nil {
		return err
	}

	return nil
}

func mapNode(node *model.Node) *Node {
	return &Node{
		Key:     Key(node.Key),
		Version: node.LastChangeID,
		Values:  node.Values,
	}
}

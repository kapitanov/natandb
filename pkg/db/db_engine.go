package db

import (
	"sort"
	"strings"
	"sync"

	"github.com/kapitanov/natandb/pkg/model"
	"github.com/kapitanov/natandb/pkg/storage"
	"github.com/kapitanov/natandb/pkg/writeahead"
)

type engineImpl struct {
	Model     *model.Root
	ModelLock *sync.Mutex
	WALog     writeahead.Log
	Snapshot  storage.SnapshotFile
}

// NewEngine creates new instance of DB engine
func NewEngine(log writeahead.Log, snapshot storage.SnapshotFile) (Engine, error) {
	model, err := model.Restore(log, snapshot)
	if err != nil {
		return nil, err
	}

	engine := &engineImpl{
		Model:     model,
		ModelLock: new(sync.Mutex),
		WALog:     log,
		Snapshot:  snapshot,
	}

	// TODO bg model flush
	// TODO bg wal compression

	return engine, nil
}

func mapNode(node *model.Node) *Node {
	return &Node{
		Key:     Key(node.Key),
		Version: node.LastChangeID,
		Values:  node.Values,
	}
}

// List returns paged list of DB keys (with values)
// Optionally list might be filtered by key prefix
// If data version is changed, a ErrDataOutOfDate error is returned
// ErrDataOutOfDate is not returned if version parameter contains zero
func (e *engineImpl) List(prefix Key, skip uint, limit uint, version uint64) (*PagedNodeList, error) {
	e.ModelLock.Lock()
	defer e.ModelLock.Unlock()

	if version != 0 && version != e.Model.LastChangeID {
		return nil, ErrDataOutOfDate
	}

	// TODO dirty and inefficient implementation
	array := make([]*Node, 0)
	for k, n := range e.Model.NodesMap {
		if prefix == "" || strings.Index(string(k), string(prefix)) == 0 {
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
		Version:    e.Model.LastChangeID,
		TotalCount: uint(len(array)),
	}

	return list, nil
}

// GetVersion retunrs current data version
func (e *engineImpl) GetVersion() uint64 {
	e.ModelLock.Lock()
	defer e.ModelLock.Unlock()

	return e.Model.LastChangeID
}

// Get gets a node value by its key
// If specified node doesn't exist, a ErrNoSuchKey error is returned
func (e *engineImpl) Get(key Key) (*Node, error) {
	e.ModelLock.Lock()
	defer e.ModelLock.Unlock()

	node := e.Model.GetNode(string(key))
	if node == nil {
		return nil, ErrNoSuchKey
	}

	return mapNode(node), nil
}

// Set sets a node value, rewritting its value if node already exists
// If specified node doesn't exists, it will be created
func (e *engineImpl) Set(key Key, values []Value) (*Node, error) {
	e.ModelLock.Lock()
	defer e.ModelLock.Unlock()

	// If new node value is empty or nil - just drop the node and exit
	if values == nil || len(values) == 0 {
		node := e.Model.GetNode(string(key))
		if node == nil {
			// No need to drop node if it doesn't exst
			return &Node{
				Key:     key,
				Values:  make([]Value, 0),
				Version: e.Model.LastChangeID,
			}, nil
		}

		// Drop existing node
		record := &writeahead.Record{
			Key:  node.Key,
			Type: writeahead.RemoveKey,
		}
		err := e.writeOneUnsafe(record)
		if err != nil {
			return nil, err
		}
		return mapNode(node), nil
	}

	node := e.Model.GetOrCreateNode(string(key))
	changeCount := len(node.Values) + len(values)

	var err error
	if changeCount == 1 {
		// Optimistic path for new nodes
		record := &writeahead.Record{
			Key:   node.Key,
			Type:  writeahead.AddValue,
			Value: values[0],
		}
		err = e.writeOneUnsafe(record)
	} else {
		records := make([]*writeahead.Record, changeCount)

		// First, drop all node's values
		i := 0
		for _, v := range node.Values {
			record := &writeahead.Record{
				Key:   node.Key,
				Type:  writeahead.RemoveValue,
				Value: v,
			}
			records[i] = record
			i++
		}

		// Then add all new values
		for _, v := range values {
			record := &writeahead.Record{
				Key:   node.Key,
				Type:  writeahead.AddValue,
				Value: v,
			}
			records[i] = record
			i++
		}

		err = e.writeManyUnsafe(records)
	}

	if err != nil {
		return nil, err
	}

	return mapNode(node), nil
}

// AddValue defines an "append value" operation
// If specified node doesn't exists, it will be created
// A specified value will be added to node even if it already exists
func (e *engineImpl) AddValue(key Key, value Value) (*Node, error) {
	e.ModelLock.Lock()
	defer e.ModelLock.Unlock()

	node := e.Model.GetOrCreateNode(string(key))
	record := &writeahead.Record{
		Key:   node.Key,
		Type:  writeahead.AddValue,
		Value: value,
	}
	err := e.writeOneUnsafe(record)
	if err != nil {
		return nil, err
	}

	return mapNode(node), nil
}

// AddUniqueValue defines an "append value" operation
// If specified node doesn't exists, it will be created
// If node already contains the same value and "unique" parameter is set to "true", a ErrDuplicateValue error is returned
func (e *engineImpl) AddUniqueValue(key Key, value Value) (*Node, error) {
	e.ModelLock.Lock()
	defer e.ModelLock.Unlock()

	node := e.Model.GetOrCreateNode(string(key))

	if node.Contains(value) {
		return nil, ErrDuplicateValue
	}

	record := &writeahead.Record{
		Key:   node.Key,
		Type:  writeahead.AddValue,
		Value: value,
	}
	err := e.writeOneUnsafe(record)
	if err != nil {
		return nil, err
	}

	return mapNode(node), nil
}

// RemoveValue defines an "remove value" operation
// If specified node doesn't exist, a ErrNoSuchKey error is returned
// If specified value doesn't exist within a node, a ErrNoSuchValue error is returned
func (e *engineImpl) RemoveValue(key Key, value Value) (*Node, error) {
	e.ModelLock.Lock()
	defer e.ModelLock.Unlock()

	node := e.Model.GetNode(string(key))
	if node == nil {
		return nil, ErrNoSuchKey
	}

	for _, v := range node.Values {
		if v.Equal(value) {
			// If node contained only one value - node should be removed
			var record *writeahead.Record
			if len(node.Values) == 1 {
				record = &writeahead.Record{
					Key:  node.Key,
					Type: writeahead.RemoveKey,
				}
			} else {
				record = &writeahead.Record{
					Key:   node.Key,
					Type:  writeahead.RemoveValue,
					Value: value,
				}
			}

			err := e.writeOneUnsafe(record)
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
func (e *engineImpl) RemoveAllValues(key Key, value Value) (*Node, error) {
	e.ModelLock.Lock()
	defer e.ModelLock.Unlock()

	node := e.Model.GetNode(string(key))
	if node == nil {
		return nil, ErrNoSuchKey
	}

	// Generate change list
	valueCount := len(node.Values)
	records := make([]*writeahead.Record, 0)
	for _, v := range node.Values {
		if v.Equal(value) {
			record := &writeahead.Record{
				Key:   node.Key,
				Type:  writeahead.RemoveValue,
				Value: value,
			}

			records = append(records, record)
			valueCount--
		}
	}

	// Return an error when trying to remove a non-existing value
	if len(records) == 0 {
		return nil, ErrNoSuchValue
	}

	if valueCount <= 0 {
		// If node is empty after RemoveAllValues operation - just drop entire node
		record := &writeahead.Record{
			Key:  node.Key,
			Type: writeahead.RemoveKey,
		}
		records = []*writeahead.Record{record}
	}

	// Apply change list
	err := e.writeManyUnsafe(records)
	if err != nil {
		return nil, err
	}

	return mapNode(node), nil
}

// Drop removes a key completely
// If specified node doesn't exist, a ErrNoSuchKey error is returned
func (e *engineImpl) Drop(key Key) error {
	e.ModelLock.Lock()
	defer e.ModelLock.Unlock()

	node := e.Model.GetNode(string(key))
	if node == nil {
		return ErrNoSuchKey
	}

	record := &writeahead.Record{
		Key:  node.Key,
		Type: writeahead.RemoveKey,
	}
	err := e.writeOneUnsafe(record)
	if err != nil {
		return err
	}

	return nil
}

// Close shuts engine down gracefully
func (e *engineImpl) Close() error {
	err := e.WALog.Close()
	if err != nil {
		return err
	}

	file, err := e.Snapshot.Write()
	if err != nil {
		return err
	}

	defer file.Close()

	err = e.Model.WriteSnapshot(file)
	if err != nil {
		return err
	}

	return nil
}

// writeOneUnsafe writes and applies one change record
// This function requires external synchronization and must be called when holding a ModelLock mutex
func (e *engineImpl) writeOneUnsafe(record *writeahead.Record) error {
	err := e.WALog.WriteOne(record)
	if err != nil {
		return err
	}

	err = e.Model.Apply(record)
	if err != nil {
		return err
	}

	return nil
}

// writeManyUnsafe writes and applies array of change records
// This function requires external synchronization and must be called when holding a ModelLock mutex
func (e *engineImpl) writeManyUnsafe(records []*writeahead.Record) error {
	err := e.WALog.WriteMany(records)
	if err != nil {
		return err
	}

	for _, record := range records {
		err = e.Model.Apply(record)
		if err != nil {
			return err
		}
	}

	return nil
}

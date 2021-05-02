package model_test

import (
	"testing"

	"github.com/kapitanov/natandb/pkg/model"
	"github.com/kapitanov/natandb/pkg/storage"
)

func TestGetNode(t *testing.T) {
	root := model.New()

	node := root.GetNode("key")
	if node != nil {
		t.Errorf("ERROR: GetNode: node should not exist")
		return
	}
}

func TestGetOrCreateNode(t *testing.T) {
	root := model.New()
	key := "key"

	node := root.GetNode(key)
	if node != nil {
		t.Errorf("ERROR: GetNode: node should not exist")
		return
	}

	node = root.GetOrCreateNode(key)
	if node == nil {
		t.Errorf("ERROR: GetOrCreateNode: node should exist")
		return
	}

	if key != node.Key {
		t.Errorf("ERROR: node key: \"%s\" != \"%s\"", key, node.Key)
		return
	}

	node = root.GetNode(key)
	if node == nil {
		t.Errorf("ERROR: GetNode: node should exist")
		return
	}
}

func TestApply_AddValue_NodeNotExists(t *testing.T) {
	root := model.New()

	// Add value
	record := &storage.WALRecord{
		ID:    1,
		Key:   "foobar",
		Type:  storage.WALAddValue,
		Value: model.Value("VAL1"),
	}
	err := root.Apply(record)
	if err != nil {
		t.Errorf("ERROR: Apply: %s", err)
		return
	}

	// Check
	node := root.GetNode(record.Key)
	if node == nil {
		t.Errorf("ERROR: GetNode: node should exist")
		return
	}

	nodeValues := node.Values
	if len(nodeValues) != 1 {
		t.Errorf("ERROR: node should have 1 value, not %d: %s", len(nodeValues), nodeValues)
		return
	}

	if !node.Contains(model.Value("VAL1")) {
		t.Errorf("ERROR: node should contain \"VAL1\" but got %s", node.Values)
		return
	}

	if record.ID != node.LastChangeID {
		t.Errorf("ERROR: node.LastChangeID: %d != %d", record.ID, node.LastChangeID)
		return
	}

	if record.ID != root.LastChangeID {
		t.Errorf("ERROR: root.LastChangeID: %d != %d", record.ID, root.LastChangeID)
		return
	}
}

func TestApply_AddValue_NodeExists(t *testing.T) {
	root := model.New()

	// Add 1st value
	record := &storage.WALRecord{
		ID:    1,
		Key:   "foobar",
		Type:  storage.WALAddValue,
		Value: model.Value("VAL1"),
	}
	err := root.Apply(record)
	if err != nil {
		t.Errorf("ERROR: Apply: %s", err)
		return
	}

	// Add 2nd value
	record = &storage.WALRecord{
		ID:    2,
		Key:   "foobar",
		Type:  storage.WALAddValue,
		Value: model.Value("VAL2"),
	}
	err = root.Apply(record)
	if err != nil {
		t.Errorf("ERROR: Apply: %s", err)
		return
	}

	// Check
	node := root.GetNode(record.Key)
	if node == nil {
		t.Errorf("ERROR: GetNode: node should exist")
		return
	}
	nodeValues := node.Values
	if len(nodeValues) != 2 {
		t.Errorf("ERROR: node should have 2 values, not %d: %s", len(nodeValues), nodeValues)
		return
	}
	if !node.Contains(model.Value("VAL1")) {
		t.Errorf("ERROR: node should contain \"VAL1\" but got %s", node.Values)
		return
	}
	if !node.Contains(model.Value("VAL2")) {
		t.Errorf("ERROR: node should contain \"VAL2\" but got %s", node.Values)
		return
	}

	if record.ID != node.LastChangeID {
		t.Errorf("ERROR: node.LastChangeID: %d != %d", record.ID, node.LastChangeID)
		return
	}

	if record.ID != root.LastChangeID {
		t.Errorf("ERROR: root.LastChangeID: %d != %d", record.ID, root.LastChangeID)
		return
	}
}

func TestApply_RemoveValue_NodeExists(t *testing.T) {
	root := model.New()

	// Add 1st value
	record := &storage.WALRecord{
		ID:    1,
		Key:   "foobar",
		Type:  storage.WALAddValue,
		Value: model.Value("VAL1"),
	}
	err := root.Apply(record)
	if err != nil {
		t.Errorf("ERROR: Apply: %s", err)
		return
	}

	// Add 2nd value
	record = &storage.WALRecord{
		ID:    2,
		Key:   "foobar",
		Type:  storage.WALAddValue,
		Value: model.Value("VAL2"),
	}
	err = root.Apply(record)
	if err != nil {
		t.Errorf("ERROR: Apply: %s", err)
		return
	}

	// Remove 1st value
	record = &storage.WALRecord{
		ID:    3,
		Key:   "foobar",
		Type:  storage.WALRemoveValue,
		Value: model.Value("VAL1"),
	}
	err = root.Apply(record)
	if err != nil {
		t.Errorf("ERROR: Apply: %s", err)
		return
	}

	// Check
	node := root.GetNode(record.Key)
	if node == nil {
		t.Errorf("ERROR: GetNode: node should exist")
		return
	}
	nodeValues := node.Values
	if len(nodeValues) != 1 {
		t.Errorf("ERROR: node should have 1 value, not %d: %s", len(nodeValues), nodeValues)
		return
	}
	if node.Contains(model.Value("VAL1")) {
		t.Errorf("ERROR: node should not contain \"VAL1\" but got %s", node.Values)
		return
	}
	if !node.Contains(model.Value("VAL2")) {
		t.Errorf("ERROR: node should contain \"VAL2\" but got %s", node.Values)
		return
	}
	if record.ID != node.LastChangeID {
		t.Errorf("ERROR: node.LastChangeID: %d != %d", record.ID, node.LastChangeID)
		return
	}

	if record.ID != root.LastChangeID {
		t.Errorf("ERROR: root.LastChangeID: %d != %d", record.ID, root.LastChangeID)
		return
	}
}

func TestApply_RemoveValue_NodeNotExists(t *testing.T) {
	root := model.New()

	// Remove value
	record := &storage.WALRecord{
		ID:    3,
		Key:   "foobar",
		Type:  storage.WALRemoveValue,
		Value: model.Value("VAL1"),
	}
	err := root.Apply(record)
	if err != nil {
		t.Errorf("ERROR: Apply: %s", err)
		return
	}

	// Check
	node := root.GetNode(record.Key)
	if node != nil {
		t.Errorf("ERROR: GetNode: node should not exist but got %s", node)
		return
	}

	if record.ID != root.LastChangeID {
		t.Errorf("ERROR: root.LastChangeID: %d != %d", record.ID, root.LastChangeID)
		return
	}
}

func TestApply_RemoveKey_NodeExists(t *testing.T) {
	root := model.New()

	// Add value
	record := &storage.WALRecord{
		ID:    1,
		Key:   "foobar",
		Type:  storage.WALAddValue,
		Value: model.Value("VAL1"),
	}
	err := root.Apply(record)
	if err != nil {
		t.Errorf("ERROR: Apply: %s", err)
		return
	}

	// Remove key
	record = &storage.WALRecord{
		ID:   2,
		Key:  "foobar",
		Type: storage.WALRemoveKey,
	}
	err = root.Apply(record)
	if err != nil {
		t.Errorf("ERROR: Apply: %s", err)
		return
	}

	// Check
	node := root.GetNode(record.Key)
	if node != nil {
		t.Errorf("ERROR: GetNode: node should not exist but got %s", node)
		return
	}

	if record.ID != root.LastChangeID {
		t.Errorf("ERROR: root.LastChangeID: %d != %d", record.ID, root.LastChangeID)
		return
	}
}

func TestApply_RemoveKey_NodeNotExists(t *testing.T) {
	root := model.New()

	// Remove key
	record := &storage.WALRecord{
		ID:   2,
		Key:  "foobar",
		Type: storage.WALRemoveKey,
	}
	err := root.Apply(record)
	if err != nil {
		t.Errorf("ERROR: Apply: %s", err)
		return
	}

	// Check
	node := root.GetNode(record.Key)
	if node != nil {
		t.Errorf("ERROR: GetNode: node should not exist but got %s", node)
		return
	}

	if record.ID != root.LastChangeID {
		t.Errorf("ERROR: root.LastChangeID: %d != %d", record.ID, root.LastChangeID)
		return
	}
}

func TestApply_AlreadyApplied(t *testing.T) {
	root := model.New()

	// Remove key
	record := &storage.WALRecord{
		ID:   1,
		Type: storage.WALNone,
	}
	err := root.Apply(record)
	if err != nil {
		t.Errorf("ERROR: Apply: %s", err)
		return
	}

	err = root.Apply(record)
	if err != model.ErrChangeAlreadyApplied {
		t.Errorf("ERROR: Apply: %s", err)
		return
	}
}

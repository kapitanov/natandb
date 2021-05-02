package db_test

import (
	"fmt"
	"github.com/kapitanov/natandb/pkg/storage"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/kapitanov/natandb/pkg/db"
	l "github.com/kapitanov/natandb/pkg/log"
)

const (
	key = "key"
)

// --------------------------------------------------------------------------------------------------------------------
// Empty DB tests
// --------------------------------------------------------------------------------------------------------------------

func TestGetNoSuchKey(t *testing.T) {
	engine := createEngine(t)

	checkGetNoNode(t, engine, key)
}

func TestRemoveValueNoSuchKey(t *testing.T) {
	engine := createEngine(t)
	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	node, err := tx.RemoveValue(key, db.Value("value"))
	if err != db.ErrNoSuchKey {
		t.Errorf("ERROR: expected %s but got %s", db.ErrNoSuchKey, err)
		return
	}

	if node != nil {
		t.Errorf("ERROR: expected node=nil but got %s", node)
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestRemoveKeyNoSuchKey(t *testing.T) {
	engine := createEngine(t)
	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = tx.RemoveKey(key)
	if err != db.ErrNoSuchKey {
		t.Errorf("ERROR: expected %s but got %s", db.ErrNoSuchKey, err)
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}
}

// --------------------------------------------------------------------------------------------------------------------
// DB modification tests
// --------------------------------------------------------------------------------------------------------------------

func TestSetNewKey(t *testing.T) {
	engine := createEngine(t)

	var node *db.Node
	values := []db.Value{db.Value("value")}

	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	node, err = tx.Set(key, values)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, values, 1)
	checkGetNode(t, engine, node)
}

func TestSetNewKeyWithEmptyValue(t *testing.T) {
	engine := createEngine(t)
	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	var values []db.Value
	node, err := tx.Set(key, values)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, values, 0)
	checkGetNoNode(t, engine, key) // No changes are actually applied
}

func TestSetExistingKey(t *testing.T) {
	engine := createEngine(t)
	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}
	values := []db.Value{db.Value("old value")}
	node, err := tx.Set(key, values)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	values = []db.Value{db.Value("value")}
	node, err = tx.Set(key, values)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, values, 3)
	checkGetNode(t, engine, node)
}

func TestAddValue(t *testing.T) {
	engine := createEngine(t)
	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	value1 := db.Value("value1")
	value2 := db.Value("value2")

	// Add value1 once
	node, err := tx.AddValue(key, value1)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, []db.Value{value1}, 1)
	checkGetNode(t, engine, node)

	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	// Add value1 twice
	node, err = tx.AddValue(key, value1)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, []db.Value{value1, value1}, 2)
	checkGetNode(t, engine, node)

	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	// Add value2 once
	node, err = tx.AddValue(key, value2)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, []db.Value{value1, value1, value2}, 3)
	checkGetNode(t, engine, node)
}

func TestRemoveValue(t *testing.T) {
	engine := createEngine(t)
	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	value1 := db.Value("value1")
	value2 := db.Value("value2")

	values := []db.Value{value1, value1, value2}
	node, err := tx.Set(key, values)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, []db.Value{value1, value1, value2}, 3)
	checkGetNode(t, engine, node)

	// Remove value1 once
	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	node, err = tx.RemoveValue(key, value1)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, []db.Value{value1, value2}, 4)
	checkGetNode(t, engine, node)

	// Remove value1 twice
	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	node, err = tx.RemoveValue(key, value1)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, []db.Value{value2}, 5)
	checkGetNode(t, engine, node)

	// Remove value1 once more
	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	_, err = tx.RemoveValue(key, value1)
	if err != db.ErrNoSuchValue {
		t.Errorf("ERROR: expected %s but got %s", db.ErrNoSuchValue, err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, []db.Value{value2}, 5)
	checkGetNode(t, engine, node)

	// Remove value2 once
	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	node, err = tx.RemoveValue(key, value2)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, []db.Value{}, 6)
	// Empty nodes are dropped automatically
	checkGetNoNode(t, engine, key)

	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	version := tx.GetVersion()
	if version != 6 {
		t.Errorf("ERROR: expected db.version=%d but got %d", 6, version)
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestAddUniqueValue(t *testing.T) {
	engine := createEngine(t)

	value1 := db.Value("value1")
	value2 := db.Value("value2")

	// Add value1 once
	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	node, err := tx.AddUniqueValue(key, value1)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, []db.Value{value1}, 1)
	checkGetNode(t, engine, node)

	// Add value1 twice
	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	_, err = tx.AddUniqueValue(key, value1)
	if err != db.ErrDuplicateValue {
		t.Errorf("ERROR: expected %s but got %s", db.ErrDuplicateValue, err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkGetNode(t, engine, node) // No changes should be applied

	// Add value2 once
	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	node, err = tx.AddUniqueValue(key, value2)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, []db.Value{value1, value2}, 2)
	checkGetNode(t, engine, node)
}

func TestRemoveAllValues(t *testing.T) {
	engine := createEngine(t)

	value1 := db.Value("value1")
	value2 := db.Value("value2")

	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}
	values := []db.Value{value1, value1, value2}
	node, err := tx.Set(key, values)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, []db.Value{value1, value1, value2}, 3)
	checkGetNode(t, engine, node)

	// Remove value1 once
	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	node, err = tx.RemoveAllValues(key, value1)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, []db.Value{value2}, 5)
	checkGetNode(t, engine, node)

	// Remove value1 twice
	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	_, err = tx.RemoveAllValues(key, value1)
	if err != db.ErrNoSuchValue {
		t.Errorf("ERROR: expected %s but got %s", db.ErrNoSuchValue, err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkGetNode(t, engine, node) // No changes should be applied

	// Remove value2 once
	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	node, err = tx.RemoveAllValues(key, value2)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, []db.Value{}, 7)
	// Empty nodes are dropped automatically
	checkGetNoNode(t, engine, key)
}

func TestRemoveKeyExistingKey(t *testing.T) {
	engine := createEngine(t)
	value1 := db.Value("value1")
	value2 := db.Value("value2")

	values := []db.Value{value1, value1, value2}
	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	node, err := tx.Set(key, values)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNode(t, node, key, []db.Value{value1, value1, value2}, 3)
	checkGetNode(t, engine, node)

	// Remove key
	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = tx.RemoveKey(key)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkGetNoNode(t, engine, key)

	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	if tx.GetVersion() != 4 {
		t.Errorf("ERROR: expected db.version=%d but got %d", 4, tx.GetVersion())
	}

	if err != nil {
		t.Fatal(err)
		return
	}
}

// --------------------------------------------------------------------------------------------------------------------
// List() tests
// --------------------------------------------------------------------------------------------------------------------

func TestListEmpty(t *testing.T) {
	engine := createEngine(t)
	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	list, err := tx.List("", 0, 100, 0)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	if list.Version != 0 {
		t.Errorf("ERROR: expected version=0 but got %d", list.Version)
		return
	}

	if list.TotalCount != 0 {
		t.Errorf("ERROR: expected total_count=0 but got %d", list.TotalCount)
		return
	}

	if len(list.Nodes) != 0 {
		t.Errorf("ERROR: expected len(nodes)=0 but got %d", len(list.Nodes))
	}

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestListNonEmpty(t *testing.T) {
	engine := createEngine(t)
	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	count := 4
	value := db.Value("value")
	nodes := make([]*db.Node, count)
	for i := 0; i < count; i++ {
		key := db.Key(fmt.Sprintf("keys/key_%02d", i))
		node, err := tx.Set(key, []db.Value{value})
		if err != nil {
			t.Errorf("ERROR: expected no error but got %s", err)
			return
		}

		nodes[i] = node
	}
	version := tx.GetVersion()

	list, err := tx.List("", 0, uint(count)+1, 0)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	// List() returns nodes sorted by key
	expectedNodes := nodes
	cmp := func(i, j int) bool {
		return strings.Compare(string(expectedNodes[i].Key), string(expectedNodes[j].Key)) < 0
	}
	sort.Slice(expectedNodes, cmp)

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNodeList(t, list, expectedNodes, uint(count), version)
}

func TestListPaged(t *testing.T) {
	engine := createEngine(t)
	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	count := 8
	value := db.Value("value")
	nodes := make([]*db.Node, count)
	for i := 0; i < count; i++ {
		key := db.Key(fmt.Sprintf("keys/key_%02d", i))
		node, err := tx.Set(key, []db.Value{value})
		if err != nil {
			t.Errorf("ERROR: expected no error but got %s", err)
			return
		}

		nodes[i] = node
	}

	version := tx.GetVersion()

	// --------------------------------------------
	// Page 1 (0.. max-1)
	// --------------------------------------------
	max := 4
	list, err := tx.List("", 0, uint(max), 0)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	// List() returns nodes sorted by key
	expectedNodes := nodes[0:max]
	cmp := func(i, j int) bool {
		return strings.Compare(string(expectedNodes[i].Key), string(expectedNodes[j].Key)) < 0
	}
	sort.Slice(expectedNodes, cmp)

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNodeList(t, list, expectedNodes, uint(count), version)

	// --------------------------------------------
	// Page 2 (max..2*max-1)
	// --------------------------------------------
	tx, err = engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	list, err = tx.List("", uint(max), uint(max), 0)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	// List() returns nodes sorted by key
	expectedNodes = nodes[max:max]
	sort.Slice(expectedNodes, cmp)

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNodeList(t, list, expectedNodes, uint(count), version)
}

func TestListFiltered(t *testing.T) {
	engine := createEngine(t)
	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}

	count := 4
	value := db.Value("value")
	nodes := make([]*db.Node, count)
	for i := 0; i < count; i++ {
		key := db.Key(fmt.Sprintf("keys/key_%02d", i))
		values := []db.Value{value}
		node, err := tx.Set(key, values)
		if err != nil {
			t.Errorf("ERROR: expected no error but got %s", err)
			return
		}

		nodes[i] = node
	}
	for i := 0; i < count; i++ {
		key := db.Key(fmt.Sprintf("non-keys/key_%02d", i))
		values := []db.Value{value}
		_, err := tx.Set(key, values)
		if err != nil {
			t.Errorf("ERROR: expected no error but got %s", err)
			return
		}
	}

	version := tx.GetVersion()

	max := 4
	list, err := tx.List("keys/", 0, uint(max), 0)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	expectedNodes := nodes[0:max]
	cmp := func(i, j int) bool {
		return strings.Compare(string(expectedNodes[i].Key), string(expectedNodes[j].Key)) < 0
	}
	sort.Slice(expectedNodes, cmp)

	err = tx.Close()
	if err != nil {
		t.Fatal(err)
		return
	}

	checkNodeList(t, list, expectedNodes, uint(count), version)
}

// --------------------------------------------------------------------------------------------------------------------
// Graceful shutdown tests
// --------------------------------------------------------------------------------------------------------------------

func TestShutdownAndRestore(t *testing.T) {
	log.SetOutput(io.Discard)
	l.SetMinLevel(l.Verbose)

	dir, err := os.MkdirTemp(os.TempDir(), "*")
	if err != nil {
		t.Fatal(err)
	}

	driver, err := storage.NewDriver(storage.DirectoryOption(dir))
	if err != nil {
		t.Errorf("ERROR: NewDriver() failed: %s", err)
		t.Fatal(err)
	}

	engine, err := db.NewEngine(driver)
	if err != nil {
		t.Fatalf("NewEngine failed: %s", err)
	}

	values := []db.Value{db.Value("value")}
	var node *db.Node
	err = engine.Tx(func(tx db.TX) error {
		var e error
		node, e = tx.Set(key, values)
		return e
	})
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	var oldVersion uint64
	err = engine.Tx(func(tx db.TX) error {
		oldVersion = tx.GetVersion()
		return nil
	})
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = engine.Close()
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	engine, err = db.NewEngine(driver)
	if err != nil {
		t.Fatalf("NewEngine failed: %s", err)
	}

	err = engine.Tx(func(tx db.TX) error {
		version := tx.GetVersion()
		if version != oldVersion {
			t.Errorf("ERROR: expected db.version=%d but got %d", oldVersion, version)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	checkNode(t, node, key, values, 1)
	checkGetNode(t, engine, node)
}

// --------------------------------------------------------------------------------------------------------------------
// Test helpers
// --------------------------------------------------------------------------------------------------------------------

func createEngine(t *testing.T) db.Engine {
	log.SetOutput(io.Discard)
	l.SetMinLevel(l.Verbose)

	dir, err := os.MkdirTemp(os.TempDir(), "*")
	if err != nil {
		t.Fatal(err)
	}

	driver, err := storage.NewDriver(storage.DirectoryOption(dir))
	if err != nil {
		t.Errorf("ERROR: NewDriver() failed: %s", err)
		t.Fatal(err)
	}

	engine, err := db.NewEngine(driver)
	if err != nil {
		t.Fatalf("NewEngine failed: %s", err)
	}

	return engine
}

func checkNode(t *testing.T, node *db.Node, key db.Key, values []db.Value, version uint64) bool {
	if node == nil {
		t.Errorf("ERROR: expected node!=nil but got nil")
		return false
	}

	if node.Key != key {
		t.Errorf("ERROR: expected node.key=\"%s\" but got \"%s\"", key, node.Key)
		return false
	}

	if node.Version != version {
		t.Errorf("ERROR: expected node.version=%d but got %d", version, node.Version)
		return false
	}

	if len(node.Values) != len(values) {
		t.Errorf("ERROR: expected len(node.values)=%d but got %d", len(values), len(node.Values))
		return false
	}

	for i := range node.Values {
		if len(node.Values[i]) != len(values[i]) {
			t.Errorf("ERROR: expected len(node.values[%d])=%d but got %d", i, len(values[i]), len(node.Values[i]))
			return false
		}
		for j := range node.Values[i] {
			if node.Values[i][j] != values[i][j] {
				t.Errorf("ERROR: expected node.values[%d][%d]=0x%02x but got 0x%02x", i, j, values[i][j], node.Values[i][j])
				return false
			}
		}
	}

	return true
}

func checkNodeList(t *testing.T, list *db.PagedNodeList, nodes []*db.Node, totalCount uint, version uint64) {
	if list.Version != version {
		t.Errorf("ERROR: expected version=%d but got %d", version, list.Version)
		return
	}

	if list.TotalCount != totalCount {
		t.Errorf("ERROR: expected total_count=%d but got %d", totalCount, list.TotalCount)
		return
	}

	if len(list.Nodes) != len(nodes) {
		t.Errorf("ERROR: expected len(nodes)=%d but got %d", len(nodes), len(list.Nodes))
	}

	for i := range list.Nodes {
		if !checkNode(t, list.Nodes[i], nodes[i].Key, nodes[i].Values, nodes[i].Version) {
			return
		}
	}
}

func checkGetNode(t *testing.T, engine db.Engine, node *db.Node) {
	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		_ = tx.Close()
	}()

	n, err := tx.Get(node.Key)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, n, node.Key, node.Values, node.Version)
}

func checkGetNoNode(t *testing.T, engine db.Engine, key db.Key) {
	tx, err := engine.BeginTx()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		_ = tx.Close()
	}()

	node, err := tx.Get(key)
	if err != db.ErrNoSuchKey {
		t.Errorf("ERROR: expected %s but got %s", db.ErrNoSuchKey, err)
		return
	}

	if node != nil {
		t.Errorf("ERROR: expected node=nil but got %s", node)
	}
}

package db_test

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/kapitanov/natandb/pkg/db"
	"github.com/kapitanov/natandb/pkg/writeahead"
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
	node, err := engine.RemoveValue(key, db.Value("value"))
	t.Logf("RemoveValue(\"%s\", %s) -> %s, %s\n", key, db.Value("value"), node, err)
	if err != db.ErrNoSuchKey {
		t.Errorf("ERROR: expected %s but got %s", db.ErrNoSuchKey, err)
		return
	}

	if node != nil {
		t.Errorf("ERROR: expected node=nil but got %s", node)
	}
}

func TestRemoveKeyNoSuchKey(t *testing.T) {
	engine := createEngine(t)

	err := engine.RemoveKey(key)
	t.Logf("RemoveKey(\"%s\") -> %s\n", key, err)
	if err != db.ErrNoSuchKey {
		t.Errorf("ERROR: expected %s but got %s", db.ErrNoSuchKey, err)
	}
}

// --------------------------------------------------------------------------------------------------------------------
// DB modification tests
// --------------------------------------------------------------------------------------------------------------------

func TestSetNewKey(t *testing.T) {
	values := []db.Value{db.Value("value")}
	engine := createEngine(t)

	node, err := engine.Set(key, values)
	t.Logf("Set(\"%s\", %s) -> %s, %s\n", key, values, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, values, 1)
	checkGetNode(t, engine, node)
}

func TestSetNewKeyWithEmptyValue(t *testing.T) {
	values := []db.Value{}
	engine := createEngine(t)
	node, err := engine.Set(key, values)
	t.Logf("Set(\"%s\", %s) -> %s, %s\n", key, values, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, values, 0)
	checkGetNoNode(t, engine, key) // No changes are actually applied
}

func TestSetExistingKey(t *testing.T) {
	engine := createEngine(t)

	values := []db.Value{db.Value("old value")}
	node, err := engine.Set(key, values)
	t.Logf("Set(\"%s\", %s) -> %s, %s\n", key, values, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	values = []db.Value{db.Value("value")}
	node, err = engine.Set(key, values)
	t.Logf("Set(\"%s\", %s) -> %s, %s\n", key, values, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, values, 3)
	checkGetNode(t, engine, node)
}

func TestAddValue(t *testing.T) {
	value1 := db.Value("value1")
	value2 := db.Value("value2")
	engine := createEngine(t)

	// Add value1 once
	node, err := engine.AddValue(key, value1)
	t.Logf("AddValue(\"%s\", %s) -> %s, %s\n", key, value1, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, []db.Value{value1}, 1)
	checkGetNode(t, engine, node)

	// Add value1 twice
	node, err = engine.AddValue(key, value1)
	t.Logf("AddValue(\"%s\", %s) -> %s, %s\n", key, value1, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, []db.Value{value1, value1}, 2)
	checkGetNode(t, engine, node)

	// Add value2 once
	node, err = engine.AddValue(key, value2)
	t.Logf("AddValue(\"%s\", %s) -> %s, %s\n", key, value2, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, []db.Value{value1, value1, value2}, 3)
	checkGetNode(t, engine, node)
}

func TestRemoveValue(t *testing.T) {
	value1 := db.Value("value1")
	value2 := db.Value("value2")
	engine := createEngine(t)

	values := []db.Value{value1, value1, value2}
	node, err := engine.Set(key, values)
	t.Logf("Set(\"%s\", %s) -> %s, %s\n", key, values, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, []db.Value{value1, value1, value2}, 3)
	checkGetNode(t, engine, node)

	// Remove value1 once
	node, err = engine.RemoveValue(key, value1)
	t.Logf("RemoveValue(\"%s\", %s) -> %s, %s\n", key, value1, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, []db.Value{value1, value2}, 4)
	checkGetNode(t, engine, node)

	// Remove value1 twice
	node, err = engine.RemoveValue(key, value1)
	t.Logf("RemoveValue(\"%s\", %s) -> %s, %s\n", key, value1, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, []db.Value{value2}, 5)
	checkGetNode(t, engine, node)

	// Remove value1 once more
	n, err := engine.RemoveValue(key, value1)
	t.Logf("RemoveValue(\"%s\", %s) -> %s, %s\n", key, value1, n, err)
	if err != db.ErrNoSuchValue {
		t.Errorf("ERROR: expected %s but got %s", db.ErrNoSuchValue, err)
		return
	}

	checkNode(t, node, key, []db.Value{value2}, 5)
	checkGetNode(t, engine, node)

	// Remove value2 once
	node, err = engine.RemoveValue(key, value2)
	t.Logf("RemoveValue(\"%s\", %s) -> %s, %s\n", key, value2, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, []db.Value{}, 6)
	// Empty nodes are dropped automatically
	checkGetNoNode(t, engine, key)
	version := engine.GetVersion()
	if version != 6 {
		t.Errorf("ERROR: expected db.version=%d but got %d", 6, version)
	}
}

func TestAddUniqueValue(t *testing.T) {
	value1 := db.Value("value1")
	value2 := db.Value("value2")
	engine := createEngine(t)

	// Add value1 once
	node, err := engine.AddUniqueValue(key, value1)
	t.Logf("AddUniqueValue(\"%s\", %s) -> %s, %s\n", key, value1, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, []db.Value{value1}, 1)
	checkGetNode(t, engine, node)

	// Add value1 twice
	_, err = engine.AddUniqueValue(key, value1)
	t.Logf("AddUniqueValue(\"%s\", %s) -> %s, %s\n", key, value1, node, err)
	if err != db.ErrDuplicateValue {
		t.Errorf("ERROR: expected %s but got %s", db.ErrDuplicateValue, err)
		return
	}

	checkGetNode(t, engine, node) // No changes should be applied

	// Add value2 once
	node, err = engine.AddUniqueValue(key, value2)
	t.Logf("AddUniqueValue(\"%s\", %s) -> %s, %s\n", key, value2, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}
	checkNode(t, node, key, []db.Value{value1, value2}, 2)
	checkGetNode(t, engine, node)
}

func TestRemoveAllValues(t *testing.T) {
	value1 := db.Value("value1")
	value2 := db.Value("value2")
	engine := createEngine(t)

	values := []db.Value{value1, value1, value2}
	node, err := engine.Set(key, values)
	t.Logf("Set(\"%s\", %s) -> %s, %s\n", key, values, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, []db.Value{value1, value1, value2}, 3)
	checkGetNode(t, engine, node)

	// Remove value1 once
	node, err = engine.RemoveAllValues(key, value1)
	t.Logf("RemoveAllValues(\"%s\", %s) -> %s, %s\n", key, value1, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, []db.Value{value2}, 5)
	checkGetNode(t, engine, node)

	// Remove value1 twice
	n, err := engine.RemoveAllValues(key, value1)
	t.Logf("RemoveAllValues(\"%s\", %s) -> %s, %s\n", key, value1, n, err)
	if err != db.ErrNoSuchValue {
		t.Errorf("ERROR: expected %s but got %s", db.ErrNoSuchValue, err)
		return
	}

	checkGetNode(t, engine, node) // No changes should be applied

	// Remove value2 once
	node, err = engine.RemoveAllValues(key, value2)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, []db.Value{}, 6)
	// Empty nodes are dropped automatically
	checkGetNoNode(t, engine, key)
}

func TestRemoveKeyExistingKey(t *testing.T) {
	value1 := db.Value("value1")
	value2 := db.Value("value2")
	engine := createEngine(t)

	values := []db.Value{value1, value1, value2}
	node, err := engine.Set(key, values)
	t.Logf("Set(\"%s\", %s) -> %s, %s\n", key, values, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, node, key, []db.Value{value1, value1, value2}, 3)
	checkGetNode(t, engine, node)

	// Remove key
	err = engine.RemoveKey(key)
	t.Logf("RemoveKey(\"%s\") -> %s\n", key, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkGetNoNode(t, engine, key)

	if engine.GetVersion() != 4 {
		t.Errorf("ERROR: expected db.version=%d but got %d", 4, engine.GetVersion())
	}
}

// --------------------------------------------------------------------------------------------------------------------
// List() tests
// --------------------------------------------------------------------------------------------------------------------

func TestListEmpty(t *testing.T) {
	engine := createEngine(t)

	list, err := engine.List("", 0, 100, 0)
	t.Logf("List(\"%s\", %d, %d, %d) -> %s, %s\n", "", 0, 100, 0, list, err)
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
}

func TestListNonEmpty(t *testing.T) {
	engine := createEngine(t)
	count := 4
	value := db.Value("value")
	nodes := make([]*db.Node, count)
	for i := 0; i < count; i++ {
		key := db.Key(fmt.Sprintf("keys/key_%02d", i))
		values := []db.Value{value}
		node, err := engine.Set(key, []db.Value{value})
		t.Logf("Set(\"%s\", %s) -> %s, %s\n", key, values, node, err)
		if err != nil {
			t.Errorf("ERROR: expected no error but got %s", err)
			return
		}

		nodes[i] = node
	}
	version := engine.GetVersion()
	t.Logf("GetVersion() -> %d\n", version)

	list, err := engine.List("", 0, uint(count)+1, 0)
	t.Logf("List(\"%s\", %d, %d, %d) -> %s, %s\n", "", 0, uint(count)+1, 0, list, err)
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

	checkNodeList(t, list, expectedNodes, uint(count), version)
}

func TestListPaged(t *testing.T) {
	engine := createEngine(t)
	count := 8
	value := db.Value("value")
	nodes := make([]*db.Node, count)
	for i := 0; i < count; i++ {
		key := db.Key(fmt.Sprintf("keys/key_%02d", i))
		node, err := engine.Set(key, []db.Value{value})
		if err != nil {
			t.Errorf("ERROR: expected no error but got %s", err)
			return
		}

		nodes[i] = node
	}

	version := engine.GetVersion()
	t.Logf("GetVersion() -> %d\n", version)

	// --------------------------------------------
	// Page 1 (0.. max-1)
	// --------------------------------------------
	max := 4
	list, err := engine.List("", 0, uint(max), 0)
	t.Logf("List(\"%s\", %d, %d, %d) -> %s, %s\n", "", 0, uint(max), 0, list, err)
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

	checkNodeList(t, list, expectedNodes, uint(count), version)

	// --------------------------------------------
	// Page 2 (max..2*max-1)
	// --------------------------------------------
	list, err = engine.List("", uint(max), uint(max), 0)
	t.Logf("List(\"%s\", %d, %d, %d) -> %s, %s\n", "", uint(max), uint(max), 0, list, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	// List() returns nodes sorted by key
	expectedNodes = nodes[max:max]
	sort.Slice(expectedNodes, cmp)

	checkNodeList(t, list, expectedNodes, uint(count), version)
}

func TestListFiltered(t *testing.T) {
	engine := createEngine(t)
	count := 4
	value := db.Value("value")
	nodes := make([]*db.Node, count)
	for i := 0; i < count; i++ {
		key := db.Key(fmt.Sprintf("keys/key_%02d", i))
		values := []db.Value{value}
		node, err := engine.Set(key, values)
		t.Logf("Set(\"%s\", %s) -> %s, %s\n", key, values, node, err)
		if err != nil {
			t.Errorf("ERROR: expected no error but got %s", err)
			return
		}

		nodes[i] = node
	}
	for i := 0; i < count; i++ {
		key := db.Key(fmt.Sprintf("non-keys/key_%02d", i))
		values := []db.Value{value}
		n, err := engine.Set(key, values)
		t.Logf("Set(\"%s\", %s) -> %s, %s\n", key, values, n, err)
		if err != nil {
			t.Errorf("ERROR: expected no error but got %s", err)
			return
		}
	}

	version := engine.GetVersion()
	t.Logf("GetVersion() -> %d\n", version)

	max := 4
	list, err := engine.List("keys/", 0, uint(max), 0)
	t.Logf("List(\"%s\", %d, %d, %d) -> %s, %s\n", "keys/", 0, uint(max), 0, list, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	expectedNodes := nodes[0:max]
	cmp := func(i, j int) bool {
		return strings.Compare(string(expectedNodes[i].Key), string(expectedNodes[j].Key)) < 0
	}
	sort.Slice(expectedNodes, cmp)

	checkNodeList(t, list, expectedNodes, uint(count), version)
}

// --------------------------------------------------------------------------------------------------------------------
// Graceful shutdown tests
// --------------------------------------------------------------------------------------------------------------------

func TestShutdownAndRestore(t *testing.T) {
	walFile := &inMemoryWriteAheadLog{0}
	snapshotFile := &inMemorySnapshotFile{make([]byte, 0)}

	engine, err := db.NewEngine(walFile, snapshotFile)
	t.Logf("NewEngine() -> _, %s\n", err)
	if err != nil {
		t.Fatalf("NewEngine failed: %s", err)
	}

	values := []db.Value{db.Value("value")}
	node, err := engine.Set(key, values)
	t.Logf("Set(\"%s\", %s) -> %s, %s\n", key, values, node, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	err = engine.Close()
	t.Logf("Close() -> %s\n", err)

	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	engine, err = db.NewEngine(walFile, snapshotFile)
	t.Logf("NewEngine() -> _, %s\n", err)
	if err != nil {
		t.Fatalf("NewEngine failed: %s", err)
	}

	version := engine.GetVersion()
	t.Logf("GetVersion() -> %d\n", version)
	if version != node.Version {
		t.Errorf("ERROR: expected db.version=%d but got %d", node.Version, version)
		return
	}

	checkNode(t, node, key, values, 1)
	checkGetNode(t, engine, node)
}

// --------------------------------------------------------------------------------------------------------------------
// Test helpers
// --------------------------------------------------------------------------------------------------------------------

func createEngine(t *testing.T) db.Engine {
	walFile := &inMemoryWriteAheadLog{0}
	snapshotFile := &inMemorySnapshotFile{make([]byte, 0)}

	engine, err := db.NewEngine(walFile, snapshotFile)
	t.Logf("NewEngine() -> _, %s\n", err)
	if err != nil {
		t.Fatalf("NewEngine failed: %s", err)
	}

	return engine
}

// Test storage.WriteAheadLogFile impl ----------------------------------------

type inMemoryWriteAheadLog struct {
	lastID uint64
}

func (t *inMemoryWriteAheadLog) WriteOne(record *writeahead.Record) error {
	t.lastID++
	record.ID = t.lastID
	return nil
}

func (t *inMemoryWriteAheadLog) WriteMany(records []*writeahead.Record) error {
	for _, r := range records {
		t.WriteOne(r)
	}
	return nil
}

func (t *inMemoryWriteAheadLog) ReadChunkForward(minID uint64, limit int) (writeahead.RecordChunk, error) {
	return make(writeahead.RecordChunk, 0), nil
}

func (t *inMemoryWriteAheadLog) ReadChunkBackward(maxID uint64, limit int) (writeahead.RecordChunk, error) {
	return make(writeahead.RecordChunk, 0), nil
}

func (t *inMemoryWriteAheadLog) Close() error {
	return nil
}

// Test storage.SnapshotFile impl ---------------------------------------------

type inMemorySnapshotFile struct {
	buffer []byte
}

func (t *inMemorySnapshotFile) Read() (io.ReadCloser, error) {
	return &inMemorySnapshotFileReader{bytes.NewBuffer(t.buffer)}, nil
}

func (t *inMemorySnapshotFile) Write() (io.WriteCloser, error) {
	return &inMemorySnapshotFileWriter{t, bytes.NewBuffer(make([]byte, 0))}, nil
}

type inMemorySnapshotFileReader struct {
	buffer *bytes.Buffer
}

func (s *inMemorySnapshotFileReader) Read(p []byte) (int, error) {
	return s.buffer.Read(p)
}

func (s *inMemorySnapshotFileReader) Close() error {
	return nil
}

type inMemorySnapshotFileWriter struct {
	file   *inMemorySnapshotFile
	buffer *bytes.Buffer
}

func (w *inMemorySnapshotFileWriter) Write(p []byte) (int, error) {
	return w.buffer.Write(p)
}

func (w *inMemorySnapshotFileWriter) Close() error {
	w.file.buffer = w.buffer.Bytes()
	return nil
}

// Test assertions ------------------------------------------------------------

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
	n, err := engine.Get(node.Key)
	t.Logf("Get(\"%s\") -> %s, %s\n", node.Key, n, err)
	if err != nil {
		t.Errorf("ERROR: expected no error but got %s", err)
		return
	}

	checkNode(t, n, node.Key, node.Values, node.Version)
}

func checkGetNoNode(t *testing.T, engine db.Engine, key db.Key) {
	node, err := engine.Get(key)
	t.Logf("Get(\"%s\") -> %s, %s\n", key, node, err)
	if err != db.ErrNoSuchKey {
		t.Errorf("ERROR: expected %s but got %s", db.ErrNoSuchKey, err)
		return
	}

	if node != nil {
		t.Errorf("ERROR: expected node=nil but got %s", node)
	}
}

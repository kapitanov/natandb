package model

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	l "log"
)

func TestRestoreModelFromEmptyFile(t *testing.T) {
	l.SetOutput(io.Discard)

	r := bytes.NewBuffer(make([]byte, 0))
	root, err := ReadSnapshot(r)
	if err != nil {
		t.Errorf("ERROR: ReadSnapshot(): %s", err)
		return
	}

	if root == nil {
		t.Error("model is nil")
	}
}

func TestEmptyModelStorage(t *testing.T) {
	root := New()
	testModelStorage(t, root)
}

func TestOneNodeModelStorage(t *testing.T) {
	root := New()
	node := root.GetOrCreateNode("key")
	node.Values = append(node.Values, Value("value"))

	testModelStorage(t, root)
}

func TestOneValuelessNodeModelStorage(t *testing.T) {
	root := New()
	root.GetOrCreateNode("key")

	testModelStorage(t, root)
}

func TestMultiNodeModelStorage(t *testing.T) {
	root := New()

	nodeCount := 10
	valueCount := 5

	for i := 0; i < nodeCount; i++ {
		node := root.GetOrCreateNode(fmt.Sprintf("key_%d", i))
		values := make([]Value, valueCount)
		for j := 0; j < valueCount; j++ {
			values[j] = Value(fmt.Sprintf("value_%d", j))
		}
		node.Values = values
		node.LastChangeID = uint64(i * valueCount)
		root.LastChangeID = node.LastChangeID
	}

	testModelStorage(t, root)
}

func testModelStorage(t *testing.T, input *Root) {
	l.SetOutput(io.Discard)

	w := bytes.NewBuffer(make([]byte, 0))
	err := input.WriteSnapshot(w)
	if err != nil {
		t.Errorf("ERROR: WriteSnapshot(): %s", err)
		return
	}

	buffer := w.Bytes()
	r := bytes.NewBuffer(buffer)
	output, err := ReadSnapshot(r)
	if err != nil {
		t.Errorf("ERROR: ReadSnapshot(): %s", err)
		return
	}

	// len(Nodes)
	if len(input.NodesMap) != len(output.NodesMap) {
		t.Errorf("ERROR: len(Nodes): %d != %d", len(input.NodesMap), len(output.NodesMap))
		return
	}

	// LastChangeID
	if input.LastChangeID != output.LastChangeID {
		t.Errorf("ERROR: LastChangeID: %d != %d", input.LastChangeID, output.LastChangeID)
		return
	}

	// for each node
	for key, inputNode := range input.NodesMap {
		outputNode := output.GetNode(key)

		// Node must exist
		if outputNode == nil {
			t.Errorf("ERROR: output.GetNode(\"%s\"): nil", key)
			return
		}

		// node.Key
		if inputNode.Key != key || inputNode.Key != outputNode.Key {
			t.Errorf("ERROR: Nodes[\"%s\"]: Key: \"%s\" != \"%s\"", key, inputNode.Key, outputNode.Key)
			return
		}

		// node.LastChangeID
		if inputNode.LastChangeID != outputNode.LastChangeID {
			t.Errorf("ERROR: Nodes[\"%s\"]: LastChangeID: %d != %d", key, inputNode.LastChangeID, outputNode.LastChangeID)
			return
		}

		// len(node.Values)
		if len(inputNode.Values) != len(outputNode.Values) {
			t.Errorf("ERROR: Nodes[\"%s\"]: len(Values) %d != %d", key, len(inputNode.Values), len(outputNode.Values))
			return
		}

		for i, inputValue := range inputNode.Values {
			outputValue := outputNode.Values[i]

			// Not null
			if outputValue == nil {
				t.Errorf("ERROR: Nodes[\"%s\"].Values[%d]: nil", key, i)
				return
			}

			// length
			if len(inputValue) != len(outputValue) {
				t.Errorf("ERROR: Nodes[\"%s\"].Values[%d]: lenght %d != %d", key, i, len(inputValue), len(outputValue))
				return
			}

			for j := range inputValue {
				if inputValue[j] != outputValue[j] {
					t.Errorf("ERROR: Nodes[\"%s\"].Values[%d][%d]: %d != %d", key, i, j, inputValue[j], outputValue[j])
					return
				}
			}
		}
	}
}

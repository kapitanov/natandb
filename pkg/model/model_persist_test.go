package model

import (
	"bytes"
	"fmt"
	"testing"
)

func TestEmptyModelStorage(t *testing.T) {
	root := New()
	testModelStorage(t, root)
}

func TestOneNodeModelStorage(t *testing.T) {
	root := New()
	node := root.GetOrCreateNode("key")
	node.values = append(node.Values(), Value("value"))

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
		node.values = values
		node.lastChangeID = uint64(i * valueCount)
		root.lastChangeID = node.lastChangeID
	}

	testModelStorage(t, root)
}

func testModelStorage(t *testing.T, input *Root) {
	w := bytes.NewBuffer(make([]byte, 0))
	err := input.WriteSnapshot(w)
	if err != nil {
		t.Errorf("WriteSnapshot(): %s", err)
		return
	}

	buffer := w.Bytes()
	r := bytes.NewBuffer(buffer)
	output, err := ReadSnapshot(r)
	if err != nil {
		t.Errorf("ReadSnapshot(): %s", err)
		return
	}

	// len(Nodes)
	if len(input.nodes) != len(output.nodes) {
		t.Errorf("len(Nodes): %d != %d", len(input.nodes), len(output.nodes))
		return
	}

	// LastChangeID
	if input.lastChangeID != output.lastChangeID {
		t.Errorf("LastChangeID: %d != %d", input.lastChangeID, output.lastChangeID)
		return
	}

	// for each node
	for key, inputNode := range input.nodes {
		outputNode := output.GetNode(key)

		// Node must exist
		if outputNode == nil {
			t.Errorf("output.GetNode(\"%s\"): nil", key)
			return
		}

		// node.Key
		if inputNode.key != key || inputNode.key != outputNode.key {
			t.Errorf("Nodes[\"%s\"]: Key: \"%s\" != \"%s\"", key, inputNode.key, outputNode.key)
			return
		}

		// node.LastChangeID
		if inputNode.lastChangeID != outputNode.lastChangeID {
			t.Errorf("Nodes[\"%s\"]: LastChangeID: %d != %d", key, inputNode.lastChangeID, outputNode.lastChangeID)
			return
		}

		// len(node.Values)
		if len(inputNode.values) != len(outputNode.values) {
			t.Errorf("Nodes[\"%s\"]: len(Values) %d != %d", key, len(inputNode.values), len(outputNode.values))
			return
		}

		for i, inputValue := range inputNode.values {
			outputValue := outputNode.values[i]

			// Not null
			if outputValue == nil {
				t.Errorf("Nodes[\"%s\"].Values[%d]: nil", key, i)
				return
			}

			// length
			if len(inputValue) != len(outputValue) {
				t.Errorf("Nodes[\"%s\"].Values[%d]: lenght %d != %d", key, i, len(inputValue), len(outputValue))
				return
			}

			for j := range inputValue {
				if inputValue[j] != outputValue[j] {
					t.Errorf("Nodes[\"%s\"].Values[%d][%d]: %d != %d", key, i, j, inputValue[j], outputValue[j])
					return
				}
			}
		}
	}
}

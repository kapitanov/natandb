package test

import (
	"context"

	"github.com/kapitanov/natandb/pkg/proto"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:   "write",
		Short: "Run a write perf test",
	}
	Command.AddCommand(cmd)

	t := &writePerfTest{}
	testCmd(cmd, t)
}

type writePerfTest struct {
	keys   []string
	values [][]byte
}

func (t *writePerfTest) Name() string {
	return "AddUniqueValue"
}

func (t *writePerfTest) Init(ctx context.Context, client proto.Client, n int) error {
	t.keys = make([]string, n/100)
	for i := 0; i < len(t.keys); i++ {
		t.keys[i] = randomString(8)
	}

	t.values = make([][]byte, n)
	for i := 0; i < len(t.values); i++ {
		t.values[i] = []byte(randomString(32))
	}

	return nil
}

func (t *writePerfTest) Run(ctx context.Context, client proto.Client, low, high int) error {
	for i := low; i < high; i++ {
		key := t.keys[i%len(t.keys)]
		value := t.values[i%len(t.values)]

		request := &proto.AddUniqueValueRequest{Key: key, Value: value}
		_, err := client.AddUniqueValue(ctx, request)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *writePerfTest) Cleanup(ctx context.Context, client proto.Client, n int) error {
	for _, key := range t.keys {
		request := &proto.RemoveKeyRequest{Key: key}
		_, err := client.RemoveKey(ctx, request)
		if err != nil {
			return err
		}
	}

	return nil
}

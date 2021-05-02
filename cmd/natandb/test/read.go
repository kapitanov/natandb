package test

import (
	"context"

	"github.com/kapitanov/natandb/pkg/proto"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:   "read",
		Short: "Run a read perf test",
	}
	Command.AddCommand(cmd)

	t := &readPerfTest{}
	testCmd(cmd, t)
}

type readPerfTest struct {
	key string
}

func (t *readPerfTest) Name() string {
	return "AddUniqueValue"
}

func (t *readPerfTest) Init(ctx context.Context, client proto.Client, n int) error {
	t.key = randomString(8)

	values := make([][]byte, 4)
	for i := 0; i < len(values); i++ {
		values[i] = []byte(randomString(32))
	}

	request := &proto.SetRequest{Key: t.key, Values: values}
	_, err := client.Set(ctx, request)
	if err != nil {
		return err
	}

	return nil
}

func (t *readPerfTest) Run(ctx context.Context, client proto.Client, low, high int) error {
	for i := low; i < high; i++ {
		request := &proto.GetRequest{Key: t.key}
		_, err := client.Get(ctx, request)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *readPerfTest) Cleanup(ctx context.Context, client proto.Client, n int) error {
	request := &proto.DeleteRequest{Key: t.key}
	_, err := client.Delete(ctx, request)
	if err != nil {
		return err
	}

	return nil
}

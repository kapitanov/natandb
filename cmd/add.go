package cmd

import (
	"context"

	"github.com/kapitanov/natandb/pkg/proto"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:     "add <key> <value>",
		Aliases: []string{"add"},
		Short:   "Add a value to a key",
		Args:    cobra.ExactArgs(2),
	}

	rootCmd.AddCommand(cmd)

	clientNodeCommand(cmd, func(args []string, client proto.Client, ctx context.Context, quiet bool) (*proto.Node, error) {
		request := proto.AddValueRequest{
			Key:   args[0],
			Value: []byte(args[1]),
		}
		response, err := client.AddValue(ctx, &request)
		if err != nil {
			log.Printf("unable to execute \"AddValue\": %s", err)
			return nil, err
		}

		return response, nil
	})
}

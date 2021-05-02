package main

import (
	"context"

	"github.com/kapitanov/natandb/pkg/proto"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:     "push <key> <value>",
		Aliases: []string{"add"},
		Short:   "Add a value to a key if it doesn't exist",
		Args:    cobra.ExactArgs(2),
	}

	rootCmd.AddCommand(cmd)

	clientNodeCommand(cmd, func(args []string, client proto.Client, ctx context.Context) (*proto.Node, error) {
		request := proto.AddRequest{
			Key:    args[0],
			Value:  []byte(args[1]),
			Unique: true,
		}
		response, err := client.Add(ctx, &request)
		if err != nil {
			log.Printf("unable to execute \"Add\": %s", err)
			return nil, err
		}
		return response, nil
	})
}

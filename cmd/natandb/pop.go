package main

import (
	"context"

	"github.com/kapitanov/natandb/pkg/proto"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:     "pop <key> <value>",
		Aliases: []string{"add"},
		Short:   "Remove a value from a key if it exists",
		Args:    cobra.ExactArgs(2),
	}

	rootCmd.AddCommand(cmd)

	all := cmd.Flags().BoolP("all", "a", false, "remove all occurrences of value")

	clientNodeCommand(cmd, func(args []string, client proto.Client, ctx context.Context) (*proto.Node, error) {
		var node *proto.Node
		var err error
		request := proto.RemoveRequest{
			Key:   args[0],
			Value: []byte(args[1]),
			All:   *all,
		}
		node, err = client.Remove(ctx, &request)
		if err != nil {
			log.Printf("unable to execute \"Remove\": %s", err)
		}

		return node, err
	})
}

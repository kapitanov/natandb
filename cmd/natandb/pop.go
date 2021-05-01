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

	all := cmd.Flags().BoolP("all", "a", false, "remove all occurences of value")

	clientNodeCommand(cmd, func(args []string, client proto.Client, ctx context.Context) (*proto.Node, error) {
		var node *proto.Node
		var err error
		if *all {
			request := proto.RemoveAllValuesRequest{
				Key:   args[0],
				Value: []byte(args[1]),
			}
			node, err = client.RemoveAllValues(ctx, &request)
			if err != nil {
				log.Printf("unable to execute \"RemoveAllValues\": %s", err)
			}
		} else {
			request := proto.RemoveValueRequest{
				Key:   args[0],
				Value: []byte(args[1]),
			}
			node, err = client.RemoveValue(ctx, &request)
			if err != nil {
				log.Printf("unable to execute \"RemoveValue\": %s", err)
			}
		}

		return node, err
	})
}

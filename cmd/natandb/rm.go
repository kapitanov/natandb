package main

import (
	"context"
	"fmt"
	"os"

	"github.com/kapitanov/natandb/pkg/proto"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:     "rm <key>",
		Aliases: []string{"rm"},
		Short:   "Remove key",
		Args:    cobra.ExactArgs(1),
	}

	rootCmd.AddCommand(cmd)

	clientCommand(cmd, func(args []string, client proto.Client, ctx context.Context) error {
		request := proto.RemoveKeyRequest{
			Key: args[0],
		}

		_, err := client.RemoveKey(ctx, &request)
		if err != nil {
			log.Printf("unable to execute \"RemoveKey\": %s", err)
			return err
		}

		if quiet {
			fmt.Fprintln(os.Stdout, request.Key)
		} else {
			fmt.Printf("Key \"%s\" has been removed", request.Key)
		}

		return nil
	})
}

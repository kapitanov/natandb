package main

import (
	"context"
	"fmt"
	"os"

	"github.com/gosuri/uitable"
	"github.com/kapitanov/natandb/pkg/proto"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:     "ls [<prefix>]",
		Aliases: []string{"ls"},
		Short:   "List keys",
		Args:    cobra.RangeArgs(0, 1),
	}

	rootCmd.AddCommand(cmd)

	skip := cmd.Flags().Uint32P("skip", "s", 0, "items to skip")
	max := cmd.Flags().Uint32P("max", "m", 100, "max items to display")
	token := cmd.Flags().Uint64P("token", "t", 0, "db concurrency token")

	clientCommand(cmd, func(args []string, client proto.Client, ctx context.Context) error {
		request := proto.ListRequest{
			Skip:    *skip,
			Limit:   *max,
			Version: *token,
		}
		if len(args) > 0 {
			request.Prefix = args[0]
		}
		response, err := client.List(ctx, &request)
		if err != nil {
			log.Printf("unable to execute \"List\": %s", err)
			return err
		}

		if quiet {
			for _, node := range response.Nodes {
				fmt.Fprintln(os.Stdout, node.Key)
			}
		} else {
			if response.TotalCount > 0 {
				table := uitable.New()
				table.MaxColWidth = 80
				table.Wrap = true
				table.AddRow("KEY", "VALUES", "VERSION")
				for _, node := range response.Nodes {
					totalBytes := 0
					for _, v := range node.Values {
						totalBytes += len(v)
					}
					table.AddRow(node.Key, fmt.Sprintf("%d bytes (%d items)", totalBytes, len(node.Values)), fmt.Sprintf("%d", node.Version))
				}
				fmt.Printf("%s\n\n", table)
				fmt.Printf("Shown %d keys out of %d\n", len(response.Nodes), response.TotalCount)
			} else {
				fmt.Printf("There are no keys to display\n")
			}
			fmt.Printf("Concurrency token: %d\n", response.Version)
		}

		return nil
	})
}

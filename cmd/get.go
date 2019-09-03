package cmd

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
		Use:     "get <key>",
		Aliases: []string{"get"},
		Short:   "Get key value",
		Args:    cobra.ExactArgs(1),
	}

	rootCmd.AddCommand(cmd)

	clientCommand(cmd, func(args []string, client proto.Client, ctx context.Context) error {
		request := proto.GetRequest{
			Key: args[0],
		}
		response, err := client.GetValue(ctx, &request)
		if err != nil {
			log.Printf("unable to execute \"GetValue\": %s", err)
			return err
		}

		if quiet {
			for _, value := range response.Values {
				_, err := os.Stdout.Write(value)
				if err != nil {
					panic(err)
				}
				fmt.Println()
			}
		} else {
			table := uitable.New()
			table.MaxColWidth = 80
			table.Wrap = true
			table.AddRow("KEY", response.Key)
			for i, value := range response.Values {
				if i == 0 {
					table.AddRow("VALUE", string(value))
				} else {
					table.AddRow("", string(value))
				}
			}
			table.AddRow("VERSION", response.Version)
			fmt.Printf("%s\n", table)
		}

		return nil
	})
}

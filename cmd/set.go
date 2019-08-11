package cmd

import (
	"context"

	"github.com/kapitanov/natandb/pkg/proto"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:     "set <key> <value> [<value>...]",
		Aliases: []string{"set"},
		Short:   "Set key value",
		Args:    cobra.MinimumNArgs(2),
	}

	rootCmd.AddCommand(cmd)

	clientNodeCommand(cmd, func(args []string, client proto.Client, ctx context.Context, quiet bool) (*proto.Node, error) {
		request := proto.SetValueRequest{
			Key:    args[0],
			Values: make([][]byte, 0),
		}

		for _, v := range args[1:] {
			request.Values = append(request.Values, []byte(v))
		}

		response, err := client.SetValue(ctx, &request)
		if err != nil {
			log.Printf("unable to execute \"SetValue\": %s", err)
			return nil, err
		}

		return response, nil
	})
}

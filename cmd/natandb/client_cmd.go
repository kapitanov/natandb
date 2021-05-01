package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/kapitanov/natandb/pkg/proto"
	"github.com/spf13/cobra"
)

type clientCommandFunc = func(args []string, client proto.Client, ctx context.Context) error

func clientCommand(cmd *cobra.Command, callback clientCommandFunc) {
	endpoint := cmd.Flags().StringP("endpoint", "e", "127.0.0.1:18081", "server endpoint")

	cmd.Run = func(c *cobra.Command, args []string) {
		log.Printf("connecting to %s...", *endpoint)
		client, err := proto.NewClient(*endpoint)
		if err != nil {
			log.Printf("unable to connect: %s", err)
			panic(err)
		}

		defer func() {
			err = client.Close()
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		go func() {
			for s := range signals {
				if s == os.Interrupt {
					cancel()
				}
			}
		}()

		err = callback(args, client, ctx)
		signals <- syscall.SIGQUIT
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
		}
	}
}

type nodeCommandFunc = func(args []string, client proto.Client, ctx context.Context) (*proto.Node, error)

func clientNodeCommand(cmd *cobra.Command, callback nodeCommandFunc) {
	clientCommand(cmd, func(args []string, client proto.Client, ctx context.Context) error {
		ctx, cancel := context.WithCancel(context.Background())

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		go func() {
			for s := range signals {
				if s == os.Interrupt {
					cancel()
				}
			}
		}()

		node, err := callback(args, client, ctx)
		signals <- syscall.SIGQUIT
		if err != nil {
			return err
		}

		if quiet {
			fmt.Fprintln(os.Stdout, node.Key)
		} else {
			totalBytes := 0
			for _, v := range node.Values {
				totalBytes += len(v)
			}

			fmt.Printf("Key:     %s\n", node.Key)
			fmt.Printf("Value:   %d bytes (%d items)\n", totalBytes, len(node.Values))
			fmt.Printf("Version: %d\n", node.Version)
		}

		return nil
	})
}

package main

import (
	"context"
	"fmt"
	"github.com/kapitanov/natandb/pkg/proto"
	"io/ioutil"
	l "log"
	"os"
	"os/signal"
	"syscall"

	"github.com/kapitanov/natandb/cmd/natandb/diag"
	"github.com/kapitanov/natandb/cmd/natandb/test"
	pkgLog "github.com/kapitanov/natandb/pkg/log"
	"github.com/spf13/cobra"
)

const (
	// Version contains application version
	Version = "unknown"
)

var log = pkgLog.New("")

var rootCmd = &cobra.Command{
	Use:              "natandb",
	Short:            "NatanDB is a simple key-value database",
	TraverseChildren: true,
}

var quiet bool

// Main is an entry point for CLI application
func main() {
	err := rootCmd.Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(-1)
	}
}

func init() {
	version := rootCmd.PersistentFlags().Bool("version", false, "display version and exit")
	verbose := rootCmd.PersistentFlags().BoolP("verbose", "v", false, "enable verbose logging")
	quietFlag := rootCmd.PersistentFlags().BoolP("quiet", "q", false, "quiet output")

	rootCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		if *version {
			cmd.Printf("v%s\n", Version)
			os.Exit(0)
		}

		quiet = *quietFlag

		if quiet {
			l.SetOutput(ioutil.Discard)
		} else {
			if *verbose {
				pkgLog.SetMinLevel(pkgLog.Verbose)
			} else {
				pkgLog.SetMinLevel(pkgLog.Info)
			}

			l.SetOutput(os.Stderr)
			l.SetFlags(l.Ldate | l.Ltime | l.LUTC)
		}
	}

	rootCmd.Run = func(cmd *cobra.Command, args []string) {
		cmd.Printf("try \"%s --help\" for more information\n", cmd.CommandPath())
		os.Exit(0)
	}

	rootCmd.AddCommand(diag.Command)
	rootCmd.AddCommand(test.Command)
}

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

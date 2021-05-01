package main

import (
	"github.com/kapitanov/natandb/pkg/util"
	"io"
	"os"

	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:    "gen",
		Short:  "Generate CLI autocompletion",
		Hidden: true,
	}
	rootCmd.AddCommand(cmd)

	pwshCmd := &cobra.Command{
		Use:   "pwsh [<path>]",
		Short: "Generate Powershell autocompletion",
		Args:  cobra.RangeArgs(0, 1),
		Run: func(c *cobra.Command, args []string) {

			var w io.Writer
			if len(args) > 0 {
				path := args[0]

				err := util.MkDir(path)
				if err != nil {
					panic(err)
				}

				f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0)
				if err != nil {
					panic(err)
				}

				defer func() {
					f.Close()
				}()

				w = f
			} else {
				w = os.Stdout
			}

			err := rootCmd.GenPowerShellCompletion(w)
			if err != nil {
				panic(err)
			}
		},
	}
	cmd.AddCommand(pwshCmd)

	bashCmd := &cobra.Command{
		Use:   "bash [<path>]",
		Short: "Generate Bash autocompletion",
		Args:  cobra.RangeArgs(0, 1),
		Run: func(c *cobra.Command, args []string) {

			var w io.Writer
			if len(args) > 0 {
				path := args[0]

				err := util.MkDir(path)
				if err != nil {
					panic(err)
				}

				f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0)
				if err != nil {
					panic(err)
				}

				defer func() {
					f.Close()
				}()

				w = f
			} else {
				w = os.Stdout
			}

			err := rootCmd.GenBashCompletion(w)
			if err != nil {
				panic(err)
			}
		},
	}
	cmd.AddCommand(bashCmd)
}

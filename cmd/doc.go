package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"

	"github.com/kapitanov/natandb/pkg/fs"
)

func init() {
	cmd := &cobra.Command{
		Use:    "doc",
		Short:  "Generate CLI help",
		Hidden: true,
	}
	rootCmd.AddCommand(cmd)

	manCmd := &cobra.Command{
		Use:   "man [<dir>]",
		Short: "Generate man pages",
		Args:  cobra.RangeArgs(0, 1),
		Run: func(c *cobra.Command, args []string) {
			path := "./help/man"
			if len(args) > 0 {
				path = args[0]
			}
			err := fs.MkDir(path)
			if err != nil {
				panic(err)
			}

			header := &doc.GenManHeader{
				Title:   "MINE",
				Section: "3",
			}

			err = doc.GenManTree(rootCmd, header, path)
			if err != nil {
				panic(err)
			}

			fmt.Fprintf(os.Stderr, "Generated MAN pages (see \"%s\")\n", path)
		},
	}
	cmd.AddCommand(manCmd)

	mdCmd := &cobra.Command{
		Use:   "md [<dir>]",
		Short: "Generate markdown pages",
		Args:  cobra.RangeArgs(0, 1),
		Run: func(c *cobra.Command, args []string) {
			path := "./help/md"
			if len(args) > 0 {
				path = args[0]
			}
			err := fs.MkDir(path)
			if err != nil {
				panic(err)
			}

			err = doc.GenMarkdownTree(rootCmd, path)
			if err != nil {
				panic(err)
			}

			fmt.Fprintf(os.Stderr, "Generated Markdown documentation (see \"%s\")\n", path)
		},
	}
	cmd.AddCommand(mdCmd)
}

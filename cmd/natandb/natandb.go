package cmd

import (
	"fmt"
	"io/ioutil"
	l "log"
	"os"

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

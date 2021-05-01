package diag

import (
	"os"

	pkgLog "github.com/kapitanov/natandb/pkg/log"
	"github.com/spf13/cobra"
)

var log = pkgLog.New("")

// Command is root for diagnostics commands
var Command = &cobra.Command{
	Use:              "diag",
	Short:            "Diagnostics tools",
	Hidden:           true,
	TraverseChildren: true,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Printf("try \"%s %s --help\" for more information\n", cmd.CommandPath(), cmd.Use)
		os.Exit(0)
	},
}

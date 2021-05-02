package diag

import (
	"fmt"
	"strings"

	"github.com/gosuri/uitable"
	"github.com/spf13/cobra"

	"github.com/kapitanov/natandb/pkg/model"
	"github.com/kapitanov/natandb/pkg/storage"
)

func init() {
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Inspect snapshot file",
	}
	Command.AddCommand(cmd)

	dataDir := cmd.Flags().StringP("data", "d", "./data", "path to data directory")

	cmd.Run = func(c *cobra.Command, args []string) {
		driver, err := storage.NewDriver(storage.DirectoryOption(*dataDir))
		if err != nil {
			log.Printf("unable to init storage driver: %s", err)
			panic(err)
		}

		f, err := driver.SnapshotFile().Read()
		if err != nil {
			log.Printf("unable to read snapshot file: %s", err)
			panic(err)
		}

		defer func() {
			err = f.Close()
			if err != nil {
				log.Printf("unable to close snapshot file: %s", err)
			}
		}()

		root, err := model.ReadSnapshot(f)
		if err != nil {
			log.Printf("unable to read snapshot: %s", err)
			panic(err)
		}

		table := uitable.New()
		table.MaxColWidth = 80
		table.Wrap = true
		table.AddRow("KEY", "VERSION", "VALUE")
		for _, node := range root.NodesMap {
			values := make([]string, len(node.Values))
			for i, v := range node.Values {
				values[i] = fmt.Sprintf("\"%s\"", string(v))
			}

			valueStr := fmt.Sprintf("[ %s ]", strings.Join(values, ", "))
			table.AddRow(node.Key, fmt.Sprintf("%d", node.LastChangeID), valueStr)
		}
		fmt.Println(table)
		fmt.Printf("Version: %d\n", root.LastChangeID)
	}
}

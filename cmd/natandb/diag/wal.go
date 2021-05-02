package diag

import (
	"fmt"
	"io"
	"os"

	"github.com/gosuri/uitable"
	"github.com/spf13/cobra"

	"github.com/kapitanov/natandb/pkg/storage"
)

func init() {
	cmd := &cobra.Command{
		Use:   "wal",
		Short: "Inspect write-ahead file",
	}
	Command.AddCommand(cmd)

	dataDir := cmd.Flags().StringP("data", "d", "./data", "path to data directory")
	min := cmd.Flags().Uint64("min", 0, "min ID to display")
	max := cmd.Flags().Uint64("max", ^uint64(0), "max ID to display")

	cmd.Run = func(c *cobra.Command, args []string) {
		driver, err := storage.NewDriver(storage.DirectoryOption(*dataDir))
		if err != nil {
			log.Printf("unable to init storage driver: %s", err)
			panic(err)
		}

		wal, err := driver.WALFile().Read()
		if err != nil {
			log.Printf("unable to init wal: %s", err)
			panic(err)
		}

		defer func() {
			err = wal.Close()
			if err != nil {
				log.Printf("unable to close wal file: %s", err)
			}
		}()

		records := make([]*storage.WALRecord, 0)

		minID := *min
		maxID := *max

		for {
			record, err := wal.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Printf("unable to read wal file: %s", err)
				os.Exit(1)
				return
			}

			if minID <= record.ID && record.ID <= maxID {
				records = append(records, record)
			}
		}

		table := uitable.New()
		table.MaxColWidth = 80
		table.Wrap = true
		table.AddRow("ID", "TYPE", "KEY", "VALUE")
		for _, r := range records {
			var typeStr string
			switch r.Type {
			case storage.WALNone:
				typeStr = "NONE"
				break
			case storage.WALAddValue:
				typeStr = "ADDVAL"
				break
			case storage.WALRemoveValue:
				typeStr = "RMVAL"
				break
			case storage.WALRemoveKey:
				typeStr = "RMKEY"
				break
			case storage.WALCommitTx:
				typeStr = "COMMIT"
				break
			default:
				typeStr = fmt.Sprintf("[%d]", r.Type)
				break
			}

			valueStr := "NULL"
			if r.Value != nil {
				valueStr = string(r.Value)
			}

			table.AddRow(fmt.Sprintf("%d", r.ID), typeStr, r.Key, valueStr)
		}

		fmt.Println(table)
	}
}

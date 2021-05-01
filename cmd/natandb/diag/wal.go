package diag

import (
	"fmt"
	"os"

	"github.com/gosuri/uitable"
	"github.com/spf13/cobra"

	"github.com/kapitanov/natandb/pkg/storage"
	"github.com/kapitanov/natandb/pkg/writeahead"
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
		driver, err := storage.NewDriver(*dataDir)
		if err != nil {
			log.Printf("unable to init storage driver: %s", err)
			panic(err)
		}

		wal, err := writeahead.NewLog(driver, writeahead.NewSerializer())
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

		records := make([]*writeahead.Record, 0)

		minID := *min
		maxID := *max

		for {
			chunk, err := wal.ReadChunkForward(minID+1, 100)
			if err != nil {
				log.Printf("unable to read wal file: %s", err)
				os.Exit(1)
				return
			}

			for _, r := range chunk {
				minID = r.ID

				if minID <= r.ID && r.ID <= maxID {
					records = append(records, r)
				}
			}

			if chunk.Empty() {
				break
			}
		}

		table := uitable.New()
		table.MaxColWidth = 80
		table.Wrap = true
		table.AddRow("ID", "TYPE", "KEY", "VALUE")
		for _, r := range records {
			var typeStr string
			switch r.Type {
			case writeahead.None:
				typeStr = "NONE"
				break
			case writeahead.AddValue:
				typeStr = "ADDVAL"
				break
			case writeahead.RemoveValue:
				typeStr = "RMVAL"
				break
			case writeahead.RemoveKey:
				typeStr = "RMKEY"
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

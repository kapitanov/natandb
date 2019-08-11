package cmd

import (
	"os"
	"os/signal"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/kapitanov/natandb/pkg/db"
	"github.com/kapitanov/natandb/pkg/proto"
	"github.com/kapitanov/natandb/pkg/storage"
	"github.com/kapitanov/natandb/pkg/writeahead"
)

func init() {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run NatanDB server",
	}
	rootCmd.AddCommand(cmd)

	dataDir := cmd.Flags().StringP("data", "d", "./data", "path to data directory")
	endpoint := cmd.Flags().StringP("listen", "l", "0.0.0.0:18081", "endpoint to listen")

	cmd.Run = func(c *cobra.Command, args []string) {
		walFile, err := storage.NewWriteAheadLogFile(filepath.Join(*dataDir, "journal.bin"))
		if err != nil {
			log.Printf("unable to init wal file: %s", err)
			panic(err)
		}

		snapshotFile, err := storage.NewSnapshotFile(filepath.Join(*dataDir, "snapshot.bin"))
		if err != nil {
			log.Printf("unable to init snapshot file: %s", err)
			panic(err)
		}

		wal, err := writeahead.NewLog(walFile, writeahead.NewSerializer())
		if err != nil {
			log.Printf("unable to init wal: %s", err)
			panic(err)
		}

		engine, err := db.NewEngine(wal, snapshotFile)
		if err != nil {
			log.Printf("unable to init engine: %s", err)
			panic(err)
		}

		server := proto.NewServer(engine, *endpoint)

		err = server.Start()
		if err != nil {
			log.Printf("unable to init api: %s", err)
			panic(err)
		}

		signals := make(chan os.Signal)
		signal.Notify(signals, os.Interrupt, os.Kill)

		_ = <-signals

		err = server.Close()
		if err != nil {
			log.Printf("unable to shutdown api: %s", err)
			panic(err)
		}
	}
}

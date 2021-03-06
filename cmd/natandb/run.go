package main

import (
	"os"
	"os/signal"

	"github.com/spf13/cobra"

	"github.com/kapitanov/natandb/pkg/db"
	"github.com/kapitanov/natandb/pkg/proto"
	"github.com/kapitanov/natandb/pkg/storage"
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
		driver, err := storage.NewDriver(storage.DirectoryOption(*dataDir))
		if err != nil {
			log.Errorf("unable to init storage driver: %s", err)
			panic(err)
		}

		engine, err := db.NewEngine(db.StorageDriverOption(driver), db.EnableBackgroundVacuumOption(true))
		if err != nil {
			log.Errorf("unable to init engine: %s", err)
			panic(err)
		}

		defer func() {
			err := engine.Close()
			if err != nil {
				log.Errorf("unable to shutdown engine: %s", err)
				panic(err)
			}
		}()

		server := proto.NewServer(engine, *endpoint)

		err = server.Start()
		if err != nil {
			log.Errorf("unable to init server: %s", err)
			panic(err)
		}

		defer func() {
			err := server.Close()
			if err != nil {
				log.Errorf("unable to shutdown engine: %s", err)
				panic(err)
			}
		}()

		signals := make(chan os.Signal)
		signal.Notify(signals, os.Interrupt, os.Kill)

		_ = <-signals
	}
}

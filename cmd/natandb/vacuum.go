package main

import (
	"github.com/spf13/cobra"

	"github.com/kapitanov/natandb/pkg/db"
	"github.com/kapitanov/natandb/pkg/storage"
)

func init() {
	cmd := &cobra.Command{
		Use:   "vacuum",
		Short: "Run a \"vacuum\" routine on NatanDB server",
	}
	rootCmd.AddCommand(cmd)

	dataDir := cmd.Flags().StringP("data", "d", "./data", "path to data directory")

	cmd.Run = func(c *cobra.Command, args []string) {
		driver, err := storage.NewDriver(storage.DirectoryOption(*dataDir))
		if err != nil {
			log.Errorf("unable to init storage driver: %s", err)
			panic(err)
		}

		engine, err := db.NewEngine(db.StorageDriverOption(driver))
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

		err = engine.Vacuum()
		if err != nil {
			log.Errorf("vacuum routine failed: %s", err)
			panic(err)
		}
	}
}

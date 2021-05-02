package db

import (
	"sync"
	"time"

	l "github.com/kapitanov/natandb/pkg/log"
	"github.com/kapitanov/natandb/pkg/model"
	"github.com/kapitanov/natandb/pkg/storage"
)

var log = l.New("engine")

const (
	vacuumPeriod = 30 * time.Hour
)

type engine struct {
	Model      *model.Root
	ModelLock  *sync.Mutex
	WAL        storage.WALWriter
	Storage    storage.Driver
	IsShutDown bool
}

type engineOptions struct {
	driver                 storage.Driver
	enableBackgroundVacuum bool
}

// Option is a configuration option of NewEngine()
type Option func(*engineOptions)

// StorageDriverOption sets a storage driver instance
func StorageDriverOption(driver storage.Driver) Option {
	return func(opts *engineOptions) {
		opts.driver = driver
	}
}

// EnableBackgroundVacuumOption turn background vacuum on and off
func EnableBackgroundVacuumOption(enable bool) Option {
	return func(opts *engineOptions) {
		opts.enableBackgroundVacuum = enable
	}
}

// NewEngine creates new instance of DB engine
func NewEngine(options ...Option) (Engine, error) {
	opts := &engineOptions{}
	for _, f := range options {
		f(opts)
	}

	log.Verbosef("initializing engine")
	root, err := model.Restore(opts.driver)
	if err != nil {
		return nil, err
	}

	log.Verbosef("opening wal file")
	wal, err := opts.driver.WALFile().Write()
	if err != nil {
		return nil, err
	}

	engine := &engine{
		Model:     root,
		ModelLock: new(sync.Mutex),
		WAL:       wal,
		Storage:   opts.driver,
	}

	if opts.enableBackgroundVacuum {
		engine.runBackgroundVacuum()
	}

	log.Printf("engine is initialized")

	return engine, nil
}

// BeginTx starts new transaction
func (e *engine) BeginTx() (TX, error) {
	e.ModelLock.Lock()

	if e.IsShutDown {
		e.ModelLock.Unlock()
		return nil, ErrShutdown
	}

	err := e.WAL.BeginTx()
	if err != nil {
		return nil, err
	}

	tx := newTransaction(e)
	return tx, nil
}

// EndTx is called when a transaction is terminated
func (e *engine) EndTx() {
	e.ModelLock.Unlock()
}

// Tx executes a function within a transaction
func (e *engine) Tx(fn func(tx TX) error) error {
	tx, err := e.BeginTx()
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Close()
	}()

	err = fn(tx)
	if err != nil {
		return err
	}

	tx.Commit()
	return nil
}

// Vacuum performs DB maintenance routine
func (e *engine) Vacuum() error {
	return e.Tx(func(tx TX) error {
		log.Printf("compressing database")

		// Write a model snapshot
		file, err := e.Storage.SnapshotFile().Write()
		if err != nil {
			return err
		}
		err = e.Model.WriteSnapshot(file)
		if err != nil {
			_ = file.Close()
			return err
		}
		err = file.Close()
		if err != nil {
			return err
		}

		// Close WAL transaction and shut down WAL
		err = e.WAL.CommitTx()
		if err != nil {
			return err
		}
		err = e.WAL.Close()
		if err != nil {
			return err
		}

		// Clear WAL file
		vacuum, err := e.Storage.WALFile().BeginVacuum(e.WAL)
		if err != nil {
			return err
		}

		// Create new WAL file
		e.WAL, err = vacuum.End()
		if err != nil {
			return err
		}

		// Dump model state to WAL
		err = e.Model.WriteToWAL(e.WAL)
		if err != nil {
			return err
		}
		err = e.WAL.Close()
		if err != nil {
			return err
		}

		// Reload engine state
		e.Model, err = model.Restore(e.Storage)
		if err != nil {
			return err
		}
		e.WAL, err = e.Storage.WALFile().Write()
		if err != nil {
			return err
		}

		// Start new WAL transaction
		err = e.WAL.BeginTx()
		if err != nil {
			return err
		}

		log.Printf("database compression completed")
		return nil
	})
}

// runBackgroundVacuum runs Vacuum routine in background
func (e *engine) runBackgroundVacuum() {
	go func() {
		for {
			time.Sleep(vacuumPeriod)

			err := e.Vacuum()
			if err != nil {
				if err == ErrShutdown {
					return
				}

				panic(err)
			}
		}
	}()
}

// Close shuts engine down gracefully
func (e *engine) Close() error {
	err := e.WAL.Close()
	if err != nil {
		return err
	}

	file, err := e.Storage.SnapshotFile().Write()
	if err != nil {
		return err
	}

	defer func() {
		_ = file.Close()
	}()

	err = e.Model.WriteSnapshot(file)
	if err != nil {
		return err
	}

	return nil
}

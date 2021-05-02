package db

import (
	"sync"

	l "github.com/kapitanov/natandb/pkg/log"
	"github.com/kapitanov/natandb/pkg/model"
	"github.com/kapitanov/natandb/pkg/storage"
)

var log = l.New("engine")

type engine struct {
	Model      *model.Root
	ModelLock  *sync.Mutex
	WAL        storage.WALWriter
	Storage    storage.Driver
	IsShutDown bool
}

// NewEngine creates new instance of DB engine
func NewEngine(driver storage.Driver) (Engine, error) {
	log.Verbosef("initializing engine")
	root, err := model.Restore(driver)
	if err != nil {
		return nil, err
	}

	wal, err := driver.WALFile().Write()
	if err != nil {
		return nil, err
	}

	engine := &engine{
		Model:     root,
		ModelLock: new(sync.Mutex),
		WAL:       wal,
		Storage:   driver,
	}

	// TODO bg model flush
	// TODO bg wal compression

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

// Close shuts engine down gracefully
func (e *engine) Close() error {
	err := e.WAL.Close()

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

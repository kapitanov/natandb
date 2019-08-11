package storage

// ----------------------------------------------------------------------------
// SnapshotFile implementation
// ----------------------------------------------------------------------------
// There are 3 snapshot files:
// 1. "snapshot.dat" - an actual snapshot file,
// 2. "snapshot.sum" - a hash of current snapshot,
// 3. "snapshot.tmp" - a pending snapshot file.
//
// READ routine
// ^^^^^^^^^^^^
//
// READ routine always reads "snapshot.dat" file.
// If there's no such file - an empty reader is returned.
//
// WRITE routine
// ^^^^^^^^^^^^^
//
// WRITE routine consists of the following steps:
// 1. Routine deletes "snapshot.tmp" if exists.
// 2. Routine creates new file "snapshot.work" and opens it for writing.
// 3. Once write operation is completed, new "snapshot.sum" file is created.
//    It contains a hash of newly created "snapshot.tmp".
// 4. Then a "snapshot.dat" is deleted if exists.
// 5. Content of "snapshot.tmp" is copied to "snapshot.dat".
// 6. "snapshot.tmp" file is deleted.
//
// INIT routine
// ^^^^^^^^^^^^
//
// Algorithm of INIT routine is show below:
// 1. If "snapshot.sum" exists, hash value is being read from it.
// 2. If "snapshot.dat" exists and it matches hash value (or no hash value is available),
//    then it's considered valid.
// 3. Otherwise a similar check is performed against "snapshot.tmp" file.
//    If "snapshot.tmp" exists and matches hash value (or no hash value is available),
//    its content is copied to "snapshot.dat", possibly overwritting it.
// 4. If "snapshot.tmp" exists, it's deleted.
//
// Only one of READ or WRITE routines can be run at the same moment.
// INIT routine is executed only during creation of SnapshotFile.
//
// This implementation allows to handle various failures without losing any data.
//
// ----------------------------------------------------------------------------

import (
	"io"
	"os"
	"path/filepath"

	"github.com/kapitanov/natandb/pkg/fs"
)

type snapshotFileImpl struct {
	path string
}

// NewSnapshotFile creates an instance of SnapshotFile based on physical files
func NewSnapshotFile(filePath string) (SnapshotFile, error) {
	filePath, err := filepath.Abs(filePath)
	if err != nil {
		log.Printf("storage: malformed path \"%s\": %s", filePath, err)
		return nil, err
	}

	log.Printf("storage: using path \"%s\"", filePath)
	directoryPath := filepath.Dir(filePath)
	err = fs.MkDir(directoryPath)
	if err != nil {
		log.Printf("storage: error! %s", err)
		return nil, err
	}

	impl := snapshotFileImpl{filePath}
	return &impl, nil
}

// Read opens data snapshot file file for reading
func (impl *snapshotFileImpl) Read() (io.ReadCloser, error) {
	f, err := os.OpenFile(impl.path, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Printf("storage: unable to open file \"%s\" for reading: %s", impl.path, err)
		return nil, err
	}

	return f, nil
}

// Write opens data snapshot file for writing
func (impl *snapshotFileImpl) Write() (io.WriteCloser, error) {
	f, err := os.OpenFile(impl.path, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Printf("storage: unable to open file \"%s\" for writing: %s", impl.path, err)
		return nil, err
	}

	return f, nil
}

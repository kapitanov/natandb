package fs

import (
	"os"

	"path/filepath"

	l "github.com/kapitanov/natandb/pkg/log"
)

var log = l.New("fs")

// MkDir ensures that specified directory exists
func MkDir(path string) error {
	if path == "." {
		log.Verbosef("mkdir \"%s\": skipped", path)
		return nil
	}

	parent := filepath.Dir(path)
	if parent != path {
		err := MkDir(parent)
		if err != nil {
			return err
		}
	}

	err := os.Mkdir(path, 0)
	if err != nil {
		if err == os.ErrExist || dirExists(path) {
			return nil
		}

		log.Verbosef("mkdir \"%s\": failed (%s)", path, err)
		return err
	}

	return nil
}

func dirExists(path string) bool {
	s, e := os.Stat(path)
	return e == nil && s.IsDir()
}

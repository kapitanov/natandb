package fs

import (
	"os"

	"path/filepath"
)

// MkDir ensures that specified directory exists
func MkDir(path string) error {
	if path == "." {
		return nil
	}

	parent := filepath.Dir(path)
	err := MkDir(parent)
	if err != nil {
		return err
	}

	for {
		err = os.Mkdir(path, 0)
		if err != nil && err != os.ErrExist {
			s, e := os.Stat(path)
			if e == nil && s.IsDir() {
				return nil
			}

			return err
		}
	}
}

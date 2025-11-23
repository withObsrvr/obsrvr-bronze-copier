package util

import "os"

func EnsureDir(path string) error {
	return os.MkdirAll(path, 0755)
}

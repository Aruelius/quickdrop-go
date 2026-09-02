//go:build !windows

package transfer

import "os"

func replaceFile(source, destination string) error { return os.Rename(source, destination) }

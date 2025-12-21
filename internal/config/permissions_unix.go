//go:build unix

package config

import (
	"fmt"
	"os"
)

// checkFilePermissions returns a warning if the config file is readable by group or others.
func checkFilePermissions(path string) string {
	info, err := os.Stat(path)
	if err != nil {
		return "" // Can't check, skip warning
	}

	mode := info.Mode().Perm()

	// Check if group or others have any access (should be 0600 or stricter)
	if mode&0077 != 0 {
		return fmt.Sprintf(
			"WARNING: Config file '%s' has insecure permissions (%04o)\n"+
				"         Other users may be able to read your database credentials.\n"+
				"         Run: chmod 600 %s\n\n",
			path, mode, path,
		)
	}
	return ""
}

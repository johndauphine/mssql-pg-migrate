//go:build windows

package config

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// checkFilePermissions returns a warning if the config file may be readable by other users.
func checkFilePermissions(path string) string {
	// Check if file exists
	if _, err := os.Stat(path); err != nil {
		return ""
	}

	// Use icacls to check permissions
	cmd := exec.Command("icacls", path)
	output, err := cmd.Output()
	if err != nil {
		return "" // Can't check, skip
	}

	outputStr := strings.ToLower(string(output))

	// Check for common insecure permission patterns
	insecurePatterns := []string{
		"everyone",
		"authenticated users",
		"users",
		"builtin\\users",
	}

	for _, pattern := range insecurePatterns {
		if strings.Contains(outputStr, pattern) {
			return fmt.Sprintf(
				"WARNING: Config file '%s' may have insecure permissions\n"+
					"         Other users may be able to read your database credentials.\n"+
					"         Run in PowerShell to secure:\n"+
					"         icacls \"%s\" /inheritance:r /grant:r \"%%USERNAME%%:F\"\n\n",
				path, path,
			)
		}
	}
	return ""
}

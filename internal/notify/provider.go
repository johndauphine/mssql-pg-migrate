package notify

import "time"

// Provider defines the notification contract for migration events.
// This interface allows for different notification backends (Slack, email, etc.)
// and enables easier testing through mock implementations.
type Provider interface {
	// MigrationStarted sends notification when migration starts.
	MigrationStarted(runID, sourceDB, targetDB string, tableCount int) error

	// MigrationCompleted sends notification when migration completes successfully.
	MigrationCompleted(runID string, startTime time.Time, duration time.Duration, tableCount int, rowCount int64, throughput float64) error

	// MigrationFailed sends notification when migration fails.
	MigrationFailed(runID string, err error, duration time.Duration) error

	// MigrationCompletedWithErrors sends notification when migration completes with some table failures.
	MigrationCompletedWithErrors(runID string, startTime time.Time, duration time.Duration, successTables int, failedTables int, rowCount int64, throughput float64, failures []string) error

	// TableTransferFailed sends notification for individual table failures.
	TableTransferFailed(runID, tableName string, err error) error
}

// Ensure Notifier implements Provider
var _ Provider = (*Notifier)(nil)

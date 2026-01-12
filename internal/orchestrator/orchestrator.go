package orchestrator

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/johndauphine/mssql-pg-migrate/internal/checkpoint"
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/logging"
	"github.com/johndauphine/mssql-pg-migrate/internal/notify"
	"github.com/johndauphine/mssql-pg-migrate/internal/pool"
	"github.com/johndauphine/mssql-pg-migrate/internal/progress"
	"github.com/johndauphine/mssql-pg-migrate/internal/source"
	"github.com/johndauphine/mssql-pg-migrate/internal/target"
	"github.com/johndauphine/mssql-pg-migrate/internal/transfer"
)

// TaskType defines the type of migration task
type TaskType string

const (
	TaskExtractSchema  TaskType = "extract_schema"
	TaskCreateTables   TaskType = "create_tables"
	TaskTransfer       TaskType = "transfer"
	TaskResetSequences TaskType = "reset_sequences"
	TaskCreatePKs      TaskType = "create_pks"
	TaskCreateIndexes  TaskType = "create_indexes"
	TaskCreateFKs      TaskType = "create_fks"
	TaskCreateChecks   TaskType = "create_checks"
	TaskValidate       TaskType = "validate"
)

// TableFailure records a table transfer failure
type TableFailure struct {
	TableName string
	Error     error
}

// Orchestrator coordinates the migration process
type Orchestrator struct {
	config     *config.Config
	sourcePool pool.SourcePool
	targetPool pool.TargetPool
	state      checkpoint.StateBackend
	progress   *progress.Tracker
	notifier   notify.Provider
	tables     []source.Table
	runProfile string
	runConfig  string
	opts       Options
	targetMode TargetModeStrategy
}

// Options configures the orchestrator.
type Options struct {
	// StateFile overrides SQLite with a YAML state file (for Airflow).
	// If empty, uses SQLite in DataDir.
	StateFile string

	// RunID allows specifying a deterministic run ID (for Airflow).
	// If empty, a UUID is generated.
	RunID string

	// ForceResume bypasses config hash validation on resume.
	ForceResume bool
}

// computeConfigHash returns a short hex hash of the sanitized config.
func computeConfigHash(cfg *config.Config) string {
	configJSON, _ := json.Marshal(cfg.Sanitized())
	hash := sha256.Sum256(configJSON)
	return hex.EncodeToString(hash[:8])
}

// isRetryableError determines if an error is transient and worth retrying.
// This includes connection errors, timeouts, and deadlocks.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"connection reset",
		"connection refused",
		"connection timed out",
		"deadlock",
		"lock timeout",
		"too many connections",
		"server is shutting down",
		"broken pipe",
		"unexpected eof",
		"i/o timeout",
		"context deadline exceeded",
		"retry",
	}
	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}
	return false
}

// MigrationResult contains the outcome of a migration run.
type MigrationResult struct {
	RunID           string        `json:"run_id"`
	Status          string        `json:"status"`
	StartedAt       time.Time     `json:"started_at"`
	CompletedAt     time.Time     `json:"completed_at"`
	DurationSeconds float64       `json:"duration_seconds"`
	TablesTotal     int           `json:"tables_total"`
	TablesSuccess   int           `json:"tables_success"`
	TablesFailed    int           `json:"tables_failed"`
	RowsTransferred int64         `json:"rows_transferred"`
	RowsPerSecond   int64         `json:"rows_per_second"`
	FailedTables    []string      `json:"failed_tables"`
	TableStats      []TableResult `json:"table_stats"`
	Error           string        `json:"error,omitempty"`
}

// TableResult contains the outcome for a single table.
type TableResult struct {
	Name   string `json:"name"`
	Rows   int64  `json:"rows"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// StatusResult contains the current status of a migration.
type StatusResult struct {
	RunID           string    `json:"run_id"`
	Status          string    `json:"status"`
	Phase           string    `json:"phase"`
	StartedAt       time.Time `json:"started_at"`
	TablesTotal     int       `json:"tables_total"`
	TablesComplete  int       `json:"tables_complete"`
	TablesRunning   int       `json:"tables_running"`
	TablesPending   int       `json:"tables_pending"`
	TablesFailed    int       `json:"tables_failed"`
	RowsTransferred int64     `json:"rows_transferred"`
	ProgressPercent float64   `json:"progress_percent"`
}

// HealthCheckResult contains connection health information.
type HealthCheckResult struct {
	Timestamp        string `json:"timestamp"`
	SourceConnected  bool   `json:"source_connected"`
	SourceLatencyMs  int64  `json:"source_latency_ms"`
	SourceDBType     string `json:"source_db_type"`
	SourceTableCount int    `json:"source_table_count,omitempty"`
	SourceError      string `json:"source_error,omitempty"`
	TargetConnected  bool   `json:"target_connected"`
	TargetLatencyMs  int64  `json:"target_latency_ms"`
	TargetDBType     string `json:"target_db_type"`
	TargetError      string `json:"target_error,omitempty"`
	Healthy          bool   `json:"healthy"`
}

// DryRunResult contains the migration plan preview.
type DryRunResult struct {
	SourceType     string        `json:"source_type"`
	TargetType     string        `json:"target_type"`
	SourceSchema   string        `json:"source_schema"`
	TargetSchema   string        `json:"target_schema"`
	Tables         []DryRunTable `json:"tables"`
	TotalRows      int64         `json:"total_rows"`
	TotalTables    int           `json:"total_tables"`
	EstimatedMemMB int64         `json:"estimated_memory_mb"`
	Workers        int           `json:"workers"`
	ChunkSize      int           `json:"chunk_size"`
	TargetMode     string        `json:"target_mode"`
}

// DryRunTable contains preview information for a single table.
type DryRunTable struct {
	Name             string `json:"name"`
	RowCount         int64  `json:"row_count"`
	PaginationMethod string `json:"pagination_method"`
	Partitions       int    `json:"partitions"`
	HasPK            bool   `json:"has_pk"`
	Columns          int    `json:"columns"`
}

// New creates a new orchestrator with default options (SQLite state).
func New(cfg *config.Config) (*Orchestrator, error) {
	return NewWithOptions(cfg, Options{})
}

// NewWithOptions creates a new orchestrator with custom options.
func NewWithOptions(cfg *config.Config, opts Options) (*Orchestrator, error) {
	// Determine max connections based on source/target types
	maxSourceConns := cfg.Migration.MaxMssqlConnections
	maxTargetConns := cfg.Migration.MaxPgConnections
	if cfg.Source.Type == "postgres" {
		maxSourceConns = cfg.Migration.MaxPgConnections
	}
	if cfg.Target.Type == "mssql" {
		maxTargetConns = cfg.Migration.MaxMssqlConnections
	}

	// Create source pool using factory
	sourcePool, err := pool.NewSourcePool(&cfg.Source, maxSourceConns)
	if err != nil {
		return nil, fmt.Errorf("creating source pool: %w", err)
	}

	// Create target pool using factory
	targetPool, err := pool.NewTargetPool(&cfg.Target, maxTargetConns, cfg.Migration.MSSQLRowsPerBatch, cfg.Source.Type)
	if err != nil {
		sourcePool.Close()
		return nil, fmt.Errorf("creating target pool: %w", err)
	}

	// Create state manager based on options
	var state checkpoint.StateBackend
	if opts.StateFile != "" {
		// Use file-based state (for Airflow/headless)
		state, err = checkpoint.NewFileState(opts.StateFile)
		if err != nil {
			sourcePool.Close()
			targetPool.Close()
			return nil, fmt.Errorf("creating file state manager: %w", err)
		}
	} else {
		// Use SQLite state (default for desktop)
		sqliteState, err := checkpoint.New(cfg.Migration.DataDir)
		if err != nil {
			sourcePool.Close()
			targetPool.Close()
			return nil, fmt.Errorf("creating state manager: %w", err)
		}

		// Cleanup old runs based on retention policy
		retentionDays := cfg.Migration.HistoryRetentionDays
		if retentionDays <= 0 {
			retentionDays = 30 // Default
		}
		if deleted, cleanupErr := sqliteState.CleanupOldRuns(retentionDays); cleanupErr != nil {
			logging.Warn("History cleanup failed: %v", cleanupErr)
		} else if deleted > 0 {
			logging.Info("Cleaned up %d old migration runs (retention: %d days)", deleted, retentionDays)
		}

		state = sqliteState
	}

	// Create notifier
	notifier := notify.New(&cfg.Slack)

	// Create target mode strategy
	targetModeStrategy := NewTargetModeStrategy(
		cfg.Migration.TargetMode,
		targetPool,
		cfg.Target.Schema,
		cfg.Migration.CreateIndexes,
		cfg.Migration.CreateForeignKeys,
		cfg.Migration.CreateCheckConstraints,
	)

	return &Orchestrator{
		config:     cfg,
		sourcePool: sourcePool,
		targetPool: targetPool,
		state:      state,
		progress:   progress.New(),
		notifier:   notifier,
		opts:       opts,
		targetMode: targetModeStrategy,
	}, nil
}

// Close releases all resources
func (o *Orchestrator) Close() {
	o.sourcePool.Close()
	o.targetPool.Close()
	o.state.Close()
}

// SetRunContext sets metadata for the current run (profile name or config path).
func (o *Orchestrator) SetRunContext(profileName, configPath string) {
	o.runProfile = profileName
	o.runConfig = configPath
}

// SetProgressReporter configures JSON progress reporting for Airflow/automation.
// When enabled, disables the terminal progress bar and emits JSON updates to stderr.
func (o *Orchestrator) SetProgressReporter(reporter progress.Reporter, interval time.Duration) {
	o.progress.SetReporter(reporter, interval)
}

// Run executes a new migration.
func (o *Orchestrator) Run(ctx context.Context) error {
	// Use provided run ID or generate a new one
	runID := o.opts.RunID
	if runID == "" {
		runID = uuid.New().String()[:8]
	}
	startTime := time.Now()
	logging.Info("Starting migration run: %s", runID)
	logging.Info("Migration: %s -> %s", o.sourcePool.DBType(), o.targetPool.DBType())

	// Log comprehensive configuration dump (always visible at INFO level)
	logging.Info("%s", o.config.DebugDump())

	// Set runtime memory limit using Go's soft limit mechanism
	// This tells the GC to work harder to stay under the limit
	effectiveMemMB := o.config.AutoConfig().EffectiveMaxMemoryMB
	if effectiveMemMB > 0 {
		memLimitBytes := effectiveMemMB * 1024 * 1024
		debug.SetMemoryLimit(memLimitBytes)
		logging.Info("Runtime memory limit set to %d MB (Go GC soft limit)", effectiveMemMB)
	}

	if err := o.state.CreateRun(runID, o.config.Source.Schema, o.config.Target.Schema, o.config.Sanitized(), o.runProfile, o.runConfig); err != nil {
		return fmt.Errorf("creating run: %w", err)
	}

	// Extract schema
	o.progress.SetPhase("extracting_schema")
	logging.Info("Extracting schema...")
	tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
	if err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("extracting schema: %w", err)
	}

	// Load additional metadata if enabled
	for i := range tables {
		t := &tables[i]

		if o.config.Migration.CreateIndexes {
			if err := o.sourcePool.LoadIndexes(ctx, t); err != nil {
				logging.Warn("Warning: loading indexes for %s: %v", t.Name, err)
			}
		}

		if o.config.Migration.CreateForeignKeys {
			if err := o.sourcePool.LoadForeignKeys(ctx, t); err != nil {
				logging.Warn("Warning: loading FKs for %s: %v", t.Name, err)
			}
		}

		if o.config.Migration.CreateCheckConstraints {
			if err := o.sourcePool.LoadCheckConstraints(ctx, t); err != nil {
				logging.Warn("Warning: loading check constraints for %s: %v", t.Name, err)
			}
		}
	}

	// Apply table filters
	tables = o.filterTables(tables)
	if len(tables) == 0 {
		o.state.CompleteRun(runID, "failed", "no tables to migrate after applying filters")
		return fmt.Errorf("no tables to migrate after applying filters")
	}

	o.tables = tables
	o.progress.SetTablesTotal(len(tables))
	logging.Info("Found %d tables", len(tables))

	// Refine memory settings based on actual row sizes from database stats
	tableRowSizes := make([]config.TableRowSize, len(tables))
	for i, t := range tables {
		tableRowSizes[i] = config.TableRowSize{
			Name:             t.Name,
			RowCount:         t.RowCount,
			EstimatedRowSize: t.EstimatedRowSize,
		}
	}
	if adjusted, changes := o.config.RefineSettingsForRowSizes(tableRowSizes); adjusted {
		logging.Info("%s", changes)
	} else if changes != "" {
		logging.Debug("%s", changes)
	}

	// Print pagination strategy summary
	keysetCount := 0
	rowNumberCount := 0
	for _, t := range tables {
		if t.SupportsKeysetPagination() {
			keysetCount++
		} else if t.HasPK() {
			rowNumberCount++
		}
	}
	logging.Debug("Pagination: %d keyset, %d ROW_NUMBER, %d no PK",
		keysetCount, rowNumberCount, len(tables)-keysetCount-rowNumberCount)

	// Send start notification
	o.notifier.MigrationStarted(runID, o.config.Source.Database, o.config.Target.Database, len(tables))

	// Create target schema and tables
	o.progress.SetPhase("creating_tables")
	if err := o.targetPool.CreateSchema(ctx, o.config.Target.Schema); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("creating schema: %w", err)
	}

	// Prepare target tables using the appropriate strategy
	if err := o.targetMode.PrepareTables(ctx, tables); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	// Transfer data
	o.progress.SetPhase("transfer")
	logging.Info("Transferring data...")
	o.state.UpdatePhase(runID, "transferring")
	tableFailures, err := o.transferAll(ctx, runID, tables, false)
	if err != nil {
		// If context was canceled (Ctrl+C), leave run as "running" so resume works
		// but reset any "running" tasks to "pending" so status shows correctly
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			o.state.MarkRunAsResumed(runID) // Reset running tasks to pending
			logging.Info("Migration interrupted - run 'resume' to continue")
			return fmt.Errorf("transferring data: %w", err)
		}
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("transferring data: %w", err)
	}

	// Log summary of table failures (individual failures already logged)
	if len(tableFailures) > 0 {
		logging.Warn("%d table(s) failed during transfer:", len(tableFailures))
		for _, f := range tableFailures {
			logging.Warn("  - %s: %v", f.TableName, f.Error)
		}
	}

	// Filter out failed tables for finalize/validate
	failedTableNames := make(map[string]bool)
	for _, f := range tableFailures {
		failedTableNames[f.TableName] = true
	}
	var successTables []source.Table
	for _, t := range tables {
		if !failedTableNames[t.Name] {
			successTables = append(successTables, t)
		}
	}

	// Finalize (only for successful tables)
	o.progress.SetPhase("finalizing")
	logging.Info("Finalizing...")
	o.state.UpdatePhase(runID, "finalizing")
	if err := o.targetMode.Finalize(ctx, successTables); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("finalizing: %w", err)
	}

	// Validate (only for successful tables)
	o.progress.SetPhase("validating")
	logging.Info("Validating...")
	o.state.UpdatePhase(runID, "validating")
	o.tables = successTables // Update for validation
	if err := o.Validate(ctx); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	// Sample validation if enabled
	if o.config.Migration.SampleValidation {
		logging.Info("Running sample validation...")
		if err := o.validateSamples(ctx); err != nil {
			logging.Warn("Warning: sample validation failed: %v", err)
		}
	}

	// Calculate stats for notification
	duration := time.Since(startTime)
	var totalRows int64
	for _, t := range successTables {
		totalRows += t.RowCount
	}
	throughput := float64(totalRows) / duration.Seconds()

	// Determine final status and send appropriate notification
	if len(tableFailures) > 0 {
		// Partial success
		failureNames := make([]string, len(tableFailures))
		for i, f := range tableFailures {
			failureNames[i] = f.TableName
		}
		o.state.CompleteRun(runID, "partial", fmt.Sprintf("%d tables failed", len(tableFailures)))
		o.notifier.MigrationCompletedWithErrors(runID, startTime, duration,
			len(successTables), len(tableFailures), totalRows, throughput, failureNames)
		logging.Warn("Migration completed with errors: %d tables succeeded, %d tables failed, %d rows in %s (%.0f rows/sec)",
			len(successTables), len(tableFailures), totalRows, duration.Round(time.Second), throughput)
	} else {
		// Full success
		o.state.CompleteRun(runID, "success", "")
		o.notifier.MigrationCompleted(runID, startTime, duration, len(tables), totalRows, throughput)
		logging.Info("Migration complete: %d tables, %d rows in %s (%.0f rows/sec)",
			len(tables), totalRows, duration.Round(time.Second), throughput)
	}

	// Log identifier changes for PostgreSQL targets
	if o.config.Target.Type == "postgres" {
		o.logPGIdentifierChanges(tables)
	}

	return nil
}

// logPGIdentifierChanges logs any identifier name changes applied during PostgreSQL migration
func (o *Orchestrator) logPGIdentifierChanges(tables []source.Table) {
	// Convert to TableInfo interface slice
	tableInfos := make([]target.TableInfo, len(tables))
	for i := range tables {
		tableInfos[i] = &tables[i]
	}

	report := target.CollectPGIdentifierChanges(tableInfos)
	if !report.HasChanges() {
		return
	}

	logging.Info("")
	logging.Info("PostgreSQL identifier changes applied:")

	for _, tc := range report.Tables {
		if tc.HasTableChange {
			logging.Info("  Table: '%s' → '%s'", tc.TableName.Original, tc.TableName.Sanitized)
		}
		for _, cc := range tc.ColumnChanges {
			tableName := tc.TableName.Sanitized
			if !tc.HasTableChange {
				tableName = tc.TableName.Original
			}
			logging.Info("    %s: column '%s' → '%s'", tableName, cc.Original, cc.Sanitized)
		}
	}

	logging.Info("")
	logging.Info("Summary: %d table(s) renamed, %d column(s) renamed across %d table(s)",
		report.TotalTableChanges, report.TotalColumnChanges, report.TablesWithChanges)
}

// notifyFailure sends a failure notification
func (o *Orchestrator) notifyFailure(runID string, err error, duration time.Duration) {
	o.notifier.MigrationFailed(runID, err, duration)
}

// markTableComplete marks a table transfer task as complete
func (o *Orchestrator) markTableComplete(runID, taskKey string) {
	o.state.MarkTaskComplete(runID, taskKey)
}

// filterTables filters tables based on include/exclude patterns
func (o *Orchestrator) filterTables(tables []source.Table) []source.Table {
	include := o.config.Migration.IncludeTables
	exclude := o.config.Migration.ExcludeTables

	// If no filters configured, return all tables
	if len(include) == 0 && len(exclude) == 0 {
		return tables
	}

	var filtered []source.Table
	var skipped []string

	for _, t := range tables {
		tableName := strings.ToLower(t.Name)

		// Check include patterns (if specified, table must match at least one)
		if len(include) > 0 {
			matched := false
			for _, pattern := range include {
				if match, _ := filepath.Match(strings.ToLower(pattern), tableName); match {
					matched = true
					break
				}
			}
			if !matched {
				skipped = append(skipped, t.Name)
				continue
			}
		}

		// Check exclude patterns (table must not match any)
		excluded := false
		for _, pattern := range exclude {
			if match, _ := filepath.Match(strings.ToLower(pattern), tableName); match {
				excluded = true
				skipped = append(skipped, t.Name)
				break
			}
		}
		if excluded {
			continue
		}

		filtered = append(filtered, t)
	}

	if len(skipped) > 0 {
		logging.Debug("Skipped %d tables by filter: %v", len(skipped), skipped)
	}

	return filtered
}

func (o *Orchestrator) transferAll(ctx context.Context, runID string, tables []source.Table, resume bool) ([]TableFailure, error) {
	var jobs []transfer.Job

	// Create progress saver for chunk-level resume
	progressSaver := checkpoint.NewProgressSaver(o.state)

	// Check if date column tracking is enabled (for timestamp recording)
	// Enabled when date_updated_columns is configured, regardless of target mode
	// This allows drop_recreate to populate timestamps for subsequent upsert runs
	dateTrackingEnabled := len(o.config.Migration.DateUpdatedColumns) > 0

	// Date filtering (incremental sync) is only applied in upsert mode
	applyDateFilter := o.config.Migration.TargetMode == "upsert"

	// Map to track date filters per table (for updating sync timestamp after success)
	tableDateFilters := make(map[string]*transfer.DateFilter)
	tableSyncStartTimes := make(map[string]time.Time)

	// Track jobs per table for completion tracking
	tableJobs := make(map[string]int) // tableName -> number of jobs

	// Track sync types for summary logging
	var tablesIncremental, tablesFirstSync, tablesNoDateColumn int
	var noDateColumnTables []string

	// Count tables that need partitioning
	var largeKeysetTables, largeRowNumTables int
	for _, t := range tables {
		if t.IsLarge(o.config.Migration.LargeTableThreshold) {
			if t.SupportsKeysetPagination() {
				largeKeysetTables++
			} else if t.HasPK() {
				largeRowNumTables++
			}
		}
	}
	// Track partition query timing
	var partitionStartTime time.Time
	var totalPartitionTime time.Duration
	if largeKeysetTables > 0 {
		logging.Info("Calculating partition boundaries for %d large tables...", largeKeysetTables)
		partitionStartTime = time.Now()
	}

	for _, t := range tables {
		logging.Debug("Processing table %s (rows=%d, large=%v)", t.Name, t.RowCount, t.IsLarge(o.config.Migration.LargeTableThreshold))

		// Determine date filter for this table (if date tracking is enabled)
		var dateFilter *transfer.DateFilter
		if dateTrackingEnabled {
			colName, colType, found := o.sourcePool.GetDateColumnInfo(
				ctx, t.Schema, t.Name, o.config.Migration.DateUpdatedColumns)
			if found {
				t.DateColumn = colName
				t.DateColumnType = colType

				// Record sync start time (capture before transfer starts)
				syncStartTime := time.Now().UTC()
				tableSyncStartTimes[t.Name] = syncStartTime
				tableDateFilters[t.Name] = nil // Mark for timestamp recording (nil = no filter, just tracking)

				// Only apply date filter in upsert mode
				if applyDateFilter {
					// Get last sync timestamp
					lastSync, err := o.state.GetLastSyncTimestamp(t.Schema, t.Name, o.config.Target.Schema)
					if err != nil {
						logging.Warn("Failed to get last sync timestamp for %s: %v", t.Name, err)
					}

					if lastSync != nil {
						dateFilter = &transfer.DateFilter{
							Column:    colName,
							Timestamp: *lastSync,
						}
						tableDateFilters[t.Name] = dateFilter
						tablesIncremental++
						logging.Info("Table %s: incremental - syncing rows where %s > %v",
							t.Name, colName, lastSync.Format(time.RFC3339))
					} else {
						// First sync - no date filter, but we'll record the sync timestamp
						tablesFirstSync++
						logging.Info("Table %s: first sync - loading all %d rows, will use %s for future incremental syncs",
							t.Name, t.RowCount, colName)
					}
				} else {
					// drop_recreate mode - full load but record timestamp for future upsert runs
					tablesFirstSync++
					logging.Info("Table %s: full load - recording %s timestamp for future incremental syncs",
						t.Name, colName)
				}
			} else {
				tablesNoDateColumn++
				noDateColumnTables = append(noDateColumnTables, t.Name)
				if applyDateFilter {
					logging.Info("Table %s: full sync - no date column, syncing all %d rows (repeated each run)",
						t.Name, t.RowCount)
				} else {
					logging.Info("Table %s: full load - no date column configured",
						t.Name)
				}
			}
		}

		if t.IsLarge(o.config.Migration.LargeTableThreshold) && t.SupportsKeysetPagination() {
			// Partition large tables with keyset pagination (PK-based boundaries)
			numPartitions := min(
				int(t.RowCount/int64(o.config.Migration.ChunkSize))+1,
				o.config.Migration.MaxPartitions,
			)

			tableStart := time.Now()
			partitions, err := o.sourcePool.GetPartitionBoundaries(ctx, &t, numPartitions)
			tableElapsed := time.Since(tableStart)
			logging.Info("  %s: %d partitions (%s)", t.Name, len(partitions), tableElapsed.Round(time.Millisecond))
			if err != nil {
				return nil, fmt.Errorf("partitioning %s: %w", t.FullName(), err)
			}

			tableJobs[t.Name] = len(partitions)
			for _, p := range partitions {
				// Create task for chunk-level tracking
				taskKey := fmt.Sprintf("transfer:%s.%s:p%d", t.Schema, t.Name, p.PartitionID)
				taskID, _ := o.state.CreateTask(runID, "transfer", taskKey)

				jobs = append(jobs, transfer.Job{
					Table:      t,
					Partition:  &p,
					TaskID:     taskID,
					Saver:      progressSaver,
					DateFilter: dateFilter,
				})
			}
		} else if t.IsLarge(o.config.Migration.LargeTableThreshold) && t.HasPK() {
			// Partition large tables with ROW_NUMBER pagination (row-based boundaries)
			numPartitions := min(
				int(t.RowCount/int64(o.config.Migration.ChunkSize))+1,
				o.config.Migration.MaxPartitions,
			)
			if numPartitions < 1 {
				numPartitions = 1
			}

			logging.Debug("  Partitioning %s (%d rows, %d partitions, row-number)...", t.Name, t.RowCount, numPartitions)
			rowsPerPartition := t.RowCount / int64(numPartitions)
			tableJobs[t.Name] = numPartitions

			for i := 0; i < numPartitions; i++ {
				startRow := int64(i) * rowsPerPartition
				endRow := startRow + rowsPerPartition
				if i == numPartitions-1 {
					endRow = t.RowCount // Last partition gets remaining rows
				}

				p := source.Partition{
					TableName:   t.Name,
					PartitionID: i + 1,
					StartRow:    startRow,
					EndRow:      endRow,
					RowCount:    endRow - startRow,
				}

				taskKey := fmt.Sprintf("transfer:%s.%s:p%d", t.Schema, t.Name, p.PartitionID)
				taskID, _ := o.state.CreateTask(runID, "transfer", taskKey)

				jobs = append(jobs, transfer.Job{
					Table:      t,
					Partition:  &p,
					TaskID:     taskID,
					Saver:      progressSaver,
					DateFilter: dateFilter,
				})
			}
		} else {
			tableJobs[t.Name] = 1
			// Create task for chunk-level tracking
			taskKey := fmt.Sprintf("transfer:%s.%s", t.Schema, t.Name)
			taskID, _ := o.state.CreateTask(runID, "transfer", taskKey)

			jobs = append(jobs, transfer.Job{
				Table:      t,
				Partition:  nil,
				TaskID:     taskID,
				Saver:      progressSaver,
				DateFilter: dateFilter,
			})
		}
	}

	// Log total partition query time
	if largeKeysetTables > 0 {
		totalPartitionTime = time.Since(partitionStartTime)
		logging.Info("Partition queries completed in %s", totalPartitionTime.Round(time.Millisecond))
	}

	// Log date tracking summary
	if dateTrackingEnabled {
		if tablesIncremental > 0 || tablesFirstSync > 0 || tablesNoDateColumn > 0 {
			if applyDateFilter {
				logging.Info("Incremental sync summary: %d tables incremental, %d tables first sync, %d tables full sync (no date column)",
					tablesIncremental, tablesFirstSync, tablesNoDateColumn)
			} else {
				logging.Info("Timestamp recording: %d tables with date columns, %d tables without",
					tablesFirstSync, tablesNoDateColumn)
			}
		}
		if tablesNoDateColumn > 0 && applyDateFilter {
			logging.Info("Tables without date columns will sync all rows every run: %v", noDateColumnTables)
		}
	}

	// Initialize progress
	logging.Debug("Created %d jobs, calculating total rows", len(jobs))
	var totalRows int64
	for _, j := range jobs {
		if j.Partition != nil {
			totalRows += j.Partition.RowCount
		} else {
			totalRows += j.Table.RowCount
		}
	}
	logging.Debug("Setting progress total to %d rows", totalRows)
	o.progress.SetTotal(totalRows)
	logging.Debug("Progress initialized")

	// Stats collection per table
	type tableStats struct {
		mu           sync.Mutex
		stats        *transfer.TransferStats
		jobsComplete int
		jobsFailed   int
	}
	statsMap := make(map[string]*tableStats)
	for _, t := range tables {
		statsMap[t.Name] = &tableStats{stats: &transfer.TransferStats{}}
	}

	// Pre-truncate partitioned tables BEFORE dispatching jobs (prevents race condition).
	// Skip this on resume so completed partitions aren't wiped.
	// Skip this in upsert mode - upserts are idempotent and don't require truncation.
	// Non-partitioned tables are truncated inside transfer.Execute as before.
	if !resume && o.targetMode.ShouldTruncateBeforeTransfer() {
		// Collect unique table names that need truncation
		tablesToTruncate := make(map[string]bool)
		for _, j := range jobs {
			if j.Partition != nil {
				tablesToTruncate[j.Table.Name] = true
			}
		}

		// Parallel truncation of partitioned tables
		if len(tablesToTruncate) > 0 {
			logging.Debug("Pre-truncating %d partitioned tables in parallel...", len(tablesToTruncate))
			var truncWg sync.WaitGroup
			truncErrs := make(chan error, len(tablesToTruncate))
			for tableName := range tablesToTruncate {
				truncWg.Add(1)
				go func(tname string) {
					defer truncWg.Done()
					if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, tname); err != nil {
						truncErrs <- fmt.Errorf("pre-truncating table %s: %w", tname, err)
					}
				}(tableName)
			}
			truncWg.Wait()
			close(truncErrs)
			if err := <-truncErrs; err != nil {
				return nil, err
			}
		}
	}

	// Execute jobs with worker pool
	logging.Debug("Starting worker pool with %d workers, %d jobs", o.config.Migration.Workers, len(jobs))
	sem := make(chan struct{}, o.config.Migration.Workers)
	var wg sync.WaitGroup

	// Track failures per table
	type tableError struct {
		tableName string
		err       error
	}
	errCh := make(chan tableError, len(jobs))

	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(j transfer.Job) {
			defer wg.Done()
			defer func() { <-sem }()

			// Mark task as running when worker starts processing
			o.state.UpdateTaskStatus(j.TaskID, "running", "")

			// Execute transfer with retry logic for transient errors
			maxRetries := o.config.Migration.MaxRetries
			if maxRetries <= 0 {
				maxRetries = 3
			}

			var stats *transfer.TransferStats
			var err error
		retryLoop:
			for attempt := 0; attempt <= maxRetries; attempt++ {
				if attempt > 0 {
					// Exponential backoff: 1s, 2s, 4s, etc.
					backoff := time.Duration(1<<(attempt-1)) * time.Second
					logging.Warn("Retry %d/%d for %s after %v (error: %v)", attempt, maxRetries, j.Table.Name, backoff, err)
					select {
					case <-ctx.Done():
						err = ctx.Err()
						break retryLoop
					case <-time.After(backoff):
					}
				}

				stats, err = transfer.Execute(ctx, o.sourcePool, o.targetPool, o.config, j, o.progress)
				if err == nil {
					break
				}
				if !isRetryableError(err) {
					break // Non-retryable error, fail immediately
				}
			}

			// Update table stats and completion tracking
			ts := statsMap[j.Table.Name]
			ts.mu.Lock()
			if err != nil {
				ts.jobsFailed++
				ts.mu.Unlock()
				// Mark this task as failed
				o.state.UpdateTaskStatus(j.TaskID, "failed", err.Error())
				errCh <- tableError{tableName: j.Table.Name, err: err}
				// Log the failure immediately
				logging.Error("Table %s failed: %v", j.Table.Name, err)
				// Check for geography/geometry comparison error in upsert mode
				errStr := err.Error()
				if strings.Contains(errStr, "Invalid operator for data type") &&
					(strings.Contains(errStr, "geography") || strings.Contains(errStr, "geometry")) {
					logging.Warn("HINT: Table %s contains geography/geometry columns which cannot be compared in MERGE statements.", j.Table.Name)
					logging.Warn("      Use 'target_mode: drop_recreate' or exclude this table with 'exclude_tables'.")
				}
				// Notify via Slack
				o.notifier.TableTransferFailed(runID, j.Table.Name, err)
				return
			}

			// Mark this task/partition as complete
			o.state.UpdateTaskStatus(j.TaskID, "success", "")

			if stats != nil {
				ts.stats.QueryTime += stats.QueryTime
				ts.stats.ScanTime += stats.ScanTime
				ts.stats.WriteTime += stats.WriteTime
				ts.stats.Rows += stats.Rows
			}
			ts.jobsComplete++

			// If all jobs for this table are complete, mark table task as success
			if ts.jobsComplete == tableJobs[j.Table.Name] && ts.jobsFailed == 0 {
				taskKey := fmt.Sprintf("transfer:%s.%s", j.Table.Schema, j.Table.Name)
				o.markTableComplete(runID, taskKey)
				o.progress.TableComplete()

				// Update sync timestamp for date-based incremental sync
				if _, hasDateFilter := tableDateFilters[j.Table.Name]; hasDateFilter {
					if syncTime, ok := tableSyncStartTimes[j.Table.Name]; ok {
						if err := o.state.UpdateSyncTimestamp(j.Table.Schema, j.Table.Name, o.config.Target.Schema, syncTime); err != nil {
							logging.Warn("Failed to update sync timestamp for %s: %v", j.Table.Name, err)
						} else {
							logging.Debug("Updated sync timestamp for %s to %v", j.Table.Name, syncTime.Format(time.RFC3339))
						}
					}
				}
			}
			ts.mu.Unlock()
		}(job)
	}

	wg.Wait()
	close(errCh)

	// Collect table failures (deduplicate by table name)
	failedTables := make(map[string]error)
	for te := range errCh {
		// Check for context cancellation
		if errors.Is(te.err, context.Canceled) || errors.Is(te.err, context.DeadlineExceeded) {
			o.progress.Finish()
			return nil, context.Canceled
		}
		// Only keep first error per table
		if _, exists := failedTables[te.tableName]; !exists {
			failedTables[te.tableName] = te.err
			o.progress.TableFailed()
		}
	}

	o.progress.Finish()

	// Print pool stats using the unified interface
	if logging.IsDebug() {
		logging.Debug("\nConnection Pool Usage:")
		logging.Debug("  Source %s", o.sourcePool.PoolStats())
		logging.Debug("  Target %s", o.targetPool.PoolStats())

		// Print profiling stats
		logging.Debug("\nTransfer Profile (per table):")
		logging.Debug("------------------------------")
		var totalQuery, totalScan, totalWrite time.Duration
		for _, t := range tables {
			ts := statsMap[t.Name]
			if ts.stats.Rows > 0 {
				logging.Debug("%-25s %s", t.Name, ts.stats.String())
				totalQuery += ts.stats.QueryTime
				totalScan += ts.stats.ScanTime
				totalWrite += ts.stats.WriteTime
			}
		}
		totalTime := totalQuery + totalScan + totalWrite
		if totalTime > 0 {
			logging.Debug("------------------------------")
			logging.Debug("%-25s query=%.1fs (%.0f%%), scan=%.1fs (%.0f%%), write=%.1fs (%.0f%%)",
				"TOTAL",
				totalQuery.Seconds(), float64(totalQuery)/float64(totalTime)*100,
				totalScan.Seconds(), float64(totalScan)/float64(totalTime)*100,
				totalWrite.Seconds(), float64(totalWrite)/float64(totalTime)*100)
		}
	}

	// Convert map to slice
	var failures []TableFailure
	for tableName, err := range failedTables {
		failures = append(failures, TableFailure{TableName: tableName, Error: err})
	}

	return failures, nil
}

// Resume continues an interrupted migration
func (o *Orchestrator) Resume(ctx context.Context) error {
	run, err := o.state.GetLastIncompleteRun()
	if err != nil {
		return fmt.Errorf("finding incomplete run: %w", err)
	}
	if run == nil {
		return fmt.Errorf("no incomplete run found - use 'run' to start a new migration")
	}

	// Check if this incomplete run has been superseded by a later successful run
	superseded, err := o.state.HasSuccessfulRunAfter(run)
	if err != nil {
		return fmt.Errorf("checking for superseding runs: %w", err)
	}
	if superseded {
		// Mark the old incomplete run as failed since it's obsolete
		o.state.CompleteRun(run.ID, "failed", "superseded by later successful migration")
		return fmt.Errorf("incomplete run %s is obsolete - a later migration with the same schemas completed successfully. Use 'run' to start a new migration", run.ID)
	}

	// Validate config hash if stored (prevents resuming with different config)
	if run.ConfigHash != "" && !o.opts.ForceResume {
		currentHash := computeConfigHash(o.config)
		if run.ConfigHash != currentHash {
			return fmt.Errorf("config changed since run started (hash %s != %s), use --force-resume to override",
				run.ConfigHash, currentHash)
		}
	}

	startTime := time.Now()
	logging.Info("Resuming run: %s (started %s)", run.ID, run.StartedAt.Format(time.RFC3339))

	// Reset any running tasks to pending
	if err := o.state.MarkRunAsResumed(run.ID); err != nil {
		return fmt.Errorf("resetting tasks: %w", err)
	}

	// Extract schema (needed to know all tables)
	logging.Info("Extracting schema...")
	tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
	if err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return fmt.Errorf("extracting schema: %w", err)
	}

	// Apply table filters
	tables = o.filterTables(tables)
	if len(tables) == 0 {
		o.state.CompleteRun(run.ID, "failed", "no tables to migrate after applying filters")
		return fmt.Errorf("no tables to migrate after applying filters")
	}

	o.tables = tables
	logging.Info("Found %d tables in source", len(tables))

	// Get tables that were successfully transferred in the previous run
	completedTables, err := o.state.GetCompletedTables(run.ID)
	if err != nil {
		return fmt.Errorf("getting completed tables: %w", err)
	}

	// Check target row counts to determine which tables need re-transfer
	var tablesToTransfer []source.Table
	var skippedTables []string

	for _, t := range tables {
		// Check if table was marked complete AND has correct row count
		taskKey := fmt.Sprintf("transfer:%s.%s", t.Schema, t.Name)
		if completedTables[taskKey] {
			// Verify row count matches
			targetCount, err := o.targetPool.GetRowCount(ctx, o.config.Target.Schema, t.Name)
			if err == nil && targetCount == t.RowCount {
				skippedTables = append(skippedTables, t.Name)
				continue
			}
		}
		tablesToTransfer = append(tablesToTransfer, t)
	}

	if len(skippedTables) > 0 {
		logging.Debug("Skipping %d already-complete tables: %v", len(skippedTables), skippedTables)
	}

	if len(tablesToTransfer) == 0 {
		logging.Info("All tables already transferred - completing migration")
		o.tables = tables // Use all tables for finalize/validate

		// Finalize
		o.progress.SetPhase("finalizing")
		logging.Info("Finalizing...")
		if err := o.targetMode.Finalize(ctx, tables); err != nil {
			o.state.CompleteRun(run.ID, "failed", err.Error())
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return fmt.Errorf("finalizing: %w", err)
		}

		// Validate
		o.progress.SetPhase("validating")
		logging.Info("Validating...")
		if err := o.Validate(ctx); err != nil {
			o.state.CompleteRun(run.ID, "failed", err.Error())
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return err
		}

		o.state.CompleteRun(run.ID, "success", "")
		logging.Info("Resume complete!")
		return nil
	}

	logging.Info("Resuming transfer of %d tables", len(tablesToTransfer))

	// For tables that need transfer, ensure target tables exist
	// Check for chunk-level progress to avoid unnecessary truncation
	progressSaver := checkpoint.NewProgressSaver(o.state)
	for _, t := range tablesToTransfer {
		taskKey := fmt.Sprintf("transfer:%s.%s", t.Schema, t.Name)
		taskID, _ := o.state.CreateTask(run.ID, "transfer", taskKey)

		exists, err := o.targetPool.TableExists(ctx, o.config.Target.Schema, t.Name)
		if err != nil {
			o.state.CompleteRun(run.ID, "failed", err.Error())
			return fmt.Errorf("checking table %s: %w", t.Name, err)
		}
		if !exists {
			// Table doesn't exist - create it and clear any stale progress
			if err := o.targetPool.CreateTable(ctx, &t, o.config.Target.Schema); err != nil {
				o.state.CompleteRun(run.ID, "failed", err.Error())
				return fmt.Errorf("creating table %s: %w", t.Name, err)
			}
			// Clear any saved progress since we're starting fresh
			o.state.ClearTransferProgress(taskID)
		} else {
			// Table exists - check if we have saved chunk progress
			lastPK, rowsDone, _ := progressSaver.GetProgress(taskID)

			// For partitioned tables, progress is saved at partition level, not table level.
			// Skip table-level truncation - partition cleanup in transfer.go handles partial data.
			isPartitioned := t.IsLarge(o.config.Migration.LargeTableThreshold) && t.SupportsKeysetPagination()

			if lastPK == nil && !isPartitioned {
				// No chunk progress and not partitioned - truncate to ensure clean re-transfer
				if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, t.Name); err != nil {
					o.state.CompleteRun(run.ID, "failed", err.Error())
					return fmt.Errorf("truncating table %s: %w", t.Name, err)
				}
			} else if lastPK != nil {
				// Have chunk progress - verify target row count matches saved progress
				// If target has fewer rows than saved progress, data was lost; start fresh
				targetCount, err := o.targetPool.GetRowCount(ctx, o.config.Target.Schema, t.Name)
				if err != nil {
					o.state.CompleteRun(run.ID, "failed", err.Error())
					return fmt.Errorf("getting row count for %s: %w", t.Name, err)
				}
				if targetCount < rowsDone {
					// Target has fewer rows than expected - clear progress and truncate
					logging.Warn("  Warning: %s has %d rows but expected %d - restarting transfer",
						t.Name, targetCount, rowsDone)
					o.state.ClearTransferProgress(taskID)
					if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, t.Name); err != nil {
						o.state.CompleteRun(run.ID, "failed", err.Error())
						return fmt.Errorf("truncating table %s: %w", t.Name, err)
					}
				}
				// If target has >= rowsDone, resume from saved progress
			}
		}
	}

	// Transfer only the incomplete tables
	logging.Info("Transferring data...")
	tableFailures, err := o.transferAll(ctx, run.ID, tablesToTransfer, true)
	if err != nil {
		// If context was canceled (Ctrl+C), leave run as "running" so resume works
		// but reset any "running" tasks to "pending" so status shows correctly
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			o.state.MarkRunAsResumed(run.ID) // Reset running tasks to pending
			logging.Info("Migration interrupted - run 'resume' to continue")
			return fmt.Errorf("transferring data: %w", err)
		}
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return fmt.Errorf("transferring data: %w", err)
	}

	// Log summary of table failures (individual failures already logged)
	if len(tableFailures) > 0 {
		logging.Warn("%d table(s) failed during transfer:", len(tableFailures))
		for _, f := range tableFailures {
			logging.Warn("  - %s: %v", f.TableName, f.Error)
		}
	}

	// Filter out failed tables for finalize/validate
	failedTableNames := make(map[string]bool)
	for _, f := range tableFailures {
		failedTableNames[f.TableName] = true
	}
	var successTables []source.Table
	for _, t := range tables {
		if !failedTableNames[t.Name] {
			successTables = append(successTables, t)
		}
	}

	// Finalize (uses successful tables for constraints)
	o.tables = successTables
	o.progress.SetPhase("finalizing")
	logging.Info("Finalizing...")
	if err := o.targetMode.Finalize(ctx, successTables); err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return fmt.Errorf("finalizing: %w", err)
	}

	// Validate successful tables
	o.progress.SetPhase("validating")
	logging.Info("Validating...")
	if err := o.Validate(ctx); err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return err
	}

	// Sample validation if enabled
	if o.config.Migration.SampleValidation {
		logging.Info("Running sample validation...")
		if err := o.validateSamples(ctx); err != nil {
			logging.Warn("Warning: sample validation failed: %v", err)
		}
	}

	// Calculate stats - only count rows from tables we attempted to transfer that succeeded
	duration := time.Since(startTime)
	var totalRows int64
	for _, t := range tablesToTransfer {
		if !failedTableNames[t.Name] {
			totalRows += t.RowCount
		}
	}
	throughput := float64(totalRows) / duration.Seconds()

	// Determine final status and send appropriate notification
	successCount := 0
	for _, t := range tablesToTransfer {
		if !failedTableNames[t.Name] {
			successCount++
		}
	}

	if len(tableFailures) > 0 {
		// Partial success
		failureNames := make([]string, len(tableFailures))
		for i, f := range tableFailures {
			failureNames[i] = f.TableName
		}
		o.state.CompleteRun(run.ID, "partial", fmt.Sprintf("%d tables failed", len(tableFailures)))
		o.notifier.MigrationCompletedWithErrors(run.ID, startTime, duration,
			successCount, len(tableFailures), totalRows, throughput, failureNames)
		logging.Warn("Resume completed with errors: %d tables succeeded, %d tables failed, %d rows in %s (%.0f rows/sec)",
			successCount, len(tableFailures), totalRows, duration.Round(time.Second), throughput)
	} else {
		// Full success
		o.state.CompleteRun(run.ID, "success", "")
		o.notifier.MigrationCompleted(run.ID, startTime, duration, len(tablesToTransfer), totalRows, throughput)
		logging.Info("Resume complete: %d tables, %d rows in %s (%.0f rows/sec)",
			len(tablesToTransfer), totalRows, duration.Round(time.Second), throughput)
	}

	// Log identifier changes for PostgreSQL targets
	if o.config.Target.Type == "postgres" {
		o.logPGIdentifierChanges(tablesToTransfer)
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Unused import suppression
var _ = sql.Named

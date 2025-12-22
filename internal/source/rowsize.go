package source

import (
	"context"
	"database/sql"
)

// EstimateRowSizeFromStats queries SQL Server system tables to get actual average row size.
// Returns the average row size in bytes, or 500 as a fallback.
func EstimateRowSizeFromStats(ctx context.Context, db *sql.DB, schema, tableName string) int64 {
	// Query sys.dm_db_partition_stats for actual storage statistics
	// This gives us the real average row size based on how SQL Server stores the data
	query := `
		SELECT
			CASE WHEN SUM(ps.row_count) > 0
				THEN CAST(SUM(ps.used_page_count) * 8.0 * 1024.0 / NULLIF(SUM(ps.row_count), 0) AS BIGINT)
				ELSE 500
			END as avg_row_size
		FROM sys.dm_db_partition_stats ps
		INNER JOIN sys.tables t ON ps.object_id = t.object_id
		INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE t.name = @p1 AND s.name = @p2
		AND ps.index_id <= 1
	`

	var avgSize sql.NullInt64
	err := db.QueryRowContext(ctx, query, tableName, schema).Scan(&avgSize)
	if err != nil || !avgSize.Valid || avgSize.Int64 == 0 {
		return 500 // Default fallback
	}

	// The storage size includes page overhead. For in-memory representation,
	// we also need to account for Go's interface{} overhead per value (~16-24 bytes)
	// and slice headers. Add ~50% overhead for Go runtime.
	return avgSize.Int64 * 3 / 2
}

// EstimateRowSizeFromStatsPostgres queries PostgreSQL system tables for average row size.
func EstimateRowSizeFromStatsPostgres(ctx context.Context, db *sql.DB, schema, tableName string) int64 {
	// Query pg_stat_user_tables for actual tuple statistics
	query := `
		SELECT
			CASE WHEN c.reltuples > 0
				THEN (pg_total_relation_size(c.oid) / GREATEST(c.reltuples, 1))::bigint
				ELSE 500
			END as avg_row_size
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = $1 AND n.nspname = $2
	`

	var avgSize sql.NullInt64
	err := db.QueryRowContext(ctx, query, tableName, schema).Scan(&avgSize)
	if err != nil || !avgSize.Valid || avgSize.Int64 == 0 {
		return 500 // Default fallback
	}

	// Add Go runtime overhead
	return avgSize.Int64 * 3 / 2
}

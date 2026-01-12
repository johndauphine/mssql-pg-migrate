package driver

// Dialect abstracts database-specific SQL syntax differences.
// Each database driver provides its own Dialect implementation.
type Dialect interface {
	// DBType returns the database type (e.g., "mssql", "postgres").
	DBType() string

	// QuoteIdentifier quotes an identifier (table, column name).
	// PostgreSQL: "identifier"
	// MSSQL: [identifier]
	// MySQL: `identifier`
	QuoteIdentifier(name string) string

	// QualifyTable returns a fully qualified table reference.
	// PostgreSQL: "schema"."table"
	// MSSQL: [schema].[table]
	QualifyTable(schema, table string) string

	// ParameterPlaceholder returns the parameter placeholder for the given index.
	// PostgreSQL: $1, $2, $3
	// MSSQL: @p1, @p2, @p3
	// MySQL: ?, ?, ?
	ParameterPlaceholder(index int) string

	// BuildDSN builds a connection string for this database.
	BuildDSN(host string, port int, database, user, password string, opts map[string]any) string

	// TableHint returns a table hint for read queries.
	// MSSQL: WITH (NOLOCK) when strictConsistency is false
	// PostgreSQL: empty (no hints)
	TableHint(strictConsistency bool) string

	// ColumnList formats a list of columns for SELECT.
	ColumnList(cols []string) string

	// ColumnListForSelect formats columns for SELECT with spatial conversions.
	// When reading from one database to write to another, spatial columns
	// need to be converted to WKT text format.
	ColumnListForSelect(cols, colTypes []string, targetDBType string) string

	// BuildKeysetQuery builds a keyset pagination query.
	BuildKeysetQuery(cols, pkCol, schema, table, tableHint string, hasMaxPK bool, dateFilter *DateFilter) string

	// BuildKeysetArgs builds arguments for a keyset pagination query.
	BuildKeysetArgs(lastPK, maxPK any, dateFilter *DateFilter) []any

	// BuildRowNumberQuery builds a ROW_NUMBER pagination query.
	BuildRowNumberQuery(cols, orderBy, schema, table, tableHint string) string

	// BuildRowNumberArgs builds arguments for a ROW_NUMBER pagination query.
	BuildRowNumberArgs(startRow, endRow int64) []any

	// PartitionBoundariesQuery returns a query to get partition boundaries.
	PartitionBoundariesQuery(pkCol, schema, table string, numPartitions int) string

	// RowCountQuery returns a query to get the row count.
	// If useStats is true, may use statistics tables for faster results.
	RowCountQuery(useStats bool) string

	// DateColumnQuery returns a query to find date columns.
	DateColumnQuery() string

	// ValidDateTypes returns a map of valid date/timestamp types.
	ValidDateTypes() map[string]bool
}

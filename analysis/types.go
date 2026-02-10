// Package analysis provides query analysis for the PostgreSQL parser.
// This file defines standalone types used by the analysis functions.
// These types are decoupled from any vendor-specific DTO layer so the parser
// can be consumed as an independent library.
package analysis

// SQLCommand identifies the high-level SQL statement type.
type SQLCommand string

const (
	SQLCommandSelect  SQLCommand = "SELECT"
	SQLCommandInsert  SQLCommand = "INSERT"
	SQLCommandUpdate  SQLCommand = "UPDATE"
	SQLCommandDelete  SQLCommand = "DELETE"
	SQLCommandMerge   SQLCommand = "MERGE"
	SQLCommandDDL     SQLCommand = "DDL"
	SQLCommandUnknown SQLCommand = "UNKNOWN"
)

// SQLTableType captures the origin of a table reference.
type SQLTableType string

const (
	SQLTableTypeBase     SQLTableType = "base"
	SQLTableTypeCTE      SQLTableType = "cte"
	SQLTableTypeFunction SQLTableType = "function"
	SQLTableTypeSubquery SQLTableType = "subquery"
	SQLTableTypeUnknown  SQLTableType = "unknown"
)

// SQLTable describes a relation referenced in the query.
type SQLTable struct {
	Schema string
	Name   string
	Alias  string
	Type   SQLTableType
	Raw    string
}

// SQLColumn records a projected expression and optional alias.
type SQLColumn struct {
	Expression string
	Alias      string
}

// SQLOrderExpression models an ORDER BY item.
type SQLOrderExpression struct {
	Expression string
	Direction  string
	Nulls      string
}

// SQLLimit captures LIMIT/OFFSET expressions.
type SQLLimit struct {
	Limit    string
	Offset   string
	IsNested bool
}

// SQLParameter represents a positional or anonymous parameter placeholder.
type SQLParameter struct {
	Raw      string
	Marker   string
	Position int
}

// SQLSetOperationType enumerates supported set-operation modifiers.
type SQLSetOperationType string

const (
	SQLSetOperationUnion        SQLSetOperationType = "UNION"
	SQLSetOperationUnionAll     SQLSetOperationType = "UNION ALL"
	SQLSetOperationIntersect    SQLSetOperationType = "INTERSECT"
	SQLSetOperationIntersectAll SQLSetOperationType = "INTERSECT ALL"
	SQLSetOperationExcept       SQLSetOperationType = "EXCEPT"
	SQLSetOperationExceptAll    SQLSetOperationType = "EXCEPT ALL"
)

// SQLSetOperation records metadata about set-operation branches.
type SQLSetOperation struct {
	Type    SQLSetOperationType
	Query   string
	Columns []string
	Tables  []SQLTable
}

// SQLSubquery references a derived table; Analysis may be nil if omitted.
type SQLSubquery struct {
	Alias    string
	Analysis *SQLAnalysis
}

// SQLCTE describes a common table expression.
type SQLCTE struct {
	Name         string
	Query        string
	Materialized string
}

// SQLUpsert captures ON CONFLICT metadata for INSERT statements.
type SQLUpsert struct {
	TargetColumns []string
	TargetWhere   string
	Constraint    string
	Action        string
	SetClauses    []string
	ActionWhere   string
}

// SQLMergeAction describes WHEN MATCHED / NOT MATCHED actions in a MERGE.
type SQLMergeAction struct {
	Type          string
	Condition     string
	SetClauses    []string
	InsertColumns []string
	InsertValues  string
}

// SQLMergeSource records the USING source of a MERGE statement.
type SQLMergeSource struct {
	Table    *SQLTable
	Subquery *SQLSubquery
}

// SQLMerge stores MERGE statement metadata.
type SQLMerge struct {
	Target    SQLTable
	Source    SQLMergeSource
	Condition string
	Actions   []SQLMergeAction
}

// SQLUsageType classifies how a column participates in the query.
type SQLUsageType string

const (
	SQLUsageTypeFilter          SQLUsageType = "filter"
	SQLUsageTypeJoin            SQLUsageType = "join"
	SQLUsageTypeProjection      SQLUsageType = "projection"
	SQLUsageTypeGroup           SQLUsageType = "group"
	SQLUsageTypeOrder           SQLUsageType = "order"
	SQLUsageTypeWindowPartition SQLUsageType = "window_partition"
	SQLUsageTypeWindowOrder     SQLUsageType = "window_order"
	SQLUsageTypeReturning       SQLUsageType = "returning"
	SQLUsageTypeDMLSet          SQLUsageType = "dml_set"
	SQLUsageTypeUpsertTarget    SQLUsageType = "upsert_target"
	SQLUsageTypeUpsertSet       SQLUsageType = "upsert_set"
	SQLUsageTypeMergeTarget     SQLUsageType = "merge_target"
	SQLUsageTypeMergeSource     SQLUsageType = "merge_source"
	SQLUsageTypeMergeSet        SQLUsageType = "merge_set"
	SQLUsageTypeMergeInsert     SQLUsageType = "merge_insert"
	SQLUsageTypeUnknown         SQLUsageType = "unknown"
)

// SQLJoinCorrelation captures column references in subqueries that refer to outer aliases.
type SQLJoinCorrelation struct {
	OuterAlias string
	InnerAlias string
	Expression string
	Type       string
}

// SQLColumnUsage records how a specific column/expression is used by a query.
type SQLColumnUsage struct {
	TableAlias string
	Column     string
	Expression string
	UsageType  SQLUsageType
	Context    string
	Functions  []string
	Operator   string
	Side       string
}

// SQLDDLColumn describes column-level metadata extracted from CREATE TABLE statements.
type SQLDDLColumn struct {
	Name     string
	Type     string
	Nullable bool
	Default  string
}

// SQLDDLAction describes a single DDL operation in the analysis result.
type SQLDDLAction struct {
	Type          string
	ObjectName    string
	Schema        string
	Columns       []string
	ColumnDetails []SQLDDLColumn
	Flags         []string
	IndexType     string
}

// SQLAnalysis is a standalone DTO representing the parsed SQL metadata.
type SQLAnalysis struct {
	RawSQL         string
	Command        SQLCommand
	Tables         []SQLTable
	Columns        []SQLColumn
	SetOperations  []SQLSetOperation
	Subqueries     []SQLSubquery
	CTEs           []SQLCTE
	Where          []string
	Having         []string
	GroupBy        []string
	OrderBy        []SQLOrderExpression
	Limit          *SQLLimit
	JoinClauses    []string
	Parameters     []SQLParameter
	InsertColumns  []string
	SetClauses     []string
	Returning      []string
	Upsert         *SQLUpsert
	Merge          *SQLMerge
	DDLActions     []SQLDDLAction
	ColumnUsage    []SQLColumnUsage
	Correlations   []SQLJoinCorrelation
	DerivedColumns map[string]string
}

// WhereCondition represents a single WHERE clause condition extracted from a query.
// Used for generating mock data that matches query constraints.
type WhereCondition struct {
	Table       string `json:"table"`
	Column      string `json:"column"`
	Operator    string `json:"operator"`
	Value       any    `json:"value"`
	IsParameter bool   `json:"is_parameter"`

	// JSONB operator support for ->> (extract as text) predicates.
	IsJSONB   bool   `json:"is_jsonb,omitempty"`
	JSONBKey  string `json:"jsonb_key,omitempty"`
	JSONBCast string `json:"jsonb_cast,omitempty"`
}

// JoinRelationship represents a foreign key relationship inferred from JOIN conditions.
// Used to generate FK values that reference actual parent table primary keys.
type JoinRelationship struct {
	ChildTable   string `json:"child_table"`
	ChildColumn  string `json:"child_column"`
	ParentTable  string `json:"parent_table"`
	ParentColumn string `json:"parent_column"`
}

// ColumnSchema describes a single column with metadata for schema-aware analysis.
type ColumnSchema struct {
	Name           string   `json:"name"`
	PGType         string   `json:"pg_type"`
	IsPrimaryKey   bool     `json:"is_primary_key,omitempty"`
	IsNullable     bool     `json:"is_nullable,omitempty"`
	DistinctCount  *int64   `json:"distinct_count,omitempty"`
	DistinctValues []string `json:"distinct_values,omitempty"`
}

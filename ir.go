// ir.go defines the intermediate representation (IR) types produced by ParseSQL.
// ParsedQuery is the top-level result containing tables, columns, conditions,
// CTEs, subqueries, set operations, and all other extracted SQL metadata.
package postgresparser

// QueryCommand represents the high-level SQL command parsed.
type QueryCommand string

const (
	// QueryCommandSelect is returned for SELECT queries.
	QueryCommandSelect QueryCommand = "SELECT"
	// QueryCommandInsert is returned for INSERT statements.
	QueryCommandInsert QueryCommand = "INSERT"
	// QueryCommandUpdate is returned for UPDATE statements.
	QueryCommandUpdate QueryCommand = "UPDATE"
	// QueryCommandDelete is returned for DELETE statements.
	QueryCommandDelete QueryCommand = "DELETE"
	// QueryCommandMerge is returned for MERGE statements.
	QueryCommandMerge QueryCommand = "MERGE"
	// QueryCommandDDL is returned for DDL statements (CREATE, ALTER, DROP, TRUNCATE).
	QueryCommandDDL QueryCommand = "DDL"
	// QueryCommandUnknown is used when the command could not be determined.
	QueryCommandUnknown QueryCommand = "UNKNOWN"
)

// ParseWarningCode identifies non-fatal parser notices in batch results.
type ParseWarningCode string

const (
	// ParseWarningCodeSyntaxError indicates ANTLR reported a syntax error while
	// parsing the input in ParseSQLAll mode.
	ParseWarningCodeSyntaxError ParseWarningCode = "SYNTAX_ERROR"
)

// ParseWarning captures non-fatal parser notices emitted by batch APIs.
type ParseWarning struct {
	Code    ParseWarningCode
	Message string
}

// TableType distinguishes between base relations, CTEs, derived tables, etc.
type TableType string

const (
	// TableTypeBase identifies a relation that maps directly to a physical table or view.
	TableTypeBase TableType = "base"
	// TableTypeCTE identifies a relation that originates from a common table expression.
	TableTypeCTE TableType = "cte"
	// TableTypeFunction identifies a relation produced by a set-returning function or lateral call.
	TableTypeFunction TableType = "function"
	// TableTypeSubquery identifies a relation backed by a derived table/subquery.
	TableTypeSubquery TableType = "subquery"
)

// TableRef captures a table-like source referenced in a query.
type TableRef struct {
	Schema string
	Name   string
	Alias  string
	Type   TableType
	Raw    string
}

// SelectColumn captures the projection list of a SELECT query.
type SelectColumn struct {
	Expression string
	Alias      string
}

// SetOperation describes a UNION/INTERSECT/EXCEPT block chained to the main SELECT.
type SetOperation struct {
	// Type is a plain string by design (not a typed constant) because the analysis
	// layer defines its own SQLSetOperationType. Values: "UNION", "UNION ALL",
	// "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL".
	Type    string
	Query   string     // Raw SQL of the RHS select
	Columns []string   // Projected column expressions from the RHS select
	Tables  []TableRef // Table references used by the RHS select
}

// UpsertClause captures ON CONFLICT metadata for INSERT statements.
type UpsertClause struct {
	TargetColumns []string // Columns listed in ON CONFLICT (column...) target.
	TargetWhere   string   // Optional WHERE clause attached to the conflict target.
	Constraint    string   // Constraint name referenced by ON CONFLICT ON CONSTRAINT.
	Action        string   // DO NOTHING or DO UPDATE.
	SetClauses    []string // SET clauses emitted by DO UPDATE.
	ActionWhere   string   // Optional WHERE clause attached to DO UPDATE.
}

// MergeAction represents a WHEN MATCHED/NOT MATCHED clause inside a MERGE.
type MergeAction struct {
	// Type is a plain string by design (not a typed constant) because the set of
	// merge action types is small and stable. Values: "INSERT", "UPDATE", "DELETE".
	Type          string
	Condition     string   // Optional predicate following AND in WHEN clause.
	SetClauses    []string // UPDATE ... SET clauses.
	InsertColumns []string // Column list for INSERT actions.
	InsertValues  string   // VALUES(...) text for INSERT actions.
}

// MergeSource captures the USING source in a MERGE statement.
type MergeSource struct {
	Table    TableRef
	Subquery *SubqueryRef
}

// MergeClause stores the metadata extracted from a MERGE statement.
type MergeClause struct {
	Target    TableRef
	Source    MergeSource
	Condition string
	Actions   []MergeAction
}

// DDLActionType identifies the specific DDL operation.
type DDLActionType string

const (
	DDLCreateTable DDLActionType = "CREATE_TABLE"
	DDLDropTable   DDLActionType = "DROP_TABLE"
	DDLDropColumn  DDLActionType = "DROP_COLUMN"
	DDLAlterTable  DDLActionType = "ALTER_TABLE"
	DDLCreateIndex DDLActionType = "CREATE_INDEX"
	DDLDropIndex   DDLActionType = "DROP_INDEX"
	DDLTruncate    DDLActionType = "TRUNCATE"
	DDLComment     DDLActionType = "COMMENT"
)

// DDLColumn describes column-level metadata extracted from CREATE TABLE statements.
type DDLColumn struct {
	Name     string
	Type     string
	Nullable bool
	Default  string
	Comment  []string // Optional line comments immediately preceding column definition.
}

// DDLAction describes a single DDL operation extracted from a statement.
type DDLAction struct {
	Type          DDLActionType
	ObjectName    string      // Unqualified table/index/object name
	ObjectType    string      // TABLE, COLUMN, INDEX, ...
	Schema        string      // Optional schema qualifier
	Columns       []string    // Affected columns
	ColumnDetails []DDLColumn // Column metadata (CREATE TABLE)
	Flags         []string    // IF_EXISTS, CONCURRENTLY, CASCADE, etc.
	IndexType     string      // btree, gin, gist, hash (CREATE INDEX only)
	Target        string      // Generic fully-qualified target path for comment-like actions.
	Comment       string      // Comment text for COMMENT ON statements.
}

// SubqueryRef records metadata for subqueries discovered in FROM or set operations.
type SubqueryRef struct {
	Alias string
	Query *ParsedQuery
}

// OrderExpression describes ORDER BY items.
type OrderExpression struct {
	Expression string
	Direction  string // ASC, DESC, or empty
	Nulls      string // FIRST, LAST, or empty
}

// LimitClause captures LIMIT/OFFSET expressions.
type LimitClause struct {
	Limit    string
	Offset   string
	IsNested bool // True if this limit is inside a subquery
}

// Parameter describes a positional or anonymous parameter placeholder.
type Parameter struct {
	Raw      string
	Marker   string // "$", "?"
	Position int    // Parsed index for $n, or sequential order for '?'
}

// CTE describes a common table expression defined in a WITH clause.
type CTE struct {
	Name         string
	Query        string
	Materialized string // "", "MATERIALIZED", or "NOT MATERIALIZED"
}

// ColumnUsageType defines the context where a column is referenced.
type ColumnUsageType string

const (
	ColumnUsageTypeFilter          ColumnUsageType = "filter"
	ColumnUsageTypeJoin            ColumnUsageType = "join"
	ColumnUsageTypeProjection      ColumnUsageType = "projection"
	ColumnUsageTypeGroupBy         ColumnUsageType = "group"
	ColumnUsageTypeOrderBy         ColumnUsageType = "order"
	ColumnUsageTypeReturning       ColumnUsageType = "returning"
	ColumnUsageTypeWindowPartition ColumnUsageType = "window_partition"
	ColumnUsageTypeWindowOrder     ColumnUsageType = "window_order"
	ColumnUsageTypeDMLSet          ColumnUsageType = "dml_set"
	ColumnUsageTypeUpsertTarget    ColumnUsageType = "upsert_target"
	ColumnUsageTypeUpsertSet       ColumnUsageType = "upsert_set"
	ColumnUsageTypeMergeTarget     ColumnUsageType = "merge_target"
	ColumnUsageTypeMergeSource     ColumnUsageType = "merge_source"
	ColumnUsageTypeMergeSet        ColumnUsageType = "merge_set"
	ColumnUsageTypeMergeInsert     ColumnUsageType = "merge_insert"
	ColumnUsageTypeUnknown         ColumnUsageType = "unknown"
)

// JoinCorrelation captures column references in subqueries that refer to outer aliases.
type JoinCorrelation struct {
	OuterAlias string // Alias from outer query
	InnerAlias string // Alias from inner query
	Expression string // Full correlation expression (e.g., "o.user_id = u.id")
	// Type is a plain string by design (not a typed constant) because there are only
	// two correlation kinds. Values: "LATERAL" or "CORRELATED".
	Type string
}

// ColumnUsage describes a single reference to a column and its role.
type ColumnUsage struct {
	TableAlias string
	Column     string
	Expression string
	UsageType  ColumnUsageType
	Context    string // Raw clause string for debugging
	Operator   string
	Side       string
	Functions  []string
}

// StatementParseResult contains the parse outcome for one input statement at the
// same index/order as it appeared in SQL text.
type StatementParseResult struct {
	Index    int
	RawSQL   string
	Query    *ParsedQuery
	Warnings []ParseWarning
}

// ParseBatchResult is returned by ParseSQLAll and includes one parse result per
// input statement plus a failure flag.
type ParseBatchResult struct {
	Statements []StatementParseResult
	// HasFailures is true when at least one statement has a nil Query or any Warnings.
	HasFailures bool
}

// ParsedQuery is the intermediate representation returned by ParseSQL.
type ParsedQuery struct {
	Command        QueryCommand
	RawSQL         string
	Columns        []SelectColumn
	Tables         []TableRef
	ColumnUsage    []ColumnUsage
	SetOperations  []SetOperation
	Subqueries     []SubqueryRef
	CTEs           []CTE
	Where          []string
	Having         []string
	GroupBy        []string
	OrderBy        []OrderExpression
	Limit          *LimitClause
	JoinConditions []string
	Parameters     []Parameter
	InsertColumns  []string
	SetClauses     []string
	Returning      []string
	Upsert         *UpsertClause
	Merge          *MergeClause
	DDLActions     []DDLAction
	Correlations   []JoinCorrelation // Join correlations for LATERAL and correlated subqueries
	DerivedColumns map[string]string // Alias -> expression mappings (e.g., "order_count" -> "COUNT(*)")
}

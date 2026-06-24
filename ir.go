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
	Schema        string
	Name          string
	Alias         string
	Type          TableType
	Raw           string
	JoinType      string // "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL", or "" for base FROM tables.
	JoinCondition string // Raw ON/USING clause text, or "" for base/CROSS tables.
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

// FKAction identifies a referential action for ON DELETE / ON UPDATE.
type FKAction string

const (
	FKNoAction   FKAction = "NO ACTION"
	FKRestrict   FKAction = "RESTRICT"
	FKCascade    FKAction = "CASCADE"
	FKSetNull    FKAction = "SET NULL"
	FKSetDefault FKAction = "SET DEFAULT"
)

// DDLColumn describes column-level metadata extracted from CREATE TABLE statements.
type DDLColumn struct {
	Name     string
	Type     string
	Nullable bool
	Default  string
	Comment  []string // Optional line comments immediately preceding column definition.
}

// DDLPrimaryKey describes a CREATE TABLE primary key constraint.
type DDLPrimaryKey struct {
	ConstraintName string
	Columns        []string
}

// DDLForeignKey describes a CREATE TABLE foreign key constraint.
type DDLForeignKey struct {
	ConstraintName    string
	Columns           []string
	ReferencesSchema  string
	ReferencesTable   string
	ReferencesColumns []string
	OnDelete          FKAction
	OnUpdate          FKAction
}

// DDLUniqueConstraint describes a CREATE TABLE UNIQUE constraint.
type DDLUniqueConstraint struct {
	ConstraintName string
	Columns        []string
}

// DDLCheckConstraint describes a CHECK constraint expression.
type DDLCheckConstraint struct {
	ConstraintName string
	Expression     string
}

// DDLConstraints bundles constraint metadata extracted from CREATE TABLE or
// ALTER TABLE ... ADD CONSTRAINT.
type DDLConstraints struct {
	PrimaryKey       *DDLPrimaryKey
	ForeignKeys      []DDLForeignKey
	UniqueKeys       []DDLUniqueConstraint
	CheckConstraints []DDLCheckConstraint
}

// DDLAction describes a single DDL operation extracted from a statement.
type DDLAction struct {
	Type          DDLActionType
	ObjectName    string          // Unqualified table/index/object name
	ObjectType    string          // TABLE, COLUMN, INDEX, ...
	Schema        string          // Optional schema qualifier
	Columns       []string        // Affected columns
	ColumnDetails []DDLColumn     // Column metadata (CREATE TABLE)
	Constraints   *DDLConstraints // PK/FK/UNIQUE constraint metadata (CREATE TABLE, ALTER TABLE ADD CONSTRAINT)
	Flags         []string        // IF_EXISTS, CONCURRENTLY, CASCADE, etc.
	IndexType     string          // btree, gin, gist, hash (CREATE INDEX only)
	// IncludeColumns lists non-key columns from CREATE INDEX ... INCLUDE (...).
	IncludeColumns []string
	// Predicate is the partial-index expression from CREATE INDEX ... WHERE ...
	// (the bare expression, without the leading WHERE keyword). Empty when the
	// index is not partial.
	Predicate string
	Target    string // Generic fully-qualified target path for comment-like actions.
	Comment   string // Comment text for COMMENT ON statements.
}

// SubqueryRef records metadata for subqueries discovered in FROM or set operations.
type SubqueryRef struct {
	Alias string
	// SourceClause is the clause the subquery was found in: "WHERE", "HAVING",
	// "SELECT", "FROM", or "SETOP". Empty when the origin was not recorded.
	SourceClause string
	Query        *ParsedQuery
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

// PlaceholderRole describes the syntactic position of a `?` or `$N`
// placeholder in a parsed SQL statement.
type PlaceholderRole int

const (
	PlaceholderRoleUnknown PlaceholderRole = iota
	PlaceholderRoleWhereValue
	PlaceholderRoleHavingValue
	PlaceholderRoleSelectExpr
	PlaceholderRoleFunctionArg
	PlaceholderRoleLimit
	PlaceholderRoleOffset
	PlaceholderRoleGroupByOrdinal
	PlaceholderRoleOrderByOrdinal
	PlaceholderRoleIntervalOperand
	PlaceholderRoleArrayMember
	PlaceholderRoleInsertValue
	PlaceholderRoleUpdateSetValue
	PlaceholderRoleCaseExpr
	PlaceholderRoleInListMember
	PlaceholderRoleBetweenLow
	PlaceholderRoleBetweenHigh
)

// String returns a stable lowercase identifier for serialization.
func (r PlaceholderRole) String() string {
	switch r {
	case PlaceholderRoleWhereValue:
		return "where_value"
	case PlaceholderRoleHavingValue:
		return "having_value"
	case PlaceholderRoleSelectExpr:
		return "select_expr"
	case PlaceholderRoleFunctionArg:
		return "function_arg"
	case PlaceholderRoleLimit:
		return "limit"
	case PlaceholderRoleOffset:
		return "offset"
	case PlaceholderRoleGroupByOrdinal:
		return "group_by_ordinal"
	case PlaceholderRoleOrderByOrdinal:
		return "order_by_ordinal"
	case PlaceholderRoleIntervalOperand:
		return "interval_operand"
	case PlaceholderRoleArrayMember:
		return "array_member"
	case PlaceholderRoleInsertValue:
		return "insert_value"
	case PlaceholderRoleUpdateSetValue:
		return "update_set_value"
	case PlaceholderRoleCaseExpr:
		return "case_expr"
	case PlaceholderRoleInListMember:
		return "in_list_member"
	case PlaceholderRoleBetweenLow:
		return "between_low"
	case PlaceholderRoleBetweenHigh:
		return "between_high"
	default:
		return "unknown"
	}
}

// FunctionRef identifies a function call site in the parsed statement.
type FunctionRef struct {
	Name     string // Canonical lowercase function name.
	ArgIndex int    // Zero-based index of this placeholder among function arguments.
	ArgCount int    // Total number of arguments at the function call site.
}

// CaseClause distinguishes positions inside a CASE expression.
type CaseClause int

const (
	CaseClauseUnknown CaseClause = iota
	CaseClausePredicate
	CaseClauseResult
	CaseClauseDefault
)

// String returns a stable lowercase identifier for serialization.
func (c CaseClause) String() string {
	switch c {
	case CaseClausePredicate:
		return "predicate"
	case CaseClauseResult:
		return "result"
	case CaseClauseDefault:
		return "default"
	default:
		return "unknown"
	}
}

// Placeholder is one occurrence of `?` or `$N` in a parsed SQL statement.
type Placeholder struct {
	Index int             // One-based positional index for `?`, or the numeric index for `$N`.
	Style string          // Placeholder marker style: "?" or "$".
	Role  PlaceholderRole // Syntactic role at this position.

	ParentFn     *FunctionRef // Function metadata when Role is PlaceholderRoleFunctionArg.
	CaseClause   CaseClause   // CASE sub-position when Role is PlaceholderRoleCaseExpr.
	InsertColumn string       // INSERT column filled by this placeholder, when known.
	UpdateColumn string       // UPDATE SET column assigned by this placeholder, when known.
	ColumnRef    string       // Predicate column reference, formatted as "table.column" or "column".

	Start int // Start byte offset in the original SQL.
	End   int // End byte offset in the original SQL.
}

// CTE describes a common table expression defined in a WITH clause.
type CTE struct {
	Name         string
	Query        string
	ParsedQuery  *ParsedQuery
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
	// Functions lists function names that wrap this column reference outside WHERE
	// clauses (SELECT projection, ORDER BY, GROUP BY, HAVING, etc.) where wrapper
	// semantics are not used by simulation/materialization. For WHERE-clause
	// wrappers, see FunctionWrapper which carries typed metadata including args.
	Functions []string
	// Function is set only for WHERE-clause predicates whose subject column is wrapped
	// by an allowlisted function (length, lower, upper, coalesce, extract, date_trunc,
	// char_length, octet_length). Nil otherwise. See FunctionWrapper.
	Function *FunctionWrapper
}

// FunctionWrapper describes a function call that wraps a bare column reference in a
// WHERE-clause predicate. Populated only on the WHERE-side ColumnUsage entries; nil
// for projection / ORDER BY / GROUP BY / HAVING / window / RETURNING / JOIN ON usage,
// for non-allowlisted functions, for schema-qualified names other than pg_catalog,
// and when the function argument is itself an expression (e.g. length(col || 'x')).
type FunctionWrapper struct {
	// Name is the canonical lowercase function name, unqualified. pg_catalog.<name>
	// is canonicalised to bare <name>; any other schema rejects the wrapper outright.
	Name string
	// Schema is the empty string for unqualified calls and for pg_catalog
	// (canonicalised away). Any other schema causes the wrapper not to be attached.
	Schema string
	// Args carries the literal arguments other than the column itself, in source
	// order (extract field, date_trunc unit, coalesce defaults, etc.). Empty for
	// single-arg wrappers like length / lower.
	Args []FunctionArg
	// IsNested is true when the wrapped column is reached through one or more
	// additional allowlisted-function wrappers (e.g. lower(lower(col)),
	// length(lower(col))). Outermost-only attribution: Name reflects the
	// outermost call; the inner chain is observable only via this flag.
	IsNested bool
	// Cast is the textual target type of a typecast applied to the wrapper as a
	// whole (length(col)::int, CAST(length(col) AS bigint)). Empty when there is
	// no cast. For chained casts the outermost cast is recorded.
	Cast string
}

// FunctionArg is a single non-column argument to a wrapping function in a WHERE
// predicate (extract field, date_trunc unit, coalesce defaults, etc.).
type FunctionArg struct {
	// Literal holds the SQL textual form of a literal argument. Nil means the
	// argument is a non-literal expression (column reference, sub-call, placeholder).
	// Consumers requiring a fixed value MUST nil-check and fall back to the
	// standard column+operator interpretation when nil. Numeric, boolean, and
	// interval literals are stringified (e.g. "0", "true", "1 day"); string
	// literals retain their surrounding quotes.
	Literal *string
	// IsNull is true when the argument is the explicit SQL NULL keyword. Disjoint
	// from Literal: a NULL has IsNull=true, Literal=nil.
	IsNull bool
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
	Placeholders   []Placeholder // Placeholder occurrences in source-text order.
	InsertColumns  []string
	SetClauses     []string
	Returning      []string
	Upsert         *UpsertClause
	Merge          *MergeClause
	DDLActions     []DDLAction
	Correlations   []JoinCorrelation // Join correlations for LATERAL and correlated subqueries
	DerivedColumns map[string]string // Alias -> expression mappings (e.g., "order_count" -> "COUNT(*)")
}

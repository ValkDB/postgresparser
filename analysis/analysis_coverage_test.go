// Package analysis provides SQL analysis capabilities using the ANTLR PostgreSQL parser.
// This file provides comprehensive coverage for analysis gaps:
// RawSQL, JoinClauses, Parameters, SetOperations, Subqueries, CTEs,
// Columns, Where/Having/GroupBy content, OrderBy Direction/Nulls,
// SetClauses, InsertColumns, Returning, Upsert, Merge, Correlations,
// and DerivedColumns.
package analysis

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// 1. RawSQL -- verify the parser preserves the original SQL text
// =============================================================================

// TestAnalyze_RawSQL_PreservesOriginalText verifies that the RawSQL field contains
// the exact SQL string that was passed to AnalyzeSQL.
func TestAnalyze_RawSQL_PreservesOriginalText(t *testing.T) {
	sql := `SELECT u.name, u.email FROM users u WHERE u.active = true ORDER BY u.name`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, sql, result.RawSQL,
		"RawSQL should contain the exact SQL string passed to AnalyzeSQL")
}

// TestAnalyze_RawSQL_MultilineQuery verifies that RawSQL preserves multi-line SQL with whitespace.
func TestAnalyze_RawSQL_MultilineQuery(t *testing.T) {
	sql := `SELECT id, name
FROM users
WHERE active = true`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, sql, result.RawSQL,
		"RawSQL should preserve multi-line formatting")
}

// =============================================================================
// 2. JoinClauses -- verify join conditions are captured as strings
// =============================================================================

// TestAnalyze_JoinClauses_InnerJoinON verifies that an INNER JOIN ON condition
// is captured in JoinClauses with the ON keyword and column references.
func TestAnalyze_JoinClauses_InnerJoinON(t *testing.T) {
	sql := `SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.JoinClauses, 1, "Should have exactly one join clause")
	assert.Contains(t, result.JoinClauses[0], "o.customer_id",
		"Join clause should reference the foreign key column")
	assert.Contains(t, result.JoinClauses[0], "c.id",
		"Join clause should reference the primary key column")
}

// TestAnalyze_JoinClauses_LeftJoinON verifies that LEFT JOIN produces
// the same JoinClauses structure as INNER JOIN.
func TestAnalyze_JoinClauses_LeftJoinON(t *testing.T) {
	sql := `SELECT * FROM orders o LEFT JOIN customers c ON o.customer_id = c.id`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.JoinClauses, 1, "LEFT JOIN should produce one join clause")
	assert.Contains(t, result.JoinClauses[0], "o.customer_id")
	assert.Contains(t, result.JoinClauses[0], "c.id")
}

// TestAnalyze_JoinClauses_USINGClause verifies that a USING clause
// is captured in JoinClauses with the column name.
func TestAnalyze_JoinClauses_USINGClause(t *testing.T) {
	sql := `SELECT * FROM orders o JOIN customers c USING (customer_id)`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.JoinClauses, 1, "USING clause should produce one join clause")
	assert.Contains(t, result.JoinClauses[0], "USING",
		"Join clause should contain the USING keyword")
	assert.Contains(t, result.JoinClauses[0], "customer_id",
		"Join clause should contain the shared column name")
}

// TestAnalyze_JoinClauses_SelfJoin verifies that a self-join (same table
// with different aliases) captures the ON condition correctly.
func TestAnalyze_JoinClauses_SelfJoin(t *testing.T) {
	sql := `SELECT e.name, m.name FROM employees e JOIN employees m ON e.manager_id = m.id`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.JoinClauses, 1, "Self-join should produce one join clause")
	assert.Contains(t, result.JoinClauses[0], "e.manager_id",
		"Self-join should reference the employee's manager_id")
	assert.Contains(t, result.JoinClauses[0], "m.id",
		"Self-join should reference the manager's id")

	// Verify both table aliases reference the same base table
	require.Len(t, result.Tables, 2)
	assert.Equal(t, "employees", result.Tables[0].Name)
	assert.Equal(t, "employees", result.Tables[1].Name)
	assert.Equal(t, "e", result.Tables[0].Alias)
	assert.Equal(t, "m", result.Tables[1].Alias)
}

// TestAnalyze_JoinClauses_MultiColumnJoin verifies that a multi-column
// join condition (AND in ON clause) is captured as a single join clause.
func TestAnalyze_JoinClauses_MultiColumnJoin(t *testing.T) {
	sql := `SELECT * FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.JoinClauses, 1,
		"Multi-column join should be captured as a single join clause")
	clause := result.JoinClauses[0]
	assert.Contains(t, clause, "t1.a")
	assert.Contains(t, clause, "t2.a")
	assert.Contains(t, clause, "t1.b")
	assert.Contains(t, clause, "t2.b")
}

// =============================================================================
// 3. Parameters -- verify parameter metadata extraction
// =============================================================================

// TestAnalyze_Parameters_PositionalDollar verifies that $1 and $2 positional
// parameters are extracted with correct Raw, Marker, and Position fields.
func TestAnalyze_Parameters_PositionalDollar(t *testing.T) {
	sql := `SELECT * FROM users WHERE id = $1 AND name = $2`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.Parameters, 2, "Should detect two positional parameters")

	assert.Equal(t, "$1", result.Parameters[0].Raw,
		"First parameter Raw should be '$1'")
	assert.Equal(t, "$", result.Parameters[0].Marker,
		"First parameter Marker should be '$'")
	assert.Equal(t, 1, result.Parameters[0].Position,
		"First parameter Position should be 1")

	assert.Equal(t, "$2", result.Parameters[1].Raw,
		"Second parameter Raw should be '$2'")
	assert.Equal(t, "$", result.Parameters[1].Marker,
		"Second parameter Marker should be '$'")
	assert.Equal(t, 2, result.Parameters[1].Position,
		"Second parameter Position should be 2")
}

// TestAnalyze_Parameters_MixedDollarAndQuestion verifies that mixed $N and ?
// parameter markers are both extracted with correct metadata.
func TestAnalyze_Parameters_MixedDollarAndQuestion(t *testing.T) {
	sql := `SELECT * FROM users WHERE id = $1 AND active = ?`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.Parameters, 2, "Should detect both parameter styles")

	// First parameter: $1
	assert.Equal(t, "$1", result.Parameters[0].Raw)
	assert.Equal(t, "$", result.Parameters[0].Marker)
	assert.Equal(t, 1, result.Parameters[0].Position)

	// Second parameter: ?
	assert.Equal(t, "?", result.Parameters[1].Raw)
	assert.Equal(t, "?", result.Parameters[1].Marker)
	assert.Equal(t, 1, result.Parameters[1].Position,
		"Anonymous ? parameters have Position=1 (sequential within their marker type)")
}

// =============================================================================
// 4. SetOperations -- verify UNION/INTERSECT/EXCEPT are fully parsed
// =============================================================================

// TestAnalyze_SetOperations_UnionAll verifies that UNION ALL is parsed with correct
// Type, Query, Columns, and Tables from the right-hand branch.
func TestAnalyze_SetOperations_UnionAll(t *testing.T) {
	sql := `SELECT id, name FROM employees WHERE dept = 'eng' UNION ALL SELECT id, name FROM contractors WHERE active = true`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.SetOperations, 1, "Should detect one set operation")

	setOp := result.SetOperations[0]
	assert.Equal(t, SQLSetOperationUnionAll, setOp.Type,
		"Set operation type should be UNION ALL")
	assert.Contains(t, setOp.Query, "contractors",
		"Set operation Query should contain the RHS select text")
	assert.ElementsMatch(t, []string{"id", "name"}, setOp.Columns,
		"Set operation Columns should list projected columns from the RHS")

	// Tables from the RHS branch
	require.Len(t, setOp.Tables, 1, "RHS should reference one table")
	assert.Equal(t, "contractors", setOp.Tables[0].Name,
		"RHS table should be contractors")

	// Main query should reference employees as well
	hasEmployees := false
	for _, tbl := range result.Tables {
		if tbl.Name == "employees" {
			hasEmployees = true
		}
	}
	assert.True(t, hasEmployees, "Main query should include the employees table")
}

// TestAnalyze_SetOperations_IntersectAndExcept verifies that a query with both
// INTERSECT and EXCEPT produces multiple set operations.
func TestAnalyze_SetOperations_IntersectAndExcept(t *testing.T) {
	sql := `(SELECT product_id FROM orders WHERE created_at > '2024-01-01')
			INTERSECT
			(SELECT product_id FROM inventory WHERE quantity > 0)
			EXCEPT
			(SELECT product_id FROM discontinued)`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.GreaterOrEqual(t, len(result.SetOperations), 2,
		"Should detect at least two set operations (INTERSECT and EXCEPT)")

	// Collect all set operation types
	var types []SQLSetOperationType
	for _, op := range result.SetOperations {
		types = append(types, op.Type)
	}
	assert.Contains(t, types, SQLSetOperationIntersect,
		"Should contain an INTERSECT operation")
	assert.Contains(t, types, SQLSetOperationExcept,
		"Should contain an EXCEPT operation")
}

// =============================================================================
// 5. Subqueries -- verify subquery metadata
// =============================================================================

// TestAnalyze_Subqueries_DerivedTable verifies that a FROM subquery is captured
// with its alias and a nested Analysis containing correct tables and columns.
func TestAnalyze_Subqueries_DerivedTable(t *testing.T) {
	sql := `SELECT * FROM (SELECT id, name FROM users) sub`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.Subqueries, 1, "Should detect one subquery")

	sub := result.Subqueries[0]
	assert.Equal(t, "sub", sub.Alias,
		"Subquery alias should be 'sub'")
	require.NotNil(t, sub.Analysis,
		"Subquery Analysis should not be nil")

	// Verify the nested analysis has correct tables
	require.Len(t, sub.Analysis.Tables, 1,
		"Nested analysis should reference one table")
	assert.Equal(t, "users", sub.Analysis.Tables[0].Name,
		"Nested analysis table should be 'users'")
	assert.Equal(t, SQLTableTypeBase, sub.Analysis.Tables[0].Type,
		"Nested table type should be 'base'")

	// Verify the nested analysis has correct projected columns
	require.Len(t, sub.Analysis.Columns, 2,
		"Nested analysis should project two columns")
	assert.Equal(t, "id", sub.Analysis.Columns[0].Expression)
	assert.Equal(t, "name", sub.Analysis.Columns[1].Expression)
}

// TestAnalyze_Subqueries_LateralWithNestedAnalysis verifies that a LATERAL
// subquery captures nested analysis with its own tables, columns, and filters.
func TestAnalyze_Subqueries_LateralWithNestedAnalysis(t *testing.T) {
	sql := `SELECT u.id, r.cnt FROM users u CROSS JOIN LATERAL (SELECT COUNT(*) AS cnt FROM orders o WHERE o.user_id = u.id) r`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.Subqueries, 1, "Should detect the LATERAL subquery")

	sub := result.Subqueries[0]
	assert.Equal(t, "r", sub.Alias,
		"LATERAL subquery alias should be 'r'")
	require.NotNil(t, sub.Analysis)

	// The nested analysis should reference the orders table
	hasOrders := false
	for _, tbl := range sub.Analysis.Tables {
		if tbl.Name == "orders" {
			hasOrders = true
		}
	}
	assert.True(t, hasOrders,
		"LATERAL subquery analysis should reference the 'orders' table")

	// The nested analysis should have a COUNT(*) column with alias cnt
	require.GreaterOrEqual(t, len(sub.Analysis.Columns), 1)
	assert.Equal(t, "COUNT(*)", sub.Analysis.Columns[0].Expression)
	assert.Equal(t, "cnt", sub.Analysis.Columns[0].Alias)
}

// =============================================================================
// 6. CTEs -- verify CTE extraction
// =============================================================================

// TestAnalyze_CTEs_BasicCTE verifies that a CTE is extracted with correct Name,
// Query text, and empty Materialized field (default behavior).
func TestAnalyze_CTEs_BasicCTE(t *testing.T) {
	sql := `WITH active AS (SELECT id, name FROM users WHERE active = true) SELECT * FROM active`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.CTEs, 1, "Should detect one CTE")

	cte := result.CTEs[0]
	assert.Equal(t, "active", cte.Name,
		"CTE name should be 'active'")
	assert.Contains(t, cte.Query, "SELECT id, name FROM users",
		"CTE Query should contain the CTE body text")
	assert.Contains(t, cte.Query, "active = true",
		"CTE Query should include the WHERE clause")
	assert.Empty(t, cte.Materialized,
		"Default CTE should have empty Materialized field")
}

// TestAnalyze_CTEs_Materialized verifies that AS MATERIALIZED sets the
// Materialized field to "MATERIALIZED".
func TestAnalyze_CTEs_Materialized(t *testing.T) {
	sql := `WITH active AS MATERIALIZED (SELECT id, name FROM users WHERE active = true) SELECT * FROM active`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.CTEs, 1)
	assert.Equal(t, "MATERIALIZED", result.CTEs[0].Materialized,
		"AS MATERIALIZED should set Materialized to 'MATERIALIZED'")
}

// TestAnalyze_CTEs_NotMaterialized verifies that AS NOT MATERIALIZED sets
// the Materialized field to "NOT MATERIALIZED".
func TestAnalyze_CTEs_NotMaterialized(t *testing.T) {
	sql := `WITH active AS NOT MATERIALIZED (SELECT id FROM users WHERE active = true) SELECT * FROM active`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.CTEs, 1)
	assert.Equal(t, "NOT MATERIALIZED", result.CTEs[0].Materialized,
		"AS NOT MATERIALIZED should set Materialized to 'NOT MATERIALIZED'")
}

// =============================================================================
// 7. Columns -- verify column expression and alias extraction
// =============================================================================

// TestAnalyze_Columns_ExpressionAndAlias verifies that SELECT expressions and
// their aliases are correctly parsed into the Columns slice.
func TestAnalyze_Columns_ExpressionAndAlias(t *testing.T) {
	sql := `SELECT u.name, COUNT(*) AS total FROM users u GROUP BY u.name`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.Columns, 2,
		"Should detect two projected columns")

	assert.Equal(t, "u.name", result.Columns[0].Expression,
		"First column expression should be 'u.name'")
	assert.Empty(t, result.Columns[0].Alias,
		"Unaliased column should have empty Alias")

	assert.Equal(t, "COUNT(*)", result.Columns[1].Expression,
		"Second column expression should be 'COUNT(*)'")
	assert.Equal(t, "total", result.Columns[1].Alias,
		"Second column alias should be 'total'")
}

// TestAnalyze_Columns_ComputedExpression verifies that arithmetic and function
// expressions in SELECT are preserved with their aliases.
func TestAnalyze_Columns_ComputedExpression(t *testing.T) {
	sql := `SELECT price * quantity AS line_total, UPPER(name) AS upper_name FROM order_items`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.Columns, 2)
	assert.Equal(t, "price * quantity", result.Columns[0].Expression,
		"Arithmetic expression should be preserved")
	assert.Equal(t, "line_total", result.Columns[0].Alias)
	assert.Equal(t, "UPPER(name)", result.Columns[1].Expression,
		"Function call expression should be preserved")
	assert.Equal(t, "upper_name", result.Columns[1].Alias)
}

// =============================================================================
// 8. Where / Having / GroupBy actual content
// =============================================================================

// TestAnalyze_WhereContent verifies the actual string content of the Where slice,
// not just its length.
func TestAnalyze_WhereContent(t *testing.T) {
	sql := `SELECT dept, COUNT(*) AS cnt FROM employees WHERE salary > 50000 GROUP BY dept HAVING COUNT(*) > 5`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Where
	require.Len(t, result.Where, 1, "Should have one WHERE clause string")
	assert.Contains(t, result.Where[0], "salary",
		"WHERE clause should reference the salary column")
	assert.Contains(t, result.Where[0], "50000",
		"WHERE clause should include the comparison value")

	// Having
	require.Len(t, result.Having, 1, "Should have one HAVING clause string")
	assert.Contains(t, result.Having[0], "COUNT(*)",
		"HAVING clause should reference the aggregate function")
	assert.Contains(t, result.Having[0], "5",
		"HAVING clause should include the threshold value")

	// GroupBy
	require.Len(t, result.GroupBy, 1, "Should have one GROUP BY expression")
	assert.Equal(t, "dept", result.GroupBy[0],
		"GROUP BY should contain the exact column name 'dept'")
}

// TestAnalyze_WhereContentWithAliasedTable verifies WHERE clause content
// in a query with table-aliased column references.
func TestAnalyze_WhereContentWithAliasedTable(t *testing.T) {
	sql := `SELECT o.id FROM orders o WHERE o.status = 'pending' AND o.total > 100`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.NotEmpty(t, result.Where, "Should have WHERE content")
	// The WHERE content should reference both conditions
	combined := strings.Join(result.Where, " ")
	assert.Contains(t, combined, "status",
		"WHERE should reference 'status'")
	assert.Contains(t, combined, "total",
		"WHERE should reference 'total'")
}

// =============================================================================
// 9. OrderBy Direction and Nulls
// =============================================================================

// TestAnalyze_OrderBy_DirectionAndNulls verifies that ORDER BY with DESC and
// NULLS LAST captures both the Direction and Nulls fields correctly.
func TestAnalyze_OrderBy_DirectionAndNulls(t *testing.T) {
	sql := `SELECT * FROM users ORDER BY created_at DESC NULLS LAST`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.OrderBy, 1, "Should have one ORDER BY expression")

	orderExpr := result.OrderBy[0]
	assert.Equal(t, "created_at", orderExpr.Expression,
		"ORDER BY expression should be 'created_at'")
	assert.Equal(t, "DESC", orderExpr.Direction,
		"Direction should be 'DESC'")
	assert.Equal(t, "NULLS LAST", orderExpr.Nulls,
		"Nulls should be 'NULLS LAST'")
}

// TestAnalyze_OrderBy_AscNullsFirst verifies ASC direction and NULLS FIRST handling.
func TestAnalyze_OrderBy_AscNullsFirst(t *testing.T) {
	sql := `SELECT * FROM products ORDER BY price ASC NULLS FIRST`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.OrderBy, 1)
	assert.Equal(t, "price", result.OrderBy[0].Expression)
	assert.Equal(t, "ASC", result.OrderBy[0].Direction)
	assert.Equal(t, "NULLS FIRST", result.OrderBy[0].Nulls)
}

// TestAnalyze_OrderBy_MultipleExpressions verifies parsing of multiple ORDER BY
// items with different directions.
func TestAnalyze_OrderBy_MultipleExpressions(t *testing.T) {
	sql := `SELECT * FROM events ORDER BY priority DESC, created_at ASC`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.OrderBy, 2, "Should have two ORDER BY expressions")
	assert.Equal(t, "priority", result.OrderBy[0].Expression)
	assert.Equal(t, "DESC", result.OrderBy[0].Direction)
	assert.Equal(t, "created_at", result.OrderBy[1].Expression)
	assert.Equal(t, "ASC", result.OrderBy[1].Direction)
}

// =============================================================================
// 10. SetClauses content
// =============================================================================

// TestAnalyze_SetClauses_UpdateContent verifies that UPDATE SET clauses are
// captured as individual assignment strings.
func TestAnalyze_SetClauses_UpdateContent(t *testing.T) {
	sql := `UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, SQLCommandUpdate, result.Command)
	require.Len(t, result.SetClauses, 2,
		"Should have two SET clauses")
	assert.Equal(t, "name = 'John'", result.SetClauses[0],
		"First SET clause should be 'name = 'John''")
	assert.Equal(t, "email = 'john@example.com'", result.SetClauses[1],
		"Second SET clause should be the email assignment")
}

// TestAnalyze_SetClauses_UpdateWithParameter verifies SET clauses with
// parameter placeholders.
func TestAnalyze_SetClauses_UpdateWithParameter(t *testing.T) {
	sql := `UPDATE orders SET status = $1, updated_at = NOW() WHERE id = $2`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Len(t, result.SetClauses, 2)
	assert.Contains(t, result.SetClauses[0], "status",
		"First SET clause should reference status")
	assert.Contains(t, result.SetClauses[1], "updated_at",
		"Second SET clause should reference updated_at")
}

// =============================================================================
// 11. InsertColumns names
// =============================================================================

// TestAnalyze_InsertColumns_ExactNames verifies that INSERT column names are
// extracted in the correct order.
func TestAnalyze_InsertColumns_ExactNames(t *testing.T) {
	sql := `INSERT INTO users (name, email, age) VALUES ('John', 'john@example.com', 30)`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, SQLCommandInsert, result.Command)
	require.Len(t, result.InsertColumns, 3,
		"Should detect three insert columns")
	assert.Equal(t, "name", result.InsertColumns[0],
		"First insert column should be 'name'")
	assert.Equal(t, "email", result.InsertColumns[1],
		"Second insert column should be 'email'")
	assert.Equal(t, "age", result.InsertColumns[2],
		"Third insert column should be 'age'")
}

// TestAnalyze_InsertColumns_WithParameters verifies insert columns
// when values use parameter placeholders.
func TestAnalyze_InsertColumns_WithParameters(t *testing.T) {
	sql := `INSERT INTO products (sku, price, category) VALUES ($1, $2, $3)`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, []string{"sku", "price", "category"}, result.InsertColumns,
		"InsertColumns should exactly match the column list")
	require.Len(t, result.Parameters, 3,
		"Should detect three parameters in VALUES")
}

// =============================================================================
// 12. Returning in DELETE / UPDATE
// =============================================================================

// TestAnalyze_Returning_DeleteReturning verifies that DELETE...RETURNING extracts
// the returned column names.
func TestAnalyze_Returning_DeleteReturning(t *testing.T) {
	sql := `DELETE FROM users WHERE id = 1 RETURNING id, name`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, SQLCommandDelete, result.Command)
	require.Len(t, result.Returning, 2,
		"DELETE RETURNING should return two column names")
	assert.Equal(t, "id", result.Returning[0])
	assert.Equal(t, "name", result.Returning[1])
}

// TestAnalyze_Returning_UpdateReturning verifies that UPDATE...RETURNING extracts
// the returned column names.
func TestAnalyze_Returning_UpdateReturning(t *testing.T) {
	sql := `UPDATE users SET name = 'John' WHERE id = 1 RETURNING id, name, email`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, SQLCommandUpdate, result.Command)
	require.Len(t, result.Returning, 3,
		"UPDATE RETURNING should return three column names")
	assert.Equal(t, "id", result.Returning[0])
	assert.Equal(t, "name", result.Returning[1])
	assert.Equal(t, "email", result.Returning[2])
}

// TestAnalyze_Returning_ColumnUsageReturning verifies that RETURNING columns
// appear in ColumnUsage with the returning usage type.
func TestAnalyze_Returning_ColumnUsageReturning(t *testing.T) {
	sql := `DELETE FROM users WHERE id = 1 RETURNING id, name`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	var returningCols []string
	for _, usage := range result.ColumnUsage {
		if usage.UsageType == SQLUsageTypeReturning {
			returningCols = append(returningCols, usage.Column)
		}
	}
	assert.Contains(t, returningCols, "id",
		"ColumnUsage should have returning usage for 'id'")
	assert.Contains(t, returningCols, "name",
		"ColumnUsage should have returning usage for 'name'")
}

// =============================================================================
// 13. Upsert -- Constraint, ActionWhere, TargetWhere
// =============================================================================

// TestAnalyze_Upsert_OnConstraint verifies that ON CONFLICT ON CONSTRAINT sets
// the Constraint field and clears TargetColumns.
func TestAnalyze_Upsert_OnConstraint(t *testing.T) {
	sql := `INSERT INTO users (id, email) VALUES (1, 'a@b.com') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET email = EXCLUDED.email`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.NotNil(t, result.Upsert, "Should detect upsert metadata")
	assert.Equal(t, "users_pkey", result.Upsert.Constraint,
		"Constraint should be 'users_pkey'")
	assert.Equal(t, "DO UPDATE", result.Upsert.Action,
		"Action should be 'DO UPDATE'")
	assert.Nil(t, result.Upsert.TargetColumns,
		"TargetColumns should be nil when using ON CONSTRAINT")
}

// TestAnalyze_Upsert_TargetWhereAndActionWhere verifies that WHERE on the
// conflict target and WHERE on DO UPDATE are both captured.
func TestAnalyze_Upsert_TargetWhereAndActionWhere(t *testing.T) {
	sql := `INSERT INTO users (id, email, status) VALUES (1, 'a@b.com', 'active') ON CONFLICT (email) WHERE status = 'active' DO UPDATE SET email = EXCLUDED.email WHERE users.status != 'deleted'`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Upsert)

	assert.Contains(t, result.Upsert.TargetWhere, "status = 'active'",
		"TargetWhere should contain the conflict target WHERE predicate")
	assert.Contains(t, result.Upsert.ActionWhere, "status != 'deleted'",
		"ActionWhere should contain the DO UPDATE WHERE predicate")
	assert.Equal(t, []string{"email"}, result.Upsert.TargetColumns,
		"TargetColumns should list 'email'")
}

// TestAnalyze_Upsert_DoNothing verifies DO NOTHING action is captured.
func TestAnalyze_Upsert_DoNothing(t *testing.T) {
	sql := `INSERT INTO users (id, email) VALUES (1, 'a@b.com') ON CONFLICT (email) DO NOTHING`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Upsert)

	assert.Equal(t, "DO NOTHING", result.Upsert.Action,
		"Action should be 'DO NOTHING'")
	assert.Empty(t, result.Upsert.SetClauses,
		"DO NOTHING should have no SET clauses")
}

// =============================================================================
// 14. Merge fields
// =============================================================================

// TestAnalyze_Merge_FullMetadata verifies that MERGE extracts Target, Source,
// Condition, and multiple WHEN actions with their details.
func TestAnalyze_Merge_FullMetadata(t *testing.T) {
	sql := `MERGE INTO target t USING source s ON t.id = s.id
			WHEN MATCHED AND t.status = 'old' THEN UPDATE SET name = s.name
			WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, SQLCommandMerge, result.Command)
	require.NotNil(t, result.Merge, "Merge metadata should be present")

	merge := result.Merge

	// Target
	assert.Equal(t, "target", merge.Target.Name,
		"MERGE target table should be 'target'")
	assert.Equal(t, "t", merge.Target.Alias,
		"MERGE target alias should be 't'")

	// Source
	require.NotNil(t, merge.Source.Table,
		"MERGE source table should be populated")
	assert.Equal(t, "source", merge.Source.Table.Name,
		"MERGE source table should be 'source'")
	assert.Equal(t, "s", merge.Source.Table.Alias,
		"MERGE source alias should be 's'")

	// Condition
	assert.Equal(t, "t.id = s.id", merge.Condition,
		"MERGE ON condition should be 't.id = s.id'")

	// Actions
	require.Len(t, merge.Actions, 2, "Should have two MERGE actions")

	// UPDATE action
	updateAction := merge.Actions[0]
	assert.Equal(t, "UPDATE", updateAction.Type)
	assert.Equal(t, "t.status = 'old'", updateAction.Condition,
		"UPDATE action should have condition 't.status = 'old''")
	require.Len(t, updateAction.SetClauses, 1)
	assert.Equal(t, "name = s.name", updateAction.SetClauses[0])

	// INSERT action
	insertAction := merge.Actions[1]
	assert.Equal(t, "INSERT", insertAction.Type)
	assert.Empty(t, insertAction.Condition,
		"INSERT action should have no condition")
	assert.Equal(t, []string{"id", "name"}, insertAction.InsertColumns,
		"INSERT action should list columns")
	assert.Contains(t, insertAction.InsertValues, "s.id",
		"INSERT action values should reference source columns")
}

// TestAnalyze_Merge_ColumnUsageTypes verifies that MERGE produces correct
// ColumnUsage types for merge_set and merge_insert.
func TestAnalyze_Merge_ColumnUsageTypes(t *testing.T) {
	sql := `MERGE INTO target t USING source s ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET name = s.name
			WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	var hasMergeSet, hasMergeInsert bool
	for _, usage := range result.ColumnUsage {
		if usage.UsageType == SQLUsageTypeMergeSet {
			hasMergeSet = true
		}
		if usage.UsageType == SQLUsageTypeMergeInsert {
			hasMergeInsert = true
		}
	}
	assert.True(t, hasMergeSet,
		"Should have merge_set usage for UPDATE SET columns")
	assert.True(t, hasMergeInsert,
		"Should have merge_insert usage for INSERT VALUES columns")
}

// =============================================================================
// 15. Correlations fields
// =============================================================================

// TestAnalyze_Correlations_LateralUnnest verifies that a LATERAL function call
// produces a Correlation with OuterAlias, Expression, and Type="LATERAL".
func TestAnalyze_Correlations_LateralUnnest(t *testing.T) {
	sql := `SELECT * FROM products p CROSS JOIN LATERAL unnest(p.tags) AS tag(name)`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.NotNil(t, result.Correlations,
		"Correlations should be populated for LATERAL joins")
	require.GreaterOrEqual(t, len(result.Correlations), 1,
		"Should have at least one correlation entry")

	corr := result.Correlations[0]
	assert.Equal(t, "p", corr.OuterAlias,
		"OuterAlias should reference the products table alias 'p'")
	assert.Equal(t, "LATERAL", corr.Type,
		"Correlation type should be 'LATERAL'")
	assert.Contains(t, corr.Expression, "unnest",
		"Expression should contain the function name 'unnest'")
	assert.Contains(t, corr.Expression, "p.tags",
		"Expression should reference the outer column 'p.tags'")
}

// =============================================================================
// 16. DerivedColumns
// =============================================================================

// TestAnalyze_DerivedColumns_ComputedAndFunction verifies that the DerivedColumns
// map correctly associates aliases with their source expressions.
func TestAnalyze_DerivedColumns_ComputedAndFunction(t *testing.T) {
	sql := `SELECT price * quantity AS line_total, UPPER(name) AS upper_name FROM order_items`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.NotNil(t, result.DerivedColumns,
		"DerivedColumns should be populated")
	assert.Equal(t, "price * quantity", result.DerivedColumns["line_total"],
		"'line_total' should map to the arithmetic expression")
	assert.Equal(t, "UPPER(name)", result.DerivedColumns["upper_name"],
		"'upper_name' should map to the UPPER function call")
}

// TestAnalyze_DerivedColumns_AggregateAlias verifies DerivedColumns tracks
// aggregate function aliases correctly.
func TestAnalyze_DerivedColumns_AggregateAlias(t *testing.T) {
	sql := `SELECT dept, COUNT(*) AS cnt FROM employees GROUP BY dept`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.NotNil(t, result.DerivedColumns)
	assert.Equal(t, "COUNT(*)", result.DerivedColumns["cnt"],
		"'cnt' should map to 'COUNT(*)'")
}

// TestAnalyze_DerivedColumns_NoAliasNotIncluded verifies that columns without
// aliases are not present in the DerivedColumns map.
func TestAnalyze_DerivedColumns_NoAliasNotIncluded(t *testing.T) {
	sql := `SELECT u.name, COUNT(*) AS total FROM users u GROUP BY u.name`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// u.name has no alias, so it should NOT be in DerivedColumns
	_, hasName := result.DerivedColumns["name"]
	assert.False(t, hasName,
		"Unaliased column 'name' should not appear in DerivedColumns")
	// total has an alias
	assert.Equal(t, "COUNT(*)", result.DerivedColumns["total"],
		"'total' alias should map to 'COUNT(*)'")
}

// =============================================================================
// Integration: end-to-end query with many features
// =============================================================================

// TestAnalyze_Integration_ComplexQuery verifies that a complex query with CTE,
// subquery, join, WHERE, GROUP BY, HAVING, ORDER BY, and LIMIT all produce
// correct analysis results simultaneously.
func TestAnalyze_Integration_ComplexQuery(t *testing.T) {
	sql := `WITH recent AS (
		SELECT user_id, MAX(created_at) AS last_order
		FROM orders
		WHERE created_at > '2024-01-01'
		GROUP BY user_id
	)
	SELECT u.name, r.last_order, COUNT(o.id) AS order_count
	FROM users u
	JOIN recent r ON u.id = r.user_id
	JOIN orders o ON o.user_id = u.id
	WHERE u.active = true
	GROUP BY u.name, r.last_order
	HAVING COUNT(o.id) > 3
	ORDER BY order_count DESC NULLS LAST
	LIMIT 10 OFFSET 5`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Command
	assert.Equal(t, SQLCommandSelect, result.Command)

	// RawSQL preserved
	assert.Equal(t, sql, result.RawSQL)

	// CTE
	require.Len(t, result.CTEs, 1)
	assert.Equal(t, "recent", result.CTEs[0].Name)

	// Tables: users (base), orders (base), recent (CTE), plus inner orders from CTE
	hasUsers := false
	hasOrders := false
	hasRecent := false
	for _, tbl := range result.Tables {
		switch tbl.Name {
		case "users":
			hasUsers = true
		case "orders":
			hasOrders = true
		case "recent":
			if tbl.Type == SQLTableTypeCTE {
				hasRecent = true
			}
		}
	}
	assert.True(t, hasUsers, "Should have users table")
	assert.True(t, hasOrders, "Should have orders table")
	assert.True(t, hasRecent, "Should have recent CTE")

	// Columns
	require.GreaterOrEqual(t, len(result.Columns), 3)

	// JoinClauses
	assert.NotEmpty(t, result.JoinClauses, "Should have join clauses")

	// Where
	assert.NotEmpty(t, result.Where, "Should have WHERE content")

	// GroupBy
	assert.GreaterOrEqual(t, len(result.GroupBy), 2,
		"Should have at least two GROUP BY columns")

	// Having
	assert.NotEmpty(t, result.Having, "Should have HAVING clause")

	// OrderBy
	require.Len(t, result.OrderBy, 1)
	assert.Equal(t, "DESC", result.OrderBy[0].Direction)
	assert.Equal(t, "NULLS LAST", result.OrderBy[0].Nulls)

	// Limit
	require.NotNil(t, result.Limit)
	assert.Equal(t, "LIMIT 10", result.Limit.Limit)
	assert.Equal(t, "OFFSET 5", result.Limit.Offset)

	// DerivedColumns
	assert.NotNil(t, result.DerivedColumns)
}

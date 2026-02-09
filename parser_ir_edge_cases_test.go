// parser_ir_edge_cases_test.go covers advanced and edge-case SQL constructs
// to improve parser coverage.
package postgresparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// 1. Complex nested subqueries
// ---------------------------------------------------------------------------

// TestIR_DeeplyNestedSubqueryInWhere validates deeply nested subqueries with IN and aggregate.
func TestIR_DeeplyNestedSubqueryInWhere(t *testing.T) {
	sql := `
SELECT id, name
FROM users
WHERE id IN (
  SELECT user_id
  FROM orders
  WHERE total > (
    SELECT AVG(total) FROM orders
  )
)`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandSelect, ir.Command, "expected SELECT command")
	require.Len(t, ir.Where, 1, "expected 1 WHERE clause")
	assert.Contains(t, ir.Where[0], "IN", "expected WHERE to contain IN")
	assert.True(t, containsTable(ir.Tables, "users"), "expected users table")
}

// TestIR_SubqueryInSelectList verifies scalar subquery in SELECT projection.
func TestIR_SubqueryInSelectList(t *testing.T) {
	sql := `
SELECT
  u.id,
  u.name,
  (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count
FROM users u`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Columns, 3, "expected 3 columns")
	assert.Equal(t, "order_count", ir.Columns[2].Alias, "expected alias 'order_count'")
	assert.Contains(t, ir.Columns[2].Expression, "SELECT COUNT(*)", "expected subquery in select list expression")
}

// TestIR_SubqueryInFrom confirms derived table in FROM with GROUP BY/HAVING.
func TestIR_SubqueryInFrom(t *testing.T) {
	sql := `
SELECT sub.total_amount, sub.user_id
FROM (
  SELECT user_id, SUM(amount) AS total_amount
  FROM payments
  GROUP BY user_id
  HAVING SUM(amount) > 100
) sub
WHERE sub.total_amount < 10000`
	ir := parseAssertNoError(t, sql)

	assert.True(t, containsTable(ir.Tables, "payments"), "expected payments table to be surfaced from subquery")

	foundSubquery := false
	for _, tbl := range ir.Tables {
		if tbl.Type == TableTypeSubquery && tbl.Alias == "sub" {
			foundSubquery = true
		}
	}
	assert.True(t, foundSubquery, "expected subquery table with alias 'sub'")

	require.NotEmpty(t, ir.Subqueries, "expected subquery metadata")

	// Verify the subquery's internal details
	var sq *SubqueryRef
	for i := range ir.Subqueries {
		if ir.Subqueries[i].Alias == "sub" {
			sq = &ir.Subqueries[i]
			break
		}
	}
	require.NotNil(t, sq, "expected parsed subquery metadata")
	require.NotNil(t, sq.Query, "expected parsed subquery query")
	require.NotEmpty(t, sq.Query.GroupBy, "expected subquery GROUP BY")
	assert.Contains(t, sq.Query.GroupBy[0], "user_id", "expected subquery GROUP BY user_id")
}

// ---------------------------------------------------------------------------
// 2. Multiple CTEs referencing each other
// ---------------------------------------------------------------------------

// TestIR_MultipleCTEsReferencingEachOther validates chained CTEs referencing each other.
func TestIR_MultipleCTEsReferencingEachOther(t *testing.T) {
	sql := `
WITH base AS (
  SELECT id, amount, category FROM transactions WHERE amount > 0
),
summarized AS (
  SELECT category, SUM(amount) AS total, COUNT(*) AS cnt
  FROM base
  GROUP BY category
),
filtered AS (
  SELECT category, total
  FROM summarized
  WHERE cnt >= 5
)
SELECT * FROM filtered ORDER BY total DESC`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.CTEs, 3, "expected 3 CTEs")
	names := make([]string, len(ir.CTEs))
	for i, cte := range ir.CTEs {
		names[i] = strings.ToLower(cte.Name)
	}
	assert.Equal(t, []string{"base", "summarized", "filtered"}, names, "unexpected CTE names")

	// The base table "transactions" should be found
	assert.True(t, containsTable(ir.Tables, "transactions"), "expected transactions table from CTE")

	// "filtered" should be a CTE reference
	foundFilteredCTE := false
	for _, tbl := range ir.Tables {
		if strings.ToLower(tbl.Name) == "filtered" && tbl.Type == TableTypeCTE {
			foundFilteredCTE = true
		}
	}
	assert.True(t, foundFilteredCTE, "expected filtered CTE reference in tables")
}

// TestIR_CTEWithMaterialized checks MATERIALIZED annotation on CTEs.
func TestIR_CTEWithMaterialized(t *testing.T) {
	sql := `
WITH active AS MATERIALIZED (
  SELECT id FROM users WHERE active = true
)
SELECT * FROM active`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.CTEs, 1, "expected 1 CTE")
	assert.Equal(t, "active", strings.ToLower(ir.CTEs[0].Name), "expected CTE name 'active'")
	// Materialized annotation should be captured
	assert.Contains(t, strings.ToUpper(ir.CTEs[0].Materialized), "MATERIALIZED", "expected MATERIALIZED annotation")
}

// ---------------------------------------------------------------------------
// 3. Complex CASE expressions
// ---------------------------------------------------------------------------

// TestIR_NestedCaseExpression validates nested CASE within CASE.
func TestIR_NestedCaseExpression(t *testing.T) {
	sql := `
SELECT
  CASE
    WHEN status = 'active' THEN
      CASE
        WHEN priority > 5 THEN 'high'
        ELSE 'normal'
      END
    WHEN status = 'inactive' THEN 'disabled'
    ELSE 'unknown'
  END AS label
FROM tasks`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Columns, 1, "expected 1 column")
	assert.Equal(t, "label", ir.Columns[0].Alias, "expected alias 'label'")
	expr := ir.Columns[0].Expression
	// Nested CASE should be present
	assert.GreaterOrEqual(t, strings.Count(expr, "CASE"), 2, "expected nested CASE expression")
}

// TestIR_CaseInWhereClause verifies CASE expression in WHERE predicate.
func TestIR_CaseInWhereClause(t *testing.T) {
	sql := `
SELECT id, name
FROM users
WHERE CASE WHEN role = 'admin' THEN true ELSE false END = true`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Where, 1, "expected 1 WHERE clause")
	assert.Contains(t, ir.Where[0], "CASE", "expected CASE in WHERE")
}

// TestIR_CaseInOrderBy confirms CASE expression in ORDER BY with direction.
func TestIR_CaseInOrderBy(t *testing.T) {
	sql := `
SELECT id, status
FROM orders
ORDER BY
  CASE status
    WHEN 'urgent' THEN 1
    WHEN 'normal' THEN 2
    ELSE 3
  END ASC`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.OrderBy, 1, "expected 1 ORDER BY")
	assert.Contains(t, ir.OrderBy[0].Expression, "CASE", "expected CASE in ORDER BY")
	assert.Equal(t, "ASC", ir.OrderBy[0].Direction, "expected ASC direction")
}

// ---------------------------------------------------------------------------
// 4. Array operations
// ---------------------------------------------------------------------------

// TestIR_ArrayAnyOperator validates ANY() array operator.
func TestIR_ArrayAnyOperator(t *testing.T) {
	sql := `SELECT id FROM users WHERE id = ANY($1)`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Where, 1, "expected 1 WHERE clause")
	assert.Contains(t, ir.Where[0], "ANY", "expected ANY in WHERE")
}

// TestIR_ArrayAllOperator verifies ALL() with subquery.
func TestIR_ArrayAllOperator(t *testing.T) {
	sql := `SELECT id FROM scores WHERE score > ALL(SELECT min_score FROM thresholds)`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Where, 1, "expected 1 WHERE clause")
	whereUpper := strings.ToUpper(ir.Where[0])
	assert.Contains(t, whereUpper, "ALL", "expected ALL in WHERE")
}

// TestIR_ArrayConstructor checks ARRAY[] constructor syntax.
func TestIR_ArrayConstructor(t *testing.T) {
	sql := `SELECT ARRAY[1, 2, 3] AS nums FROM generate_series(1,1)`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Columns, 1, "expected 1 column")
	assert.Equal(t, "nums", ir.Columns[0].Alias, "expected alias 'nums'")
}

// ---------------------------------------------------------------------------
// 5. Type casting
// ---------------------------------------------------------------------------

// TestIR_TypeCastInProjection validates :: type cast in SELECT list.
func TestIR_TypeCastInProjection(t *testing.T) {
	sql := `SELECT id::text, amount::numeric(10,2) AS amt FROM orders`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Columns, 2, "expected 2 columns")
	assert.Contains(t, ir.Columns[0].Expression, "::", "expected type cast in first column")
	assert.Equal(t, "amt", ir.Columns[1].Alias, "expected alias 'amt'")
}

// TestIR_TypeCastInWhere verifies :: cast in WHERE clause.
func TestIR_TypeCastInWhere(t *testing.T) {
	sql := `SELECT id FROM events WHERE created_at::date = '2024-01-01'::date`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Where, 1, "expected 1 WHERE clause")
	assert.Contains(t, ir.Where[0], "::", "expected type cast in WHERE")
}

// TestIR_CastFunction confirms CAST(expr AS type) syntax.
func TestIR_CastFunction(t *testing.T) {
	sql := `SELECT CAST(price AS integer) AS int_price FROM products`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Columns, 1, "expected 1 column")
	assert.Contains(t, strings.ToUpper(ir.Columns[0].Expression), "CAST", "expected CAST in column expression")
	assert.Equal(t, "int_price", ir.Columns[0].Alias, "expected alias 'int_price'")
}

// ---------------------------------------------------------------------------
// 6. Complex JOIN conditions
// ---------------------------------------------------------------------------

// TestIR_MultiColumnJoin validates multi-column join condition.
func TestIR_MultiColumnJoin(t *testing.T) {
	sql := `
SELECT a.id, b.name
FROM table_a a
JOIN table_b b ON a.col1 = b.col1 AND a.col2 = b.col2 AND a.col3 = b.col3`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Tables, 2, "expected 2 tables")
	require.Len(t, ir.JoinConditions, 1, "expected 1 join condition")
	join := normalise(ir.JoinConditions[0])
	assert.Contains(t, join, "a.col1=b.col1", "expected multi-column join condition")
	assert.Contains(t, join, "a.col2=b.col2", "expected multi-column join condition")
	assert.Contains(t, join, "a.col3=b.col3", "expected multi-column join condition")
}

// TestIR_JoinWithSubquery verifies JOIN on a derived table.
func TestIR_JoinWithSubquery(t *testing.T) {
	sql := `
SELECT o.id, totals.sum_amount
FROM orders o
JOIN (
  SELECT order_id, SUM(amount) AS sum_amount
  FROM line_items
  GROUP BY order_id
) totals ON totals.order_id = o.id
WHERE totals.sum_amount > 500`
	ir := parseAssertNoError(t, sql)

	assert.True(t, containsTable(ir.Tables, "orders"), "expected orders table")
	assert.True(t, containsTable(ir.Tables, "line_items"), "expected line_items table from subquery")

	foundSubq := false
	for _, tbl := range ir.Tables {
		if tbl.Type == TableTypeSubquery && tbl.Alias == "totals" {
			foundSubq = true
		}
	}
	assert.True(t, foundSubq, "expected subquery table 'totals'")
}

// TestIR_MultipleJoinTypes checks INNER, LEFT, CROSS join mix.
func TestIR_MultipleJoinTypes(t *testing.T) {
	sql := `
SELECT u.id, o.id, p.name
FROM users u
INNER JOIN orders o ON u.id = o.user_id
LEFT JOIN products p ON o.product_id = p.id
CROSS JOIN settings s`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Tables, 4, "expected 4 tables")
	expectedTables := []string{"users", "orders", "products", "settings"}
	for _, name := range expectedTables {
		assert.True(t, containsTable(ir.Tables, name), "expected table %q", name)
	}
}

// TestIR_JoinUSING confirms JOIN with USING clause.
func TestIR_JoinUSING(t *testing.T) {
	sql := `
SELECT *
FROM departments d
JOIN employees e USING (department_id)`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Tables, 2, "expected 2 tables")
	require.Len(t, ir.JoinConditions, 1, "expected 1 join condition")
	assert.Contains(t, strings.ToUpper(ir.JoinConditions[0]), "USING", "expected USING in join condition")
}

// ---------------------------------------------------------------------------
// 7. LATERAL joins with correlated subqueries
// ---------------------------------------------------------------------------

// TestIR_LateralJoinWithSubquery validates LATERAL subquery with LIMIT.
func TestIR_LateralJoinWithSubquery(t *testing.T) {
	sql := `
SELECT u.id, recent.order_id
FROM users u
CROSS JOIN LATERAL (
  SELECT o.id AS order_id
  FROM orders o
  WHERE o.user_id = u.id
  ORDER BY o.created_at DESC
  LIMIT 3
) recent`
	ir := parseAssertNoError(t, sql)

	assert.True(t, containsTable(ir.Tables, "users"), "expected users table")

	foundSubq := false
	for _, tbl := range ir.Tables {
		if tbl.Type == TableTypeSubquery && tbl.Alias == "recent" {
			foundSubq = true
		}
	}
	assert.True(t, foundSubq, "expected LATERAL subquery 'recent'")
}

// TestIR_LateralJoinWithFunction verifies LATERAL function call (unnest).
func TestIR_LateralJoinWithFunction(t *testing.T) {
	sql := `
SELECT p.id, tag.value
FROM products p
CROSS JOIN LATERAL unnest(p.tags) AS tag(value)`
	ir := parseAssertNoError(t, sql)

	assert.True(t, containsTable(ir.Tables, "products"), "expected products table")
	foundFunc := false
	for _, tbl := range ir.Tables {
		if tbl.Type == TableTypeFunction && tbl.Alias == "tag" {
			foundFunc = true
		}
	}
	assert.True(t, foundFunc, "expected LATERAL function 'tag'")
}

// ---------------------------------------------------------------------------
// 8. String functions - COALESCE, NULLIF, GREATEST, LEAST, concatenation
// ---------------------------------------------------------------------------

// TestIR_CoalesceFunction validates COALESCE with multiple arguments.
func TestIR_CoalesceFunction(t *testing.T) {
	sql := `SELECT COALESCE(nickname, first_name, 'Anonymous') AS display_name FROM users`
	ir := parseAssertNoError(t, sql)

	if len(ir.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(ir.Columns))
	}
	if !strings.Contains(ir.Columns[0].Expression, "COALESCE") {
		t.Fatalf("expected COALESCE in expression, got %q", ir.Columns[0].Expression)
	}
	if ir.Columns[0].Alias != "display_name" {
		t.Fatalf("expected alias 'display_name', got %q", ir.Columns[0].Alias)
	}
}

// TestIR_NullIfFunction verifies NULLIF function.
func TestIR_NullIfFunction(t *testing.T) {
	sql := `SELECT NULLIF(status, '') AS clean_status FROM orders`
	ir := parseAssertNoError(t, sql)

	if len(ir.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(ir.Columns))
	}
	if !strings.Contains(ir.Columns[0].Expression, "NULLIF") {
		t.Fatalf("expected NULLIF in expression, got %q", ir.Columns[0].Expression)
	}
}

// TestIR_GreatestLeastFunctions checks GREATEST and LEAST functions.
func TestIR_GreatestLeastFunctions(t *testing.T) {
	sql := `
SELECT
  GREATEST(a, b, c) AS max_val,
  LEAST(a, b, c) AS min_val
FROM measurements`
	ir := parseAssertNoError(t, sql)

	if len(ir.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ir.Columns))
	}
	if !strings.Contains(ir.Columns[0].Expression, "GREATEST") {
		t.Fatalf("expected GREATEST in expression, got %q", ir.Columns[0].Expression)
	}
	if !strings.Contains(ir.Columns[1].Expression, "LEAST") {
		t.Fatalf("expected LEAST in expression, got %q", ir.Columns[1].Expression)
	}
}

// TestIR_StringConcatenation validates || string concatenation operator.
func TestIR_StringConcatenation(t *testing.T) {
	sql := `SELECT first_name || ' ' || last_name AS full_name FROM users`
	ir := parseAssertNoError(t, sql)

	if len(ir.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(ir.Columns))
	}
	if !strings.Contains(ir.Columns[0].Expression, "||") {
		t.Fatalf("expected || operator in expression, got %q", ir.Columns[0].Expression)
	}
	if ir.Columns[0].Alias != "full_name" {
		t.Fatalf("expected alias 'full_name', got %q", ir.Columns[0].Alias)
	}
}

// ---------------------------------------------------------------------------
// 9. Date/time operations
// ---------------------------------------------------------------------------

// TestIR_ExtractFunction validates EXTRACT(field FROM expr).
func TestIR_ExtractFunction(t *testing.T) {
	sql := `SELECT EXTRACT(YEAR FROM created_at) AS yr FROM events`
	ir := parseAssertNoError(t, sql)

	if len(ir.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(ir.Columns))
	}
	expr := strings.ToUpper(ir.Columns[0].Expression)
	if !strings.Contains(expr, "EXTRACT") {
		t.Fatalf("expected EXTRACT in column expression, got %q", ir.Columns[0].Expression)
	}
	if ir.Columns[0].Alias != "yr" {
		t.Fatalf("expected alias 'yr', got %q", ir.Columns[0].Alias)
	}
}

// TestIR_DateTruncFunction verifies DATE_TRUNC in SELECT and GROUP BY.
func TestIR_DateTruncFunction(t *testing.T) {
	sql := `
SELECT DATE_TRUNC('month', created_at) AS month, COUNT(*) AS cnt
FROM orders
GROUP BY DATE_TRUNC('month', created_at)`
	ir := parseAssertNoError(t, sql)

	if len(ir.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ir.Columns))
	}
	if !strings.Contains(ir.Columns[0].Expression, "DATE_TRUNC") {
		t.Fatalf("expected DATE_TRUNC in expression, got %q", ir.Columns[0].Expression)
	}
	if len(ir.GroupBy) != 1 {
		t.Fatalf("expected 1 GROUP BY, got %d", len(ir.GroupBy))
	}
	if !strings.Contains(ir.GroupBy[0], "DATE_TRUNC") {
		assert.Contains(t, ir.GroupBy[0], "DATE_TRUNC", "expected DATE_TRUNC in GROUP BY")
	}
}

// TestIR_IntervalExpression checks INTERVAL literal in WHERE.
func TestIR_IntervalExpression(t *testing.T) {
	sql := `SELECT * FROM events WHERE created_at > NOW() - INTERVAL '30 days'`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Where, 1, "expected 1 WHERE clause")
	assert.Contains(t, ir.Where[0], "INTERVAL", "expected INTERVAL in WHERE")
}

// TestIR_AtTimeZone validates AT TIME ZONE expression.
func TestIR_AtTimeZone(t *testing.T) {
	sql := `SELECT created_at AT TIME ZONE 'UTC' AS utc_time FROM events`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Columns, 1, "expected 1 column")
	exprUpper := strings.ToUpper(ir.Columns[0].Expression)
	assert.Contains(t, exprUpper, "AT TIME ZONE", "expected AT TIME ZONE in expression")
	assert.Equal(t, "utc_time", ir.Columns[0].Alias, "expected alias 'utc_time'")
}

// ---------------------------------------------------------------------------
// 10. Complex GROUP BY
// ---------------------------------------------------------------------------

// TestIR_GroupByMultipleColumns validates multiple GROUP BY columns.
func TestIR_GroupByMultipleColumns(t *testing.T) {
	sql := `
SELECT region, category, SUM(sales)
FROM metrics
GROUP BY region, category`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.GroupBy, 2, "expected 2 GROUP BY items")
	assert.Equal(t, "region", ir.GroupBy[0], "unexpected GROUP BY item 1")
	assert.Equal(t, "category", ir.GroupBy[1], "unexpected GROUP BY item 2")
}

// TestIR_GroupByWithExpression verifies expression-based GROUP BY.
func TestIR_GroupByWithExpression(t *testing.T) {
	sql := `
SELECT DATE_TRUNC('month', created_at) AS m, COUNT(*)
FROM orders
GROUP BY DATE_TRUNC('month', created_at)`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.GroupBy, 1, "expected 1 GROUP BY")
	assert.Contains(t, ir.GroupBy[0], "DATE_TRUNC", "expected DATE_TRUNC in GROUP BY")
}

// ---------------------------------------------------------------------------
// 11. HAVING with complex conditions
// ---------------------------------------------------------------------------

// TestIR_HavingWithMultipleConditions validates compound HAVING with COUNT and AVG.
func TestIR_HavingWithMultipleConditions(t *testing.T) {
	sql := `
SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price
FROM products
GROUP BY category
HAVING COUNT(*) >= 10 AND AVG(price) < 100`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Having, 1, "expected 1 HAVING clause")
	havingNorm := normalise(ir.Having[0])
	assert.Contains(t, havingNorm, "count(*)>=10", "expected COUNT condition in HAVING")
	assert.Contains(t, havingNorm, "avg(price)<100", "expected AVG condition in HAVING")
}

// TestIR_HavingWithSubquery verifies subquery in HAVING clause.
func TestIR_HavingWithSubquery(t *testing.T) {
	sql := `
SELECT department_id, AVG(salary) AS avg_sal
FROM employees
GROUP BY department_id
HAVING AVG(salary) > (SELECT AVG(salary) FROM employees)`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Having, 1, "expected 1 HAVING clause")
	assert.Contains(t, ir.Having[0], "SELECT AVG(salary)", "expected subquery in HAVING")
}

// ---------------------------------------------------------------------------
// 12. Complex ON CONFLICT
// ---------------------------------------------------------------------------

// TestIR_OnConflictMultipleColumns validates composite conflict target.
func TestIR_OnConflictMultipleColumns(t *testing.T) {
	sql := `
INSERT INTO inventory (product_id, warehouse_id, quantity)
VALUES ($1, $2, $3)
ON CONFLICT (product_id, warehouse_id) DO UPDATE
SET quantity = inventory.quantity + EXCLUDED.quantity,
    updated_at = NOW()
RETURNING product_id, warehouse_id, quantity`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandInsert, ir.Command, "expected INSERT")
	require.NotNil(t, ir.Upsert, "expected Upsert metadata")
	assert.Equal(t, "DO UPDATE", ir.Upsert.Action, "expected DO UPDATE")
	require.Len(t, ir.Upsert.TargetColumns, 2, "expected 2 conflict target columns")
	assert.GreaterOrEqual(t, len(ir.Upsert.SetClauses), 2, "expected at least 2 set clauses")
	require.Len(t, ir.InsertColumns, 3, "expected 3 insert columns")
}

// TestIR_OnConflictDoUpdateWithWhere verifies DO UPDATE with WHERE filter.
func TestIR_OnConflictDoUpdateWithWhere(t *testing.T) {
	sql := `
INSERT INTO users (id, email, login_count)
VALUES ($1, $2, 1)
ON CONFLICT (id) DO UPDATE
SET login_count = users.login_count + 1,
    last_login = NOW()
WHERE users.active = true`
	ir := parseAssertNoError(t, sql)

	require.NotNil(t, ir.Upsert, "expected Upsert metadata")
	assert.Equal(t, "DO UPDATE", ir.Upsert.Action, "expected DO UPDATE")
	require.NotEmpty(t, ir.Upsert.ActionWhere, "expected ActionWhere to be populated")
	assert.Contains(t, ir.Upsert.ActionWhere, "active", "expected active in ActionWhere")
}

// TestIR_OnConflictOnConstraint checks ON CONSTRAINT conflict target.
func TestIR_OnConflictOnConstraint(t *testing.T) {
	sql := `
INSERT INTO accounts (id, name)
VALUES ($1, $2)
ON CONFLICT ON CONSTRAINT accounts_pkey DO NOTHING`
	ir := parseAssertNoError(t, sql)

	require.NotNil(t, ir.Upsert, "expected Upsert metadata")
	assert.Equal(t, "DO NOTHING", ir.Upsert.Action, "expected DO NOTHING")
	assert.Equal(t, "accounts_pkey", ir.Upsert.Constraint, "expected constraint 'accounts_pkey'")
}

// ---------------------------------------------------------------------------
// 13. Multi-table DELETE with USING
// ---------------------------------------------------------------------------

// TestIR_DeleteWithUsing validates DELETE with single USING table.
func TestIR_DeleteWithUsing(t *testing.T) {
	sql := `
DELETE FROM orders o
USING customers c
WHERE o.customer_id = c.id AND c.status = 'inactive'
RETURNING o.id`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDelete, ir.Command, "expected DELETE")
	assert.True(t, containsTable(ir.Tables, "orders"), "expected orders table")
	assert.True(t, containsTable(ir.Tables, "customers"), "expected customers table from USING")
	require.NotEmpty(t, ir.Where, "expected WHERE clause")
	require.Len(t, ir.Returning, 1, "expected 1 RETURNING clause")
}

// TestIR_DeleteWithMultipleUsingTables verifies DELETE with multiple USING tables.
func TestIR_DeleteWithMultipleUsingTables(t *testing.T) {
	sql := `
DELETE FROM line_items li
USING orders o, customers c
WHERE li.order_id = o.id
  AND o.customer_id = c.id
  AND c.deleted_at IS NOT NULL`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandDelete, ir.Command, "expected DELETE")
	assert.True(t, containsTable(ir.Tables, "line_items"), "expected line_items table")
	assert.True(t, containsTable(ir.Tables, "orders"), "expected orders table from USING")
	assert.True(t, containsTable(ir.Tables, "customers"), "expected customers table from USING")
}

// ---------------------------------------------------------------------------
// 14. UPDATE with FROM and complex joins
// ---------------------------------------------------------------------------

// TestIR_UpdateWithFrom validates UPDATE FROM with RETURNING.
func TestIR_UpdateWithFrom(t *testing.T) {
	sql := `
UPDATE products p
SET price = p.price * (1 - d.rate),
    updated_at = NOW()
FROM discounts d
WHERE p.category_id = d.category_id
  AND d.active = true
RETURNING p.id, p.price`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandUpdate, ir.Command, "expected UPDATE")
	assert.True(t, containsTable(ir.Tables, "products"), "expected products table")
	assert.True(t, containsTable(ir.Tables, "discounts"), "expected discounts table from FROM")
	assert.GreaterOrEqual(t, len(ir.SetClauses), 2, "expected at least 2 set clauses")
	require.Len(t, ir.Returning, 1, "expected 1 RETURNING clause")
	ret := normalise(ir.Returning[0])
	assert.Contains(t, ret, "p.id", "expected RETURNING to include p.id")
	assert.Contains(t, ret, "p.price", "expected RETURNING to include p.price")
}

// TestIR_UpdateWithSubqueryInSet verifies correlated subquery in SET clause.
func TestIR_UpdateWithSubqueryInSet(t *testing.T) {
	sql := `
UPDATE users
SET total_orders = (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id)
WHERE active = true`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandUpdate, ir.Command, "expected UPDATE")
	require.Len(t, ir.SetClauses, 1, "expected 1 set clause")
	assert.Contains(t, ir.SetClauses[0], "SELECT COUNT(*)", "expected subquery in SET")
}

// TestIR_UpdateWithCTE checks CTE-powered UPDATE.
func TestIR_UpdateWithCTE(t *testing.T) {
	sql := `
WITH expired AS (
  SELECT id FROM sessions WHERE expires_at < NOW()
)
UPDATE sessions
SET active = false
WHERE id IN (SELECT id FROM expired)`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandUpdate, ir.Command, "expected UPDATE")
	require.Len(t, ir.CTEs, 1, "expected 1 CTE")
	assert.Equal(t, "expired", strings.ToLower(ir.CTEs[0].Name), "expected CTE 'expired'")
}

// ---------------------------------------------------------------------------
// 15. Edge cases
// ---------------------------------------------------------------------------

// TestIR_EmptyStringLiteral validates empty string literal in WHERE.
func TestIR_EmptyStringLiteral(t *testing.T) {
	sql := `SELECT id FROM users WHERE name = ''`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Where, 1, "expected 1 WHERE clause")
	assert.Contains(t, ir.Where[0], "''", "expected empty string literal in WHERE")
}

// TestIR_QuotedIdentifiers verifies double-quoted reserved words as identifiers.
func TestIR_QuotedIdentifiers(t *testing.T) {
	sql := `SELECT "user"."order" FROM "user" WHERE "user"."group" = 'admin'`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Tables, 1, "expected 1 table")
	require.Len(t, ir.Columns, 1, "expected 1 column")
}

// TestIR_SchemaQualifiedTable confirms schema.table notation.
func TestIR_SchemaQualifiedTable(t *testing.T) {
	sql := `SELECT id FROM public.users WHERE active = true`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Tables, 1, "expected 1 table")
	assert.Equal(t, "public", ir.Tables[0].Schema, "expected schema 'public'")
	assert.Equal(t, "users", ir.Tables[0].Name, "expected table 'users'")
}

// TestIR_MultipleSchemaQualifiedTables validates cross-schema joins.
func TestIR_MultipleSchemaQualifiedTables(t *testing.T) {
	sql := `
SELECT a.id, b.name
FROM schema_one.table_a a
JOIN schema_two.table_b b ON a.id = b.a_id`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Tables, 2, "expected 2 tables")
	hasTableA := false
	hasTableB := false
	for _, tbl := range ir.Tables {
		if tbl.Schema == "schema_one" && tbl.Name == "table_a" && tbl.Alias == "a" {
			hasTableA = true
		}
		if tbl.Schema == "schema_two" && tbl.Name == "table_b" && tbl.Alias == "b" {
			hasTableB = true
		}
	}
	assert.True(t, hasTableA, "expected schema_one.table_a alias a")
	assert.True(t, hasTableB, "expected schema_two.table_b alias b")
}

// TestIR_SelectDistinct checks SELECT DISTINCT.
func TestIR_SelectDistinct(t *testing.T) {
	sql := `SELECT DISTINCT category FROM products`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Columns, 1, "expected 1 column")
	assert.Equal(t, "category", ir.Columns[0].Expression, "expected column 'category'")
	assert.True(t, containsTable(ir.Tables, "products"), "expected products table")
}

// TestIR_SelectDistinctOn validates DISTINCT ON with ORDER BY extraction.
func TestIR_SelectDistinctOn(t *testing.T) {
	sql := `SELECT DISTINCT ON (user_id) user_id, created_at FROM events ORDER BY user_id, created_at DESC`
	ir := parseAssertNoError(t, sql)

	assert.NotEmpty(t, ir.Columns, "expected at least 1 column")
	assert.Len(t, ir.OrderBy, 2, "expected 2 ORDER BY")
}

// TestIR_BooleanExpressions verifies IS TRUE and IS NOT TRUE predicates in WHERE.
func TestIR_BooleanExpressions(t *testing.T) {
	sql := `
SELECT id FROM users
WHERE active = true
  AND deleted IS NOT TRUE
  AND verified IS TRUE`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Where, 1, "expected 1 WHERE clause")
	whereUpper := strings.ToUpper(ir.Where[0])
	assert.Contains(t, whereUpper, "IS NOT TRUE", "expected IS NOT TRUE in WHERE")
}

// TestIR_InListExpression validates IN list with multiple string literals.
func TestIR_InListExpression(t *testing.T) {
	sql := `SELECT * FROM users WHERE status IN ('active', 'pending', 'trial')`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Where, 1, "expected 1 WHERE clause")
	assert.Contains(t, strings.ToUpper(ir.Where[0]), "IN", "expected IN in WHERE")
}

// TestIR_BetweenExpression checks BETWEEN range predicate in WHERE.
func TestIR_BetweenExpression(t *testing.T) {
	sql := `SELECT * FROM orders WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Where, 1, "expected 1 WHERE clause")
	assert.Contains(t, strings.ToUpper(ir.Where[0]), "BETWEEN", "expected BETWEEN in WHERE")
}

// TestIR_LikeExpression confirms LIKE pattern matching in WHERE.
func TestIR_LikeExpression(t *testing.T) {
	sql := `SELECT * FROM users WHERE email LIKE '%@example.com'`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Where, 1, "expected 1 WHERE clause")
	assert.Contains(t, strings.ToUpper(ir.Where[0]), "LIKE", "expected LIKE in WHERE")
}

// TestIR_ILikeExpression verifies case-insensitive ILIKE in WHERE.
func TestIR_ILikeExpression(t *testing.T) {
	sql := `SELECT * FROM users WHERE name ILIKE $1`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Where, 1, "expected 1 WHERE clause")
	assert.Contains(t, strings.ToUpper(ir.Where[0]), "ILIKE", "expected ILIKE in WHERE")
}

// TestIR_IsNullIsNotNull validates IS NULL and IS NOT NULL predicates together.
func TestIR_IsNullIsNotNull(t *testing.T) {
	sql := `SELECT * FROM orders WHERE shipped_at IS NULL AND cancelled_at IS NOT NULL`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Where, 1, "expected 1 WHERE clause")

	whereUpper := strings.ToUpper(ir.Where[0])
	if !strings.Contains(whereUpper, "IS NULL") {
		t.Fatalf("expected IS NULL in WHERE, got %q", ir.Where[0])
	}
	if !strings.Contains(whereUpper, "IS NOT NULL") {
		t.Fatalf("expected IS NOT NULL in WHERE, got %q", ir.Where[0])
	}
}

// TestIR_NotExists checks NOT EXISTS correlated subquery in WHERE.
func TestIR_NotExists(t *testing.T) {
	sql := `
SELECT id FROM products p
WHERE NOT EXISTS (
  SELECT 1 FROM order_items oi WHERE oi.product_id = p.id
)`
	ir := parseAssertNoError(t, sql)

	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
	if !strings.Contains(strings.ToUpper(ir.Where[0]), "NOT EXISTS") {
		t.Fatalf("expected NOT EXISTS in WHERE, got %q", ir.Where[0])
	}
}

// TestIR_MultipleOrderBy validates multi-column ORDER BY with direction and NULLS.
func TestIR_MultipleOrderBy(t *testing.T) {
	sql := `
SELECT id, name, age
FROM users
ORDER BY age DESC, name ASC NULLS LAST, id`
	ir := parseAssertNoError(t, sql)

	if len(ir.OrderBy) != 3 {
		t.Fatalf("expected 3 ORDER BY items, got %d", len(ir.OrderBy))
	}
	if ir.OrderBy[0].Expression != "age" || ir.OrderBy[0].Direction != "DESC" {
		t.Fatalf("unexpected first ORDER BY %+v", ir.OrderBy[0])
	}
	if ir.OrderBy[1].Expression != "name" || ir.OrderBy[1].Direction != "ASC" || ir.OrderBy[1].Nulls != "NULLS LAST" {
		t.Fatalf("unexpected second ORDER BY %+v", ir.OrderBy[1])
	}
	if ir.OrderBy[2].Expression != "id" {
		t.Fatalf("unexpected third ORDER BY %+v", ir.OrderBy[2])
	}
}

// TestIR_WindowFunctionRankDenseRank verifies RANK and DENSE_RANK window functions.
func TestIR_WindowFunctionRankDenseRank(t *testing.T) {
	sql := `
SELECT
  id,
  RANK() OVER (ORDER BY score DESC) AS rnk,
  DENSE_RANK() OVER (PARTITION BY category ORDER BY score DESC) AS dense_rnk
FROM leaderboard`
	ir := parseAssertNoError(t, sql)

	if len(ir.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(ir.Columns))
	}
	if ir.Columns[1].Alias != "rnk" || !strings.Contains(ir.Columns[1].Expression, "RANK()") {
		t.Fatalf("unexpected second column %+v", ir.Columns[1])
	}
	if ir.Columns[2].Alias != "dense_rnk" || !strings.Contains(ir.Columns[2].Expression, "DENSE_RANK()") {
		t.Fatalf("unexpected third column %+v", ir.Columns[2])
	}
}

// TestIR_MultipleWindowFunctions validates ROW_NUMBER, LAG, LEAD, and running SUM windows.
func TestIR_MultipleWindowFunctions(t *testing.T) {
	sql := `
SELECT
  id,
  ROW_NUMBER() OVER (ORDER BY id) AS rn,
  LAG(amount) OVER (ORDER BY created_at) AS prev_amount,
  LEAD(amount) OVER (ORDER BY created_at) AS next_amount,
  SUM(amount) OVER (ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM transactions`
	ir := parseAssertNoError(t, sql)

	if len(ir.Columns) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(ir.Columns))
	}
	if ir.Columns[1].Alias != "rn" {
		t.Fatalf("expected rn alias, got %q", ir.Columns[1].Alias)
	}
	if ir.Columns[2].Alias != "prev_amount" {
		t.Fatalf("expected prev_amount alias, got %q", ir.Columns[2].Alias)
	}
	if ir.Columns[3].Alias != "next_amount" {
		t.Fatalf("expected next_amount alias, got %q", ir.Columns[3].Alias)
	}
	if ir.Columns[4].Alias != "running_total" {
		t.Fatalf("expected running_total alias, got %q", ir.Columns[4].Alias)
	}
}

// TestIR_InsertSelectWithCTE validates INSERT ... SELECT sourced from a CTE.
func TestIR_InsertSelectWithCTE(t *testing.T) {
	sql := `
WITH source AS (
  SELECT id, name FROM staging_users WHERE verified = true
)
INSERT INTO users (id, name)
SELECT id, name FROM source`
	ir := parseAssertNoError(t, sql)

	if ir.Command != QueryCommandInsert {
		t.Fatalf("expected INSERT, got %s", ir.Command)
	}
	if len(ir.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(ir.CTEs))
	}
	if len(ir.InsertColumns) != 2 {
		t.Fatalf("expected 2 insert columns, got %+v", ir.InsertColumns)
	}
}

// TestIR_DeleteWithCTE verifies DELETE driven by a CTE subquery.
func TestIR_DeleteWithCTE(t *testing.T) {
	sql := `
WITH old AS (
  SELECT id FROM logs WHERE created_at < NOW() - INTERVAL '90 days'
)
DELETE FROM logs WHERE id IN (SELECT id FROM old)`
	ir := parseAssertNoError(t, sql)

	if ir.Command != QueryCommandDelete {
		t.Fatalf("expected DELETE, got %s", ir.Command)
	}
	if len(ir.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(ir.CTEs))
	}
	if !containsTable(ir.Tables, "logs") {
		t.Fatalf("expected logs table")
	}
}

// TestIR_SelectStarFromMultipleTables checks implicit cross join with aliased tables.
func TestIR_SelectStarFromMultipleTables(t *testing.T) {
	sql := `SELECT * FROM users u, orders o WHERE u.id = o.user_id`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Tables, 2, "expected 2 tables")
	hasUsers := false
	hasOrders := false
	for _, tbl := range ir.Tables {
		if tbl.Name == "users" && tbl.Alias == "u" {
			hasUsers = true
		}
		if tbl.Name == "orders" && tbl.Alias == "o" {
			hasOrders = true
		}
	}
	assert.True(t, hasUsers, "expected users alias u")
	assert.True(t, hasOrders, "expected orders alias o")
}

// TestIR_NaturalJoin validates NATURAL JOIN table extraction.
func TestIR_NaturalJoin(t *testing.T) {
	sql := `SELECT * FROM departments NATURAL JOIN employees`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Tables, 2, "expected 2 tables")
	assert.True(t, containsTable(ir.Tables, "departments"), "expected departments")
	assert.True(t, containsTable(ir.Tables, "employees"), "expected employees")
}

// TestIR_SelfJoin confirms self-join produces two table references with distinct aliases.
func TestIR_SelfJoin(t *testing.T) {
	sql := `
SELECT e.name AS employee, m.name AS manager
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id`
	ir := parseAssertNoError(t, sql)

	// Self-join results in two references to the same table
	require.Len(t, ir.Tables, 2, "expected 2 tables for self-join")
	hasEmployeeAlias := false
	hasManagerAlias := false
	for _, tbl := range ir.Tables {
		if tbl.Name == "employees" && tbl.Alias == "e" {
			hasEmployeeAlias = true
		}
		if tbl.Name == "employees" && tbl.Alias == "m" {
			hasManagerAlias = true
		}
	}
	assert.True(t, hasEmployeeAlias, "expected employees alias e")
	assert.True(t, hasManagerAlias, "expected employees alias m")
}

// TestIR_DerivedColumnsTracking validates alias-to-expression mapping in DerivedColumns.
func TestIR_DerivedColumnsTracking(t *testing.T) {
	sql := `SELECT COUNT(*) AS total, MAX(price) AS highest FROM orders`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.DerivedColumns, 2, "expected 2 derived columns")
	assert.Equal(t, "COUNT(*)", ir.DerivedColumns["total"], "expected 'total' -> 'COUNT(*)'")
	assert.Equal(t, "MAX(price)", ir.DerivedColumns["highest"], "expected 'highest' -> 'MAX(price)'")
}

// TestIR_ComplexWhereWithOrAnd verifies compound OR/AND/NOT predicates in WHERE.
func TestIR_ComplexWhereWithOrAnd(t *testing.T) {
	sql := `
SELECT id FROM users
WHERE (status = 'active' OR status = 'trial')
  AND (age >= 18 AND age <= 65)
  AND NOT deleted`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Where, 1, "expected 1 WHERE clause")
	whereNorm := normalise(ir.Where[0])
	assert.Contains(t, whereNorm, "status='active'", "expected OR conditions in WHERE")
	assert.Contains(t, whereNorm, "status='trial'", "expected OR conditions in WHERE")
}

// TestIR_InsertWithDefaultValues checks INSERT DEFAULT VALUES with RETURNING.
func TestIR_InsertWithDefaultValues(t *testing.T) {
	sql := `INSERT INTO counters DEFAULT VALUES RETURNING id`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandInsert, ir.Command, "expected INSERT")
	assert.True(t, containsTable(ir.Tables, "counters"), "expected counters table")
	require.Len(t, ir.Returning, 1, "expected 1 RETURNING clause")
}

// TestIR_InsertMultipleValues validates INSERT with multiple VALUES rows.
func TestIR_InsertMultipleValues(t *testing.T) {
	sql := `
INSERT INTO users (name, email)
VALUES ('Alice', 'alice@example.com'),
       ('Bob', 'bob@example.com'),
       ('Charlie', 'charlie@example.com')
RETURNING id`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandInsert, ir.Command, "expected INSERT")
	require.Len(t, ir.InsertColumns, 2, "expected 2 insert columns")
}

// TestIR_ColumnUsageInFilter confirms filter columns are tracked in ColumnUsage.
func TestIR_ColumnUsageInFilter(t *testing.T) {
	sql := `SELECT id, name FROM users WHERE age > 18 AND status = 'active'`
	ir := parseAssertNoError(t, sql)

	// Check that column usage tracking records filter columns
	filterUsages := 0
	for _, usage := range ir.ColumnUsage {
		if usage.UsageType == ColumnUsageTypeFilter {
			filterUsages++
		}
	}
	assert.Greater(t, filterUsages, 0, "expected filter column usages")
}

// TestIR_ColumnUsageInProjection verifies projection columns are tracked in ColumnUsage.
func TestIR_ColumnUsageInProjection(t *testing.T) {
	sql := `SELECT u.name, u.email FROM users u`
	ir := parseAssertNoError(t, sql)

	projUsages := 0
	for _, usage := range ir.ColumnUsage {
		if usage.UsageType == ColumnUsageTypeProjection {
			projUsages++
		}
	}
	assert.Greater(t, projUsages, 0, "expected projection column usages")
}

// TestIR_ColumnUsageInGroupBy checks GROUP BY columns are tracked in ColumnUsage.
func TestIR_ColumnUsageInGroupBy(t *testing.T) {
	sql := `SELECT region, COUNT(*) FROM metrics GROUP BY region`
	ir := parseAssertNoError(t, sql)

	groupUsages := 0
	for _, usage := range ir.ColumnUsage {
		if usage.UsageType == ColumnUsageTypeGroupBy {
			groupUsages++
		}
	}
	assert.Greater(t, groupUsages, 0, "expected group column usages")
}

// TestIR_ColumnUsageInOrderBy validates ORDER BY columns are tracked in ColumnUsage.
func TestIR_ColumnUsageInOrderBy(t *testing.T) {
	sql := `SELECT id, name FROM users ORDER BY name ASC`
	ir := parseAssertNoError(t, sql)

	orderUsages := 0
	for _, usage := range ir.ColumnUsage {
		if usage.UsageType == ColumnUsageTypeOrderBy {
			orderUsages++
		}
	}
	assert.Greater(t, orderUsages, 0, "expected order column usages")
}

// TestIR_ColumnUsageInJoin confirms join columns are tracked in ColumnUsage.
func TestIR_ColumnUsageInJoin(t *testing.T) {
	sql := `SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id`
	ir := parseAssertNoError(t, sql)

	joinUsages := 0
	for _, usage := range ir.ColumnUsage {
		if usage.UsageType == ColumnUsageTypeJoin {
			joinUsages++
		}
	}
	assert.Greater(t, joinUsages, 0, "expected join column usages")
}

// TestIR_PositionalParameters verifies $1/$2/$3 positional parameter extraction.
func TestIR_PositionalParameters(t *testing.T) {
	sql := `SELECT * FROM users WHERE id = $1 AND name = $2 AND age > $3`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Parameters, 3, "expected 3 parameters")
	for i, param := range ir.Parameters {
		assert.Equal(t, i+1, param.Position, "expected parameter at position %d", i+1)
		assert.Equal(t, "$", param.Marker, "expected $ marker")
	}
}

// TestIR_MixedParameterTypes checks coexistence of $N and ? parameter markers.
func TestIR_MixedParameterTypes(t *testing.T) {
	sql := `SELECT * FROM users WHERE id = $1 AND status = ?`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Parameters, 2, "expected 2 parameters")
	assert.Equal(t, "$", ir.Parameters[0].Marker, "unexpected first parameter marker")
	assert.Equal(t, 1, ir.Parameters[0].Position, "unexpected first parameter position")
	assert.Equal(t, "?", ir.Parameters[1].Marker, "unexpected second parameter marker")
	assert.Equal(t, 1, ir.Parameters[1].Position, "unexpected second parameter position")
}

// ---------------------------------------------------------------------------
// Additional error handling edge cases
// ---------------------------------------------------------------------------

// TestIR_EmptyInput validates that empty string input returns an error.
func TestIR_EmptyInput(t *testing.T) {
	_, err := ParseSQL("")
	require.Error(t, err, "expected error for empty input")
}

// TestIR_WhitespaceOnly verifies that whitespace-only input returns an error.
func TestIR_WhitespaceOnly(t *testing.T) {
	_, err := ParseSQL("   \n\t  ")
	require.Error(t, err, "expected error for whitespace-only input")
}

// TestIR_SemicolonOnly confirms that a bare semicolon returns an error.
func TestIR_SemicolonOnly(t *testing.T) {
	_, err := ParseSQL(";")
	require.Error(t, err, "expected error for semicolon-only input")
}

// ---------------------------------------------------------------------------
// ParseErrors formatting
// ---------------------------------------------------------------------------

// TestParseErrors_Error_NilReceiver validates Error() on a nil ParseErrors receiver.
func TestParseErrors_Error_NilReceiver(t *testing.T) {
	var pe *ParseErrors
	assert.Equal(t, "parse error", pe.Error(), "expected 'parse error'")
}

// TestParseErrors_Error_Empty checks Error() with an empty error list.
func TestParseErrors_Error_Empty(t *testing.T) {
	pe := &ParseErrors{SQL: "test", Errors: nil}
	assert.Equal(t, "parse error", pe.Error(), "expected 'parse error'")
}

// TestParseErrors_Error_Single verifies Error() formatting with one syntax error.
func TestParseErrors_Error_Single(t *testing.T) {
	pe := &ParseErrors{
		SQL:    "test",
		Errors: []SyntaxError{{Line: 1, Column: 5, Message: "bad token"}},
	}
	s := pe.Error()
	assert.Contains(t, s, "line 1:5", "unexpected error string")
	assert.Contains(t, s, "bad token", "unexpected error string")
}

// TestParseErrors_Error_Multiple confirms Error() formatting with multiple syntax errors.
func TestParseErrors_Error_Multiple(t *testing.T) {
	pe := &ParseErrors{
		SQL: "test",
		Errors: []SyntaxError{
			{Line: 1, Column: 5, Message: "bad token"},
			{Line: 2, Column: 3, Message: "unexpected EOF"},
		},
	}
	s := pe.Error()
	assert.Contains(t, s, "parse error(s)", "expected 'parse error(s)'")
	assert.Contains(t, s, "line 1:5", "expected error location 1")
	assert.Contains(t, s, "line 2:3", "expected error location 2")
}

// ---------------------------------------------------------------------------
// Subquery with LIMIT (IsNested flag)
// ---------------------------------------------------------------------------

// TestIR_SubqueryLimitIsNested validates IsNested flag on LIMIT inside a subquery.
func TestIR_SubqueryLimitIsNested(t *testing.T) {
	sql := `
SELECT *
FROM (
  SELECT id FROM users ORDER BY id LIMIT 10
) sub`
	ir := parseAssertNoError(t, sql)

	require.NotEmpty(t, ir.Subqueries, "expected subquery metadata")
	sq := ir.Subqueries[0].Query
	require.NotNil(t, sq, "expected parsed subquery")
	require.NotNil(t, sq.Limit, "expected LIMIT in subquery")
	assert.True(t, sq.Limit.IsNested, "expected IsNested=true for subquery LIMIT")
}

// ---------------------------------------------------------------------------
// Complex real-world query combining many constructs
// ---------------------------------------------------------------------------

// TestIR_ComplexRealWorldQuery stress-tests a multi-CTE query with window functions, COALESCE, and LIMIT.
func TestIR_ComplexRealWorldQuery(t *testing.T) {
	sql := `
WITH monthly_sales AS (
  SELECT
    DATE_TRUNC('month', o.created_at) AS month,
    p.category,
    SUM(oi.quantity * oi.unit_price) AS revenue
  FROM orders o
  JOIN order_items oi ON o.id = oi.order_id
  JOIN products p ON oi.product_id = p.id
  WHERE o.status = 'completed'
    AND o.created_at >= '2024-01-01'
  GROUP BY DATE_TRUNC('month', o.created_at), p.category
),
ranked AS (
  SELECT
    month,
    category,
    revenue,
    RANK() OVER (PARTITION BY month ORDER BY revenue DESC) AS rnk
  FROM monthly_sales
)
SELECT
  r.month,
  r.category,
  r.revenue,
  r.rnk,
  COALESCE(r.revenue / NULLIF(SUM(r.revenue) OVER (PARTITION BY r.month), 0), 0) AS pct_of_month
FROM ranked r
WHERE r.rnk <= 5
ORDER BY r.month DESC, r.rnk ASC
LIMIT 50`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandSelect, ir.Command, "expected SELECT")
	require.Len(t, ir.CTEs, 2, "expected 2 CTEs")

	// Check base tables from CTEs are extracted
	assert.True(t, containsTable(ir.Tables, "orders"), "expected orders table from CTE")
	assert.True(t, containsTable(ir.Tables, "order_items"), "expected order_items table from CTE")
	assert.True(t, containsTable(ir.Tables, "products"), "expected products table from CTE")

	// Check main query references ranked CTE
	foundRanked := false
	for _, tbl := range ir.Tables {
		if strings.ToLower(tbl.Name) == "ranked" && tbl.Type == TableTypeCTE {
			foundRanked = true
		}
	}
	assert.True(t, foundRanked, "expected ranked CTE reference")

	// Check ORDER BY
	require.Len(t, ir.OrderBy, 2, "expected 2 ORDER BY items")

	// Check LIMIT
	require.NotNil(t, ir.Limit, "expected LIMIT clause")

	// Check columns
	require.Len(t, ir.Columns, 5, "expected 5 columns")
	assert.Equal(t, "pct_of_month", ir.Columns[4].Alias, "expected pct_of_month alias")
}

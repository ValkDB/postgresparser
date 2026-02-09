// parser_ir_edge_cases_test.go covers advanced and edge-case SQL constructs
// to improve parser coverage.
package postgresparser

import (
	"strings"
	"testing"
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

	if ir.Command != QueryCommandSelect {
		t.Fatalf("expected SELECT, got %s", ir.Command)
	}
	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
	if !strings.Contains(ir.Where[0], "IN") {
		t.Fatalf("expected WHERE to contain IN, got %q", ir.Where[0])
	}
	if !containsTable(ir.Tables, "users") {
		t.Fatalf("expected users table, got %+v", ir.Tables)
	}
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

	if len(ir.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d: %+v", len(ir.Columns), ir.Columns)
	}
	if ir.Columns[2].Alias != "order_count" {
		t.Fatalf("expected alias 'order_count', got %q", ir.Columns[2].Alias)
	}
	expr := ir.Columns[2].Expression
	if !strings.Contains(expr, "SELECT COUNT(*)") {
		t.Fatalf("expected subquery in select list expression, got %q", expr)
	}
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

	if !containsTable(ir.Tables, "payments") {
		t.Fatalf("expected payments table to be surfaced from subquery, got %+v", ir.Tables)
	}
	foundSubquery := false
	for _, tbl := range ir.Tables {
		if tbl.Type == TableTypeSubquery && tbl.Alias == "sub" {
			foundSubquery = true
		}
	}
	if !foundSubquery {
		t.Fatalf("expected subquery table with alias 'sub', got %+v", ir.Tables)
	}
	if len(ir.Subqueries) == 0 {
		t.Fatalf("expected subquery metadata, got none")
	}
	// Verify the subquery's internal details
	var sq *SubqueryRef
	for i := range ir.Subqueries {
		if ir.Subqueries[i].Alias == "sub" {
			sq = &ir.Subqueries[i]
			break
		}
	}
	if sq == nil || sq.Query == nil {
		t.Fatalf("expected parsed subquery metadata")
	}
	if len(sq.Query.GroupBy) == 0 || !strings.Contains(sq.Query.GroupBy[0], "user_id") {
		t.Fatalf("expected subquery GROUP BY user_id, got %+v", sq.Query.GroupBy)
	}
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

	if len(ir.CTEs) != 3 {
		t.Fatalf("expected 3 CTEs, got %d: %+v", len(ir.CTEs), ir.CTEs)
	}
	names := make([]string, len(ir.CTEs))
	for i, cte := range ir.CTEs {
		names[i] = strings.ToLower(cte.Name)
	}
	if names[0] != "base" || names[1] != "summarized" || names[2] != "filtered" {
		t.Fatalf("unexpected CTE names: %v", names)
	}
	// The base table "transactions" should be found
	if !containsTable(ir.Tables, "transactions") {
		t.Fatalf("expected transactions table from CTE, got %+v", ir.Tables)
	}
	// "filtered" should be a CTE reference
	foundFilteredCTE := false
	for _, tbl := range ir.Tables {
		if strings.ToLower(tbl.Name) == "filtered" && tbl.Type == TableTypeCTE {
			foundFilteredCTE = true
		}
	}
	if !foundFilteredCTE {
		t.Fatalf("expected filtered CTE reference in tables, got %+v", ir.Tables)
	}
}

// TestIR_CTEWithMaterialized checks MATERIALIZED annotation on CTEs.
func TestIR_CTEWithMaterialized(t *testing.T) {
	sql := `
WITH active AS MATERIALIZED (
  SELECT id FROM users WHERE active = true
)
SELECT * FROM active`
	ir := parseAssertNoError(t, sql)

	if len(ir.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(ir.CTEs))
	}
	if strings.ToLower(ir.CTEs[0].Name) != "active" {
		t.Fatalf("expected CTE name 'active', got %q", ir.CTEs[0].Name)
	}
	// Materialized annotation should be captured
	if !strings.Contains(strings.ToUpper(ir.CTEs[0].Materialized), "MATERIALIZED") {
		t.Fatalf("expected MATERIALIZED annotation, got %q", ir.CTEs[0].Materialized)
	}
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

	if len(ir.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(ir.Columns))
	}
	if ir.Columns[0].Alias != "label" {
		t.Fatalf("expected alias 'label', got %q", ir.Columns[0].Alias)
	}
	expr := ir.Columns[0].Expression
	// Nested CASE should be present
	if strings.Count(expr, "CASE") < 2 {
		t.Fatalf("expected nested CASE expression, got %q", expr)
	}
}

// TestIR_CaseInWhereClause verifies CASE expression in WHERE predicate.
func TestIR_CaseInWhereClause(t *testing.T) {
	sql := `
SELECT id, name
FROM users
WHERE CASE WHEN role = 'admin' THEN true ELSE false END = true`
	ir := parseAssertNoError(t, sql)

	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
	if !strings.Contains(ir.Where[0], "CASE") {
		t.Fatalf("expected CASE in WHERE, got %q", ir.Where[0])
	}
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

	if len(ir.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY, got %d", len(ir.OrderBy))
	}
	if !strings.Contains(ir.OrderBy[0].Expression, "CASE") {
		t.Fatalf("expected CASE in ORDER BY, got %q", ir.OrderBy[0].Expression)
	}
	if ir.OrderBy[0].Direction != "ASC" {
		t.Fatalf("expected ASC direction, got %q", ir.OrderBy[0].Direction)
	}
}

// ---------------------------------------------------------------------------
// 4. Array operations
// ---------------------------------------------------------------------------

// TestIR_ArrayAnyOperator validates ANY() array operator.
func TestIR_ArrayAnyOperator(t *testing.T) {
	sql := `SELECT id FROM users WHERE id = ANY($1)`
	ir := parseAssertNoError(t, sql)

	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
	if !strings.Contains(ir.Where[0], "ANY") {
		t.Fatalf("expected ANY in WHERE, got %q", ir.Where[0])
	}
}

// TestIR_ArrayAllOperator verifies ALL() with subquery.
func TestIR_ArrayAllOperator(t *testing.T) {
	sql := `SELECT id FROM scores WHERE score > ALL(SELECT min_score FROM thresholds)`
	ir := parseAssertNoError(t, sql)

	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
	whereUpper := strings.ToUpper(ir.Where[0])
	if !strings.Contains(whereUpper, "ALL") {
		t.Fatalf("expected ALL in WHERE, got %q", ir.Where[0])
	}
}

// TestIR_ArrayConstructor checks ARRAY[] constructor syntax.
func TestIR_ArrayConstructor(t *testing.T) {
	sql := `SELECT ARRAY[1, 2, 3] AS nums FROM generate_series(1,1)`
	ir := parseAssertNoError(t, sql)

	if len(ir.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(ir.Columns))
	}
	if ir.Columns[0].Alias != "nums" {
		t.Fatalf("expected alias 'nums', got %q", ir.Columns[0].Alias)
	}
}

// ---------------------------------------------------------------------------
// 5. Type casting
// ---------------------------------------------------------------------------

// TestIR_TypeCastInProjection validates :: type cast in SELECT list.
func TestIR_TypeCastInProjection(t *testing.T) {
	sql := `SELECT id::text, amount::numeric(10,2) AS amt FROM orders`
	ir := parseAssertNoError(t, sql)

	if len(ir.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ir.Columns))
	}
	if !strings.Contains(ir.Columns[0].Expression, "::") {
		t.Fatalf("expected type cast in first column, got %q", ir.Columns[0].Expression)
	}
	if ir.Columns[1].Alias != "amt" {
		t.Fatalf("expected alias 'amt', got %q", ir.Columns[1].Alias)
	}
}

// TestIR_TypeCastInWhere verifies :: cast in WHERE clause.
func TestIR_TypeCastInWhere(t *testing.T) {
	sql := `SELECT id FROM events WHERE created_at::date = '2024-01-01'::date`
	ir := parseAssertNoError(t, sql)

	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
	if !strings.Contains(ir.Where[0], "::") {
		t.Fatalf("expected type cast in WHERE, got %q", ir.Where[0])
	}
}

// TestIR_CastFunction confirms CAST(expr AS type) syntax.
func TestIR_CastFunction(t *testing.T) {
	sql := `SELECT CAST(price AS integer) AS int_price FROM products`
	ir := parseAssertNoError(t, sql)

	if len(ir.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(ir.Columns))
	}
	if !strings.Contains(strings.ToUpper(ir.Columns[0].Expression), "CAST") {
		t.Fatalf("expected CAST in column expression, got %q", ir.Columns[0].Expression)
	}
	if ir.Columns[0].Alias != "int_price" {
		t.Fatalf("expected alias 'int_price', got %q", ir.Columns[0].Alias)
	}
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

	if len(ir.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(ir.Tables))
	}
	if len(ir.JoinConditions) != 1 {
		t.Fatalf("expected 1 join condition, got %d", len(ir.JoinConditions))
	}
	join := normalise(ir.JoinConditions[0])
	if !strings.Contains(join, "a.col1=b.col1") || !strings.Contains(join, "a.col2=b.col2") || !strings.Contains(join, "a.col3=b.col3") {
		t.Fatalf("expected multi-column join condition, got %q", ir.JoinConditions[0])
	}
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

	if !containsTable(ir.Tables, "orders") {
		t.Fatalf("expected orders table, got %+v", ir.Tables)
	}
	if !containsTable(ir.Tables, "line_items") {
		t.Fatalf("expected line_items table from subquery, got %+v", ir.Tables)
	}
	foundSubq := false
	for _, tbl := range ir.Tables {
		if tbl.Type == TableTypeSubquery && tbl.Alias == "totals" {
			foundSubq = true
		}
	}
	if !foundSubq {
		t.Fatalf("expected subquery table 'totals', got %+v", ir.Tables)
	}
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

	if len(ir.Tables) != 4 {
		t.Fatalf("expected 4 tables, got %d: %+v", len(ir.Tables), ir.Tables)
	}
	expectedTables := []string{"users", "orders", "products", "settings"}
	for _, name := range expectedTables {
		if !containsTable(ir.Tables, name) {
			t.Fatalf("expected table %q, got %+v", name, ir.Tables)
		}
	}
}

// TestIR_JoinUSING confirms JOIN with USING clause.
func TestIR_JoinUSING(t *testing.T) {
	sql := `
SELECT *
FROM departments d
JOIN employees e USING (department_id)`
	ir := parseAssertNoError(t, sql)

	if len(ir.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(ir.Tables))
	}
	if len(ir.JoinConditions) != 1 {
		t.Fatalf("expected 1 join condition, got %d", len(ir.JoinConditions))
	}
	if !strings.Contains(strings.ToUpper(ir.JoinConditions[0]), "USING") {
		t.Fatalf("expected USING in join condition, got %q", ir.JoinConditions[0])
	}
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

	if !containsTable(ir.Tables, "users") {
		t.Fatalf("expected users table, got %+v", ir.Tables)
	}
	foundSubq := false
	for _, tbl := range ir.Tables {
		if tbl.Type == TableTypeSubquery && tbl.Alias == "recent" {
			foundSubq = true
		}
	}
	if !foundSubq {
		t.Fatalf("expected LATERAL subquery 'recent', got %+v", ir.Tables)
	}
}

// TestIR_LateralJoinWithFunction verifies LATERAL function call (unnest).
func TestIR_LateralJoinWithFunction(t *testing.T) {
	sql := `
SELECT p.id, tag.value
FROM products p
CROSS JOIN LATERAL unnest(p.tags) AS tag(value)`
	ir := parseAssertNoError(t, sql)

	if !containsTable(ir.Tables, "products") {
		t.Fatalf("expected products table, got %+v", ir.Tables)
	}
	foundFunc := false
	for _, tbl := range ir.Tables {
		if tbl.Type == TableTypeFunction && tbl.Alias == "tag" {
			foundFunc = true
		}
	}
	if !foundFunc {
		t.Fatalf("expected LATERAL function 'tag', got %+v", ir.Tables)
	}
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
		t.Fatalf("expected DATE_TRUNC in GROUP BY, got %q", ir.GroupBy[0])
	}
}

// TestIR_IntervalExpression checks INTERVAL literal in WHERE.
func TestIR_IntervalExpression(t *testing.T) {
	sql := `SELECT * FROM events WHERE created_at > NOW() - INTERVAL '30 days'`
	ir := parseAssertNoError(t, sql)

	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
	if !strings.Contains(ir.Where[0], "INTERVAL") {
		t.Fatalf("expected INTERVAL in WHERE, got %q", ir.Where[0])
	}
}

// TestIR_AtTimeZone validates AT TIME ZONE expression.
func TestIR_AtTimeZone(t *testing.T) {
	sql := `SELECT created_at AT TIME ZONE 'UTC' AS utc_time FROM events`
	ir := parseAssertNoError(t, sql)

	if len(ir.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(ir.Columns))
	}
	exprUpper := strings.ToUpper(ir.Columns[0].Expression)
	if !strings.Contains(exprUpper, "AT TIME ZONE") {
		t.Fatalf("expected AT TIME ZONE in expression, got %q", ir.Columns[0].Expression)
	}
	if ir.Columns[0].Alias != "utc_time" {
		t.Fatalf("expected alias 'utc_time', got %q", ir.Columns[0].Alias)
	}
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

	if len(ir.GroupBy) != 2 {
		t.Fatalf("expected 2 GROUP BY items, got %d: %+v", len(ir.GroupBy), ir.GroupBy)
	}
	if ir.GroupBy[0] != "region" || ir.GroupBy[1] != "category" {
		t.Fatalf("unexpected GROUP BY: %+v", ir.GroupBy)
	}
}

// TestIR_GroupByWithExpression verifies expression-based GROUP BY.
func TestIR_GroupByWithExpression(t *testing.T) {
	sql := `
SELECT DATE_TRUNC('month', created_at) AS m, COUNT(*)
FROM orders
GROUP BY DATE_TRUNC('month', created_at)`
	ir := parseAssertNoError(t, sql)

	if len(ir.GroupBy) != 1 {
		t.Fatalf("expected 1 GROUP BY, got %d", len(ir.GroupBy))
	}
	if !strings.Contains(ir.GroupBy[0], "DATE_TRUNC") {
		t.Fatalf("expected DATE_TRUNC in GROUP BY, got %q", ir.GroupBy[0])
	}
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

	if len(ir.Having) != 1 {
		t.Fatalf("expected 1 HAVING clause, got %d", len(ir.Having))
	}
	havingNorm := normalise(ir.Having[0])
	if !strings.Contains(havingNorm, "count(*)>=10") {
		t.Fatalf("expected COUNT condition in HAVING, got %q", ir.Having[0])
	}
	if !strings.Contains(havingNorm, "avg(price)<100") {
		t.Fatalf("expected AVG condition in HAVING, got %q", ir.Having[0])
	}
}

// TestIR_HavingWithSubquery verifies subquery in HAVING clause.
func TestIR_HavingWithSubquery(t *testing.T) {
	sql := `
SELECT department_id, AVG(salary) AS avg_sal
FROM employees
GROUP BY department_id
HAVING AVG(salary) > (SELECT AVG(salary) FROM employees)`
	ir := parseAssertNoError(t, sql)

	if len(ir.Having) != 1 {
		t.Fatalf("expected 1 HAVING clause, got %d", len(ir.Having))
	}
	if !strings.Contains(ir.Having[0], "SELECT AVG(salary)") {
		t.Fatalf("expected subquery in HAVING, got %q", ir.Having[0])
	}
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

	if ir.Command != QueryCommandInsert {
		t.Fatalf("expected INSERT, got %s", ir.Command)
	}
	if ir.Upsert == nil {
		t.Fatalf("expected Upsert metadata")
	}
	if ir.Upsert.Action != "DO UPDATE" {
		t.Fatalf("expected DO UPDATE, got %q", ir.Upsert.Action)
	}
	if len(ir.Upsert.TargetColumns) != 2 {
		t.Fatalf("expected 2 conflict target columns, got %+v", ir.Upsert.TargetColumns)
	}
	if len(ir.Upsert.SetClauses) < 2 {
		t.Fatalf("expected at least 2 set clauses, got %+v", ir.Upsert.SetClauses)
	}
	if len(ir.InsertColumns) != 3 {
		t.Fatalf("expected 3 insert columns, got %+v", ir.InsertColumns)
	}
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

	if ir.Upsert == nil {
		t.Fatalf("expected Upsert metadata")
	}
	if ir.Upsert.Action != "DO UPDATE" {
		t.Fatalf("expected DO UPDATE, got %q", ir.Upsert.Action)
	}
	if ir.Upsert.ActionWhere == "" {
		t.Fatalf("expected ActionWhere to be populated")
	}
	if !strings.Contains(ir.Upsert.ActionWhere, "active") {
		t.Fatalf("expected active in ActionWhere, got %q", ir.Upsert.ActionWhere)
	}
}

// TestIR_OnConflictOnConstraint checks ON CONSTRAINT conflict target.
func TestIR_OnConflictOnConstraint(t *testing.T) {
	sql := `
INSERT INTO accounts (id, name)
VALUES ($1, $2)
ON CONFLICT ON CONSTRAINT accounts_pkey DO NOTHING`
	ir := parseAssertNoError(t, sql)

	if ir.Upsert == nil {
		t.Fatalf("expected Upsert metadata")
	}
	if ir.Upsert.Action != "DO NOTHING" {
		t.Fatalf("expected DO NOTHING, got %q", ir.Upsert.Action)
	}
	if ir.Upsert.Constraint != "accounts_pkey" {
		t.Fatalf("expected constraint 'accounts_pkey', got %q", ir.Upsert.Constraint)
	}
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

	if ir.Command != QueryCommandDelete {
		t.Fatalf("expected DELETE, got %s", ir.Command)
	}
	if !containsTable(ir.Tables, "orders") {
		t.Fatalf("expected orders table, got %+v", ir.Tables)
	}
	if !containsTable(ir.Tables, "customers") {
		t.Fatalf("expected customers table from USING, got %+v", ir.Tables)
	}
	if len(ir.Where) < 1 {
		t.Fatalf("expected WHERE clause, got %+v", ir.Where)
	}
	if len(ir.Returning) != 1 {
		t.Fatalf("expected 1 RETURNING clause, got %+v", ir.Returning)
	}
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

	if ir.Command != QueryCommandDelete {
		t.Fatalf("expected DELETE, got %s", ir.Command)
	}
	if !containsTable(ir.Tables, "line_items") {
		t.Fatalf("expected line_items table")
	}
	if !containsTable(ir.Tables, "orders") {
		t.Fatalf("expected orders table from USING")
	}
	if !containsTable(ir.Tables, "customers") {
		t.Fatalf("expected customers table from USING")
	}
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

	if ir.Command != QueryCommandUpdate {
		t.Fatalf("expected UPDATE, got %s", ir.Command)
	}
	if !containsTable(ir.Tables, "products") {
		t.Fatalf("expected products table, got %+v", ir.Tables)
	}
	if !containsTable(ir.Tables, "discounts") {
		t.Fatalf("expected discounts table from FROM, got %+v", ir.Tables)
	}
	if len(ir.SetClauses) < 2 {
		t.Fatalf("expected at least 2 set clauses, got %+v", ir.SetClauses)
	}
	if len(ir.Returning) != 1 {
		t.Fatalf("expected 1 RETURNING clause, got %+v", ir.Returning)
	}
}

// TestIR_UpdateWithSubqueryInSet verifies correlated subquery in SET clause.
func TestIR_UpdateWithSubqueryInSet(t *testing.T) {
	sql := `
UPDATE users
SET total_orders = (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id)
WHERE active = true`
	ir := parseAssertNoError(t, sql)

	if ir.Command != QueryCommandUpdate {
		t.Fatalf("expected UPDATE, got %s", ir.Command)
	}
	if len(ir.SetClauses) != 1 {
		t.Fatalf("expected 1 set clause, got %+v", ir.SetClauses)
	}
	if !strings.Contains(ir.SetClauses[0], "SELECT COUNT(*)") {
		t.Fatalf("expected subquery in SET, got %q", ir.SetClauses[0])
	}
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

	if ir.Command != QueryCommandUpdate {
		t.Fatalf("expected UPDATE, got %s", ir.Command)
	}
	if len(ir.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(ir.CTEs))
	}
	if strings.ToLower(ir.CTEs[0].Name) != "expired" {
		t.Fatalf("expected CTE 'expired', got %q", ir.CTEs[0].Name)
	}
}

// ---------------------------------------------------------------------------
// 15. Edge cases
// ---------------------------------------------------------------------------

// TestIR_EmptyStringLiteral validates empty string literal in WHERE.
func TestIR_EmptyStringLiteral(t *testing.T) {
	sql := `SELECT id FROM users WHERE name = ''`
	ir := parseAssertNoError(t, sql)

	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
	if !strings.Contains(ir.Where[0], "''") {
		t.Fatalf("expected empty string literal in WHERE, got %q", ir.Where[0])
	}
}

// TestIR_QuotedIdentifiers verifies double-quoted reserved words as identifiers.
func TestIR_QuotedIdentifiers(t *testing.T) {
	sql := `SELECT "user"."order" FROM "user" WHERE "user"."group" = 'admin'`
	ir := parseAssertNoError(t, sql)

	if len(ir.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d: %+v", len(ir.Tables), ir.Tables)
	}
	if len(ir.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(ir.Columns))
	}
}

// TestIR_SchemaQualifiedTable confirms schema.table notation.
func TestIR_SchemaQualifiedTable(t *testing.T) {
	sql := `SELECT id FROM public.users WHERE active = true`
	ir := parseAssertNoError(t, sql)

	if len(ir.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(ir.Tables))
	}
	if ir.Tables[0].Schema != "public" {
		t.Fatalf("expected schema 'public', got %q", ir.Tables[0].Schema)
	}
	if ir.Tables[0].Name != "users" {
		t.Fatalf("expected table 'users', got %q", ir.Tables[0].Name)
	}
}

// TestIR_MultipleSchemaQualifiedTables validates cross-schema joins.
func TestIR_MultipleSchemaQualifiedTables(t *testing.T) {
	sql := `
SELECT a.id, b.name
FROM schema_one.table_a a
JOIN schema_two.table_b b ON a.id = b.a_id`
	ir := parseAssertNoError(t, sql)

	if len(ir.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(ir.Tables))
	}
	if ir.Tables[0].Schema != "schema_one" || ir.Tables[0].Name != "table_a" {
		t.Fatalf("unexpected first table %+v", ir.Tables[0])
	}
	if ir.Tables[1].Schema != "schema_two" || ir.Tables[1].Name != "table_b" {
		t.Fatalf("unexpected second table %+v", ir.Tables[1])
	}
}

// TestIR_SelectDistinct checks SELECT DISTINCT.
func TestIR_SelectDistinct(t *testing.T) {
	sql := `SELECT DISTINCT category FROM products`
	ir := parseAssertNoError(t, sql)

	if len(ir.Columns) != 1 || ir.Columns[0].Expression != "category" {
		t.Fatalf("expected column 'category', got %+v", ir.Columns)
	}
	if !containsTable(ir.Tables, "products") {
		t.Fatalf("expected products table")
	}
}

// TestIR_SelectDistinctOn validates DISTINCT ON with ORDER BY extraction.
func TestIR_SelectDistinctOn(t *testing.T) {
	sql := `SELECT DISTINCT ON (user_id) user_id, created_at FROM events ORDER BY user_id, created_at DESC`
	ir := parseAssertNoError(t, sql)

	if len(ir.Columns) < 1 {
		t.Fatalf("expected at least 1 column, got %d", len(ir.Columns))
	}
	if len(ir.OrderBy) != 2 {
		t.Fatalf("expected 2 ORDER BY, got %d", len(ir.OrderBy))
	}
}

// TestIR_BooleanExpressions verifies IS TRUE and IS NOT TRUE predicates in WHERE.
func TestIR_BooleanExpressions(t *testing.T) {
	sql := `
SELECT id FROM users
WHERE active = true
  AND deleted IS NOT TRUE
  AND verified IS TRUE`
	ir := parseAssertNoError(t, sql)

	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
	whereUpper := strings.ToUpper(ir.Where[0])
	if !strings.Contains(whereUpper, "IS NOT TRUE") {
		t.Fatalf("expected IS NOT TRUE in WHERE, got %q", ir.Where[0])
	}
}

// TestIR_InListExpression validates IN list with multiple string literals.
func TestIR_InListExpression(t *testing.T) {
	sql := `SELECT * FROM users WHERE status IN ('active', 'pending', 'trial')`
	ir := parseAssertNoError(t, sql)

	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
	if !strings.Contains(strings.ToUpper(ir.Where[0]), "IN") {
		t.Fatalf("expected IN in WHERE, got %q", ir.Where[0])
	}
}

// TestIR_BetweenExpression checks BETWEEN range predicate in WHERE.
func TestIR_BetweenExpression(t *testing.T) {
	sql := `SELECT * FROM orders WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'`
	ir := parseAssertNoError(t, sql)

	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
	if !strings.Contains(strings.ToUpper(ir.Where[0]), "BETWEEN") {
		t.Fatalf("expected BETWEEN in WHERE, got %q", ir.Where[0])
	}
}

// TestIR_LikeExpression confirms LIKE pattern matching in WHERE.
func TestIR_LikeExpression(t *testing.T) {
	sql := `SELECT * FROM users WHERE email LIKE '%@example.com'`
	ir := parseAssertNoError(t, sql)

	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
	if !strings.Contains(strings.ToUpper(ir.Where[0]), "LIKE") {
		t.Fatalf("expected LIKE in WHERE, got %q", ir.Where[0])
	}
}

// TestIR_ILikeExpression verifies case-insensitive ILIKE in WHERE.
func TestIR_ILikeExpression(t *testing.T) {
	sql := `SELECT * FROM users WHERE name ILIKE $1`
	ir := parseAssertNoError(t, sql)

	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
	if !strings.Contains(strings.ToUpper(ir.Where[0]), "ILIKE") {
		t.Fatalf("expected ILIKE in WHERE, got %q", ir.Where[0])
	}
}

// TestIR_IsNullIsNotNull validates IS NULL and IS NOT NULL predicates together.
func TestIR_IsNullIsNotNull(t *testing.T) {
	sql := `SELECT * FROM orders WHERE shipped_at IS NULL AND cancelled_at IS NOT NULL`
	ir := parseAssertNoError(t, sql)

	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
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

	if len(ir.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(ir.Tables))
	}
	if ir.Tables[0].Name != "users" || ir.Tables[0].Alias != "u" {
		t.Fatalf("unexpected first table %+v", ir.Tables[0])
	}
	if ir.Tables[1].Name != "orders" || ir.Tables[1].Alias != "o" {
		t.Fatalf("unexpected second table %+v", ir.Tables[1])
	}
}

// TestIR_NaturalJoin validates NATURAL JOIN table extraction.
func TestIR_NaturalJoin(t *testing.T) {
	sql := `SELECT * FROM departments NATURAL JOIN employees`
	ir := parseAssertNoError(t, sql)

	if len(ir.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(ir.Tables))
	}
	if !containsTable(ir.Tables, "departments") || !containsTable(ir.Tables, "employees") {
		t.Fatalf("expected departments and employees, got %+v", ir.Tables)
	}
}

// TestIR_SelfJoin confirms self-join produces two table references with distinct aliases.
func TestIR_SelfJoin(t *testing.T) {
	sql := `
SELECT e.name AS employee, m.name AS manager
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id`
	ir := parseAssertNoError(t, sql)

	// Self-join results in two references to the same table
	if len(ir.Tables) != 2 {
		t.Fatalf("expected 2 tables for self-join, got %d", len(ir.Tables))
	}
	if ir.Tables[0].Name != "employees" || ir.Tables[0].Alias != "e" {
		t.Fatalf("unexpected first table %+v", ir.Tables[0])
	}
	if ir.Tables[1].Name != "employees" || ir.Tables[1].Alias != "m" {
		t.Fatalf("unexpected second table %+v", ir.Tables[1])
	}
}

// TestIR_DerivedColumnsTracking validates alias-to-expression mapping in DerivedColumns.
func TestIR_DerivedColumnsTracking(t *testing.T) {
	sql := `SELECT COUNT(*) AS total, MAX(price) AS highest FROM orders`
	ir := parseAssertNoError(t, sql)

	if len(ir.DerivedColumns) != 2 {
		t.Fatalf("expected 2 derived columns, got %d: %+v", len(ir.DerivedColumns), ir.DerivedColumns)
	}
	if ir.DerivedColumns["total"] != "COUNT(*)" {
		t.Fatalf("expected 'total' -> 'COUNT(*)', got %q", ir.DerivedColumns["total"])
	}
	if ir.DerivedColumns["highest"] != "MAX(price)" {
		t.Fatalf("expected 'highest' -> 'MAX(price)', got %q", ir.DerivedColumns["highest"])
	}
}

// TestIR_ComplexWhereWithOrAnd verifies compound OR/AND/NOT predicates in WHERE.
func TestIR_ComplexWhereWithOrAnd(t *testing.T) {
	sql := `
SELECT id FROM users
WHERE (status = 'active' OR status = 'trial')
  AND (age >= 18 AND age <= 65)
  AND NOT deleted`
	ir := parseAssertNoError(t, sql)

	if len(ir.Where) != 1 {
		t.Fatalf("expected 1 WHERE clause, got %d", len(ir.Where))
	}
	whereNorm := normalise(ir.Where[0])
	if !strings.Contains(whereNorm, "status='active'") || !strings.Contains(whereNorm, "status='trial'") {
		t.Fatalf("expected OR conditions in WHERE, got %q", ir.Where[0])
	}
}

// TestIR_InsertWithDefaultValues checks INSERT DEFAULT VALUES with RETURNING.
func TestIR_InsertWithDefaultValues(t *testing.T) {
	sql := `INSERT INTO counters DEFAULT VALUES RETURNING id`
	ir := parseAssertNoError(t, sql)

	if ir.Command != QueryCommandInsert {
		t.Fatalf("expected INSERT, got %s", ir.Command)
	}
	if !containsTable(ir.Tables, "counters") {
		t.Fatalf("expected counters table, got %+v", ir.Tables)
	}
	if len(ir.Returning) != 1 {
		t.Fatalf("expected 1 RETURNING clause, got %+v", ir.Returning)
	}
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

	if ir.Command != QueryCommandInsert {
		t.Fatalf("expected INSERT, got %s", ir.Command)
	}
	if len(ir.InsertColumns) != 2 {
		t.Fatalf("expected 2 insert columns, got %+v", ir.InsertColumns)
	}
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
	if filterUsages == 0 {
		t.Fatalf("expected filter column usages, got none in %+v", ir.ColumnUsage)
	}
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
	if projUsages == 0 {
		t.Fatalf("expected projection column usages, got none in %+v", ir.ColumnUsage)
	}
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
	if groupUsages == 0 {
		t.Fatalf("expected group column usages, got none in %+v", ir.ColumnUsage)
	}
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
	if orderUsages == 0 {
		t.Fatalf("expected order column usages, got none in %+v", ir.ColumnUsage)
	}
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
	if joinUsages == 0 {
		t.Fatalf("expected join column usages, got none in %+v", ir.ColumnUsage)
	}
}

// TestIR_PositionalParameters verifies $1/$2/$3 positional parameter extraction.
func TestIR_PositionalParameters(t *testing.T) {
	sql := `SELECT * FROM users WHERE id = $1 AND name = $2 AND age > $3`
	ir := parseAssertNoError(t, sql)

	if len(ir.Parameters) != 3 {
		t.Fatalf("expected 3 parameters, got %+v", ir.Parameters)
	}
	for i, param := range ir.Parameters {
		if param.Position != i+1 {
			t.Fatalf("expected parameter at position %d, got %+v", i+1, param)
		}
		if param.Marker != "$" {
			t.Fatalf("expected $ marker, got %q", param.Marker)
		}
	}
}

// TestIR_MixedParameterTypes checks coexistence of $N and ? parameter markers.
func TestIR_MixedParameterTypes(t *testing.T) {
	sql := `SELECT * FROM users WHERE id = $1 AND status = ?`
	ir := parseAssertNoError(t, sql)

	if len(ir.Parameters) != 2 {
		t.Fatalf("expected 2 parameters, got %+v", ir.Parameters)
	}
	if ir.Parameters[0].Marker != "$" || ir.Parameters[0].Position != 1 {
		t.Fatalf("unexpected first parameter %+v", ir.Parameters[0])
	}
	if ir.Parameters[1].Marker != "?" || ir.Parameters[1].Position != 1 {
		t.Fatalf("unexpected second parameter %+v", ir.Parameters[1])
	}
}

// ---------------------------------------------------------------------------
// Additional error handling edge cases
// ---------------------------------------------------------------------------

// TestIR_EmptyInput validates that empty string input returns an error.
func TestIR_EmptyInput(t *testing.T) {
	_, err := ParseSQL("")
	if err == nil {
		t.Fatalf("expected error for empty input")
	}
}

// TestIR_WhitespaceOnly verifies that whitespace-only input returns an error.
func TestIR_WhitespaceOnly(t *testing.T) {
	_, err := ParseSQL("   \n\t  ")
	if err == nil {
		t.Fatalf("expected error for whitespace-only input")
	}
}

// TestIR_SemicolonOnly confirms that a bare semicolon returns an error.
func TestIR_SemicolonOnly(t *testing.T) {
	_, err := ParseSQL(";")
	if err == nil {
		t.Fatalf("expected error for semicolon-only input")
	}
}

// ---------------------------------------------------------------------------
// ParseErrors formatting
// ---------------------------------------------------------------------------

// TestParseErrors_Error_NilReceiver validates Error() on a nil ParseErrors receiver.
func TestParseErrors_Error_NilReceiver(t *testing.T) {
	var pe *ParseErrors
	if pe.Error() != "parse error" {
		t.Fatalf("expected 'parse error', got %q", pe.Error())
	}
}

// TestParseErrors_Error_Empty checks Error() with an empty error list.
func TestParseErrors_Error_Empty(t *testing.T) {
	pe := &ParseErrors{SQL: "test", Errors: nil}
	if pe.Error() != "parse error" {
		t.Fatalf("expected 'parse error', got %q", pe.Error())
	}
}

// TestParseErrors_Error_Single verifies Error() formatting with one syntax error.
func TestParseErrors_Error_Single(t *testing.T) {
	pe := &ParseErrors{
		SQL:    "test",
		Errors: []SyntaxError{{Line: 1, Column: 5, Message: "bad token"}},
	}
	s := pe.Error()
	if !strings.Contains(s, "line 1:5") || !strings.Contains(s, "bad token") {
		t.Fatalf("unexpected error string %q", s)
	}
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
	if !strings.Contains(s, "parse error(s)") {
		t.Fatalf("expected 'parse error(s)', got %q", s)
	}
	if !strings.Contains(s, "line 1:5") || !strings.Contains(s, "line 2:3") {
		t.Fatalf("expected both error locations in %q", s)
	}
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

	if len(ir.Subqueries) == 0 {
		t.Fatalf("expected subquery metadata")
	}
	sq := ir.Subqueries[0].Query
	if sq == nil {
		t.Fatalf("expected parsed subquery")
	}
	if sq.Limit == nil {
		t.Fatalf("expected LIMIT in subquery")
	}
	if !sq.Limit.IsNested {
		t.Fatalf("expected IsNested=true for subquery LIMIT")
	}
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

	if ir.Command != QueryCommandSelect {
		t.Fatalf("expected SELECT, got %s", ir.Command)
	}
	if len(ir.CTEs) != 2 {
		t.Fatalf("expected 2 CTEs, got %d", len(ir.CTEs))
	}
	// Check base tables from CTEs are extracted
	if !containsTable(ir.Tables, "orders") {
		t.Fatalf("expected orders table from CTE")
	}
	if !containsTable(ir.Tables, "order_items") {
		t.Fatalf("expected order_items table from CTE")
	}
	if !containsTable(ir.Tables, "products") {
		t.Fatalf("expected products table from CTE")
	}
	// Check main query references ranked CTE
	foundRanked := false
	for _, tbl := range ir.Tables {
		if strings.ToLower(tbl.Name) == "ranked" && tbl.Type == TableTypeCTE {
			foundRanked = true
		}
	}
	if !foundRanked {
		t.Fatalf("expected ranked CTE reference")
	}
	// Check ORDER BY
	if len(ir.OrderBy) != 2 {
		t.Fatalf("expected 2 ORDER BY items, got %d", len(ir.OrderBy))
	}
	// Check LIMIT
	if ir.Limit == nil {
		t.Fatalf("expected LIMIT clause")
	}
	// Check columns
	if len(ir.Columns) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(ir.Columns))
	}
	if ir.Columns[4].Alias != "pct_of_month" {
		t.Fatalf("expected pct_of_month alias, got %q", ir.Columns[4].Alias)
	}
}

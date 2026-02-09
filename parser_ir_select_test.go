// parser_ir_select_test.go covers SELECT-focused IR behaviours.
package postgresparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIR_MinimalSelect validates baseline SELECT parsing.
func TestIR_MinimalSelect(t *testing.T) {
	sql := `SELECT id FROM users`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandSelect, ir.Command, "expected command SELECT")
	require.Len(t, ir.Tables, 1, "expected 1 table")
	assert.Equal(t, "users", ir.Tables[0].Name, "expected table 'users'")
	require.Len(t, ir.Columns, 1, "expected 1 column")
	assert.Equal(t, "id", ir.Columns[0].Expression, "unexpected column")
}

// TestIR_TableAlias ensures table aliases are captured correctly.
func TestIR_TableAlias(t *testing.T) {
	sql := `SELECT u.id, u.email FROM users u`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Tables, 1, "expected 1 table")
	assert.Equal(t, "users", ir.Tables[0].Name, "expected table 'users'")
	assert.Equal(t, "u", ir.Tables[0].Alias, "expected alias 'u'")
	require.Len(t, ir.Columns, 2, "expected 2 columns")
	assert.Equal(t, "u.id", ir.Columns[0].Expression, "unexpected column")
}

// TestIR_FunctionsOnColumns confirms scalar expressions and aliases persist.
func TestIR_FunctionsOnColumns(t *testing.T) {
	sql := `SELECT COALESCE(email, 'unknown') AS email, LOWER(name) FROM users`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Columns, 2, "expected 2 columns")
	assert.Equal(t, "COALESCE(email, 'unknown')", ir.Columns[0].Expression, "unexpected first column expression")
	assert.Equal(t, "email", ir.Columns[0].Alias, "unexpected first column alias")
	assert.Equal(t, "LOWER(name)", ir.Columns[1].Expression, "unexpected second column expression")
}

// TestIR_AggregateFunctions verifies aggregate projections and WHERE clauses.
func TestIR_AggregateFunctions(t *testing.T) {
	sql := `SELECT SUM(price) AS total, COUNT(*) FROM orders WHERE status = 'shipped'`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Columns, 2, "expected 2 columns")
	assert.Equal(t, "SUM(price)", ir.Columns[0].Expression, "unexpected first column")
	assert.Equal(t, "total", ir.Columns[0].Alias, "unexpected alias")
	assert.Equal(t, "COUNT(*)", ir.Columns[1].Expression, "unexpected second column")
	require.Len(t, ir.Where, 1, "expected WHERE clause")
	assert.Equal(t, "status='shipped'", normalise(ir.Where[0]), "unexpected WHERE clause")
}

// TestIR_ArithmeticInProjection checks arithmetic expressions in projections.
func TestIR_ArithmeticInProjection(t *testing.T) {
	sql := `SELECT price * quantity AS total_cost FROM orders`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Columns, 1, "expected 1 column")
	assert.Equal(t, "price * quantity", ir.Columns[0].Expression, "unexpected column expression")
	assert.Equal(t, "total_cost", ir.Columns[0].Alias, "unexpected column alias")
}

// TestIR_CaseExpressions ensures CASE expressions survive in projections.
func TestIR_CaseExpressions(t *testing.T) {
	sql := `
SELECT
  CASE
    WHEN status = 'shipped' THEN 'done'
    WHEN status = 'pending' THEN 'waiting'
    ELSE 'other'
  END AS state
FROM orders`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Columns, 1, "expected 1 column")
	expr := ir.Columns[0].Expression
	assert.Contains(t, expr, "CASE", "unexpected CASE expression")
	assert.Contains(t, expr, "WHEN status = 'shipped'", "unexpected CASE expression content")
	assert.Equal(t, "state", ir.Columns[0].Alias, "unexpected alias")
}

// TestIR_JSONOperators confirms JSON operators are preserved.
func TestIR_JSONOperators(t *testing.T) {
	sql := `
SELECT data->>'name' AS username, data->'meta'->>'id' AS user_id
FROM profiles
WHERE (data->>'active')::boolean = true`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Columns, 2, "expected 2 columns")
	assert.Equal(t, "data->>'name'", ir.Columns[0].Expression, "unexpected first column")
	assert.Equal(t, "username", ir.Columns[0].Alias, "unexpected alias")
	assert.Equal(t, "data->'meta'->>'id'", ir.Columns[1].Expression, "unexpected second column")
	assert.Equal(t, "user_id", ir.Columns[1].Alias, "unexpected alias")
	require.Len(t, ir.Where, 1, "expected WHERE clause")
	assert.Contains(t, ir.Where[0], "data->>'active'", "unexpected WHERE clause")
}

// TestIR_WindowFunctions checks window definitions are retained.
func TestIR_WindowFunctions(t *testing.T) {
	sql := `
SELECT id,
       ROW_NUMBER() OVER (PARTITION BY tenant ORDER BY created_at DESC) AS seq
FROM orders`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Columns, 2, "expected 2 columns")
	expr := ir.Columns[1].Expression
	assert.Contains(t, expr, "ROW_NUMBER() OVER", "unexpected window expression")
	assert.Contains(t, expr, "PARTITION BY tenant", "unexpected window expression")
	assert.Contains(t, expr, "ORDER BY created_at DESC", "unexpected window expression")
	assert.Equal(t, "seq", ir.Columns[1].Alias, "unexpected alias")
}

// TestIR_GroupByHavingFunctions validates GROUP BY and HAVING extraction.
func TestIR_GroupByHavingFunctions(t *testing.T) {
	sql := `
SELECT region, SUM(sales) AS total
FROM metrics
GROUP BY region
HAVING SUM(sales) > 100`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.GroupBy, 1, "expected 1 GROUP BY")
	assert.Equal(t, "region", ir.GroupBy[0], "unexpected GROUP BY")
	require.Len(t, ir.Having, 1, "expected 1 HAVING")
	assert.Equal(t, "sum(sales)>100", normalise(ir.Having[0]), "unexpected HAVING")
}

// TestIR_OrderByWithFunctions verifies ORDER BY expressions and modifiers.
func TestIR_OrderByWithFunctions(t *testing.T) {
	sql := `
SELECT id, name
FROM users
ORDER BY COALESCE(last_login, created_at) DESC NULLS FIRST`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.OrderBy, 1, "expected 1 ORDER BY")
	order := ir.OrderBy[0]
	assert.Equal(t, "COALESCE(last_login, created_at)", order.Expression, "unexpected ORDER BY expression")
	assert.Equal(t, "DESC", order.Direction, "unexpected ORDER BY direction")
	assert.Equal(t, "NULLS FIRST", order.Nulls, "unexpected ORDER BY nulls")
}

// TestIR_CTEAndNestedCTE checks multiple CTEs are captured with raw SQL.
func TestIR_CTEAndNestedCTE(t *testing.T) {
	sql := `
WITH recent AS (
  SELECT id, created_at FROM orders WHERE created_at > NOW() - INTERVAL '30 days'
), active_users AS (
  SELECT user_id FROM logins WHERE last_login > NOW() - INTERVAL '7 days'
)
SELECT r.id
FROM recent r
JOIN active_users a ON r.id = a.user_id`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.CTEs, 2, "expected 2 CTEs")

	assert.Equal(t, "recent", strings.ToLower(ir.CTEs[0].Name), "unexpected first CTE name")
	assert.Contains(t, ir.CTEs[0].Query, "SELECT id, created_at", "unexpected first CTE query")

	assert.Equal(t, "active_users", strings.ToLower(ir.CTEs[1].Name), "unexpected second CTE name")
	assert.Contains(t, ir.CTEs[1].Query, "SELECT user_id", "unexpected second CTE query")
}

// TestIR_LimitOffset confirms LIMIT/OFFSET text is stored verbatim.
func TestIR_LimitOffset(t *testing.T) {
	sql := `SELECT * FROM logs LIMIT 10 OFFSET 5`
	ir := parseAssertNoError(t, sql)

	require.NotNil(t, ir.Limit, "expected limit clause")
	assert.Contains(t, strings.ToUpper(ir.Limit.Limit), "LIMIT 10", "unexpected limit text")
	assert.Contains(t, strings.ToUpper(ir.Limit.Offset), "OFFSET 5", "unexpected offset text")
}

// TestIR_Parameters ensures positional and anonymous parameters are recorded.
func TestIR_Parameters(t *testing.T) {
	sql := `SELECT * FROM users WHERE age > ? AND id = $2`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Parameters, 2, "expected 2 parameters")

	assert.Equal(t, "?", ir.Parameters[0].Raw, "unexpected first parameter raw")
	assert.Equal(t, 1, ir.Parameters[0].Position, "unexpected first parameter position")

	assert.Equal(t, "$2", ir.Parameters[1].Raw, "unexpected second parameter raw")
	assert.Equal(t, 2, ir.Parameters[1].Position, "unexpected second parameter position")
}

// TestIR_LateralCorrelationNoFalseMatch ensures short aliases don't false-match
// substrings in LATERAL function text (e.g., alias "a" should not match "data.").
func TestIR_LateralCorrelationNoFalseMatch(t *testing.T) {
	sql := `SELECT * FROM products a
		CROSS JOIN LATERAL unnest(a.tags) AS tag(name)`
	ir := parseAssertNoError(t, sql)

	// Alias "a" should match "a.tags" via word boundary.
	foundA := false
	for _, corr := range ir.Correlations {
		if corr.OuterAlias == "a" && corr.Type == "LATERAL" {
			foundA = true
			break
		}
	}
	assert.True(t, foundA, "expected LATERAL correlation for alias 'a'")

	// Verify that a column named "data" does NOT false-match alias "a".
	sql2 := `SELECT * FROM metadata
		CROSS JOIN LATERAL unnest(data.items) AS item(val)`
	ir2 := parseAssertNoError(t, sql2)

	for _, corr := range ir2.Correlations {
		if corr.OuterAlias == "a" {
			t.Fatalf("alias 'a' should NOT match 'data.items', but got correlation %+v", corr)
		}
	}
}

// TestIR_IgnoresDoubleSlashComment verifies preprocessing strips // comments.
func TestIR_IgnoresDoubleSlashComment(t *testing.T) {
	sql := `SELECT id FROM users // extra metadata`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Tables, 1, "expected 1 table")
	assert.Equal(t, "users", ir.Tables[0].Name, "unexpected table")
	require.Len(t, ir.Columns, 1, "expected 1 column")
	assert.Equal(t, "id", ir.Columns[0].Expression, "unexpected column")
}

// TestIR_ComplexMonsterQuery stress-tests complex real-world SELECT shapes.
func TestIR_ComplexMonsterQuery(t *testing.T) {
	sql := `
WITH recent_orders AS (
  SELECT
    o.id,
    o.customer_id,
    o.created_at,
    SUM(oi.quantity * oi.price) AS total_amount
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.id
  WHERE o.created_at >= NOW() - INTERVAL '30 days'
  GROUP BY o.id, o.customer_id, o.created_at
),
ranked_customers AS (
  SELECT
    c.id AS customer_id,
    c.name,
    COUNT(ro.id) AS order_count,
    SUM(ro.total_amount) AS revenue,
    RANK() OVER (ORDER BY SUM(ro.total_amount) DESC) AS rank
  FROM customers c
  JOIN recent_orders ro ON ro.customer_id = c.id
  GROUP BY c.id, c.name
),
top_customers AS (
  SELECT customer_id
  FROM ranked_customers
  WHERE rank <= 10
),
expanded_data AS (
  SELECT
    tc.customer_id,
    jsonb_build_object(
      'info', c.metadata ->> 'info',
      'orders', (
        SELECT jsonb_agg(to_jsonb(ro.*))
        FROM recent_orders ro
        WHERE ro.customer_id = tc.customer_id
      )
    ) AS full_data
  FROM top_customers tc
  JOIN customers c ON c.id = tc.customer_id
)
SELECT
  ed.customer_id,
  ed.full_data ->> 'info' AS customer_info,
  COUNT(DISTINCT (o.order_json ->> 'id')) AS order_count,
  SUM((o.order_json ->> 'total_amount')::numeric) AS total_revenue
FROM expanded_data ed
CROSS JOIN LATERAL jsonb_array_elements(ed.full_data -> 'orders') AS o(order_json)
WHERE (o.order_json ->> 'total_amount')::numeric > 100
GROUP BY ed.customer_id, ed.full_data
HAVING SUM((o.order_json ->> 'total_amount')::numeric) > 5000
ORDER BY total_revenue DESC
LIMIT 5;
`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.CTEs, 4, "expected 4 CTEs")
	assert.Equal(t, "expanded_data", strings.ToLower(ir.CTEs[3].Name), "unexpected final CTE")

	assert.GreaterOrEqual(t, len(ir.Tables), 2, "expected at least expanded_data and lateral function in tables")

	// Find expanded_data CTE reference (not necessarily first due to base tables from CTEs)
	foundExpandedData := false
	for _, tbl := range ir.Tables {
		if tbl.Type == TableTypeCTE && strings.ToLower(tbl.Name) == "expanded_data" {
			foundExpandedData = true
			break
		}
	}
	assert.True(t, foundExpandedData, "expected to find expanded_data CTE reference in tables")

	foundLateral := false
	for _, tbl := range ir.Tables {
		if tbl.Type == TableTypeFunction && strings.Contains(tbl.Raw, "jsonb_array_elements") {
			foundLateral = true
			break
		}
	}
	assert.True(t, foundLateral, "expected to find jsonb_array_elements lateral function in tables")

	require.Len(t, ir.OrderBy, 1, "expected 1 ORDER BY")
	assert.Equal(t, "total_revenue", strings.ToLower(ir.OrderBy[0].Expression), "unexpected ORDER BY")

	require.NotNil(t, ir.Limit, "expected limit clause")
	assert.Contains(t, strings.ToUpper(ir.Limit.Limit), "LIMIT 5", "unexpected LIMIT")
}

// TestIR_InnerJoinWithConditions checks join predicates capture multiple clauses.
func TestIR_InnerJoinWithConditions(t *testing.T) {
	sql := `
SELECT o.id, c.name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id AND c.status = 'active'`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Tables, 2, "expected 2 tables")
	assert.Equal(t, "orders", ir.Tables[0].Name, "unexpected table 1")
	assert.Equal(t, "customers", ir.Tables[1].Name, "unexpected table 2")

	require.Len(t, ir.JoinConditions, 1, "expected 1 join condition")
	joinExpr := normalise(ir.JoinConditions[0])
	assert.Contains(t, joinExpr, "o.customer_id=c.id", "unexpected join expression")
	assert.Contains(t, joinExpr, "c.status='active'", "unexpected join expression")
}

// TestIR_LeftJoinWithFunctions ensures LEFT JOIN filters and expressions persist.
func TestIR_LeftJoinWithFunctions(t *testing.T) {
	sql := `
SELECT u.id, COALESCE(p.name, 'N/A') AS product_name
FROM users u
LEFT JOIN purchases p ON u.id = p.user_id AND p.amount > 0`
	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.Tables, 2, "expected 2 tables")
	assert.Equal(t, "users", ir.Tables[0].Name, "unexpected table 1")
	assert.Equal(t, "purchases", ir.Tables[1].Name, "unexpected table 2")

	assert.Equal(t, "COALESCE(p.name, 'N/A')", ir.Columns[1].Expression, "unexpected second column expression")
	assert.Equal(t, "product_name", ir.Columns[1].Alias, "unexpected second column alias")

	require.Len(t, ir.JoinConditions, 1, "expected 1 join condition")
	assert.Contains(t, ir.JoinConditions[0], "p.amount > 0", "unexpected join condition")
}

// TestIR_FallbackToUnknown confirms unsupported statements return UNKNOWN.
func TestIR_FallbackToUnknown(t *testing.T) {
	sql := `CREATE TABLE demo(id INT PRIMARY KEY);`
	ir := parseAssertNoError(t, sql)

	assert.Equal(t, QueryCommandUnknown, ir.Command, "expected UNKNOWN command for unsupported statement")
}

// TestContainsWordDot verifies the word-boundary dot matching used by LATERAL correlation detection.
func TestContainsWordDot(t *testing.T) {
	tests := []struct {
		text string
		word string
		want bool
	}{
		{"t.id", "t", true},
		{"data.id", "a", false},        // "a" is a substring of "data", not at word boundary
		{"data.id", "data", true},      // "data" at start
		{"foo(data.id)", "data", true}, // preceded by non-ident char
		{"x_data.id", "data", false},   // preceded by underscore (ident char)
		{"t.id + s.value", "s", true},  // preceded by space
		{"t.id + s.value", "t", true},  // at start
		{"", "t", false},               // empty text
		{"t.id", "", false},            // empty word
	}
	for _, tt := range tests {
		got := containsWordDot(tt.text, tt.word)
		assert.Equal(t, tt.want, got, "containsWordDot(%q, %q)", tt.text, tt.word)
	}
}

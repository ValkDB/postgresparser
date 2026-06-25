package postgresparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIR_CTEParsedQuery_Select(t *testing.T) {
	sql := `WITH active_users AS (
    SELECT users.id, users.name FROM users WHERE users.active = $1
)
SELECT active_users.id, active_users.name
FROM active_users
WHERE active_users.id = $2`

	ir := parseAssertNoError(t, sql)
	require.Len(t, ir.CTEs, 1)

	cte := ir.CTEs[0]
	require.NotNil(t, cte.ParsedQuery)
	assert.Equal(t, QueryCommandSelect, cte.ParsedQuery.Command)
	assert.Equal(t, cte.Query, cte.ParsedQuery.RawSQL)

	require.Len(t, cte.ParsedQuery.Columns, 2)
	assert.Equal(t, "users.id", cte.ParsedQuery.Columns[0].Expression)
	assert.Equal(t, "users.name", cte.ParsedQuery.Columns[1].Expression)
	assert.True(t, containsTable(cte.ParsedQuery.Tables, "users"))

	require.Len(t, cte.ParsedQuery.Parameters, 1)
	assert.Equal(t, "$1", cte.ParsedQuery.Parameters[0].Raw)

	var foundFilter bool
	for _, usage := range cte.ParsedQuery.ColumnUsage {
		if usage.UsageType == ColumnUsageTypeFilter && usage.Column == "active" {
			foundFilter = true
			break
		}
	}
	assert.True(t, foundFilter, "expected CTE parsed query to retain filter column usage")
}

// TestIR_NestedTableMarking verifies tables surfaced from CTE and subquery
// bodies are marked Nested, while the query's own FROM relations are not.
func TestIR_NestedTableMarking(t *testing.T) {
	nestedOf := func(ir *ParsedQuery, name string) (bool, bool) {
		for _, tbl := range ir.Tables {
			if tbl.Name == name {
				return tbl.Nested, true
			}
		}
		return false, false
	}

	t.Run("CTE body table is nested, FROM relations are not", func(t *testing.T) {
		ir := parseAssertNoError(t, `WITH ro AS (SELECT id FROM orders) SELECT * FROM ro JOIN customers c ON c.id = ro.id`)

		nested, ok := nestedOf(ir, "orders")
		require.True(t, ok, "expected nested base table 'orders'")
		assert.True(t, nested, "CTE body table 'orders' should be nested")

		nested, ok = nestedOf(ir, "ro")
		require.True(t, ok, "expected CTE relation 'ro'")
		assert.False(t, nested, "CTE reference 'ro' is a direct FROM relation")

		nested, ok = nestedOf(ir, "customers")
		require.True(t, ok, "expected base table 'customers'")
		assert.False(t, nested, "joined base table 'customers' is a direct FROM relation")
	})

	t.Run("derived table body is nested, derived relation is not", func(t *testing.T) {
		ir := parseAssertNoError(t, `SELECT * FROM (SELECT id FROM orders) sub WHERE sub.id > 1`)

		nested, ok := nestedOf(ir, "orders")
		require.True(t, ok, "expected nested base table 'orders'")
		assert.True(t, nested, "derived-table body 'orders' should be nested")

		nested, ok = nestedOf(ir, "sub")
		require.True(t, ok, "expected derived relation 'sub'")
		assert.False(t, nested, "derived relation 'sub' is a direct FROM relation")
	})

	t.Run("plain FROM tables are not nested", func(t *testing.T) {
		ir := parseAssertNoError(t, `SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id`)
		for _, tbl := range ir.Tables {
			assert.False(t, tbl.Nested, "plain FROM table %q should not be nested", tbl.Name)
		}
	})
}

func TestIR_CTEParsedQuery_Update(t *testing.T) {
	sql := `WITH updated_users AS (
    UPDATE users SET active = true WHERE id = $1 RETURNING id
)
SELECT * FROM updated_users`

	ir := parseAssertNoError(t, sql)
	require.Len(t, ir.CTEs, 1)

	cte := ir.CTEs[0]
	require.NotNil(t, cte.ParsedQuery)
	assert.Equal(t, QueryCommandUpdate, cte.ParsedQuery.Command)
	assert.True(t, containsTable(cte.ParsedQuery.Tables, "users"))
	assert.NotEmpty(t, cte.ParsedQuery.SetClauses)
	assert.NotEmpty(t, cte.ParsedQuery.Returning)
	require.Len(t, cte.ParsedQuery.Parameters, 1)
	assert.Equal(t, "$1", cte.ParsedQuery.Parameters[0].Raw)
}

// TestCTETablesExtraction tests that tables referenced inside CTEs are properly extracted.
func TestCTETablesExtraction(t *testing.T) {
	sql := `WITH expanded AS (
    SELECT payload
    FROM slow_smoke.unindexed
    WHERE payload LIKE ?
)
SELECT COUNT(*)
FROM expanded e1
JOIN expanded e2 ON e2.payload >= e1.payload`

	ir := parseAssertNoError(t, sql)

	require.Len(t, ir.CTEs, 1, "expected 1 CTE")
	assert.Equal(t, "expanded", ir.CTEs[0].Name, "expected CTE name 'expanded'")

	var foundUnindexed bool
	var cteReferences int

	for _, table := range ir.Tables {
		t.Logf("Found table: %+v", table)

		if table.Schema == "slow_smoke" && table.Name == "unindexed" && table.Type == TableTypeBase {
			foundUnindexed = true
		} else if table.Name == "expanded" && table.Type == TableTypeCTE {
			cteReferences++
		}
	}

	assert.True(t, foundUnindexed, "Expected to find 'slow_smoke.unindexed' as a base table")
	assert.Equal(t, 2, cteReferences, "Expected 2 CTE references to 'expanded'")
}

// TestSimpleCTETablesExtraction tests a simpler case.
func TestSimpleCTETablesExtraction(t *testing.T) {
	sql := `WITH recent AS (
    SELECT * FROM orders WHERE created_at > NOW()
)
SELECT * FROM recent`

	ir := parseAssertNoError(t, sql)

	var foundOrders bool
	for _, table := range ir.Tables {
		if table.Name == "orders" && table.Type == TableTypeBase {
			foundOrders = true
		}
	}

	assert.True(t, foundOrders, "Expected to find 'orders' as a base table from inside CTE")
}

// TestMultipleCTEsWithTables tests multiple CTEs each referencing tables.
func TestMultipleCTEsWithTables(t *testing.T) {
	sql := `WITH
	users_cte AS (SELECT * FROM users WHERE active = true),
	orders_cte AS (SELECT * FROM orders JOIN products ON orders.product_id = products.id)
SELECT * FROM users_cte JOIN orders_cte ON users_cte.id = orders_cte.user_id`

	ir := parseAssertNoError(t, sql)

	expectedTables := map[string]bool{
		"users":    false,
		"orders":   false,
		"products": false,
	}

	for _, table := range ir.Tables {
		if table.Type == TableTypeBase {
			if _, ok := expectedTables[table.Name]; ok {
				expectedTables[table.Name] = true
			}
		}
	}

	for name, found := range expectedTables {
		assert.True(t, found, "Expected to find '%s' as a base table from inside CTEs", name)
	}
}

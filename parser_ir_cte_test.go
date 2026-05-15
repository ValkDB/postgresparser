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

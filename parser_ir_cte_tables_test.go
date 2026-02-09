package postgresparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCTETablesExtraction tests that tables referenced inside CTEs are properly extracted
func TestCTETablesExtraction(t *testing.T) {
	// Test case from the E2E scenario
	sql := `WITH expanded AS (
    SELECT payload
    FROM slow_smoke.unindexed
    WHERE payload LIKE ?
)
SELECT COUNT(*)
FROM expanded e1
JOIN expanded e2 ON e2.payload >= e1.payload`

	ir := parseAssertNoError(t, sql)

	// Check CTEs
	require.Len(t, ir.CTEs, 1, "expected 1 CTE")
	assert.Equal(t, "expanded", ir.CTEs[0].Name, "expected CTE name 'expanded'")

	// Check tables
	// We should have:
	// - slow_smoke.unindexed (base table from inside the CTE)
	// - expanded e1 (CTE reference)
	// - expanded e2 (CTE reference)

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

// TestSimpleCTETablesExtraction tests a simpler case
func TestSimpleCTETablesExtraction(t *testing.T) {
	sql := `WITH recent AS (
    SELECT * FROM orders WHERE created_at > NOW()
)
SELECT * FROM recent`

	ir := parseAssertNoError(t, sql)

	// Should find 'orders' as a base table
	var foundOrders bool
	for _, table := range ir.Tables {
		if table.Name == "orders" && table.Type == TableTypeBase {
			foundOrders = true
		}
	}

	assert.True(t, foundOrders, "Expected to find 'orders' as a base table from inside CTE")
}

// TestMultipleCTEsWithTables tests multiple CTEs each referencing tables
func TestMultipleCTEsWithTables(t *testing.T) {
	sql := `WITH
	users_cte AS (SELECT * FROM users WHERE active = true),
	orders_cte AS (SELECT * FROM orders JOIN products ON orders.product_id = products.id)
SELECT * FROM users_cte JOIN orders_cte ON users_cte.id = orders_cte.user_id`

	ir := parseAssertNoError(t, sql)

	// Should find 'users', 'orders', and 'products' as base tables
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

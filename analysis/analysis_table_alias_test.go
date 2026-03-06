// Package analysis provides SQL analysis capabilities using the ANTLR PostgreSQL parser.
// This file tests that table aliases are preserved in column usage when multiple tables share column names.
package analysis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnalyze_PreserveTableAlias tests that table aliases are preserved when multiple tables
// have columns with the same name (e.g., orders.status vs inventory.status).
func TestAnalyze_PreserveTableAlias(t *testing.T) {
	sql := `SELECT * FROM orders o
		JOIN inventory i ON o.product_id = i.product_id
		WHERE o.status = 'active' AND i.status = 'in_stock'`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Find all filter usages for "status" column
	var statusFilters []SQLColumnUsage
	for _, usage := range result.ColumnUsage {
		if usage.UsageType == SQLUsageTypeFilter && usage.Column == "status" {
			statusFilters = append(statusFilters, usage)
		}
	}

	// We should have TWO separate filter entries for status, one for each table
	assert.Len(t, statusFilters, 2, "Should have two separate status filter entries")

	// Check that table aliases are different
	if len(statusFilters) == 2 {
		aliases := []string{statusFilters[0].TableAlias, statusFilters[1].TableAlias}
		assert.Contains(t, aliases, "o", "Should have orders alias 'o'")
		assert.Contains(t, aliases, "i", "Should have inventory alias 'i'")
		assert.NotEqual(t, statusFilters[0].TableAlias, statusFilters[1].TableAlias,
			"The two status filters should have different table aliases")
	}
}

// TestAnalyze_PreserveTableAliasInSubquery tests that table aliases are preserved
// in IN subqueries without leaking inner filter usage into the outer scope.
func TestAnalyze_PreserveTableAliasInSubquery(t *testing.T) {
	sql := `SELECT * FROM orders o
		WHERE o.customer_id IN (
			SELECT c.id FROM customers c WHERE c.status = 'active'
		) AND o.status = 'pending'`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Outer scope should keep only the orders.status filter.
	var outerStatusFilters []SQLColumnUsage
	for _, usage := range result.ColumnUsage {
		if usage.UsageType == SQLUsageTypeFilter && usage.Column == "status" {
			outerStatusFilters = append(outerStatusFilters, usage)
		}
	}
	require.Len(t, outerStatusFilters, 1, "Outer scope should keep only one status filter")
	assert.Equal(t, "o", outerStatusFilters[0].TableAlias, "Outer status filter should keep alias 'o'")

	require.Len(t, result.Subqueries, 1, "Should capture the IN subquery")
	require.NotNil(t, result.Subqueries[0].Analysis, "Subquery analysis should be present")

	var nestedStatusFilters []SQLColumnUsage
	for _, usage := range result.Subqueries[0].Analysis.ColumnUsage {
		if usage.UsageType == SQLUsageTypeFilter && usage.Column == "status" {
			nestedStatusFilters = append(nestedStatusFilters, usage)
		}
	}
	require.Len(t, nestedStatusFilters, 1, "Nested scope should keep the customer status filter")
	assert.Equal(t, "c", nestedStatusFilters[0].TableAlias, "Nested status filter should keep alias 'c'")
}

// TestAnalyze_MultipleOperatorsOnSameColumn tests that multiple operators on the same column
// from different tables are all preserved with correct aliases.
func TestAnalyze_MultipleOperatorsOnSameColumn(t *testing.T) {
	sql := `SELECT * FROM products p
		JOIN inventory i ON p.id = i.product_id
		WHERE p.price > 100 AND i.price <= 50`

	result, err := AnalyzeSQL(sql)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Find all filter usages for "price" column
	var priceFilters []SQLColumnUsage
	for _, usage := range result.ColumnUsage {
		if usage.UsageType == SQLUsageTypeFilter && usage.Column == "price" {
			priceFilters = append(priceFilters, usage)
		}
	}

	// We should have TWO separate filter entries for price
	assert.Len(t, priceFilters, 2, "Should have two separate price filter entries")

	// Verify we have both operators with correct aliases
	var hasProductsGT, hasInventoryLTE bool
	for _, filter := range priceFilters {
		if filter.TableAlias == "p" && filter.Operator == ">" {
			hasProductsGT = true
		}
		if filter.TableAlias == "i" && filter.Operator == "<=" {
			hasInventoryLTE = true
		}
	}
	assert.True(t, hasProductsGT, "Should have products.price > 100 with alias 'p'")
	assert.True(t, hasInventoryLTE, "Should have inventory.price <= 50 with alias 'i'")
}

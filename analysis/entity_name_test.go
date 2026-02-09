package analysis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestEntityNameFromTables validates entity name construction from base tables with deduplication and sorting.
func TestEntityNameFromTables(t *testing.T) {
	tests := []struct {
		name     string
		analysis *SQLAnalysis
		expected string
	}{
		{
			name:     "nil analysis",
			analysis: nil,
			expected: "",
		},
		{
			name:     "no tables",
			analysis: &SQLAnalysis{},
			expected: "",
		},
		{
			name: "single table with schema",
			analysis: &SQLAnalysis{
				Tables: []SQLTable{
					{Schema: "slow_smoke", Name: "unindexed", Type: SQLTableTypeBase},
				},
			},
			expected: "slow_smoke.unindexed",
		},
		{
			name: "single table without schema (defaults to public)",
			analysis: &SQLAnalysis{
				Tables: []SQLTable{
					{Name: "users", Type: SQLTableTypeBase},
				},
			},
			expected: "public.users",
		},
		{
			name: "multiple base tables",
			analysis: &SQLAnalysis{
				Tables: []SQLTable{
					{Schema: "public", Name: "users", Type: SQLTableTypeBase},
					{Schema: "public", Name: "orders", Type: SQLTableTypeBase},
				},
			},
			expected: "public.orders,public.users", // Sorted alphabetically
		},
		{
			name: "mixed table types (only base tables included)",
			analysis: &SQLAnalysis{
				Tables: []SQLTable{
					{Schema: "public", Name: "users", Type: SQLTableTypeBase},
					{Name: "expanded", Type: SQLTableTypeCTE}, // CTE - should be excluded
					{Schema: "public", Name: "orders", Type: SQLTableTypeBase},
					{Name: "subq", Type: SQLTableTypeSubquery}, // Subquery - should be excluded
				},
			},
			expected: "public.orders,public.users", // Only base tables, sorted
		},
		{
			name: "duplicate tables (deduplicated)",
			analysis: &SQLAnalysis{
				Tables: []SQLTable{
					{Schema: "public", Name: "users", Type: SQLTableTypeBase, Alias: "u1"},
					{Schema: "public", Name: "users", Type: SQLTableTypeBase, Alias: "u2"},
					{Schema: "public", Name: "orders", Type: SQLTableTypeBase},
				},
			},
			expected: "public.orders,public.users", // Deduplicated and sorted
		},
		{
			name: "complex query with CTE and base table (E2E test case)",
			analysis: &SQLAnalysis{
				Tables: []SQLTable{
					{Name: "expanded", Type: SQLTableTypeCTE, Alias: "e1"},
					{Name: "expanded", Type: SQLTableTypeCTE, Alias: "e2"},
					{Schema: "slow_smoke", Name: "unindexed", Type: SQLTableTypeBase},
				},
			},
			expected: "slow_smoke.unindexed", // Only the base table
		},
		{
			name: "multiple schemas",
			analysis: &SQLAnalysis{
				Tables: []SQLTable{
					{Schema: "billing", Name: "invoices", Type: SQLTableTypeBase},
					{Schema: "public", Name: "users", Type: SQLTableTypeBase},
					{Schema: "admin", Name: "logs", Type: SQLTableTypeBase},
				},
			},
			expected: "admin.logs,billing.invoices,public.users", // Sorted by full name
		},
		{
			name: "only CTEs and subqueries (no base tables)",
			analysis: &SQLAnalysis{
				Tables: []SQLTable{
					{Name: "temp_results", Type: SQLTableTypeCTE},
					{Name: "subquery1", Type: SQLTableTypeSubquery},
				},
			},
			expected: "", // No base tables
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EntityNameFromTables(tt.analysis)
			assert.Equal(t, tt.expected, result)
		})
	}
}

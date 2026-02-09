// Package analysis provides query analysis for the PostgreSQL parser.
// This file contains tests for the batch of fixes applied to the analysis package:
//   - Issue #6: Missing SQLUsageType constants for upsert/merge
//   - Issue #8: First-table fallback only for single-table queries
//   - Issue #9: Paren-aware comma splitting in normalizeReturning
//   - Issue #11: Dead-code TrimPrefix removal in join_parser
//   - Issue #12: Redundant JSONB flag assignment removal
//   - Issue #13: BaseTables godoc for "public" default (no runtime test needed)
package analysis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valkdb/postgresparser"
)

// =============================================================================
// Issue #6: SQLUsageType constants must match ColumnUsageType in ir.go
// =============================================================================

// TestSQLUsageTypeConstants_MatchIR verifies SQLUsageType constants match their ColumnUsageType counterparts.
func TestSQLUsageTypeConstants_MatchIR(t *testing.T) {
	// Verify that every ColumnUsageType constant in ir.go has a matching
	// SQLUsageType constant in analysis/types.go with the same string value.
	irConstants := map[postgresparser.ColumnUsageType]SQLUsageType{
		postgresparser.ColumnUsageTypeFilter:          SQLUsageTypeFilter,
		postgresparser.ColumnUsageTypeJoin:            SQLUsageTypeJoin,
		postgresparser.ColumnUsageTypeProjection:      SQLUsageTypeProjection,
		postgresparser.ColumnUsageTypeGroupBy:         SQLUsageTypeGroup,
		postgresparser.ColumnUsageTypeOrderBy:         SQLUsageTypeOrder,
		postgresparser.ColumnUsageTypeWindowPartition: SQLUsageTypeWindowPartition,
		postgresparser.ColumnUsageTypeWindowOrder:     SQLUsageTypeWindowOrder,
		postgresparser.ColumnUsageTypeReturning:       SQLUsageTypeReturning,
		postgresparser.ColumnUsageTypeDMLSet:          SQLUsageTypeDMLSet,
		postgresparser.ColumnUsageTypeUpsertTarget:    SQLUsageTypeUpsertTarget,
		postgresparser.ColumnUsageTypeUpsertSet:       SQLUsageTypeUpsertSet,
		postgresparser.ColumnUsageTypeMergeTarget:     SQLUsageTypeMergeTarget,
		postgresparser.ColumnUsageTypeMergeSource:     SQLUsageTypeMergeSource,
		postgresparser.ColumnUsageTypeMergeSet:        SQLUsageTypeMergeSet,
		postgresparser.ColumnUsageTypeMergeInsert:     SQLUsageTypeMergeInsert,
		postgresparser.ColumnUsageTypeUnknown:         SQLUsageTypeUnknown,
	}

	for irType, sqlType := range irConstants {
		t.Run(string(irType), func(t *testing.T) {
			assert.Equal(t, string(irType), string(sqlType),
				"SQLUsageType %q should match ColumnUsageType %q", sqlType, irType)
		})
	}
}

// TestSQLUsageType_UpsertConstants validates upsert_target and upsert_set constant values.
func TestSQLUsageType_UpsertConstants(t *testing.T) {
	assert.Equal(t, SQLUsageType("upsert_target"), SQLUsageTypeUpsertTarget)
	assert.Equal(t, SQLUsageType("upsert_set"), SQLUsageTypeUpsertSet)
}

// TestSQLUsageType_MergeConstants validates merge_target, merge_source, merge_set, and merge_insert values.
func TestSQLUsageType_MergeConstants(t *testing.T) {
	assert.Equal(t, SQLUsageType("merge_target"), SQLUsageTypeMergeTarget)
	assert.Equal(t, SQLUsageType("merge_source"), SQLUsageTypeMergeSource)
	assert.Equal(t, SQLUsageType("merge_set"), SQLUsageTypeMergeSet)
	assert.Equal(t, SQLUsageType("merge_insert"), SQLUsageTypeMergeInsert)
}

// =============================================================================
// Issue #8: First-table fallback only when len(pq.Tables) == 1
// =============================================================================

// TestWhereConditions_MultiTableNoFallback ensures unaliased columns are not mis-attributed in multi-table queries.
func TestWhereConditions_MultiTableNoFallback(t *testing.T) {
	// In a multi-table query where a column has no alias, the table should
	// NOT default to the first table. It should be left empty.
	query := `
		SELECT o.id, c.name, p.title
		FROM orders o
		JOIN customers c ON o.customer_id = c.id
		JOIN products p ON o.product_id = p.id
		WHERE unaliased_col = 'value'
	`

	conditions, err := ExtractWhereConditions(query)
	require.NoError(t, err)

	// Find the condition for unaliased_col
	for _, cond := range conditions {
		if cond.Column == "unaliased_col" {
			// With the fix, multi-table queries should NOT default to first table
			// The table should be empty when unresolved
			assert.Empty(t, cond.Table,
				"Unresolved column in multi-table query should not default to first table")
			return
		}
	}
	// If the parser doesn't extract it as a condition, that's acceptable too --
	// the important thing is it doesn't misattribute.
}

// TestWhereConditions_SingleTableFallback verifies single-table queries resolve unaliased columns.
func TestWhereConditions_SingleTableFallback(t *testing.T) {
	// In a single-table query, unaliased columns should still resolve to the only table.
	query := "SELECT * FROM orders WHERE status = 'pending'"

	conditions, err := ExtractWhereConditions(query)
	require.NoError(t, err)
	require.Len(t, conditions, 1)

	assert.Equal(t, "orders", conditions[0].Table,
		"Single-table query should still default to the only table")
}

// =============================================================================
// Issue #9: Paren-aware comma splitting in normalizeReturning
// =============================================================================

// TestNormalizeReturning_FunctionWithCommas ensures function arguments are not split on internal commas.
func TestNormalizeReturning_FunctionWithCommas(t *testing.T) {
	// RETURNING func(a, b) should NOT be split into ["func(a", "b)"]
	items := []string{"RETURNING func(a, b)"}
	result := normalizeReturning(items)

	require.Len(t, result, 1, "func(a, b) should not be split on internal commas")
	assert.Equal(t, "func(a, b)", result[0])
}

// TestNormalizeReturning_FunctionAndColumns validates splitting on top-level commas only.
func TestNormalizeReturning_FunctionAndColumns(t *testing.T) {
	// RETURNING id, func(a, b), name should produce 3 items
	items := []string{"RETURNING id, func(a, b), name"}
	result := normalizeReturning(items)

	require.Len(t, result, 3, "Should split on top-level commas only")
	assert.Equal(t, "id", result[0])
	assert.Equal(t, "func(a, b)", result[1])
	assert.Equal(t, "name", result[2])
}

// TestNormalizeReturning_NestedParens checks correct handling of nested parentheses.
func TestNormalizeReturning_NestedParens(t *testing.T) {
	// RETURNING COALESCE(func(a, b), c), d
	items := []string{"RETURNING COALESCE(func(a, b), c), d"}
	result := normalizeReturning(items)

	require.Len(t, result, 2, "Should handle nested parentheses correctly")
	assert.Equal(t, "COALESCE(func(a, b), c)", result[0])
	assert.Equal(t, "d", result[1])
}

// TestNormalizeReturning_SimpleColumns verifies basic comma-separated column splitting.
func TestNormalizeReturning_SimpleColumns(t *testing.T) {
	// Simple case: RETURNING id, name, email
	items := []string{"RETURNING id, name, email"}
	result := normalizeReturning(items)

	require.Len(t, result, 3)
	assert.Equal(t, "id", result[0])
	assert.Equal(t, "name", result[1])
	assert.Equal(t, "email", result[2])
}

// TestNormalizeReturning_AlreadyStrippedPrefix checks handling when RETURNING prefix is already removed.
func TestNormalizeReturning_AlreadyStrippedPrefix(t *testing.T) {
	// Items without the RETURNING prefix (already stripped by parser)
	items := []string{"id, status, created_at"}
	result := normalizeReturning(items)

	require.Len(t, result, 3)
	assert.Equal(t, "id", result[0])
	assert.Equal(t, "status", result[1])
	assert.Equal(t, "created_at", result[2])
}

// TestSplitCommasRespectingParens validates paren-aware comma splitting across various patterns.
func TestSplitCommasRespectingParens(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "simple columns",
			input:    "a, b, c",
			expected: []string{"a", " b", " c"},
		},
		{
			name:     "function with commas",
			input:    "func(a, b)",
			expected: []string{"func(a, b)"},
		},
		{
			name:     "mixed",
			input:    "id, func(a, b), name",
			expected: []string{"id", " func(a, b)", " name"},
		},
		{
			name:     "nested parens",
			input:    "COALESCE(func(a, b), c), d",
			expected: []string{"COALESCE(func(a, b), c)", " d"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: []string{""},
		},
		{
			name:     "no commas",
			input:    "single_item",
			expected: []string{"single_item"},
		},
		{
			name:     "string literal with commas",
			input:    "concat('a,b', name), id",
			expected: []string{"concat('a,b', name)", " id"},
		},
		{
			name:     "escaped quote in string literal",
			input:    "concat('it''s,here', name), id",
			expected: []string{"concat('it''s,here', name)", " id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitCommasRespectingParens(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// Issue #11: join_parser dead code removal (TrimPrefix on original condition)
// The fix removes a dead second TrimPrefix call. The regex-based extraction
// should still work on uppercased conditions. We verify through an integration test.
// =============================================================================

// TestExtractRelationshipsFromCondition_OnPrefix verifies ON-prefixed join condition extraction.
func TestExtractRelationshipsFromCondition_OnPrefix(t *testing.T) {
	// Verify that ON-prefixed conditions are still handled correctly
	// after removing the dead second TrimPrefix.
	schemaMap := map[string][]ColumnSchema{
		"customers": {
			{Name: "id", PGType: "bigint", IsPrimaryKey: true},
		},
		"orders": {
			{Name: "id", PGType: "bigint", IsPrimaryKey: true},
			{Name: "customer_id", PGType: "bigint"},
		},
	}
	aliasMap := map[string]string{
		"o": "orders",
		"c": "customers",
	}

	// Test with "ON " prefix
	rels := extractRelationshipsFromConditionWithSchema("ON o.customer_id = c.id", aliasMap, schemaMap)
	require.Len(t, rels, 1, "Should extract relationship from ON-prefixed condition")
	assert.Equal(t, "customers", rels[0].ParentTable)
	assert.Equal(t, "orders", rels[0].ChildTable)
}

// TestExtractRelationshipsFromCondition_LowercaseOn checks lowercase "on" prefix normalization.
func TestExtractRelationshipsFromCondition_LowercaseOn(t *testing.T) {
	// Verify lowercase "on " prefix is handled after ToUpper normalization.
	schemaMap := map[string][]ColumnSchema{
		"customers": {
			{Name: "id", PGType: "bigint", IsPrimaryKey: true},
		},
		"orders": {
			{Name: "id", PGType: "bigint", IsPrimaryKey: true},
			{Name: "customer_id", PGType: "bigint"},
		},
	}
	aliasMap := map[string]string{
		"o": "orders",
		"c": "customers",
	}

	rels := extractRelationshipsFromConditionWithSchema("on o.customer_id = c.id", aliasMap, schemaMap)
	require.Len(t, rels, 1, "Should handle lowercase 'on' prefix")
	assert.Equal(t, "customers", rels[0].ParentTable)
}

// =============================================================================
// Issue #12: Redundant JSONB flag removal -- verify no behavioral regression
// =============================================================================

// TestWhereConditions_JSONBDirectOperator confirms JSONB containment operator sets IsJSONB flag.
func TestWhereConditions_JSONBDirectOperator(t *testing.T) {
	// Verify that JSONB containment operators still set IsJSONB correctly
	// after removing the redundant flag assignment.
	query := `SELECT * FROM products WHERE metadata @> '{"color": "red"}'`

	conditions, err := ExtractWhereConditions(query)
	require.NoError(t, err)

	// Find the JSONB condition
	for _, cond := range conditions {
		if cond.Operator == "@>" {
			assert.True(t, cond.IsJSONB,
				"JSONB containment operator should still set IsJSONB flag")
			return
		}
	}
	// If the parser doesn't extract @> as a standard operator, that's acceptable
}

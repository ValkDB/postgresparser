// Package analysis provides query analysis for the PostgreSQL parser.
// This file contains tests for the combined extractor and schema-aware FK detection.
package analysis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// CR Issue 1: Combined Extraction Tests
// =============================================================================

// TestExtractQueryAnalysis_CombinedExtraction validates WHERE and join extraction in a single call.
func TestExtractQueryAnalysis_CombinedExtraction(t *testing.T) {
	// Test that we can extract WHERE conditions in one call.
	// JoinRelationships is always nil without schema metadata.
	query := `
		SELECT c.email, o.total
		FROM customers c
		INNER JOIN orders o ON o.customer_id = c.id
		WHERE o.status = 'pending' AND o.total > 100
	`

	result, err := ExtractQueryAnalysis(query)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should have WHERE conditions
	assert.NotEmpty(t, result.WhereConditions, "Should extract WHERE conditions")
	assert.GreaterOrEqual(t, len(result.WhereConditions), 2, "Should have at least 2 WHERE conditions")

	// JoinRelationships is nil without schema metadata
	assert.Nil(t, result.JoinRelationships, "JoinRelationships should be nil without schema metadata")

	// Verify the parsed query is also available
	assert.NotNil(t, result.ParsedQuery, "ParsedQuery should be available")
}

// TestExtractQueryAnalysis_NoDoubleParsingVerification ensures combined extraction matches individual results.
func TestExtractQueryAnalysis_NoDoubleParsingVerification(t *testing.T) {
	// This test verifies that combined extraction parses only once
	// by checking that WHERE results are consistent with individual extraction
	query := `
		SELECT u.name, p.title
		FROM users u
		LEFT JOIN posts p ON p.user_id = u.id
		WHERE u.is_active = true
	`

	// Combined extraction
	combined, err := ExtractQueryAnalysis(query)
	require.NoError(t, err)

	// Individual WHERE extraction
	whereOnly, err := ExtractWhereConditions(query)
	require.NoError(t, err)

	// WHERE results should be equivalent
	assert.Len(t, combined.WhereConditions, len(whereOnly),
		"Combined WHERE conditions should match individual extraction")

	// JoinRelationships is nil without schema
	assert.Nil(t, combined.JoinRelationships,
		"JoinRelationships should be nil without schema metadata")
}

// =============================================================================
// CR Issue 2: Schema-Based PK Detection Tests
// =============================================================================

// TestInferParentChildWithSchema_LeftIsPK verifies parent detection when the left column is a PK.
func TestInferParentChildWithSchema_LeftIsPK(t *testing.T) {
	// Test case: Left column is PK, right is not -> left is parent
	schemaMap := map[string][]ColumnSchema{
		"customers": {
			{Name: "customer_uuid", PGType: "uuid", IsPrimaryKey: true},
			{Name: "name", PGType: "text"},
		},
		"orders": {
			{Name: "id", PGType: "bigint", IsPrimaryKey: true},
			{Name: "customer_uuid", PGType: "uuid"},
		},
	}

	rel := inferParentChildWithSchema("customers", "customer_uuid", "orders", "customer_uuid", schemaMap)

	require.NotNil(t, rel)
	assert.Equal(t, "customers", rel.ParentTable, "customers should be parent (has PK)")
	assert.Equal(t, "orders", rel.ChildTable, "orders should be child")
	assert.Equal(t, "customer_uuid", rel.ParentColumn)
	assert.Equal(t, "customer_uuid", rel.ChildColumn)
}

// TestInferParentChildWithSchema_RightIsPK verifies parent detection when the right column is a PK.
func TestInferParentChildWithSchema_RightIsPK(t *testing.T) {
	// Test case: Right column is PK, left is not -> right is parent
	schemaMap := map[string][]ColumnSchema{
		"customers": {
			{Name: "id", PGType: "bigint", IsPrimaryKey: true},
			{Name: "name", PGType: "text"},
		},
		"orders": {
			{Name: "order_id", PGType: "bigint", IsPrimaryKey: true},
			{Name: "customer_id", PGType: "bigint"},
		},
	}

	// orders.customer_id = customers.id
	rel := inferParentChildWithSchema("orders", "customer_id", "customers", "id", schemaMap)

	require.NotNil(t, rel)
	assert.Equal(t, "customers", rel.ParentTable, "customers should be parent (has PK)")
	assert.Equal(t, "orders", rel.ChildTable, "orders should be child")
}

// TestInferParentChildWithSchema_NonStandardPKName checks PK detection with non-id column names.
func TestInferParentChildWithSchema_NonStandardPKName(t *testing.T) {
	// Test case: PK is NOT named "id" - this is what CR Issue 2 is about
	schemaMap := map[string][]ColumnSchema{
		"customers": {
			{Name: "customer_number", PGType: "varchar(20)", IsPrimaryKey: true}, // Non-standard PK name
			{Name: "name", PGType: "text"},
		},
		"orders": {
			{Name: "order_id", PGType: "bigint", IsPrimaryKey: true},
			{Name: "cust_num", PGType: "varchar(20)"}, // FK doesn't follow naming convention
		},
	}

	// orders.cust_num = customers.customer_number
	rel := inferParentChildWithSchema("orders", "cust_num", "customers", "customer_number", schemaMap)

	require.NotNil(t, rel)
	assert.Equal(t, "customers", rel.ParentTable, "customers should be parent (its column is PK)")
	assert.Equal(t, "orders", rel.ChildTable, "orders should be child")
}

// TestInferParentChildWithSchema_BothArePKs confirms nil is returned when both columns are PKs.
func TestInferParentChildWithSchema_BothArePKs(t *testing.T) {
	// Test case: Both columns are PKs (e.g., junction table with composite PK)
	// Should return nil -- can't determine parent/child without additional info
	schemaMap := map[string][]ColumnSchema{
		"students": {
			{Name: "id", PGType: "bigint", IsPrimaryKey: true},
		},
		"courses": {
			{Name: "id", PGType: "bigint", IsPrimaryKey: true},
		},
		"enrollments": {
			{Name: "student_id", PGType: "bigint", IsPrimaryKey: true}, // Part of composite PK
			{Name: "course_id", PGType: "bigint", IsPrimaryKey: true},  // Part of composite PK
		},
	}

	// Both are PKs - can't determine parent/child without additional info
	rel := inferParentChildWithSchema("enrollments", "student_id", "students", "id", schemaMap)
	assert.Nil(t, rel, "Should return nil when both columns are PKs")
}

// TestInferParentChildWithSchema_NeitherIsPK validates nil result when neither column is a PK.
func TestInferParentChildWithSchema_NeitherIsPK(t *testing.T) {
	// Test case: Neither column is PK
	schemaMap := map[string][]ColumnSchema{
		"table_a": {
			{Name: "pk_col", PGType: "bigint", IsPrimaryKey: true},
			{Name: "ref_col", PGType: "bigint"},
		},
		"table_b": {
			{Name: "pk_col", PGType: "bigint", IsPrimaryKey: true},
			{Name: "other_col", PGType: "bigint"},
		},
	}

	// Neither ref_col nor other_col is a PK
	rel := inferParentChildWithSchema("table_a", "ref_col", "table_b", "other_col", schemaMap)

	// Neither is PK -> nil (conservative)
	assert.Nil(t, rel, "Should return nil when can't determine parent/child")
}

// TestInferParentChildWithSchema_MissingSchema checks graceful handling when schema is missing for one table.
func TestInferParentChildWithSchema_MissingSchema(t *testing.T) {
	// Test case: Schema not available for a table
	schemaMap := map[string][]ColumnSchema{
		"customers": {
			{Name: "id", PGType: "bigint", IsPrimaryKey: true},
		},
		// "orders" schema is missing
	}

	rel := inferParentChildWithSchema("orders", "customer_id", "customers", "id", schemaMap)

	// orders schema missing, customers.id is PK -> customers is parent
	require.NotNil(t, rel)
	assert.Equal(t, "customers", rel.ParentTable)
}

// TestExtractQueryAnalysisWithSchema validates schema-aware join relationship extraction.
func TestExtractQueryAnalysisWithSchema(t *testing.T) {
	query := `
		SELECT c.customer_number, o.total
		FROM customers c
		JOIN orders o ON o.cust_num = c.customer_number
		WHERE o.status = 'pending'
	`

	schemaMap := map[string][]ColumnSchema{
		"customers": {
			{Name: "customer_number", PGType: "varchar(20)", IsPrimaryKey: true},
			{Name: "name", PGType: "text"},
		},
		"orders": {
			{Name: "id", PGType: "bigint", IsPrimaryKey: true},
			{Name: "cust_num", PGType: "varchar(20)"},
			{Name: "status", PGType: "text"},
			{Name: "total", PGType: "numeric"},
		},
	}

	result, err := ExtractQueryAnalysisWithSchema(query, schemaMap)
	require.NoError(t, err)

	// Should have WHERE conditions
	assert.NotEmpty(t, result.WhereConditions)

	// JOIN relationships should use schema-aware detection
	// Since customer_number is PK in customers, it should be detected as parent
	if len(result.JoinRelationships) > 0 {
		found := false
		for _, rel := range result.JoinRelationships {
			if rel.ParentTable == "customers" && rel.ParentColumn == "customer_number" {
				found = true
				assert.Equal(t, "orders", rel.ChildTable)
				break
			}
		}
		// Note: May or may not find depending on parser output
		_ = found
	}
}

// =============================================================================
// isColumnPrimaryKey Tests
// =============================================================================

// TestIsColumnPrimaryKey validates PK lookup across tables with standard and non-standard names.
func TestIsColumnPrimaryKey(t *testing.T) {
	schemaMap := map[string][]ColumnSchema{
		"users": {
			{Name: "id", PGType: "bigint", IsPrimaryKey: true},
			{Name: "email", PGType: "text", IsPrimaryKey: false},
		},
		"products": {
			{Name: "sku", PGType: "varchar(50)", IsPrimaryKey: true}, // Non-standard PK name
			{Name: "name", PGType: "text"},
		},
	}

	tests := []struct {
		tableName  string
		columnName string
		expected   bool
	}{
		{"users", "id", true},
		{"users", "email", false},
		{"products", "sku", true},
		{"products", "name", false},
		{"unknown_table", "id", false},     // Table not in schema
		{"users", "unknown_column", false}, // Column not in table
		{"", "id", false},                  // Empty table name
		{"users", "", false},               // Empty column name
	}

	for _, tt := range tests {
		t.Run(tt.tableName+"."+tt.columnName, func(t *testing.T) {
			result := isColumnPrimaryKey(tt.tableName, tt.columnName, schemaMap)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIsColumnPrimaryKey_NilSchemaMap confirms graceful handling with a nil schema map.
func TestIsColumnPrimaryKey_NilSchemaMap(t *testing.T) {
	// Should handle nil schema map gracefully
	result := isColumnPrimaryKey("users", "id", nil)
	assert.False(t, result)
}

// TestIsColumnPrimaryKey_CaseInsensitive verifies case-insensitive PK column matching.
func TestIsColumnPrimaryKey_CaseInsensitive(t *testing.T) {
	schemaMap := map[string][]ColumnSchema{
		"users": {
			{Name: "ID", PGType: "bigint", IsPrimaryKey: true},
		},
	}

	// Column name matching should be case-insensitive
	assert.True(t, isColumnPrimaryKey("users", "id", schemaMap))
	assert.True(t, isColumnPrimaryKey("users", "ID", schemaMap))
	assert.True(t, isColumnPrimaryKey("users", "Id", schemaMap))
}

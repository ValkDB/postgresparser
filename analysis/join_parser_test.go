// Package analysis provides query analysis for the PostgreSQL parser.
// This file contains tests for JOIN relationship extraction.
package analysis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeduplicateRelationships validates removal of duplicate join relationships.
func TestDeduplicateRelationships(t *testing.T) {
	// Test that duplicate relationships are removed
	rels := []JoinRelationship{
		{ChildTable: "orders", ChildColumn: "customer_id", ParentTable: "customers", ParentColumn: "id"},
		{ChildTable: "orders", ChildColumn: "customer_id", ParentTable: "customers", ParentColumn: "id"},
		{ChildTable: "items", ChildColumn: "order_id", ParentTable: "orders", ParentColumn: "id"},
	}

	result := deduplicateRelationships(rels)
	assert.Len(t, result, 2, "Should remove duplicate relationship")
}

// TestExtractJoinRelationshipsWithSchema validates exported extraction behavior.
func TestExtractJoinRelationshipsWithSchema(t *testing.T) {
	query := `
		SELECT o.id
		FROM orders o
		JOIN customers c ON o.customer_id = c.id
		JOIN customers c2 ON o.customer_id = c2.id
	`

	schemaMap := map[string][]ColumnSchema{
		"customers": {
			{Name: "id", PGType: "bigint", IsPrimaryKey: true},
		},
		"orders": {
			{Name: "id", PGType: "bigint", IsPrimaryKey: true},
			{Name: "customer_id", PGType: "bigint"},
		},
	}

	relationships, err := ExtractJoinRelationshipsWithSchema(query, schemaMap)
	require.NoError(t, err)
	require.Len(t, relationships, 1, "duplicate JOIN relationships should be deduplicated")

	assert.Equal(t, "orders", relationships[0].ChildTable)
	assert.Equal(t, "customer_id", relationships[0].ChildColumn)
	assert.Equal(t, "customers", relationships[0].ParentTable)
	assert.Equal(t, "id", relationships[0].ParentColumn)
}

// TestExtractJoinRelationshipsWithSchema_ParseError validates parse errors are returned.
func TestExtractJoinRelationshipsWithSchema_ParseError(t *testing.T) {
	relationships, err := ExtractJoinRelationshipsWithSchema("SELECT FROM", nil)
	require.Error(t, err)
	assert.Nil(t, relationships)
}

// Package analysis provides query analysis for the PostgreSQL parser.
// This file contains tests for JOIN relationship extraction.
package analysis

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

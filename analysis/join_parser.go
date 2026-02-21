// Package analysis provides query analysis for the PostgreSQL parser.
// This file implements JOIN relationship extraction for FK inference.
//
// Design decisions:
// - FK relationship extraction requires schema metadata (IsPrimaryKey field)
// - No heuristic guessing: if schema metadata is not provided, no FK relationships are returned
// - Accurate results only: parent/child is determined by PK detection, not naming conventions
package analysis

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/valkdb/postgresparser"
	"github.com/valkdb/postgresparser/internal/ident"
)

// equalityPattern matches equality conditions like "alias.column = alias.column" in JOIN ON clauses.
var equalityPattern = regexp.MustCompile(`(?i)([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\s*=\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)`)

// ExtractJoinRelationshipsWithSchema parses a query and extracts FK relationships using schema metadata.
// This is the only supported method for FK relationship extraction.
// Schema metadata with IsPrimaryKey populated is required for accurate parent/child determination.
//
// Parameters:
//   - query: The SQL query containing JOIN conditions
//   - schemaMap: Map of table name (lowercase) to column schemas with IsPrimaryKey populated
//
// Returns JoinRelationship structs with accurate parent/child based on PK detection.
// If schemaMap is nil or missing entries, relationships that cannot be determined are skipped.
func ExtractJoinRelationshipsWithSchema(query string, schemaMap map[string][]ColumnSchema) ([]JoinRelationship, error) {
	pq, err := postgresparser.ParseSQL(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	return extractJoinRelationshipsWithSchema(pq, schemaMap), nil
}

// buildAliasMap creates a map from table alias/name to actual table name.
// This mapping is intentionally limited to base tables for FK inference.
// For WHERE/table-resolution behavior across base+CTE+subquery, see buildWhereAliasMap.
func buildAliasMap(tables []postgresparser.TableRef) map[string]string {
	aliasMap := make(map[string]string)

	for _, table := range tables {
		// Skip non-base tables (CTEs, subqueries, etc.)
		if table.Type != postgresparser.TableTypeBase {
			continue
		}

		tableName := strings.ToLower(ident.TrimQuotes(strings.TrimSpace(table.Name)))
		if tableName == "" {
			continue
		}
		tableAlias := strings.ToLower(ident.TrimQuotes(strings.TrimSpace(table.Alias)))

		// Map alias to table name
		if tableAlias != "" {
			aliasMap[tableAlias] = tableName
		}
		// Also map table name to itself (for unaliased references)
		aliasMap[tableName] = tableName
	}

	return aliasMap
}

// extractRelationshipsFromConditionWithSchema parses a single JOIN condition string
// using schema metadata for parent/child inference.
// Example: "o.customer_id = c.id" -> JoinRelationship{Child: orders, Parent: customers}
func extractRelationshipsFromConditionWithSchema(condition string, aliasMap map[string]string, schemaMap map[string][]ColumnSchema) []JoinRelationship {
	var relationships []JoinRelationship

	// Normalize the condition
	condition = strings.TrimSpace(condition)

	// Strip "ON " prefix case-insensitively without uppercasing the whole string,
	// which would destroy the original casing needed for column/table name matching.
	if len(condition) >= 3 && strings.EqualFold(condition[:3], "ON ") {
		condition = condition[3:]
	}

	// USING clauses cannot determine parent/child without schema-based FK metadata,
	// so we skip them entirely.
	if strings.Contains(strings.ToUpper(condition), "USING") {
		return nil
	}

	// Parse equality conditions: "left = right"
	matches := equalityPattern.FindAllStringSubmatch(condition, -1)
	for _, match := range matches {
		if len(match) < 3 {
			continue
		}

		leftRef := parseColumnReference(match[1])
		rightRef := parseColumnReference(match[2])

		// Resolve aliases to table names
		leftTable := resolveAlias(leftRef.tableAlias, aliasMap)
		rightTable := resolveAlias(rightRef.tableAlias, aliasMap)

		// Skip if we can't resolve both tables
		if leftTable == "" || rightTable == "" {
			continue
		}

		// Determine parent/child relationship using schema metadata
		rel := inferParentChildWithSchema(leftTable, leftRef.column, rightTable, rightRef.column, schemaMap)
		if rel != nil {
			relationships = append(relationships, *rel)
		}
	}

	return relationships
}

// columnReference holds parsed alias and column name
type columnReference struct {
	tableAlias string
	column     string
}

// parseColumnReference parses "alias.column" or "column" into parts
func parseColumnReference(ref string) columnReference {
	ref = strings.TrimSpace(ref)
	parts := strings.Split(ref, ".")

	if len(parts) >= 2 {
		return columnReference{
			tableAlias: strings.ToLower(parts[len(parts)-2]),
			column:     strings.ToLower(parts[len(parts)-1]),
		}
	}

	return columnReference{
		tableAlias: "",
		column:     strings.ToLower(ref),
	}
}

// resolveAlias converts alias to actual table name
func resolveAlias(alias string, aliasMap map[string]string) string {
	if alias == "" {
		return ""
	}
	if tableName, ok := aliasMap[strings.ToLower(alias)]; ok {
		return tableName
	}
	// If no mapping found, assume alias is the table name
	return alias
}

// inferParentChildWithSchema determines parent/child using actual schema metadata.
// Uses IsPrimaryKey field from schema -- no heuristic fallbacks.
//
// Logic:
//  1. If one side has IsPrimaryKey=true and the other doesn't, the PK side is parent
//  2. If both or neither have IsPrimaryKey, return nil (can't determine without more info)
func inferParentChildWithSchema(leftTable, leftCol, rightTable, rightCol string, schemaMap map[string][]ColumnSchema) *JoinRelationship {
	leftIsPK := isColumnPrimaryKey(leftTable, leftCol, schemaMap)
	rightIsPK := isColumnPrimaryKey(rightTable, rightCol, schemaMap)

	if leftIsPK && !rightIsPK {
		return &JoinRelationship{
			ChildTable:   rightTable,
			ChildColumn:  rightCol,
			ParentTable:  leftTable,
			ParentColumn: leftCol,
		}
	}
	if rightIsPK && !leftIsPK {
		return &JoinRelationship{
			ChildTable:   leftTable,
			ChildColumn:  leftCol,
			ParentTable:  rightTable,
			ParentColumn: rightCol,
		}
	}

	// Both are PKs, neither is PK, or schema missing -- can't determine
	return nil
}

// isColumnPrimaryKey checks if a column is marked as a primary key in the schema.
// Uses actual schema metadata instead of name-based heuristics.
func isColumnPrimaryKey(tableName, columnName string, schemaMap map[string][]ColumnSchema) bool {
	if schemaMap == nil {
		return false
	}

	columns, exists := schemaMap[strings.ToLower(tableName)]
	if !exists {
		return false
	}

	for _, col := range columns {
		if strings.EqualFold(col.Name, columnName) {
			return col.IsPrimaryKey
		}
	}

	return false
}

// extractRelationshipsFromColumnUsageWithSchema extracts relationships from parsed ColumnUsage
// using schema metadata for parent/child inference.
func extractRelationshipsFromColumnUsageWithSchema(usages []postgresparser.ColumnUsage, aliasMap map[string]string, schemaMap map[string][]ColumnSchema) []JoinRelationship {
	var relationships []JoinRelationship

	// Collect join usages and pair them
	var joinUsages []postgresparser.ColumnUsage
	for _, usage := range usages {
		if usage.UsageType == postgresparser.ColumnUsageTypeJoin {
			joinUsages = append(joinUsages, usage)
		}
	}

	// Process pairs of join usages that share the same Context
	contextPairs := make(map[string][]postgresparser.ColumnUsage)
	for _, usage := range joinUsages {
		contextPairs[usage.Context] = append(contextPairs[usage.Context], usage)
	}

	for _, usages := range contextPairs {
		// For join condition, we expect pairs of columns
		if len(usages) < 2 {
			continue
		}

		// Take first two columns as the join pair
		left := usages[0]
		right := usages[1]

		leftTable := resolveAlias(left.TableAlias, aliasMap)
		rightTable := resolveAlias(right.TableAlias, aliasMap)

		if leftTable == "" || rightTable == "" {
			continue
		}

		leftCol := strings.ToLower(left.Column)
		rightCol := strings.ToLower(right.Column)

		rel := inferParentChildWithSchema(leftTable, leftCol, rightTable, rightCol, schemaMap)
		if rel != nil {
			relationships = append(relationships, *rel)
		}
	}

	return relationships
}

// deduplicateRelationships removes duplicate relationships.
func deduplicateRelationships(rels []JoinRelationship) []JoinRelationship {
	seen := make(map[string]struct{}, len(rels))
	result := make([]JoinRelationship, 0, len(rels))

	for _, rel := range rels {
		// Create a unique key
		key := strings.ToLower(rel.ChildTable) + "." + strings.ToLower(rel.ChildColumn) +
			"->" + strings.ToLower(rel.ParentTable) + "." + strings.ToLower(rel.ParentColumn)

		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			result = append(result, rel)
		}
	}

	return result
}

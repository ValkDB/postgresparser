// Package analysis provides query analysis for the PostgreSQL parser.
// This file implements a combined extractor that extracts both WHERE conditions and
// JOIN relationships in a single parse pass, avoiding double parsing.
package analysis

import (
	"fmt"
	"strings"

	"github.com/valkdb/postgresparser"
	"github.com/valkdb/postgresparser/internal/ident"
)

// QueryAnalysisResult holds the combined results of query analysis.
// This struct allows a single parse to extract multiple
// pieces of information, avoiding wasteful double parsing.
type QueryAnalysisResult struct {
	// WhereConditions extracted from the WHERE clause
	WhereConditions []WhereCondition

	// JoinRelationships inferred from JOIN ON conditions
	JoinRelationships []JoinRelationship

	// ParsedQuery is the underlying parsed query for advanced use cases
	ParsedQuery *postgresparser.ParsedQuery
}

// ExtractQueryAnalysis parses a query once and extracts WHERE conditions.
// JoinRelationships will always be empty (nil) because FK relationship detection
// requires schema metadata. Use ExtractQueryAnalysisWithSchema for FK relationship detection.
//
// Example usage:
//
//	result, err := ExtractQueryAnalysis(query)
//	if err != nil {
//	    return err
//	}
//	// Use result.WhereConditions for constraint generation
//	// result.JoinRelationships is always nil -- use ExtractQueryAnalysisWithSchema instead
func ExtractQueryAnalysis(query string) (*QueryAnalysisResult, error) {
	// Parse the query ONCE
	pq, err := postgresparser.ParseSQL(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	result := &QueryAnalysisResult{
		ParsedQuery: pq,
	}

	// Extract WHERE conditions from the parsed query
	result.WhereConditions = extractWhereConditionsFromParsed(pq, nil)

	// JoinRelationships is nil: FK detection requires schema metadata.
	// Use ExtractQueryAnalysisWithSchema for FK relationship extraction.

	return result, nil
}

// resolveColumnTableFromSchema resolves an unqualified column to its owning table
// by looking it up across the query's base tables in schemaMap. Returns the table
// name when exactly one base table contains the column, "" otherwise.
func resolveColumnTableFromSchema(column string, pq *postgresparser.ParsedQuery, schemaMap map[string][]ColumnSchema) string {
	// A nil schemaMap opts out of resolution (the ExtractWhereConditions path).
	// An empty but non-nil map still resolves CTE/derived relations, which do
	// not need base-table schema metadata.
	if schemaMap == nil {
		return ""
	}
	col := strings.ToLower(ident.TrimQuotes(strings.TrimSpace(column)))
	if col == "" {
		return ""
	}

	match := ""
	matched := false
	for _, table := range pq.Tables {
		// Only the query's own FROM relations are candidates, not tables
		// surfaced from inside a CTE or subquery body.
		if table.Nested {
			continue
		}
		name, contains := relationHasColumn(table, col, pq, schemaMap)
		if name == "" || !contains {
			continue
		}
		if matched && name != match {
			// The column exists in more than one distinct direct relation.
			// Repeats of the same relation (self-joins) resolve to the shared name.
			return ""
		}
		match = name
		matched = true
	}
	return match
}

// relationHasColumn reports whether a direct FROM relation exposes col, and
// returns the relation's resolved name. Base tables are checked against
// schemaMap; CTEs and derived tables against their own projection. A relation
// projecting "*" cannot be enumerated and is treated as a possible owner.
func relationHasColumn(table postgresparser.TableRef, col string, pq *postgresparser.ParsedQuery, schemaMap map[string][]ColumnSchema) (string, bool) {
	switch table.Type {
	case postgresparser.TableTypeBase:
		name := strings.ToLower(ident.TrimQuotes(strings.TrimSpace(table.Name)))
		if name == "" {
			return "", false
		}
		for _, cs := range schemaMap[name] {
			if strings.ToLower(cs.Name) == col {
				return name, true
			}
		}
		return name, false
	case postgresparser.TableTypeCTE:
		name := strings.ToLower(ident.TrimQuotes(strings.TrimSpace(table.Name)))
		cols, star := cteProjection(pq, name)
		return name, star || cols[col]
	case postgresparser.TableTypeSubquery:
		alias := strings.ToLower(ident.TrimQuotes(strings.TrimSpace(table.Name)))
		cols, star := subqueryProjection(pq, alias)
		return alias, star || cols[col]
	default:
		return "", false
	}
}

// cteProjection returns the lowercased output column names of the named CTE and
// whether they cannot be fully enumerated (a "*" projection).
func cteProjection(pq *postgresparser.ParsedQuery, name string) (map[string]bool, bool) {
	for _, cte := range pq.CTEs {
		if strings.ToLower(ident.TrimQuotes(strings.TrimSpace(cte.Name))) != name {
			continue
		}
		var projection []postgresparser.SelectColumn
		if cte.ParsedQuery != nil {
			projection = projectionOrReturning(cte.ParsedQuery)
		}
		return exposedColumns(projection, cte.ColumnAliases)
	}
	return nil, false
}

// subqueryProjection returns the lowercased output column names of the FROM
// subquery with the given alias and whether they cannot be fully enumerated.
func subqueryProjection(pq *postgresparser.ParsedQuery, alias string) (map[string]bool, bool) {
	for _, sub := range pq.Subqueries {
		if sub.SourceClause != "FROM" {
			continue
		}
		if strings.ToLower(ident.TrimQuotes(strings.TrimSpace(sub.Alias))) != alias {
			continue
		}
		var projection []postgresparser.SelectColumn
		if sub.Query != nil {
			projection = sub.Query.Columns
		}
		return exposedColumns(projection, sub.ColumnAliases)
	}
	return nil, false
}

// exposedColumns returns the lowercased output column names a relation exposes,
// and whether they cannot be fully enumerated. A column alias list renames the
// leading projected columns positionally; columns beyond the list keep their
// own names. A "*" projection expands to an unknown set, so the relation is
// opaque (treated as a possible owner of any column).
func exposedColumns(projection []postgresparser.SelectColumn, aliasList []string) (map[string]bool, bool) {
	if len(projection) == 0 {
		// No enumerable projection (e.g. unparsed body); fall back to any
		// declared alias list.
		return lowerSet(aliasList), false
	}

	names := make(map[string]bool, len(projection))
	for i, c := range projection {
		expr := strings.TrimSpace(c.Expression)
		if expr == "*" || strings.HasSuffix(expr, ".*") {
			return names, true
		}
		name := ""
		switch {
		case i < len(aliasList):
			name = aliasList[i]
		case c.Alias != "":
			name = c.Alias
		default:
			name = simpleColumnName(expr)
		}
		if name = strings.ToLower(ident.TrimQuotes(strings.TrimSpace(name))); name != "" {
			names[name] = true
		}
	}
	return names, false
}

// lowerSet builds a lowercased, unquoted lookup set from identifiers.
func lowerSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, v := range values {
		if key := strings.ToLower(ident.TrimQuotes(strings.TrimSpace(v))); key != "" {
			out[key] = true
		}
	}
	return out
}

// projectionOrReturning returns the columns a relation exposes: the SELECT
// projection, or the RETURNING list for a data-modifying CTE body.
func projectionOrReturning(pq *postgresparser.ParsedQuery) []postgresparser.SelectColumn {
	if len(pq.Columns) > 0 || len(pq.Returning) == 0 {
		return pq.Columns
	}
	items := normalizeReturning(pq.Returning)
	cols := make([]postgresparser.SelectColumn, 0, len(items))
	for _, item := range items {
		expr, alias := splitAsAlias(item)
		cols = append(cols, postgresparser.SelectColumn{Expression: expr, Alias: alias})
	}
	return cols
}

// splitAsAlias splits a projection item into expression and output alias,
// handling both "expr AS alias" and the implicit "expr alias" form. The
// implicit form is only honored when the item is exactly two bare identifiers,
// to avoid misreading expressions like "total + 1".
func splitAsAlias(item string) (expr, alias string) {
	item = strings.TrimSpace(item)
	if idx := strings.LastIndex(strings.ToLower(item), " as "); idx >= 0 {
		return strings.TrimSpace(item[:idx]), strings.TrimSpace(item[idx+4:])
	}
	if fields := strings.Fields(item); len(fields) == 2 && simpleColumnName(fields[0]) != "" && simpleColumnName(fields[1]) != "" {
		return fields[0], fields[1]
	}
	return item, ""
}

// simpleColumnName returns the lowercased final identifier of a simple column
// reference (e.g. "o.amount" -> "amount", "total" -> "total"). It returns ""
// for computed expressions (functions, arithmetic, casts) whose output column
// name cannot be derived from the text.
func simpleColumnName(expr string) string {
	// A trailing ::type cast keeps the underlying column's name (total::numeric
	// is exposed as total), so drop the cast before inspecting the reference.
	if i := strings.Index(expr, "::"); i >= 0 {
		expr = strings.TrimSpace(expr[:i])
	}
	if expr == "" || strings.ContainsAny(expr, "()[] +-*/:") {
		return ""
	}
	parts := strings.Split(expr, ".")
	return strings.ToLower(ident.TrimQuotes(strings.TrimSpace(parts[len(parts)-1])))
}

// extractWhereConditionsFromParsed extracts WHERE conditions from an already-parsed query.
// This is the internal implementation shared by both ExtractWhereConditions and ExtractQueryAnalysis.
// A non-nil schemaMap resolves unqualified columns in multi-table queries to their owning table.
func extractWhereConditionsFromParsed(pq *postgresparser.ParsedQuery, schemaMap map[string][]ColumnSchema) []WhereCondition {
	var conditions []WhereCondition
	// WHERE extraction uses its own alias map that includes base tables, CTEs,
	// and subqueries so Table resolution stays consistent across relation types.
	aliasMap := buildWhereAliasMap(pq.Tables)

	// Extract conditions from ColumnUsage with filter type
	for _, usage := range pq.ColumnUsage {
		if usage.UsageType != postgresparser.ColumnUsageTypeFilter {
			continue
		}

		// Skip if no operator (shouldn't happen in WHERE clauses, but safety check)
		if usage.Operator == "" {
			continue
		}

		// Skip JSONB extraction operators (->>, ->, #>>, #>) if they are the main operator
		// These are usually part of larger comparison expressions and are handled via Context analysis
		if jsonbOperators[usage.Operator] {
			continue
		}

		// Resolve table name from alias, or use first table only for single-table queries.
		tableName := resolveAlias(usage.TableAlias, aliasMap)
		if tableName == "" {
			if len(pq.Tables) == 1 {
				// No alias and no resolution - default to first table only for single-table queries
				tableName = pq.Tables[0].Name
			} else {
				tableName = resolveColumnTableFromSchema(usage.Column, pq, schemaMap)
			}
		}

		condition := WhereCondition{
			Table:    tableName,
			Column:   usage.Column,
			Operator: normalizeOperator(usage.Operator),
		}

		// Check for JSONB-specific operators (@>, <@, ?, ?|, ?&)
		// These operate directly on JSONB columns without extraction
		switch condition.Operator {
		case "@>", "<@", "?", "?|", "?&":
			condition.IsJSONB = true
		}

		// Check if this is a JSONB comparison (context contains JSONB pattern)
		if jsonbInfo := extractJSONBInfo(usage.Context); jsonbInfo != nil {
			condition.IsJSONB = true
			condition.Column = jsonbInfo.column // The JSONB column name
			condition.JSONBKey = jsonbInfo.key  // The key being extracted
			if jsonbInfo.castType != "" {
				condition.JSONBCast = jsonbInfo.castType
			}
		}

		// Extract value from context (full comparison expression)
		// Context contains the full comparison like "status = 'pending'"
		value, isParam := extractValueFromContext(usage.Context, usage.Column, usage.Operator)
		condition.Value = value
		condition.IsParameter = isParam

		conditions = append(conditions, condition)
	}

	return conditions
}

// ExtractQueryAnalysisWithSchema parses a query and extracts analysis results,
// using schema metadata for FK relationship inference and for resolving
// unqualified WHERE columns to their owning table.
//
// Schema-aware extraction uses the IsPrimaryKey field from
// schema metadata instead of heuristic name-based detection.
//
// The schemaMap should be keyed by lowercase table name, with each value
// containing the column schemas for that table.
func ExtractQueryAnalysisWithSchema(query string, schemaMap map[string][]ColumnSchema) (*QueryAnalysisResult, error) {
	// Parse the query ONCE
	pq, err := postgresparser.ParseSQL(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	result := &QueryAnalysisResult{
		ParsedQuery: pq,
	}

	// Extract WHERE conditions, resolving unqualified columns via schema metadata
	result.WhereConditions = extractWhereConditionsFromParsed(pq, schemaMap)

	// Extract JOIN relationships with schema awareness
	result.JoinRelationships = extractJoinRelationshipsWithSchema(pq, schemaMap)

	return result, nil
}

// extractJoinRelationshipsWithSchema extracts JOIN relationships using schema metadata
// for accurate parent/child inference based on IsPrimaryKey field.
func extractJoinRelationshipsWithSchema(pq *postgresparser.ParsedQuery, schemaMap map[string][]ColumnSchema) []JoinRelationship {
	var relationships []JoinRelationship

	// Build alias -> table name map
	aliasMap := buildAliasMap(pq.Tables)

	// Extract relationships from JoinConditions with schema awareness
	for _, joinCond := range pq.JoinConditions {
		rels := extractRelationshipsFromConditionWithSchema(joinCond, aliasMap, schemaMap)
		relationships = append(relationships, rels...)
	}

	// Also extract from ColumnUsage with join type
	rels := extractRelationshipsFromColumnUsageWithSchema(pq.ColumnUsage, aliasMap, schemaMap)
	relationships = append(relationships, rels...)

	// Deduplicate relationships
	relationships = deduplicateRelationships(relationships)

	return relationships
}

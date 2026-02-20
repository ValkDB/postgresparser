// Package analysis provides query analysis for the PostgreSQL parser.
// This file implements a combined extractor that extracts both WHERE conditions and
// JOIN relationships in a single parse pass, avoiding double parsing.
package analysis

import (
	"fmt"

	"github.com/valkdb/postgresparser"
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
	result.WhereConditions = extractWhereConditionsFromParsed(pq)

	// JoinRelationships is nil: FK detection requires schema metadata.
	// Use ExtractQueryAnalysisWithSchema for FK relationship extraction.

	return result, nil
}

// extractWhereConditionsFromParsed extracts WHERE conditions from an already-parsed query.
// This is the internal implementation shared by both ExtractWhereConditions and ExtractQueryAnalysis.
func extractWhereConditionsFromParsed(pq *postgresparser.ParsedQuery) []WhereCondition {
	var conditions []WhereCondition
	aliasMap := buildAliasMap(pq.Tables)

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
		tableName := resolveTableName(usage.TableAlias, aliasMap)
		if tableName == "" && len(pq.Tables) == 1 {
			// No alias and no resolution - default to first table only for single-table queries
			tableName = pq.Tables[0].Name
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
// using schema metadata to improve FK relationship inference accuracy.
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

	// Extract WHERE conditions (schema-independent)
	result.WhereConditions = extractWhereConditionsFromParsed(pq)

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

// Package analysis provides query analysis for the PostgreSQL parser.
// This file implements WHERE condition extraction from SQL queries.
package analysis

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/valkdb/postgresparser"
)

var parameterRegex = regexp.MustCompile(`^\$\d+$|^\?$`)

// jsonbExtractTextPattern matches JSONB extraction patterns like: column->>'key' = 'value'
var jsonbExtractTextPattern = regexp.MustCompile(`(?i)(?:(\w+)\.)?(\w+)\s*(?:->>|->|#>>|#>)\s*'([^']+)'`)

// jsonbCastedPattern matches casted JSONB expressions like (metadata->>'score')::int
var jsonbCastedPattern = regexp.MustCompile(`(?i)\(\s*(?:(\w+)\.)?(\w+)\s*(?:->>|->|#>>|#>)\s*'([^']+)'\s*\)::(\w+)`)

// betweenAndRegex splits BETWEEN range values on the AND keyword.
var betweenAndRegex = regexp.MustCompile(`(?i)\s+AND\s+`)

// inKeywordRegex matches the SQL IN keyword using word boundaries,
// avoiding false matches inside identifiers like "invoice_id".
var inKeywordRegex = regexp.MustCompile(`(?i)\bIN\b`)

// betweenKeywordRegex matches the SQL BETWEEN keyword using word boundaries,
// avoiding false matches inside identifiers like "in_between_value".
var betweenKeywordRegex = regexp.MustCompile(`(?i)\bBETWEEN\b`)

// jsonbOperators is the set of JSONB operators that should be skipped when they appear as
// top-level operators in ColumnUsage, because they are usually part of a larger comparison.
var jsonbOperators = map[string]bool{
	"->>": true, // Extract as text
	"->":  true, // Extract as JSONB
	"#>>": true, // Path as text
	"#>":  true, // Path as JSONB
}

// ExtractWhereConditions parses a query and extracts WHERE clause conditions.
// Returns a list of conditions with table, column, operator, and value information.
// Supports all standard SQL operators: =, !=, <>, >, <, >=, <=, BETWEEN, IN, LIKE, IS NULL, etc.
// Also supports JSONB operators (@>, ?, ?|, ?&) and extraction patterns.
func ExtractWhereConditions(query string) ([]WhereCondition, error) {
	pq, err := postgresparser.ParseSQL(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}
	return extractWhereConditionsFromParsed(pq), nil
}

// jsonbInfo holds extracted JSONB operator information.
type jsonbInfo struct {
	column   string // The JSONB column name (e.g., "order_details")
	key      string // The key being extracted (e.g., "shipping_method")
	castType string // Optional: the type it's being cast to (e.g., "int")
}

// extractJSONBInfo checks if the context contains a JSONB pattern and extracts the info.
// Detects patterns like "order_details->>'shipping_method' = 'express'"
// and casted versions like "(metadata->>'score')::int >= 90"
func extractJSONBInfo(context string) *jsonbInfo {
	// Try casted pattern first (more specific)
	if matches := jsonbCastedPattern.FindStringSubmatch(context); len(matches) == 5 {
		return &jsonbInfo{
			column:   matches[2], // Column name (group 1 is alias)
			key:      matches[3], // Extracted key
			castType: matches[4], // Cast type
		}
	}

	// Try standard extraction pattern
	if matches := jsonbExtractTextPattern.FindStringSubmatch(context); len(matches) == 4 {
		return &jsonbInfo{
			column: matches[2], // Column name (group 1 is alias)
			key:    matches[3], // Extracted key
		}
	}

	return nil
}

// resolveTableName converts a table alias to the actual table name.
// If no alias is found, returns the alias itself (it might be the actual table name).
func resolveTableName(alias string, aliasMap map[string]string) string {
	if alias == "" {
		return ""
	}
	return resolveAlias(alias, aliasMap)
}

// normalizeOperator standardizes operator representation.
func normalizeOperator(op string) string {
	op = strings.ToUpper(strings.TrimSpace(op))

	// Normalize variants
	switch op {
	case "<>":
		return "!="
	case "~~":
		return "LIKE"
	case "!~~":
		return "NOT LIKE"
	case "~~*":
		return "ILIKE"
	case "!~~*":
		return "NOT ILIKE"
	}

	return op
}

// extractValueFromContext extracts the literal value from the full context string.
// Context contains the full comparison like "status = 'pending'" or "total > 100".
// Returns the value and whether it's a parameter placeholder.
func extractValueFromContext(context, column, operator string) (any, bool) {
	if context == "" {
		return nil, false
	}

	// Handle NULL checks - no value to extract
	if strings.Contains(strings.ToUpper(operator), "NULL") {
		return nil, false
	}

	// Find the operator position in the context
	// Try to split on the operator to get the value side
	opUpper := strings.ToUpper(operator)
	contextUpper := strings.ToUpper(context)

	var valuePart string

	// Special handling for multi-word operators
	switch opUpper {
	case "BETWEEN":
		// For BETWEEN, the value is after the BETWEEN keyword
		// E.g., "total BETWEEN 10 AND 100"
		// Use word-boundary regex to avoid false matches inside identifiers
		loc := betweenKeywordRegex.FindStringIndex(context)
		if loc != nil {
			valuePart = strings.TrimSpace(context[loc[1]:])
		}
	case "IN":
		// Use word-boundary-aware split to avoid matching "IN" inside identifiers
		// E.g., "invoice_id IN (1,2)" must not split on "IN" inside "invoice_id"
		loc := inKeywordRegex.FindStringIndex(context)
		if loc != nil {
			valuePart = strings.TrimSpace(context[loc[1]:])
		}
	case "IS", "IS NOT":
		// IS NULL, IS NOT NULL - already handled above
		return nil, false
	default:
		// For binary operators (=, >, <, >=, <=, !=, LIKE, etc.)
		// Find the operator in the original context and take everything after it
		opIdx := strings.Index(contextUpper, opUpper)
		if opIdx == -1 {
			// Try the original operator (might have different case/format)
			opIdx = strings.Index(context, operator)
		}
		if opIdx >= 0 {
			valuePart = strings.TrimSpace(context[opIdx+len(operator):])
		}
	}

	if valuePart == "" {
		return nil, false
	}

	// Check for parameter placeholders ($1, $2, ?)
	if parameterRegex.MatchString(valuePart) {
		return valuePart, true
	}

	// Handle IN operator - extract array
	if opUpper == "IN" {
		values := extractInValues(valuePart)
		if len(values) > 0 {
			return values, false
		}
	}

	// Handle BETWEEN operator - extract range
	if opUpper == "BETWEEN" {
		values := extractBetweenValues(valuePart)
		if len(values) == 2 {
			return values, false
		}
	}

	// For LIKE, extract pattern
	if strings.Contains(opUpper, "LIKE") {
		pattern := strings.Trim(valuePart, "'\"")
		return pattern, false
	}

	// For comparison operators, try to extract literal value
	// Remove quotes for string literals
	if strings.HasPrefix(valuePart, "'") && strings.HasSuffix(valuePart, "'") {
		return strings.Trim(valuePart, "'"), false
	}
	if strings.HasPrefix(valuePart, "\"") && strings.HasSuffix(valuePart, "\"") {
		return strings.Trim(valuePart, "\""), false
	}

	// Return as-is for numeric or complex expressions
	return valuePart, false
}

// extractInValues parses IN clause values.
// Example: "(1, 2, 3)" -> ["1", "2", "3"]
func extractInValues(expr string) []string {
	expr = strings.TrimSpace(expr)

	// Remove parentheses
	expr = strings.TrimPrefix(expr, "(")
	expr = strings.TrimSuffix(expr, ")")

	// Split by comma
	parts := strings.Split(expr, ",")
	values := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Remove quotes
		part = strings.Trim(part, "'\"")
		values = append(values, part)
	}

	return values
}

// extractBetweenValues parses BETWEEN clause range.
// Example: "10 AND 100" -> ["10", "100"]
func extractBetweenValues(expr string) []string {
	// Look for AND separator
	parts := betweenAndRegex.Split(expr, 2)

	if len(parts) != 2 {
		return nil
	}

	lower := strings.TrimSpace(strings.Trim(parts[0], "'\""))
	upper := strings.TrimSpace(strings.Trim(parts[1], "'\""))

	return []string{lower, upper}
}

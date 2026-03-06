// helpers_comparison.go contains enhanced helpers for extracting comparison operators from expressions.
// This file implements a comprehensive ANTLR listener that visits all comparison node types
// to properly extract operators from compound conditions.
package postgresparser

import (
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/valkdb/postgresparser/gen"
)

// comparisonInfo holds information about a single comparison expression.
type comparisonInfo struct {
	column     string
	tableAlias string
	expression string
	operator   string
	context    string
}

// comprehensiveComparisonCollector implements a full listener for all A_expr node types
// to properly extract operators from deeply nested parse trees.
type comprehensiveComparisonCollector struct {
	*gen.BasePostgreSQLParserListener
	comparisons   []comparisonInfo
	tokens        antlr.TokenStream
	subqueryDepth int
}

// EnterSelect_with_parens increments nested subquery depth so outer comparison
// extraction ignores inner SELECT bodies.
func (c *comprehensiveComparisonCollector) EnterSelect_with_parens(ctx *gen.Select_with_parensContext) {
	c.subqueryDepth++
}

// ExitSelect_with_parens decrements nested subquery depth after leaving an
// inner SELECT body.
func (c *comprehensiveComparisonCollector) ExitSelect_with_parens(ctx *gen.Select_with_parensContext) {
	if c.subqueryDepth > 0 {
		c.subqueryDepth--
	}
}

// EnterA_expr_compare handles comparison operators like >, <, =, etc.
func (c *comprehensiveComparisonCollector) EnterA_expr_compare(ctx *gen.A_expr_compareContext) {
	if ctx == nil {
		return
	}
	if c.subqueryDepth > 0 {
		return
	}

	exprText := ctxText(c.tokens, ctx)

	// Find the operator in this comparison
	operator := ""
	childCount := ctx.GetChildCount()
	for i := 0; i < childCount; i++ {
		if child, ok := ctx.GetChild(i).(antlr.TerminalNode); ok {
			text := strings.ToUpper(child.GetText())
			switch text {
			case "=", "<>", "!=", ">", ">=", "<", "<=":
				operator = text
			}
		}
	}

	if operator != "" {
		// Extract columns from this comparison
		columns := extractColumnsFromContext(ctx)
		for _, col := range columns {
			c.comparisons = append(c.comparisons, comparisonInfo{
				column:     col.Name,
				tableAlias: col.TableAlias,
				expression: col.Expr,
				operator:   operator,
				context:    exprText,
			})
		}
	}
}

// EnterA_expr_like handles LIKE and ILIKE operators
func (c *comprehensiveComparisonCollector) EnterA_expr_like(ctx *gen.A_expr_likeContext) {
	if ctx == nil {
		return
	}
	if c.subqueryDepth > 0 {
		return
	}

	exprText := ctxText(c.tokens, ctx)
	operator := ""

	// Check for LIKE, ILIKE, NOT LIKE, NOT ILIKE
	childCount := ctx.GetChildCount()
	hasNot := false
	for i := 0; i < childCount; i++ {
		if child, ok := ctx.GetChild(i).(antlr.TerminalNode); ok {
			text := strings.ToUpper(child.GetText())
			if text == "NOT" {
				hasNot = true
			} else if text == "LIKE" || text == "ILIKE" {
				if hasNot {
					operator = "NOT " + text
				} else {
					operator = text
				}
				break
			}
		}
	}

	if operator != "" {
		columns := extractColumnsFromContext(ctx)
		for _, col := range columns {
			c.comparisons = append(c.comparisons, comparisonInfo{
				column:     col.Name,
				tableAlias: col.TableAlias,
				expression: col.Expr,
				operator:   operator,
				context:    exprText,
			})
		}
	}
}

// EnterA_expr_in handles IN and NOT IN operators
func (c *comprehensiveComparisonCollector) EnterA_expr_in(ctx *gen.A_expr_inContext) {
	if ctx == nil {
		return
	}
	if c.subqueryDepth > 0 {
		return
	}

	exprText := ctxText(c.tokens, ctx)
	operator := ""

	// Check for IN or NOT IN
	childCount := ctx.GetChildCount()
	hasNot := false
	for i := 0; i < childCount; i++ {
		if child, ok := ctx.GetChild(i).(antlr.TerminalNode); ok {
			text := strings.ToUpper(child.GetText())
			if text == "NOT" {
				hasNot = true
			} else if text == "IN" {
				if hasNot {
					operator = "NOT IN"
				} else {
					operator = "IN"
				}
				break
			}
		}
	}

	if operator != "" {
		columns := extractColumnsFromContext(ctx)
		for _, col := range columns {
			c.comparisons = append(c.comparisons, comparisonInfo{
				column:     col.Name,
				tableAlias: col.TableAlias,
				expression: col.Expr,
				operator:   operator,
				context:    exprText,
			})
		}
	}
}

// EnterA_expr_between handles BETWEEN and NOT BETWEEN operators
func (c *comprehensiveComparisonCollector) EnterA_expr_between(ctx *gen.A_expr_betweenContext) {
	if ctx == nil {
		return
	}
	if c.subqueryDepth > 0 {
		return
	}

	exprText := ctxText(c.tokens, ctx)
	operator := ""

	// Check for BETWEEN or NOT BETWEEN
	childCount := ctx.GetChildCount()
	hasNot := false
	for i := 0; i < childCount; i++ {
		if child, ok := ctx.GetChild(i).(antlr.TerminalNode); ok {
			text := strings.ToUpper(child.GetText())
			if text == "NOT" {
				hasNot = true
			} else if text == "BETWEEN" {
				if hasNot {
					operator = "NOT BETWEEN"
				} else {
					operator = "BETWEEN"
				}
				break
			}
		}
	}

	if operator != "" {
		columns := extractColumnsFromContext(ctx)
		for _, col := range columns {
			c.comparisons = append(c.comparisons, comparisonInfo{
				column:     col.Name,
				tableAlias: col.TableAlias,
				expression: col.Expr,
				operator:   operator,
				context:    exprText,
			})
		}
	}
}

// EnterA_expr_isnull handles IS NULL and IS NOT NULL
func (c *comprehensiveComparisonCollector) EnterA_expr_isnull(ctx *gen.A_expr_isnullContext) {
	if ctx == nil {
		return
	}
	if c.subqueryDepth > 0 {
		return
	}

	exprText := ctxText(c.tokens, ctx)
	operator := ""

	// Check for IS NULL or NOTNULL
	childCount := ctx.GetChildCount()

	// Check if the child is A_expr_is_not which contains IS NULL
	// This happens when the grammar parses "IS NULL" through the A_expr_is_not production
	if childCount == 1 {
		if isNotCtx, ok := ctx.GetChild(0).(*gen.A_expr_is_notContext); ok {
			// Check the children of A_expr_is_not for IS NULL pattern
			isNotChildCount := isNotCtx.GetChildCount()
			for j := 0; j < isNotChildCount; j++ {
				if term, ok := isNotCtx.GetChild(j).(antlr.TerminalNode); ok {
					text := term.GetText()
					if strings.ToUpper(text) == "IS" && j+1 < isNotChildCount {
						if nextTerm, ok := isNotCtx.GetChild(j + 1).(antlr.TerminalNode); ok {
							nextText := nextTerm.GetText()
							if strings.ToUpper(nextText) == "NULL" {
								// Extract just "IS" as the operator (test expectation)
								operator = "IS"
								break
							}
						}
					}
				}
			}
		}
	}

	// Original check for other patterns (ISNULL/NOTNULL shorthand syntax).
	// Only run if the IS NULL branch above did not already set the operator,
	// to prevent ISNULL/NOTNULL from overwriting the "IS" value.
	if operator == "" {
		for i := 0; i < childCount; i++ {
			child := ctx.GetChild(i)
			if term, ok := child.(antlr.TerminalNode); ok {
				text := term.GetText()
				textUpper := strings.ToUpper(text)
				if textUpper == "ISNULL" {
					operator = "IS NULL"
					break
				} else if textUpper == "NOTNULL" {
					operator = "IS NOT NULL"
					break
				}
			}
		}
	}

	if operator != "" {
		columns := extractColumnsFromContext(ctx)
		for _, col := range columns {
			c.comparisons = append(c.comparisons, comparisonInfo{
				column:     col.Name,
				tableAlias: col.TableAlias,
				expression: col.Expr,
				operator:   operator,
				context:    exprText,
			})
		}
	}
}

// EnterA_expr_is_not handles IS NOT expressions
func (c *comprehensiveComparisonCollector) EnterA_expr_is_not(ctx *gen.A_expr_is_notContext) {
	if ctx == nil {
		return
	}
	if c.subqueryDepth > 0 {
		return
	}

	exprText := ctxText(c.tokens, ctx)
	operator := ""

	// Check what follows IS NOT - we need to make sure this is actually an IS NOT expression
	childCount := ctx.GetChildCount()
	hasISNOT := false
	for i := 0; i < childCount; i++ {
		if child, ok := ctx.GetChild(i).(antlr.TerminalNode); ok {
			text := strings.ToUpper(child.GetText())
			if text == "IS" {
				// Check if next token is NOT
				if i+1 < childCount {
					if next, ok := ctx.GetChild(i + 1).(antlr.TerminalNode); ok {
						if strings.ToUpper(next.GetText()) == "NOT" {
							hasISNOT = true
							// Now look for what follows
							if i+2 < childCount {
								if followup, ok := ctx.GetChild(i + 2).(antlr.TerminalNode); ok {
									text := strings.ToUpper(followup.GetText())
									if text == "NULL" || text == "TRUE" || text == "FALSE" {
										operator = "IS NOT " + text
										break
									}
								}
							}
						}
					}
				}
			}
		}
	}

	// Only process if we actually found an IS NOT expression
	if operator != "" && hasISNOT {
		columns := extractColumnsFromContext(ctx)
		for _, col := range columns {
			c.comparisons = append(c.comparisons, comparisonInfo{
				column:     col.Name,
				tableAlias: col.TableAlias,
				expression: col.Expr,
				operator:   operator,
				context:    exprText,
			})
		}
	}
}

// EnterA_expr_qual_op handles qualified operators (schema.operator)
func (c *comprehensiveComparisonCollector) EnterA_expr_qual_op(ctx *gen.A_expr_qual_opContext) {
	if ctx == nil {
		return
	}
	if c.subqueryDepth > 0 {
		return
	}

	exprText := ctxText(c.tokens, ctx)

	// Try to extract the operator
	operator := ""
	if ctx.Qual_op(0) != nil {
		operator = ctxText(c.tokens, ctx.Qual_op(0))
	}

	if operator != "" {
		columns := extractColumnsFromContext(ctx)
		for _, col := range columns {
			c.comparisons = append(c.comparisons, comparisonInfo{
				column:     col.Name,
				tableAlias: col.TableAlias,
				expression: col.Expr,
				operator:   operator,
				context:    exprText,
			})
		}
	}
}

// Note: We explicitly don't implement EnterA_expr_and and EnterA_expr_or
// because those are logical operators, not comparison operators.
// The walker will automatically visit their children.

// extractColumnsFromContext extracts full column references from any context node,
// preserving table alias and expression information for proper deduplication.
func extractColumnsFromContext(ctx antlr.ParserRuleContext) []columnRef {
	if ctx == nil {
		return nil
	}

	// Use the existing column collector
	collector := &columnRefCollector{BasePostgreSQLParserListener: &gen.BasePostgreSQLParserListener{}}
	antlr.ParseTreeWalkerDefault.Walk(collector, ctx)

	var columns []columnRef
	seen := make(map[string]bool)
	for _, ref := range collector.refs {
		col := parseColRefFromContext(ref)
		if col.Name != "" {
			// Deduplicate based on full column reference (alias + name)
			key := col.TableAlias + "." + col.Name
			if !seen[key] {
				columns = append(columns, col)
				seen[key] = true
			}
		}
	}
	return columns
}

// findAndRecordComparisons finds individual comparison expressions and records them with their operators.
// This version uses a comprehensive listener to handle all node types properly.
func findAndRecordComparisons(result *ParsedQuery, ctx antlr.RuleContext, role ColumnUsageType, tokens antlr.TokenStream) {
	if ctx == nil {
		return
	}

	// Create a comprehensive comparison collector
	collector := &comprehensiveComparisonCollector{
		BasePostgreSQLParserListener: &gen.BasePostgreSQLParserListener{},
		tokens:                       tokens,
	}

	// Walk the tree with the comprehensive collector
	antlr.ParseTreeWalkerDefault.Walk(collector, ctx)

	// Deduplicate and record each comparison as a separate column usage
	seen := make(map[string]bool)
	for _, comp := range collector.comparisons {
		if comp.column != "" && comp.operator != "" {
			// Create a unique key for deduplication that includes table alias
			// This ensures orders.status and inventory.status are treated as separate columns
			key := comp.tableAlias + "|" + comp.column + "|" + comp.operator + "|" + comp.context
			if !seen[key] {
				seen[key] = true
				result.ColumnUsage = append(result.ColumnUsage, ColumnUsage{
					Column:     comp.column,
					TableAlias: comp.tableAlias,
					Expression: comp.expression,
					UsageType:  role,
					Context:    comp.context,
					Operator:   comp.operator,
				})
			}
		}
	}
}
